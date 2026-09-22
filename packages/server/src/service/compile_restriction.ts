// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { LogMessage, Model, Runtime } from "@malloydata/malloy";
import { Malloy, MalloyError } from "@malloydata/malloy";
import { CompileRefusedError } from "../errors";

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
         throw new CompileRefusedError(
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
