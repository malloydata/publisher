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
 * than bulk extraction -- which is why it is worth closing at the compiler
 * rather than only at the sandbox.
 *
 * The query path already contains this with Malloy's restricted mode
 * (`loadRestrictedQuery`). This applies the same containment to the one
 * compile scope whose text is a fragment against a curated model.
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
 * Only restricted-construct rejections are acted on here. Every other
 * diagnostic -- an undefined field, a syntax error, a redefinition -- is left
 * untouched for the real compile to report, so this gate never becomes a second
 * source of ordinary compile errors with its own coordinates and wording.
 *
 * The gate is deliberately a separate compile from the one whose diagnostics
 * the caller receives. Restricted mode changes what compiles, so reusing its
 * result as the answer would change the positions and the problem set an
 * author sees for legitimate text. This runs first, refuses or passes, and the
 * unrestricted compile then proceeds exactly as before.
 */
export async function assertNoRestrictedConstructs(
   runtime: Runtime,
   model: Model | undefined,
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
   if (rejected.length === 0) return;

   // A caller-input refusal, so 400 rather than a compile-diagnostics response:
   // the text is not being reported as invalid Malloy, it is being refused.
   throw new CompileRefusedError(
      `This Malloy cannot be compiled at scope "append", which validates a ` +
         `fragment against the model's published surface: ` +
         `${rejected.map((problem) => problem.message).join(" ")} ` +
         `Fix: reference the model's own sources, or save the file and ` +
         `compile it at scope "file" or "package", where a model may define ` +
         `its own sources and imports.`,
   );
}
