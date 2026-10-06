// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { type LogMessage, MalloyError } from "@malloydata/malloy";

/**
 * Malloy's translator throws a plain Error, with no problem list, where it
 * hits a case it did not expect in the caller's text (`order_date ~ @2025`
 * reaches "mysterious error in range computation"; `order_date ~ 2025` throws
 * a TypeMismatch). Malloy's own runtime reports such an Error as one problem;
 * this does the same, so the caller gets a compile problem rather than a 500,
 * or a 503 that tells it to retry.
 *
 * Only an Error thrown from the translator (the top stack frame is in
 * `@malloydata/malloy/dist/lang/`) counts. A connection or filesystem failure
 * is also a plain Error and must keep surfacing as it did, so it stays
 * undefined here.
 *
 * Every place a compile catches errors and treats only a `MalloyError` as the
 * caller's mistake calls this first: the query path, `/compile` at each scope,
 * `Model.create`, and the package-load worker. The worker must call it BEFORE
 * serializing, because the check reads the stack and the classification has
 * to be on the wire for the main thread to answer as a compile error.
 */
const TRANSLATOR_FRAME = /[\\/]@malloydata[\\/]malloy[\\/]dist[\\/]lang[\\/]/;
const RANGE_COMPARISON_MESSAGE = "mysterious error in range computation";

export function translatorInvariantProblem(
   error: unknown,
): LogMessage | undefined {
   if (!(error instanceof Error) || error instanceof MalloyError) {
      return undefined;
   }
   const topFrame = (error.stack ?? "")
      .split("\n")
      .find((line) => line.trimStart().startsWith("at "));
   if (!topFrame || !TRANSLATOR_FRAME.test(topFrame)) return undefined;
   const hint =
      error.message === RANGE_COMPARISON_MESSAGE
         ? " This comes from comparing a date or timestamp to a date literal such as " +
           "@2025 with `~`, which Malloy cannot compile. Use `=` to match the whole " +
           "year, month or day (`order_date = @2025`), or an explicit range " +
           "(`order_date ? @2025-01-01 to @2026-01-01`)."
         : "";
   return {
      code: "translator-error",
      severity: "error",
      message: `Malloy could not compile this query: ${error.message}.` + hint,
   } as LogMessage;
}

/**
 * The translator's plain Error as the `MalloyError` Malloy would have thrown
 * for an ordinary compile problem, or undefined when `error` is anything else.
 */
export function translatorMalloyError(error: unknown): MalloyError | undefined {
   const problem = translatorInvariantProblem(error);
   return problem ? new MalloyError(problem.message, [problem]) : undefined;
}
