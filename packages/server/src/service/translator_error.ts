// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { type LogMessage, MalloyError } from "@malloydata/malloy";

const TRANSLATOR_FRAME = /[\\/]@malloydata[\\/]malloy[\\/]dist[\\/]lang[\\/]/;
const RANGE_COMPARISON_MESSAGE = "mysterious error in range computation";

/**
 * Malloy's translator throws a plain Error, with no problem list, on some
 * inputs it did not expect (`order_date ~ @2025` reaches "mysterious error in
 * range computation"). This turns such an Error into one compile problem, as
 * Malloy's own runtime does, so the caller gets a compile error rather than a
 * 500 or a retryable 503.
 *
 * Only an Error whose top stack frame is in `@malloydata/malloy/dist/lang/`
 * counts: a connection or filesystem failure is also a plain Error and must
 * keep surfacing as one. Every compile catch that treats only `MalloyError`
 * as the caller's mistake calls this first. The package-load worker must call
 * it BEFORE serializing, because the check reads the stack.
 */
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
