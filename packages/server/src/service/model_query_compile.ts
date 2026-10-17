// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { MalloyError, type LogMessage } from "@malloydata/malloy";
import { InvalidArgumentError, QueryCompileError } from "../errors";
import { formatProblem, locateProblemsInCallerText } from "./query_text";
import { translatorMalloyError } from "./translator_error";

/**
 * Backtick-quote a Malloy identifier for safe interpolation into a `run:`
 * query string. Escapes backslashes and backticks (in that order) so a name
 * that needs Malloy quoting (hyphen, space, reserved word, leading digit) or
 * contains an embedded backtick cannot break out of the quotes. Mirrors
 * malloy's internal `identifierCode` / `escapeIdentifier` (to_stable.ts), which
 * is not exported.
 */
export function quoteMalloyIdentifier(name: string | undefined): string {
   return "`" + (name ?? "").replace(/\\/g, "\\\\").replace(/`/g, "\\`") + "`";
}

/**
 * What ad-hoc query text is compiled behind. Compile problems are located in
 * the compiled document, so their line numbers are shifted back by this
 * prefix's line count before a caller sees them.
 */
const AD_HOC_QUERY_PREFIX = "\n";

/** A compile failure of ad-hoc text, as the caller's own text locates it. */
export function queryCompileError(
   error: MalloyError,
   callerText: string,
): QueryCompileError {
   const problems = locateProblemsInCallerText(
      error.problems,
      callerText,
      AD_HOC_QUERY_PREFIX.split("\n").length - 1,
   );
   return new QueryCompileError(describeProblems(problems, error), problems);
}

/**
 * The error problems as one message, each led by its 1-based `line:column` in
 * the caller's text when it has one, so a client that reads only `message`
 * still learns where. Falls back to the compiler's own message when no problem
 * is an error.
 */
function describeProblems(problems: LogMessage[], error: MalloyError): string {
   const errors = problems.filter((p) => p.severity === "error");
   if (errors.length === 0) return error.message;
   return errors.map(formatProblem).join("; ");
}

/**
 * The compile failure of a runnable, or undefined when it compiles or fails for
 * a non-compile reason (e.g. a connection failure fetching a schema, left to
 * surface at run). Malloy memoizes the compile per runnable, so a later compile
 * of the same runnable is free.
 */
export async function compileErrorOf(runnable: {
   getPreparedQuery(): Promise<unknown>;
}): Promise<MalloyError | undefined> {
   try {
      await runnable.getPreparedQuery();
      return undefined;
   } catch (error) {
      if (error instanceof MalloyError) return error;
      return translatorMalloyError(error);
   }
}

/**
 * How many `run:` statements the compiled text holds, or undefined when it does
 * not compile (the run path then answers that failure itself). Malloy counts
 * only the text's own `run:` statements here: `source:` and `query:`
 * definitions add none, and the base model's statements are not carried into
 * an extension. Reads the memoized compile, so it costs nothing extra.
 */
export async function runStatementCount(runnable: {
   getPreparedQuery(): Promise<unknown>;
}): Promise<number | undefined> {
   let prepared: { model?: { queries?(): { unnamed: number } } } | undefined;
   try {
      prepared = (await runnable.getPreparedQuery()) as typeof prepared;
   } catch {
      return undefined;
   }
   return prepared?.model?.queries?.().unnamed;
}

/**
 * Malloy runs only the LAST `run:` of a text and drops the rest without a word,
 * so a caller that sends several would silently lose every answer but one.
 */
export function multipleRunStatementsError(
   count: number,
): InvalidArgumentError {
   return new InvalidArgumentError(
      `The query has ${count} run: statements; only one runs per call, so the ` +
         "others would be ignored. Send each as its own request. (source: and " +
         "query: definitions before a single run: are fine.)",
   );
}
