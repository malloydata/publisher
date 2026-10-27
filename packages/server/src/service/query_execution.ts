// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The execution core a model query and a notebook cell share once each has
 * decided WHAT to run: prepare, cap rows, run, then serialize within the row
 * and byte caps — and the mapping of a run failure onto the status the caller
 * sees. Everything before that (boundary, authorize, routing) differs between
 * the two and stays with the caller.
 */

import { withSpan } from "../telemetry";
import {
   API,
   MalloyError,
   type QueryMaterializer,
   type Result,
} from "@malloydata/malloy";
import * as Malloy from "@malloydata/malloy-interfaces";
import { getDefaultQueryRowLimit } from "../config";
import { BadRequestError, databaseAccessFailure } from "../errors";
import { logger } from "../logger";
import { type QueryCapSource } from "../query_cap_metrics";
import { bigIntReplacer } from "../json_utils";
import { correctDrillMetadata } from "./drill_metadata";
import {
   assertWithinModelByteLimit,
   assertWithinModelRowLimit,
   queryRowLimitSource,
   resolveModelQueryRowLimit,
   stringifyQueryResponse,
   type QueryRowLimitSource,
} from "./model_limits";
import { type QueryMetadata } from "./query_metadata";

/**
 * The options prepare and run must agree on. Spread into both calls, so a key
 * the caller leaves out is left out of both.
 */
export type PrepareOptions = NonNullable<
   Parameters<QueryMaterializer["getPreparedResult"]>[0]
>;

export interface PreparedRun {
   result: Result;
   rowLimit: number;
   rowLimitSource: QueryRowLimitSource;
   queryMetadata: QueryMetadata | undefined;
}

/**
 * Prepare `runnable`, derive the row cap from the prepared shape, resolve the
 * metadata to tag the statement with (it needs the executing connection's
 * name, which only the prepared result knows), then run.
 */
export async function prepareAndRun(
   runnable: QueryMaterializer,
   options: PrepareOptions,
   run: {
      abortSignal: AbortSignal | undefined;
      maxRows: number;
      queryMetadata: (
         connectionName: string,
      ) => QueryMetadata | undefined | Promise<QueryMetadata | undefined>;
   },
): Promise<PreparedRun> {
   return withSpan("publisher.query.execute", {}, () =>
      prepareAndRunUntraced(runnable, options, run),
   );
}

async function prepareAndRunUntraced(
   runnable: QueryMaterializer,
   options: PrepareOptions,
   run: {
      abortSignal: AbortSignal | undefined;
      maxRows: number;
      queryMetadata: (
         connectionName: string,
      ) => QueryMetadata | undefined | Promise<QueryMetadata | undefined>;
   },
): Promise<PreparedRun> {
   const prepared = await runnable.getPreparedResult(options);
   const preparedLimit = prepared.resultExplore.limit;
   const rowLimitSource = queryRowLimitSource(preparedLimit);
   const rowLimit = resolveModelQueryRowLimit(preparedLimit, {
      defaultLimit: getDefaultQueryRowLimit(),
      maxRows: run.maxRows,
   });
   const queryMetadata = await run.queryMetadata(prepared.connectionName);
   const result = await runnable.run({
      rowLimit,
      ...options,
      abortSignal: run.abortSignal,
      queryMetadata,
   });
   return { result, rowLimit, rowLimitSource, queryMetadata };
}

/**
 * Check the row cap, wrap the result, serialize the shape the caller will
 * send, and check the byte cap. Rows first, and above `wrapResult`: a row
 * overflow is a `maxRows + 1`-row result by construction, and wrapping it
 * would build an object graph larger than its JSON only to refuse it.
 */
export function serializeWithinCaps(
   result: Result,
   caps: {
      maxRows: number;
      maxBytes: number;
      source: QueryCapSource;
      compact?: boolean;
   },
): { wrapped: Malloy.Result; serialized: string } {
   assertWithinModelRowLimit(result.totalRows, caps.maxRows, caps.source);
   correctDrillMetadata(result);
   const wrapped = API.util.wrapResult(result);
   const serialized = stringifyQueryResponse(
      caps.compact ? result.data.value : wrapped,
      result.totalRows,
      caps.maxBytes,
      caps.source,
      caps.compact ? bigIntReplacer : undefined,
   );
   assertWithinModelByteLimit(serialized, caps.maxBytes, caps.source);
   return { wrapped, serialized };
}

type ErrorClass = abstract new (...args: never[]) => Error;

export interface RunErrorContext {
   packageName: string;
   modelPath: string;
   /** Prefix of the generic 400's message, e.g. `Query execution failed`. */
   failurePrefix: string;
   /** Error classes that keep their own status rather than becoming a 400. */
   passThrough: readonly ErrorClass[];
   /** Logs an unclassified failure just before it becomes the generic 400. */
   logFailure: (errorMessage: string, err: unknown) => void;
   /**
    * A message that is not a failure at all: when the error's trimmed message
    * equals it, this returns instead of throwing.
    */
   benignMessage?: string;
}

/**
 * Map a failure from preparing or running a query onto the error the caller
 * sees. Bad client-supplied givens surface as a Malloy `runtime-given-*` error
 * and become a 400 carrying Malloy's message; a `MalloyError` (and any
 * `passThrough` class) keeps its own mapping; a database that could not be
 * reached or refused the credentials is a 502/424; anything else is a 400.
 */
export function mapRunError(
   err: unknown,
   ctx: RunErrorContext & { benignMessage?: undefined },
): never;
export function mapRunError(err: unknown, ctx: RunErrorContext): void;
export function mapRunError(err: unknown, ctx: RunErrorContext): void {
   // Duck-type on `.code`: MalloyCompileError is not a MalloyError and is not
   // root-exported. `runtime-given-` is pinned to Malloy's error codes
   // (given_binding.ts / runtime.ts); keep it in step with environment.ts, or
   // these degrade to the generic 400 and /compile silently omits `sql`.
   const givenCode = (err as { code?: string })?.code;
   if (
      typeof givenCode === "string" &&
      givenCode.startsWith("runtime-given-")
   ) {
      logger.debug("Rejected client-supplied given", {
         environmentName: ctx.packageName,
         modelPath: ctx.modelPath,
         error: err instanceof Error ? err.message : String(err),
      });
      throw new BadRequestError(
         err instanceof Error ? err.message : String(err),
      );
   }

   if (err instanceof MalloyError) throw err;
   for (const passThrough of ctx.passThrough) {
      if (err instanceof passThrough) throw err;
   }

   const errorMessage = err instanceof Error ? err.message : String(err);
   // Unreachable database (502) or rejected credentials (424): the query never
   // ran, so a 400 would wrongly tell the caller to fix it.
   const accessFailure = databaseAccessFailure(err);
   if (accessFailure) throw accessFailure;

   if (
      ctx.benignMessage !== undefined &&
      errorMessage.trim() === ctx.benignMessage
   ) {
      return;
   }
   ctx.logFailure(errorMessage, err);
   throw new BadRequestError(`${ctx.failurePrefix}: ${errorMessage}`);
}

/**
 * Whether `err` is Malloy refusing to BIND a given: the runtime form carries
 * a `runtime-given-*` code on the error itself, while a prepare-time failure
 * arrives as a `MalloyError` whose `problems` carry a `compiler-given-*` one.
 * Duck-typed on `.code` for the same reason every other check here is —
 * neither error class is root-exported.
 */
export function isGivenBindingFailure(err: unknown): boolean {
   const isGivenCode = (code: unknown): boolean =>
      typeof code === "string" &&
      (code.startsWith("runtime-given-") || code.startsWith("compiler-given-"));
   if (isGivenCode((err as { code?: unknown })?.code)) return true;
   const problems = (err as { problems?: unknown })?.problems;
   return (
      Array.isArray(problems) &&
      problems.some((p) => isGivenCode((p as { code?: unknown })?.code))
   );
}
