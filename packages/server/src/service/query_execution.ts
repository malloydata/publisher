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
   EMPTY_BUILD_MANIFEST,
   MalloyError,
   type QueryMaterializer,
   type Result,
} from "@malloydata/malloy";
import type { GovernedRunnable } from "./governance/governed_runnable";
import * as Malloy from "@malloydata/malloy-interfaces";
import { getDefaultQueryRowLimit } from "../config";
import {
   BadRequestError,
   ConnectionError,
   databaseAccessFailure,
   ResultShapeError,
   storageAccessFailure,
} from "../errors";
import { redactStorageUrls } from "../pg_helpers";
import { logger } from "../logger";
import { type QueryCapSource } from "../query_cap_metrics";
import { bigIntReplacer } from "../json_utils";
import { wrapWithCorrectDrillMetadata } from "./drill_metadata";
import { fillDeclaredRecordKeys } from "./result_records";
import {
   assertWithinModelByteLimit,
   assertWithinModelRowLimit,
   queryRowLimitSource,
   resolveModelQueryRowLimit,
   stringifyQueryResponse,
   type QueryRowLimitSource,
} from "./model_limits";
import { type QueryMetadata } from "./query_metadata";
import { withStatementUsage } from "./statement_usage";

/**
 * The options prepare and run must agree on. Spread into both calls, so a key
 * the caller leaves out is left out of both.
 */
type PrepareOptions = NonNullable<
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
   /** Only a runnable governance returned runs (`./governance/governed_runnable`). */
   runnable: GovernedRunnable,
   options: PrepareOptions,
   run: {
      abortSignal: AbortSignal | undefined;
      maxRows: number;
      queryMetadata: (
         connectionName: string,
      ) => QueryMetadata | undefined | Promise<QueryMetadata | undefined>;
      /** Emit a usage record when the executing connection is metered. */
      usage?: {
         metered: (connectionName: string) => boolean;
         /** The DuckLake catalog of a metered connection, for its record. */
         catalog?: (connectionName: string) => string | undefined;
         environment: string;
         package?: string;
      };
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
      usage?: {
         metered: (connectionName: string) => boolean;
         /** The DuckLake catalog of a metered connection, for its record. */
         catalog?: (connectionName: string) => string | undefined;
         environment: string;
         package?: string;
      };
   },
): Promise<PreparedRun> {
   // Never undefined: Malloy reads an undefined `buildManifest` as "use the
   // Runtime's", and a host's manifest bind leaves the whole manifest on the
   // Runtime, unfiltered by freshness. A caller with nothing to substitute
   // (a strip that left nothing, the cutover's live leg, a serve shape) must
   // read no table at all.
   options = {
      ...options,
      buildManifest: options.buildManifest ?? EMPTY_BUILD_MANIFEST,
   };
   const prepared = await runnable.getPreparedResult(options);
   const preparedLimit = prepared.resultExplore.limit;
   const rowLimitSource = queryRowLimitSource(preparedLimit);
   const rowLimit = resolveModelQueryRowLimit(preparedLimit, {
      defaultLimit: getDefaultQueryRowLimit(),
      maxRows: run.maxRows,
   });
   const queryMetadata = await run.queryMetadata(prepared.connectionName);
   const execute = () =>
      runnable.run({
         rowLimit,
         ...options,
         abortSignal: run.abortSignal,
         queryMetadata,
      });
   const usage = run.usage;
   const result =
      usage && safeMetered(usage.metered, prepared.connectionName)
         ? await withStatementUsage(
              {
                 environment: usage.environment,
                 connection: prepared.connectionName,
                 surface: "serve",
                 package: usage.package,
                 catalog: safeCatalog(usage.catalog, prepared.connectionName),
              },
              execute,
              (r) => r.data.rowCount,
           )
         : await execute();
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
   fillDeclaredRecordKeys(result);
   const wrapped = wrapResultShape(result);
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

/**
 * Wrap the result, reading a value Malloy cannot convert as a row the
 * database returned that does not fit the result's schema
 * ({@link ResultShapeError}, a 502), not as a server crash or the caller's
 * bad query. Any other throw (a bug in Publisher's own drill-metadata code,
 * say) is rethrown unchanged.
 */
function wrapResultShape(result: Result): Malloy.Result {
   try {
      return wrapWithCorrectDrillMetadata(result);
   } catch (err) {
      if (!isValueConversionFailure(err)) throw err;
      logger.error("Query result does not fit its schema", {
         error: err instanceof Error ? err.message : String(err),
         connection: result.connectionName,
      });
      throw new ResultShapeError({ cause: err });
   }
}

/**
 * The messages Malloy's result wrapper (`API.util.wrapResult`, its
 * `mapValue` and `rowDataToNumber`) throws on a value its field's type
 * cannot hold.
 */
const VALUE_CONVERSION_MESSAGE =
   /^(?:Invalid (?:boolean|string|array) |Invalid record in result |Cannot convert \w+ to number: )/;

/**
 * The functions of Malloy's row mapping (`api/util.js`'s `mapData`,
 * `mapValue`, `mapRow`, and `row_data_utils.js`'s `rowDataTo*`), as a stack
 * frame names them.
 */
const ROW_MAPPING_FRAME = /\bat (?:mapData|mapValue|mapRow|rowDataTo\w+) /;

/**
 * Whether `err` is Malloy's wrapper refusing a value: one of its own
 * messages, the `RangeError` a date or timestamp no date parses raises
 * (`rowDataToDate(value).toISOString()`: "Invalid time value" in V8,
 * "Invalid Date" in Bun), or a `TypeError` its row mapping raised on a
 * value, told by the throwing frame rather than by the engine's wording.
 */
function isValueConversionFailure(err: unknown): boolean {
   if (!(err instanceof Error)) return false;
   if (err instanceof RangeError) {
      return /^Invalid (?:time value|Date)$/.test(err.message);
   }
   if (err instanceof TypeError) {
      return ROW_MAPPING_FRAME.test(throwingFrameOf(err));
   }
   return VALUE_CONVERSION_MESSAGE.test(err.message);
}

/** The first stack frame of `err`: the function that threw. */
function throwingFrameOf(err: Error): string {
   return (err.stack ?? "").split("\n").find((l) => /^\s*at /.test(l)) ?? "";
}

type ErrorClass = abstract new (...args: never[]) => Error;

interface RunErrorContext {
   packageName: string;
   modelPath: string;
   /** Prefix of the generic 400's message, e.g. `Query execution failed`. */
   failurePrefix: string;
   /**
    * What the caller read, in its own terms (the run target's source), for
    * an answer that may not quote the driver's text.
    */
   subject?: string;
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
 * and become a 400 carrying Malloy's message; a `MalloyError`, a
 * {@link ConnectionError} (a truncated page, a misshapen row, an exhausted
 * pool) and any `passThrough` class keeps its own mapping; a database that could not be reached or refused the credentials is
 * a 502/424; anything else is a 400.
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
   // A classified database failure (truncated page, misshapen row, exhausted
   // pool, unreachable database) keeps its own status on every run path.
   if (err instanceof ConnectionError) throw err;
   for (const passThrough of ctx.passThrough) {
      if (err instanceof passThrough) throw err;
   }

   const errorMessage = err instanceof Error ? err.message : String(err);
   // Unreachable database (502) or rejected credentials (424): the query never
   // ran, so a 400 would wrongly tell the caller to fix it.
   const accessFailure = databaseAccessFailure(err);
   if (accessFailure) throw accessFailure;
   // The storage behind a table refused the read (403): the driver's text
   // quotes the object's URL, so the answer names the source instead.
   const storageFailure = storageAccessFailure(err, ctx.subject);
   if (storageFailure) throw storageFailure;

   if (
      ctx.benignMessage !== undefined &&
      errorMessage.trim() === ctx.benignMessage
   ) {
      return;
   }
   ctx.logFailure(errorMessage, err);
   // Driver text in a 4xx body names no storage path.
   throw new BadRequestError(
      redactStorageUrls(`${ctx.failurePrefix}: ${errorMessage}`),
   );
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

/** Reading a record's catalog must never fail a query either. */
function safeCatalog(
   catalog: ((connectionName: string) => string | undefined) | undefined,
   connectionName: string,
): string | undefined {
   try {
      return catalog?.(connectionName);
   } catch {
      return undefined;
   }
}

/** A metering check must never fail a query. */
function safeMetered(
   metered: (connectionName: string) => boolean,
   connectionName: string,
): boolean {
   try {
      return metered(connectionName);
   } catch {
      return false;
   }
}
