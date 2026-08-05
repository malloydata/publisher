/**
 * Memory guards for the Malloy model-query path (the `runnable.run`
 * flow used by `getQueryResults` and notebook cell execution).
 *
 * Three layered defenses:
 *
 *   1. {@link resolveModelQueryRowLimit} — compute the effective
 *      `rowLimit` to push down to `runnable.run`. The user's Malloy
 *      `LIMIT` clause wins when present; otherwise the operator-
 *      tunable default ({@link getDefaultQueryRowLimit}) fills in.
 *      Either way the result is clamped to `maxRows + 1` so the
 *      database itself stops producing rows when a user-supplied
 *      `LIMIT 1_000_000` would otherwise blow up the process.
 *
 *   2. {@link assertWithinModelResponseLimits} — post-run overflow
 *      detection. If the connector returned `maxRows + 1` rows
 *      (the sentinel) or the JSON-serialized response exceeds the
 *      byte cap, throw `PayloadTooLargeError` so the caller sees a
 *      clean HTTP 413.
 *
 *   3. {@link stringifyQueryResponse}: serialize the response for
 *      that byte check. The check measures by stringifying, so a
 *      result too large to stringify fails *inside* the guard before
 *      it can compare anything to the cap; this wrapper turns that
 *      into the same 413 rather than a bare 500.
 *
 * Caveat on the byte cap: this path runs `runnable.run` (buffered),
 * not `runStream`, so by the time we measure bytes the result has
 * already been materialized in memory. The byte cap here is loud-
 * failure detection — it surfaces oversize responses with a 413
 * instead of letting the client receive a half-transmitted payload
 * — not OOM prevention. True prevention requires streaming +
 * `Result` reconstruction from `DataRecord`s, which is out of scope
 * for this step (the model-query streaming path entangles with
 * Malloy's `Result` schema metadata in non-trivial ways).
 *
 * The first two helpers are pure, so they can be unit-tested without
 * spinning up a model runtime; the caller injects the env-derived
 * limits. {@link stringifyQueryResponse} is not: it records a
 * cap-exceeded metric before throwing, which lazily creates and
 * caches OpenTelemetry instruments in module state.
 */

import { PayloadTooLargeError, ResponseUnserializableError } from "../errors";
import {
   recordQueryCapExceeded,
   type QueryCapSource,
} from "../query_cap_metrics";

export interface ResolveRowLimitConfig {
   /**
    * Result of {@link getDefaultQueryRowLimit}. Applied when the
    * user's Malloy query doesn't carry a `LIMIT` clause.
    */
   defaultLimit: number;
   /**
    * Result of {@link getMaxQueryRows}. The effective row limit is
    * clamped to `maxRows + 1` so a sentinel-count overflow check can
    * distinguish "ran right up to the cap" from "would have
    * overflowed". A value of `0` disables the cap.
    */
   maxRows: number;
}

/**
 * Compute the `rowLimit` to pass to `runnable.run`. The +1 sentinel
 * mirrors the Step 1 / Step 2 patterns on the connection-query path
 * so behavior is uniform across all query surfaces.
 */
export function resolveModelQueryRowLimit(
   userLimit: number | undefined,
   { defaultLimit, maxRows }: ResolveRowLimitConfig,
): number {
   const requested = userLimit && userLimit > 0 ? userLimit : defaultLimit;
   if (maxRows <= 0) return requested;
   return Math.min(requested, maxRows + 1);
}

/** Whether the cap came from the query itself or from the server default. */
export type QueryRowLimitSource = "query" | "server_default";

/**
 * Which of the two the cap in {@link resolveModelQueryRowLimit} came from.
 *
 * The distinction is the whole difference between "the database cut this off
 * and you were not told" and "you asked for exactly this many". A `limit:` or
 * `top:` the author wrote is deliberate and complete for what it asked; only the
 * silently-applied default means rows were probably left behind.
 *
 * The condition deliberately mirrors `requested` above and must stay in step
 * with it, so a change to which limit wins cannot leave the reported source
 * describing the other one.
 */
export function queryRowLimitSource(
   userLimit: number | undefined,
): QueryRowLimitSource {
   return userLimit && userLimit > 0 ? "query" : "server_default";
}

export interface ModelResponseLimitsConfig {
   /** Result of {@link getMaxQueryRows}. `0` disables the row cap. */
   maxRows: number;
   /** Result of {@link getMaxResponseBytes}. `0` disables the byte cap. */
   maxBytes: number;
}

/**
 * Throw {@link PayloadTooLargeError} (HTTP 413) when a model-query
 * response exceeds either configured cap. `rowCount` should be the
 * raw row count Malloy actually fetched (typically
 * `result._queryResult.data.rawData.length`); `serializedBytes`
 * should be the byte length of the JSON-stringified response that
 * would otherwise be returned to the client.
 *
 * Row check uses the `> maxRows` sentinel (not `>= maxRows`), since
 * {@link resolveModelQueryRowLimit} asked the connector for
 * `maxRows + 1` and we want to fail only when that sentinel fires.
 */
export function assertWithinModelResponseLimits(
   rowCount: number,
   serializedBytes: number,
   { maxRows, maxBytes }: ModelResponseLimitsConfig,
   source: QueryCapSource,
): void {
   if (maxRows > 0 && rowCount > maxRows) {
      // Tick the counter *before* throwing so it reflects the
      // event even if a downstream `catch` swallows the error
      // (notebook handlers and MCP tools both do this in places).
      recordQueryCapExceeded("rows", source);
      throw new PayloadTooLargeError(
         `Query returned more than ${maxRows} rows. Refine the query (add a LIMIT or more selective WHERE) or raise PUBLISHER_MAX_QUERY_ROWS.`,
      );
   }
   if (maxBytes > 0 && serializedBytes > maxBytes) {
      recordQueryCapExceeded("bytes", source);
      throw new PayloadTooLargeError(
         `Query response exceeded ${maxBytes} bytes (was ${serializedBytes}). Project fewer columns, add a LIMIT, or raise PUBLISHER_MAX_RESPONSE_BYTES.`,
      );
   }
}

/**
 * JSON-serialize a query response, reporting a payload too large to serialize
 * at all as the same {@link PayloadTooLargeError} (HTTP 413) the byte cap
 * produces.
 *
 * The byte cap measures by stringifying, so the measurement is the thing that
 * fails first: past a certain size `JSON.stringify` throws and the guard never
 * reaches its comparison, leaving the caller a bare 500 that names neither the
 * cap nor a remedy. One step smaller the same query gets a 413 that names both.
 *
 * Both engines Publisher runs on signal this as a `RangeError`, but with
 * different messages, so the error *class* is the portable signal and the
 * message is not. Measured: V8 (node) throws "Invalid string length" at ~512 MB;
 * JSC (bun) serializes 2.1 GB fine and throws "Out of memory" by 2.5 GB. A stack
 * overflow is also a `RangeError` and is not a size problem, so it is left to
 * propagate as the 500 it is.
 *
 * Where this guard is and is not load-bearing, since the two engines differ by
 * 4x: on node it fires well inside a normal pod, so a 600 MB response at the
 * default 50 MB cap gets a 413. Under bun, which runs the Docker image, JSC's
 * threshold sits above the 1-2 GB budget `constants.ts` sizes for, so the kernel
 * is liable to OOM-kill the container before JSC throws anything to catch. The
 * row cap and the byte cap are what protect that configuration; this is the
 * backstop for when they are raised or the rows are individually huge.
 *
 * Raising `PUBLISHER_MAX_RESPONSE_BYTES` is deliberately not offered here: a
 * response that cannot be serialized will not serialize under a higher cap
 * either, so the remedies are the ones that shrink the response. For the same
 * reason the message reports the cap as context rather than claiming the
 * response exceeded it: past ~512 MB on node the engine's own limit is what
 * fired, which can be well below a raised cap.
 */
export function stringifyQueryResponse(
   response: unknown,
   rowCount: number,
   maxBytes: number,
   source: QueryCapSource,
): string {
   try {
      return JSON.stringify(response);
   } catch (error) {
      if (
         !(error instanceof RangeError) ||
         // "Maximum call stack size exceeded": deep nesting, not size.
         /call stack/i.test(error.message)
      ) {
         throw error;
      }
      recordQueryCapExceeded("unserializable", source);
      throw new ResponseUnserializableError(
         // The cap is reported as context, not as the thing exceeded, and only
         // when there is one: with the byte cap disabled `maxBytes` is 0 and
         // "byte cap: 0" would read as a cap of zero bytes.
         `Query response could not be serialized: the ${rowCount}-row result is too large to turn into JSON${
            maxBytes > 0 ? ` (byte cap: ${maxBytes})` : ""
         }. Project fewer columns, add a LIMIT, or filter wide values.`,
      );
   }
}
