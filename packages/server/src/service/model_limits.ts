/**
 * Memory guards for the Malloy model-query path (the `runnable.run`
 * flow used by `getQueryResults` and notebook cell execution).
 *
 * Two layered defenses:
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
 * Both helpers are pure so they can be unit-tested without spinning
 * up a model runtime; the caller injects the env-derived limits.
 */

import { PayloadTooLargeError } from "../errors";

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
): void {
   if (maxRows > 0 && rowCount > maxRows) {
      throw new PayloadTooLargeError(
         `Query returned more than ${maxRows} rows. Refine the query (add a LIMIT or more selective WHERE) or raise PUBLISHER_MAX_QUERY_ROWS.`,
      );
   }
   if (maxBytes > 0 && serializedBytes > maxBytes) {
      throw new PayloadTooLargeError(
         `Query response exceeded ${maxBytes} bytes (was ${serializedBytes}). Project fewer columns, add a LIMIT, or raise PUBLISHER_MAX_RESPONSE_BYTES.`,
      );
   }
}
