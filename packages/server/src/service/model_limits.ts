/**
 * Memory guards for the Malloy model-query path (the `runnable.run`
 * flow used by `getQueryResults` and notebook cell execution).
 *
 * Four guards, numbered in the order a caller runs them:
 *
 *   1. {@link resolveModelQueryRowLimit}: compute the effective
 *      `rowLimit` to push down to `runnable.run`. The user's Malloy
 *      `LIMIT` clause wins when present; otherwise the operator-
 *      tunable default ({@link getDefaultQueryRowLimit}) fills in.
 *      Either way the result is clamped to `maxRows + 1` so the
 *      database itself stops producing rows when a user-supplied
 *      `LIMIT 1_000_000` would otherwise blow up the process.
 *
 *   2. {@link assertWithinModelRowLimit}: post-run row overflow
 *      detection. If the connector returned `maxRows + 1` rows (the
 *      sentinel), throw `PayloadTooLargeError` for a clean HTTP 413.
 *      Runs before the response is built at all, so an overflow costs
 *      neither the wrap nor the serialize.
 *
 *   3. {@link stringifyQueryResponse}: serialize the response. This
 *      produces the bytes item 4 measures, and for the REST caller the
 *      bytes it transmits as well, which is the point. The check
 *      measures by stringifying, so a result too large to stringify
 *      fails *inside* the guard before it can compare anything to the
 *      cap, and a caller that stringified its own copy would hit the
 *      same wall unguarded. Either way this reports it as the same 413
 *      rather than a bare 500. The MCP caller is the exception: it asks
 *      for the compact shape so the cap measures what its envelope is
 *      built from, then builds and serializes that envelope itself.
 *
 *   4. {@link assertWithinModelByteLimit}: the byte cap. Necessarily
 *      after item 3, because the serialized length IS the measurement.
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
 * Two of the five exports are pure: {@link resolveModelQueryRowLimit}
 * and {@link queryRowLimitSource}. The other three, items 2, 3 and 4
 * above, each record a cap-exceeded metric before throwing, which
 * lazily creates and caches OpenTelemetry instruments in module state,
 * so a unit test that drives them without the metrics harness binds
 * instruments to whatever provider happens to be installed. All of
 * them take their limits as arguments, which is what lets them be
 * tested without spinning up a model runtime.
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

/**
 * The row check, deliberately separate from the byte check so a caller can run it
 * BEFORE building the response at all.
 *
 * `rowCount` is the count Malloy actually fetched, and the comparison is
 * `> maxRows` rather than `>= maxRows`, so only the sentinel row fires it.
 *
 * That ordering matters for more than tidiness. {@link resolveModelQueryRowLimit}
 * asks the connector for `maxRows + 1`, so a row overflow is a `maxRows + 1`-row
 * result sitting in memory. Serializing it first makes the largest single allocation on
 * the path for a response that is about to be refused anyway, and if that
 * serialization is what hits the engine's limit the caller is told the response
 * could not be serialized, and the counter ticks `unserializable`, when the true
 * answer was that it exceeded `PUBLISHER_MAX_QUERY_ROWS`. That is exactly the
 * dashboard confusion the separate label exists to prevent.
 */
export function assertWithinModelRowLimit(
   rowCount: number,
   maxRows: number,
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
}

/**
 * The byte cap, checked after the response is serialized because the serialized
 * length is the measurement. Takes the response rather than its length so that a
 * deployment with the cap disabled (`maxBytes` 0, documented) pays no UTF-8 length
 * scan over a response that could be hundreds of megabytes.
 */
export function assertWithinModelByteLimit(
   serialized: string,
   maxBytes: number,
   source: QueryCapSource,
): void {
   if (maxBytes <= 0) return;
   const serializedBytes = Buffer.byteLength(serialized, "utf8");
   if (serializedBytes > maxBytes) {
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
   /**
    * Passed through to `JSON.stringify`. The compact response shape needs
    * `bigIntReplacer`, and it needs this guard for the same reason the full one
    * does: it is a payload that can be too large to serialize.
    */
   replacer?: (key: string, value: unknown) => unknown,
): string {
   try {
      return JSON.stringify(response, replacer);
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
