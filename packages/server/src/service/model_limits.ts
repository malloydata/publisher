// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Memory guards for the Malloy model-query path (`runnable.run`, used by
 * `getQueryResults` and notebook cell execution), in the order a caller runs
 * them:
 *
 *   1. {@link resolveModelQueryRowLimit}: the `rowLimit` to push down. The
 *      query's own `LIMIT` wins, else the server default; either way clamped
 *      to `maxRows + 1` so the database stops producing rows.
 *   2. {@link assertWithinModelRowLimit}: a `maxRows + 1` (sentinel) result is
 *      a 413, checked before the response is built at all.
 *   3. {@link stringifyQueryResponse}: serialize, reporting "too large to
 *      stringify" as a 413 rather than a bare 500. (The MCP caller asks for
 *      the compact shape and serializes its own envelope.)
 *   4. {@link assertWithinModelByteLimit}: the byte cap, necessarily after 3.
 *
 * The byte cap is loud-failure detection, not OOM prevention: this path is
 * buffered, so the result is already in memory when bytes are measured.
 * Prevention would need streaming plus `Result` reconstruction.
 *
 * Guards 2–4 record a cap-exceeded metric (lazily created OpenTelemetry
 * instruments in module state) before throwing. All take their limits as
 * arguments so they test without a model runtime.
 */

import { PayloadTooLargeError, ResponseUnserializableError } from "../errors";
import {
   recordQueryCapExceeded,
   type QueryCapSource,
} from "../query_cap_metrics";

export interface ResolveRowLimitConfig {
   /** Applied when the query carries no `LIMIT`. */
   defaultLimit: number;
   /**
    * The effective row limit is clamped to `maxRows + 1` so the overflow check
    * can tell "ran right up to the cap" from "would have overflowed". `0`
    * disables the cap.
    */
   maxRows: number;
}

/**
 * Compute the `rowLimit` to pass to `runnable.run`. The +1 sentinel matches
 * the connection-query path so every query surface behaves the same.
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
 * Which of the two the cap in {@link resolveModelQueryRowLimit} came from:
 * only the silently-applied default means rows were probably left behind.
 * Must stay in step with `requested` there.
 */
export function queryRowLimitSource(
   userLimit: number | undefined,
): QueryRowLimitSource {
   return userLimit && userLimit > 0 ? "query" : "server_default";
}

/**
 * The row check, separate from the byte check so a caller runs it BEFORE
 * building the response. `> maxRows`, so only the sentinel row fires it.
 *
 * Running it first matters: serializing an overflowing result is the largest
 * allocation on the path, and if that serialization fails the caller would be
 * told `unserializable` when the true cause was `PUBLISHER_MAX_QUERY_ROWS`.
 */
export function assertWithinModelRowLimit(
   rowCount: number,
   maxRows: number,
   source: QueryCapSource,
): void {
   if (maxRows > 0 && rowCount > maxRows) {
      // Tick before throwing: notebook handlers and MCP tools sometimes
      // swallow the error.
      recordQueryCapExceeded("rows", source);
      throw new PayloadTooLargeError(
         `Query returned more than ${maxRows} rows. Refine the query (add a LIMIT or more selective WHERE) or raise PUBLISHER_MAX_QUERY_ROWS.`,
      );
   }
}

/**
 * The byte cap. Takes the response rather than its length so a deployment with
 * the cap disabled (`maxBytes` 0) pays no UTF-8 length scan.
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
 * produces, instead of a bare 500 that names neither cap nor remedy.
 *
 * The error *class* (`RangeError`) is the portable signal; messages differ.
 * V8 (node) throws "Invalid string length" at ~512 MB; JSC (bun) throws "Out of
 * memory" by ~2.5 GB, above the container budget `constants.ts` sizes for, so
 * under bun the row and byte caps are the real protection and this is the
 * backstop. A stack overflow is also a `RangeError` but not a size problem, so
 * it propagates.
 *
 * The message does not suggest raising `PUBLISHER_MAX_RESPONSE_BYTES`, and
 * reports the cap only as context: an unserializable response will not
 * serialize under a higher cap either.
 */
export function stringifyQueryResponse(
   response: unknown,
   rowCount: number,
   maxBytes: number,
   source: QueryCapSource,
   /** Passed through to `JSON.stringify` (the compact shape needs `bigIntReplacer`). */
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
         // Omitted when disabled: "byte cap: 0" would read as a zero-byte cap.
         `Query response could not be serialized: the ${rowCount}-row result is too large to turn into JSON${
            maxBytes > 0 ? ` (byte cap: ${maxBytes})` : ""
         }. Project fewer columns, add a LIMIT, or filter wide values.`,
      );
   }
}
