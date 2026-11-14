// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Streaming for the ad-hoc connection SQL endpoints, so the byte cap
 * (`PUBLISHER_MAX_RESPONSE_BYTES`) bounds memory as rows arrive. Row count is
 * a poor proxy for memory (one wide JSON column defeats it), and `runSQL`
 * buffers the whole result before bytes could be counted. Connections that
 * cannot stream stay on the buffered, row-capped path.
 */

import type {
   Connection,
   MalloyQueryData,
   QueryRecord,
   RunSQLOptions,
   StreamingConnection,
} from "@malloydata/malloy";

import { PayloadTooLargeError } from "./errors";
import { recordQueryCapExceeded } from "./query_cap_metrics";

/** Runtime check and type narrow for streaming-capable connections. */
export function isStreamingConnection(
   connection: Connection,
): connection is StreamingConnection {
   return (
      typeof (connection as { canStream?: () => boolean }).canStream ===
         "function" && (connection as StreamingConnection).canStream()
   );
}

export interface StreamBudget {
   /** `0` disables the row cap (`runSQLOptions.rowLimit` may still bound it). */
   maxRows: number;
   /**
    * Cap on the summed `Buffer.byteLength(JSON.stringify(row))` of returned
    * rows — the bytes the response will carry. `0` disables it.
    */
   maxBytes: number;
}

/**
 * Drain `runSQLStream` into a `MalloyQueryData`, aborting the driver the
 * moment either cap is breached and throwing `PayloadTooLargeError`.
 *
 * The row cap uses the `cap + 1` sentinel: the controller has clamped
 * `rowLimit` to `cap + 1`, so receiving that many rows means overflow, the
 * same detection the non-streaming path uses.
 */
export async function streamSqlWithBudget(
   connection: StreamingConnection,
   sql: string,
   runSQLOptions: RunSQLOptions,
   budget: StreamBudget,
): Promise<MalloyQueryData> {
   const { maxRows, maxBytes } = budget;
   const capAc = new AbortController();
   const rows: QueryRecord[] = [];
   let byteTotal = 0;
   let overflowMessage: string | undefined;

   // Both the caller's signal (query timeout → 504) and the cap abort
   // (→ 413) must cancel the iterator. The typeof guard falls back to
   // cap-only on a runtime without `AbortSignal.any`.
   const externalSignal = runSQLOptions.abortSignal;
   const composedSignal: AbortSignal =
      externalSignal && typeof AbortSignal.any === "function"
         ? AbortSignal.any([externalSignal, capAc.signal])
         : capAc.signal;

   try {
      for await (const row of connection.runSQLStream(sql, {
         ...runSQLOptions,
         abortSignal: composedSignal,
      })) {
         rows.push(row);
         if (maxBytes > 0) {
            // Duplicates the final stringify per row; the early abort on
            // overflow is worth it.
            byteTotal += Buffer.byteLength(JSON.stringify(row), "utf8");
            if (byteTotal > maxBytes) {
               recordQueryCapExceeded("bytes", "connection_sql");
               overflowMessage = `Query response exceeded ${maxBytes} bytes (had at least ${byteTotal}). Refine the query (project fewer columns, add a LIMIT, or filter wide values) or raise PUBLISHER_MAX_RESPONSE_BYTES.`;
               capAc.abort();
               break;
            }
         }
         if (maxRows > 0 && rows.length > maxRows) {
            recordQueryCapExceeded("rows", "connection_sql");
            overflowMessage = `Query returned more than ${maxRows} rows. Refine the query (add a LIMIT or more selective WHERE) or raise PUBLISHER_MAX_QUERY_ROWS.`;
            capAc.abort();
            break;
         }
      }
   } catch (err) {
      // Some `pg-query-stream` versions surface our own abort as an error.
      // Swallow it only when we aborted; anything else (a connection error,
      // the caller's timeout) must propagate.
      if (!overflowMessage) throw err;
   }

   if (overflowMessage) throw new PayloadTooLargeError(overflowMessage);

   return { rows, totalRows: rows.length };
}
