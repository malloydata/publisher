/**
 * Helpers for streaming the ad-hoc connection SQL endpoints
 * (`/environments/.../connections/.../sqlQuery` and the deprecated
 * `queryData` GET twin) so the publisher process never has to hold
 * a whole result set in memory before returning it.
 *
 * The Step 1 row cap (`PUBLISHER_MAX_QUERY_ROWS`) is necessary but
 * not sufficient: row count is a poor proxy for memory pressure
 * because a single 10 MB JSON column blows past the 100k-row cap's
 * safe envelope. The byte cap (`PUBLISHER_MAX_RESPONSE_BYTES`) is
 * the actual memory bound, and bytes can only be enforced by
 * iterating row-at-a-time on `runSQLStream` — `runSQL` returns a
 * fully-buffered result, so by the time we'd count bytes the
 * connector has already done the damage.
 *
 * On streaming-capable connections (Postgres, DuckDB, ...) the
 * controller routes here. On other connections it stays on the
 * Step 1 path; client-side byte counting after the fact would be
 * security theatre.
 */

import type {
   Connection,
   MalloyQueryData,
   QueryRecord,
   RunSQLOptions,
   StreamingConnection,
} from "@malloydata/malloy";

import { PayloadTooLargeError } from "./errors";

/**
 * Runtime check + type narrow for streaming-capable connections.
 * `Connection.canStream` is declared as `this is StreamingConnection`
 * by the Malloy SDK, so a positive result is enough to safely call
 * `runSQLStream`.
 */
export function isStreamingConnection(
   connection: Connection,
): connection is StreamingConnection {
   return (
      typeof (connection as { canStream?: () => boolean }).canStream ===
         "function" && (connection as StreamingConnection).canStream()
   );
}

export interface StreamBudget {
   /**
    * Maximum number of rows to return. A value of `0` disables the
    * row cap (the caller-supplied `rowLimit` in `runSQLOptions` may
    * still bound the stream, but the helper will not raise on
    * overflow).
    */
   maxRows: number;
   /**
    * Maximum aggregate JSON-serialized byte size of returned rows.
    * `0` disables the byte cap. Measured as the sum of
    * `Buffer.byteLength(JSON.stringify(row))` for each yielded row
    * — the same bytes the eventual response will contain inside
    * `result.rows`.
    */
   maxBytes: number;
}

/**
 * Drain `runSQLStream` into a `MalloyQueryData`-shaped buffer,
 * enforcing both a row cap and a byte cap. Aborts the underlying
 * iterator via an internal `AbortController` the moment either cap
 * is breached so the driver stops producing rows immediately
 * (Postgres' `pg-query-stream` and DuckDB's streaming iterator both
 * honor `abortSignal`).
 *
 * The row cap is detected by the `cap + 1` sentinel pattern: the
 * controller has already clamped `runSQLOptions.rowLimit` to
 * `min(callerLimit, cap + 1)`, so receiving `cap + 1` rows
 * unambiguously means the request would have overflowed. This
 * matches the non-streaming path's overflow detection so behavior
 * is identical regardless of which connector served the request.
 *
 * On overflow the helper throws `PayloadTooLargeError` directly —
 * the message includes the relevant env-var name so the operator
 * sees a self-contained tuning hint without having to cross-
 * reference the controller.
 */
export async function streamSqlWithBudget(
   connection: StreamingConnection,
   sql: string,
   runSQLOptions: RunSQLOptions,
   budget: StreamBudget,
): Promise<MalloyQueryData> {
   const { maxRows, maxBytes } = budget;
   const ac = new AbortController();
   const rows: QueryRecord[] = [];
   let byteTotal = 0;
   let overflowMessage: string | undefined;

   try {
      for await (const row of connection.runSQLStream(sql, {
         ...runSQLOptions,
         abortSignal: ac.signal,
      })) {
         rows.push(row);
         if (maxBytes > 0) {
            // Measure exactly what the eventual response body will
            // contain for this row. O(rowSize) per row, duplicated
            // against the final `JSON.stringify(result)` — the
            // early-abort win on overflow dwarfs the bookkeeping
            // cost in the bounded-success case.
            byteTotal += Buffer.byteLength(JSON.stringify(row), "utf8");
            if (byteTotal > maxBytes) {
               overflowMessage = `Query response exceeded ${maxBytes} bytes (had at least ${byteTotal}). Refine the query (project fewer columns, add a LIMIT, or filter wide values) or raise PUBLISHER_MAX_RESPONSE_BYTES.`;
               ac.abort();
               break;
            }
         }
         if (maxRows > 0 && rows.length > maxRows) {
            overflowMessage = `Query returned more than ${maxRows} rows. Refine the query (add a LIMIT or more selective WHERE) or raise PUBLISHER_MAX_QUERY_ROWS.`;
            ac.abort();
            break;
         }
      }
   } catch (err) {
      // `pg-query-stream` surfaces `query.destroy()` (which our
      // abort handler triggers) as a synthetic error in some
      // versions. Swallow it iff we triggered the abort ourselves —
      // otherwise it's a real connection error that the controller
      // must see.
      if (!overflowMessage) throw err;
   }

   if (overflowMessage) throw new PayloadTooLargeError(overflowMessage);

   return { rows, totalRows: rows.length };
}
