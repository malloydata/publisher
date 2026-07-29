import { bigIntReplacer } from "../json_utils";

/**
 * The agent-facing shape for a query result.
 *
 * Three separate things can shorten a result, and before this envelope a caller
 * could distinguish none of them:
 *
 *   1. The query row cap, pushed into the SQL. A query carrying no LIMIT of its
 *      own gets DEFAULT_QUERY_ROW_LIMIT (1000) rows. That is well under
 *      PUBLISHER_MAX_QUERY_ROWS, so assertWithinModelResponseLimits raises
 *      nothing and the caller sees 1000 rows with no indication that the table
 *      held 150,000. This is the one that silently corrupts an answer, and it is
 *      why `query_row_limit` and `limit_hit` exist.
 *   2. The hard ceiling (maxRows / maxBytes), which throws 413 and is therefore
 *      already loud.
 *   3. This payload cap, applied here so a large result degrades to a truncated
 *      one with a warning instead of overflowing the client's per-result limit.
 *
 * `limit_hit` costs nothing to compute: no second query, no COUNT(*). It is the
 * honest bound rather than a true total, which the server cannot know because
 * the cap was applied by the database. If you got exactly the limit, you cannot
 * conclude the result is complete.
 *
 * `rows` is the same flat row shape REST returns for `compactJson: true` and
 * that `Publisher.query()` hands an in-package data app, so an agent previewing
 * a query sees the shape its render code will actually receive.
 */
export interface QueryEnvelope {
   rows: unknown;
   row_count: number;
   query_row_limit: number;
   limit_hit: boolean;
   truncated_for_size: boolean;
   warning?: string;
   renderLogErrors?: string[];
}

/**
 * Cap on the serialized envelope, in characters.
 *
 * Host-loop MCP clients enforce a per-tool-result ceiling of roughly 25k tokens;
 * past it the result is spilled to disk or rejected outright, and a model then
 * struggles to recover it. Chars stand in for tokens at about 4:1, and this sits
 * under that with headroom for the envelope itself.
 *
 * This belongs in the MCP layer rather than the REST controller: it is tuned to
 * a client's context budget, which is not a property of the query.
 */
export const MAX_RESULT_CHARS = 90_000;

function serialize(envelope: QueryEnvelope): string {
   return JSON.stringify(envelope, bigIntReplacer, 2);
}

/**
 * Drop rows until the serialized envelope fits, by binary search on the row
 * count. Rows are uniform enough that halving is a good estimator, and this
 * avoids re-serializing once per dropped row on a wide result.
 */
function fitToBudget(envelope: QueryEnvelope, limit: number): QueryEnvelope {
   if (!Array.isArray(envelope.rows) || serialize(envelope).length <= limit) {
      return envelope;
   }
   const all = envelope.rows;
   let low = 0;
   let high = all.length;
   while (low < high) {
      const mid = Math.ceil((low + high) / 2);
      const candidate = { ...envelope, rows: all.slice(0, mid) };
      if (serialize(candidate).length <= limit) {
         low = mid;
      } else {
         high = mid - 1;
      }
   }
   return { ...envelope, rows: all.slice(0, low), truncated_for_size: true };
}

/**
 * Build the envelope, applying the payload cap and attaching the warnings that
 * make each kind of shortening actionable.
 *
 * @param rows        compactResult: flat row objects, straight from the driver.
 * @param rowLimit    the cap pushed into the SQL (query's own LIMIT, or default).
 * @param renderLogErrors error/warn render-tag messages, if any.
 */
export function buildQueryEnvelope(
   rows: unknown,
   rowLimit: number,
   renderLogErrors: string[] = [],
   limit = MAX_RESULT_CHARS,
): QueryEnvelope {
   const rowCount = Array.isArray(rows) ? rows.length : 0;
   // Equality, not >=: the row cap is pushed into the SQL, so the database
   // cannot return more than it. Landing exactly on it is the signal.
   const limitHit = rowLimit > 0 && rowCount === rowLimit;

   const limitWarning = limitHit
      ? `Returned exactly ${rowLimit} rows, the row limit applied to this query, so there are probably more. This is not a complete result: add an explicit limit, aggregate, or filter rather than reporting these rows as the whole set.`
      : "";
   const sizeWarning = (kept: number) =>
      `Showing ${kept} of ${rowCount} rows; the rest were dropped to fit the result size limit. Narrow the query rather than paging through it.`;

   // Fit against a budget reduced by the longest warning text this call could
   // produce. The warning is added AFTER the rows are trimmed, so fitting
   // against the full budget first would push the finished envelope back over
   // it: the truncation meant to keep the result under the cap would leave it
   // above. `rowCount` is the widest the kept-count can render as.
   const worstCaseWarning = [limitWarning, sizeWarning(rowCount)]
      .filter(Boolean)
      .join(" ");
   const reserve = worstCaseWarning.length + `,\n  "warning": ""`.length;

   const fitted = fitToBudget(
      {
         rows,
         row_count: rowCount,
         query_row_limit: rowLimit,
         limit_hit: limitHit,
         truncated_for_size: false,
         ...(renderLogErrors.length > 0 && { renderLogErrors }),
      },
      Math.max(limit - reserve, 0),
   );

   // Recount after fitting so row_count always describes the rows present.
   const returned = Array.isArray(fitted.rows) ? fitted.rows.length : 0;
   const warnings = [
      limitWarning,
      fitted.truncated_for_size ? sizeWarning(returned) : "",
   ].filter(Boolean);

   return {
      ...fitted,
      row_count: returned,
      ...(warnings.length > 0 && { warning: warnings.join(" ") }),
   };
}

/** Serialize an envelope for transport, BigInt-safe. */
export function serializeEnvelope(envelope: QueryEnvelope): string {
   return serialize(envelope);
}
