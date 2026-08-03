import type * as Malloy from "@malloydata/malloy-interfaces";
import { bigIntReplacer } from "../json_utils";
import type { QueryRowLimitSource } from "../service/model_limits";

/**
 * The agent-facing shape for a query result, matched to what Credible's
 * `execute_query` returns.
 *
 * Matching matters for a specific workflow: a data app is authored locally
 * against Publisher and then served through Credible. An agent that sees one
 * response shape while developing and a different one in production has to
 * learn both, and a shared skill cannot describe either without forking. So the
 * field names here are Credible's, deliberately, including the leading
 * underscores and the mixed casing of `renderLogErrors`.
 *
 * Also following Credible: the truncation fields appear only when truncation
 * happened, rather than always carrying `false`.
 *
 * The two `_limit_...` fields are new to both products. Three separate things
 * can shorten a result and only one of them was ever reported:
 *
 *   1. The query row cap, pushed into the SQL. A query with no `limit:` of its
 *      own gets DEFAULT_QUERY_ROW_LIMIT (1000) rows, which is far under
 *      PUBLISHER_MAX_QUERY_ROWS, so nothing raises and nothing warns. An agent
 *      reports statistics on a silent sample. This is why `_limit_hit` exists,
 *      and unlike the others it is not derivable by a client: the cap depends on
 *      server config and on the query's own LIMIT.
 *   2. The hard ceiling (maxRows / maxBytes), which throws 413 and is loud.
 *   3. The payload cap below, which degrades to a truncated result plus a
 *      warning instead of overflowing the client's per-result limit.
 *
 * `_limit_hit` is a bound, not a total. The server cannot know the true row
 * count, because the database applied the cap. Landing exactly on the limit is
 * the only evidence available that rows were left behind.
 *
 * It is reported only when the cap was the server default, which `_limit_source`
 * names. `resolveModelQueryRowLimit` folds the query's own `limit:`/`top:` into
 * the same number, so equality also holds every time an author limited the query
 * on purpose: three of the eight views in the bundled storefront example use
 * `top:`, and each would otherwise report its complete answer as cut off. The
 * silent sample this exists to catch only ever happens under the default.
 */
export interface QueryEnvelope {
   rows: unknown;
   /** Malloy metadata the flat rows drop: field types, render tags, timezone. */
   _meta: {
      schema: Malloy.Schema;
      annotations: Malloy.Annotation[];
      connection_name: string;
      model_annotations?: Malloy.Annotation[];
      query_timezone?: string;
      source_annotations?: Malloy.Annotation[];
   };
   /** The cap pushed into the SQL: the query's own LIMIT, else the server default. */
   _query_row_limit: number;
   /** Which of those two the cap came from. */
   _limit_source: QueryRowLimitSource;
   /**
    * Row count equals the cap AND the cap was the server default, so rows were
    * almost certainly left behind. A deliberate `limit:`/`top:` that returns
    * exactly what it asked for is a complete answer and does not set this.
    */
   _limit_hit: boolean;
   /** Present only when the payload cap dropped rows. */
   _rows_truncated?: boolean;
   _total_rows?: number;
   _returned_rows?: number;
   /**
    * The `query_id` property attached to this query's statements: the join key
    * into the backend's own query record (`QUERY_HISTORY`, `JOBS.labels`, the
    * statement text). Publisher-side and new to both products, like the two
    * `_limit_...` fields. Absent when nothing was attached, which is every query
    * on a deployment that has not enabled query metadata.
    */
   _query_id?: string;
   warning?: string;
   renderLogErrors?: string[];
}

/**
 * Cap on the serialized envelope, in characters. Same value Credible uses.
 *
 * Host-loop MCP clients enforce a per-tool-result ceiling of roughly 25k tokens;
 * past it the result is spilled to disk or rejected outright, and the model then
 * struggles to recover it. Chars stand in for tokens at about 4:1, with headroom
 * for the envelope itself.
 */
export const MAX_RESULT_CHARS = 90_000;

function serialize(envelope: QueryEnvelope): string {
   return JSON.stringify(envelope, bigIntReplacer, 2);
}

/**
 * Build the envelope, applying the payload cap.
 *
 * @param rows      compactResult: flat row objects, straight from the driver.
 * @param rowLimit  the cap pushed into the SQL.
 * @param result    the full Malloy result, read only for its metadata.
 */
export function buildQueryEnvelope(
   rows: unknown,
   rowLimit: number,
   result: Malloy.Result,
   renderLogErrors: string[] = [],
   limit = MAX_RESULT_CHARS,
   rowLimitSource: QueryRowLimitSource = "server_default",
   queryCorrelationId: string | null = null,
): QueryEnvelope {
   const rowCount = Array.isArray(rows) ? rows.length : 0;
   // Equality, not >=: the cap is pushed into the SQL, so the database cannot
   // return more than it. Landing exactly on it is the signal.
   //
   // Restricted to the server default on purpose. resolveModelQueryRowLimit
   // folds the query's own limit:/top: into the same cap, so equality also holds
   // every time an author deliberately limited the query: a modelled `top: 10`
   // view returning its 10 rows would otherwise be reported as cut off, and the
   // Contract rule would tell an agent that a top-N is "not the answer". Only
   // the silently-applied default is evidence that rows were left behind.
   const limitHit =
      rowLimitSource === "server_default" &&
      rowLimit > 0 &&
      rowCount === rowLimit;

   const envelope: QueryEnvelope = {
      rows,
      _meta: {
         schema: result.schema,
         annotations: result.annotations ?? [],
         connection_name: result.connection_name,
         ...(result.model_annotations !== undefined && {
            model_annotations: result.model_annotations,
         }),
         ...(result.query_timezone !== undefined && {
            query_timezone: result.query_timezone,
         }),
         ...(result.source_annotations !== undefined && {
            source_annotations: result.source_annotations,
         }),
      },
      _query_row_limit: rowLimit,
      _limit_source: rowLimitSource,
      _limit_hit: limitHit,
      ...(queryCorrelationId !== null && { _query_id: queryCorrelationId }),
      ...(renderLogErrors.length > 0 && { renderLogErrors }),
   };

   if (limitHit) {
      envelope.warning =
         `Returned exactly ${rowLimit} rows, the row limit applied to this query, so there are probably more. ` +
         `This is not a complete result: add an explicit limit, aggregate, or filter rather than reporting these rows as the whole set.`;
   }

   return fitToBudget(envelope, limit);
}

/**
 * Drop rows until the serialized envelope fits, by binary search on the row
 * count.
 *
 * The marker fields are set BEFORE the search, so their serialized size counts
 * against the limit. Adding them afterwards pushes the payload back over the cap
 * the truncation existed to respect. `_returned_rows` is measured at `total`
 * first, whose digit width is at least that of any value it ends up holding, so
 * the finished payload stays under the limit. Credible's `_truncate_rows` does
 * the same thing for the same reason.
 *
 * The search can land on zero rows, which needs its own wording rather than
 * "Showing 0 of N rows": that reads as an empty result set, and an agent will
 * report "no rows matched" for what is really one row too large to send. It is
 * reachable, because the hard ceiling upstream is maxBytes (50MB by default), so
 * a single row carrying a large text or JSON column passes
 * assertWithinModelResponseLimits and arrives here. Both wordings are measured,
 * and the search uses whichever is longer, so swapping one for the other after
 * the fact cannot push the payload back over the cap.
 *
 * One case cannot be fixed by dropping rows: when the envelope minus its rows
 * already exceeds the limit (a very wide schema in `_meta`). The result is then
 * returned over-limit rather than emptied further, and the warning says so.
 */
function fitToBudget(envelope: QueryEnvelope, limit: number): QueryEnvelope {
   if (serialize(envelope).length <= limit) return envelope;

   const rows = envelope.rows;
   if (!Array.isArray(rows) || rows.length === 0) return envelope;
   const total = rows.length;

   const truncating: QueryEnvelope = {
      ...envelope,
      _rows_truncated: true,
      _total_rows: total,
      _returned_rows: total,
   };
   // The size warning joins any limit warning already present, and is included
   // in the measurement for the same reason as the counts.
   const sizeWarning = (kept: number) =>
      `Showing ${kept} of ${total} rows; the rest were dropped to fit the result size limit. Narrow the query rather than paging through it.`;
   const noneFitWarning =
      `No rows fit the result size limit: the first of ${total} rows is too large to send on its own. ` +
      `This is NOT an empty result and does not mean nothing matched. Select fewer columns, or truncate the oversized field, then run it again.`;
   // Measure with whichever wording is longer, so the one actually returned is
   // never bigger than the one the search sized the payload against.
   const measured =
      noneFitWarning.length > sizeWarning(total).length
         ? noneFitWarning
         : sizeWarning(total);
   truncating.warning = [envelope.warning, measured].filter(Boolean).join(" ");

   let low = 0;
   let high = total;
   let best = 0;
   while (low <= high) {
      const mid = Math.floor((low + high) / 2);
      if (
         serialize({ ...truncating, rows: rows.slice(0, mid) }).length <= limit
      ) {
         best = mid;
         low = mid + 1;
      } else {
         high = mid - 1;
      }
   }

   return {
      ...truncating,
      rows: rows.slice(0, best),
      _returned_rows: best,
      warning: [
         envelope.warning,
         best === 0 ? noneFitWarning : sizeWarning(best),
      ]
         .filter(Boolean)
         .join(" "),
   };
}

/** Serialize an envelope for transport, BigInt-safe. */
export function serializeEnvelope(envelope: QueryEnvelope): string {
   return serialize(envelope);
}
