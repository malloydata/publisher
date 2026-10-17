// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { lazyCounter } from "../telemetry";
import type * as Malloy from "@malloydata/malloy-interfaces";
import { bigIntReplacer } from "../json_utils";
import type { QueryRowLimitSource } from "../service/model_limits";

const rowLimitHits = lazyCounter(
   "publisher_mcp_row_limit_hits_total",
   "execute_query answers that returned exactly the server's default row limit, so the agent saw a truncated sample.",
);

/**
 * The agent-facing shape for a query result. Field names (leading underscores,
 * `renderLogErrors` casing) match Credible's `execute_query` on purpose, so an
 * agent and a shared skill see one shape locally and in production. As in
 * Credible, truncation fields appear only when truncation happened.
 *
 * Three things can shorten a result: the query row cap pushed into the SQL
 * (silent, hence `_limit_hit`), the hard maxRows/maxBytes ceiling (a loud 413),
 * and the payload cap below (truncation plus a warning).
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
    * almost certainly left behind. A bound, not a total: the database applied
    * the cap, so the true count is unknown. A deliberate `limit:`/`top:` that
    * returns exactly what it asked for is complete and does not set this.
    */
   _limit_hit: boolean;
   /** Present only when the payload cap dropped rows. */
   _rows_truncated?: boolean;
   _total_rows?: number;
   _returned_rows?: number;
   /**
    * The `query_id` attached to this query's statements: the join key into the
    * backend's own query record (`QUERY_HISTORY`, `JOBS.labels`, the statement
    * text). Absent unless query metadata is enabled.
    */
   _query_id?: string;
   warning?: string;
   renderLogErrors?: string[];
}

/**
 * Cap on the serialized envelope, in characters (Credible's value). MCP clients
 * cap a tool result at roughly 25k tokens and spill or reject past it; chars
 * approximate tokens at about 4:1, with headroom.
 */
export const MAX_RESULT_CHARS = 90_000;

/**
 * Drop repeated annotation lines, keeping the LAST copy of each.
 *
 * Malloy lists a model's annotations once per file in its import graph, so a
 * model whose six imports each open with `##! experimental.access_modifiers`
 * returns that line six times. Tags fold last-wins and the importing model
 * comes last, so keeping the last copy preserves what the lines resolve to;
 * keeping the first could let an import's setting override the model's own.
 *
 * A clone line (`## b := $a`) copies what precedes it, so no copy is dropped
 * across one: with `## a { x=1 }`, `## b := $a`, `## a { x=1 }`, dropping the
 * first copy would change what `b` clones. Any line holding `:=` counts, even
 * inside a string, which only dedupes less.
 */
export function dedupeAnnotations(
   annotations: Malloy.Annotation[],
): Malloy.Annotation[] {
   let seen = new Set<string>();
   const kept: Malloy.Annotation[] = [];
   for (let i = annotations.length - 1; i >= 0; i--) {
      const annotation = annotations[i];
      if (seen.has(annotation.value)) continue;
      kept.push(annotation);
      if (annotation.value.includes(":=")) {
         seen = new Set<string>();
         continue;
      }
      seen.add(annotation.value);
   }
   return kept.reverse();
}

/** Serialize an envelope as it is measured against the budget, BigInt-safe. */
export function serializeEnvelope(envelope: QueryEnvelope): string {
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
   // return more than it. Only the server default counts, because
   // resolveModelQueryRowLimit folds a query's own limit:/top: into the same
   // cap, and a `top: 10` view returning 10 rows is complete.
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
            model_annotations: dedupeAnnotations(result.model_annotations),
         }),
         ...(result.query_timezone !== undefined && {
            query_timezone: result.query_timezone,
         }),
         ...(result.source_annotations !== undefined && {
            source_annotations: dedupeAnnotations(result.source_annotations),
         }),
      },
      _query_row_limit: rowLimit,
      _limit_source: rowLimitSource,
      _limit_hit: limitHit,
      ...(queryCorrelationId !== null && { _query_id: queryCorrelationId }),
      ...(renderLogErrors.length > 0 && { renderLogErrors }),
   };

   if (limitHit) {
      rowLimitHits().add(1);
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
 * The marker fields and warning are set BEFORE the search so their size counts
 * against the limit; `_returned_rows` is measured at `total`, the widest value
 * it can hold, so the finished payload stays under the cap.
 *
 * Zero rows gets its own wording, because "Showing 0 of N rows" reads as an
 * empty result. It is reachable: one row with a large text column passes the
 * upstream maxBytes ceiling. The search measures the longer of the two wordings.
 *
 * When the envelope without rows already exceeds the limit (a very wide
 * schema), the result is returned over-limit rather than emptied further.
 */
function fitToBudget(envelope: QueryEnvelope, limit: number): QueryEnvelope {
   if (serializeEnvelope(envelope).length <= limit) return envelope;

   const rows = envelope.rows;
   if (!Array.isArray(rows) || rows.length === 0) return envelope;
   const total = rows.length;

   const truncating: QueryEnvelope = {
      ...envelope,
      _rows_truncated: true,
      _total_rows: total,
      _returned_rows: total,
   };
   const sizeWarning = (kept: number) =>
      `Showing ${kept} of ${total} rows; the rest were dropped to fit the result size limit. Narrow the query rather than paging through it.`;
   const noneFitWarning =
      `No rows fit the result size limit: the first of ${total} rows is too large to send on its own. ` +
      `This is NOT an empty result and does not mean nothing matched. Select fewer columns, or truncate the oversized field, then run it again.`;
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
         serializeEnvelope({ ...truncating, rows: rows.slice(0, mid) })
            .length <= limit
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
