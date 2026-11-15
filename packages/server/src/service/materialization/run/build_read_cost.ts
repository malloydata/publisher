// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What a `storage=` build's warehouse read cost, as BigQuery's job record or
 * Snowflake's session history accounts for it, and which of the session's
 * queries was the build's read. The queries that read the cost are
 * warehouse_sql.ts's.
 */

import { BIGQUERY_COST_COLUMNS, snowflakeCostSQL } from "./warehouse_sql";

export { BIGQUERY_COST_COLUMNS, snowflakeCostSQL };

/**
 * What a `storage=` build's warehouse read cost, as the warehouse accounts for it.
 *
 * Fields are nullable and per-engine on purpose: BigQuery bills bytes, Snowflake
 * bills warehouse-seconds, so there is no cross-engine "cost" number and callers
 * must compare like for like.
 */
export interface BuildReadCost {
   engine: "bigquery" | "snowflake";
   /** The warehouse's own id for the job that ran the read and carries these numbers. */
   jobId: string | null;
   /**
    * The job to look {@link jobId} up BY when it is not findable on its own:
    * BigQuery runs a labelled read as a script, and a script's child job is absent
    * from the default `bigquery_jobs()` listing. Null where `jobId` is directly
    * findable.
    */
   parentJobId: string | null;
   /** Bytes the query read. BigQuery `totalBytesProcessed`; Snowflake `BYTES_SCANNED`. */
   bytesScanned: number | null;
   /**
    * Bytes BILLED, which differs from scanned: BigQuery rounds up to a 10MB
    * minimum per query, and refreshes are mostly small reads. BigQuery only —
    * Snowflake has no per-query billed quantity.
    */
   bytesBilled: number | null;
   /**
    * BigQuery slot-milliseconds: aggregate parallel compute, NOT wall clock.
    * Null on Snowflake; deliberately not merged with its execution time, which
    * measures something else.
    */
   slotTimeMs: number | null;
   /** Snowflake wall-clock execution ms. Null on BigQuery, where slotTimeMs is the meaningful number. */
   executionTimeMs: number | null;
   /**
    * The warehouse answered from cache. On a build this means the source data had
    * not moved (the cache is invalidated on change), so the rebuild did no useful
    * work.
    */
   cacheHit: boolean | null;
}

/** Read {@link BIGQUERY_COST_COLUMNS} off a job row. */
export function bigQueryReadCost(
   row: Record<string, unknown>,
   jobId: string | null,
   parentJobId: string | null,
): BuildReadCost {
   return {
      engine: "bigquery",
      jobId,
      parentJobId,
      bytesScanned: num(row.bytes_processed),
      bytesBilled: num(row.bytes_billed),
      slotTimeMs: num(row.total_slot_time_ms),
      executionTimeMs: null,
      cacheHit: bool(row.cache_hit),
   };
}

/**
 * Pick the build's own read out of the queries that carried its tag (typically
 * the read, two driver probes, and the accounting query).
 *
 * Text is compared HERE rather than in the warehouse query so the compiled SQL is
 * never embedded in SQL and needs no escaping. Returns null unless exactly one
 * row matches.
 */
export function pickSnowflakeReadRow(
   rows: Record<string, unknown>[],
   buildSQL: string,
): Record<string, unknown> | null {
   const matches = rows.filter(
      (row) => str(col(row, "query_text"))?.trim() === buildSQL.trim(),
   );
   return matches.length === 1 ? matches[0] : null;
}

/** Read {@link snowflakeCostSQL}'s row. */
export function snowflakeReadCost(row: Record<string, unknown>): BuildReadCost {
   const scanned = num(col(row, "bytes_scanned"));
   const produced = num(col(row, "rows_produced"));
   return {
      engine: "snowflake",
      jobId: str(col(row, "job_id")),
      // QUERY_HISTORY is keyed on the query id directly; there is no parent.
      parentJobId: null,
      bytesScanned: scanned,
      bytesBilled: null,
      slotTimeMs: null,
      executionTimeMs: num(col(row, "execution_time_ms")),
      // Inferred (Snowflake has no cache-hit column): scanned nothing yet
      // produced rows. Null when an input is missing, so an absent column never
      // reads as "not cached". It over-reports (e.g. GENERATOR), so it is a hint
      // that nothing should bill against.
      cacheHit:
         scanned === null || produced === null
            ? null
            : scanned === 0 && produced > 0,
   };
}

/**
 * Read a column case-insensitively: a second line of defence behind the quoted
 * aliases, in case a transport folds case.
 */
function col(row: Record<string, unknown>, key: string): unknown {
   if (key in row) return row[key];
   const upper = key.toUpperCase();
   if (upper in row) return row[upper];
   const hit = Object.keys(row).find((k) => k.toLowerCase() === key);
   return hit === undefined ? undefined : row[hit];
}

function num(value: unknown): number | null {
   if (value === null || value === undefined) return null;
   const n = Number(value);
   return Number.isFinite(n) ? n : null;
}

/** A non-empty string value, or null for anything else. */
export function str(value: unknown): string | null {
   return typeof value === "string" && value.length > 0 ? value : null;
}

function bool(value: unknown): boolean | null {
   if (typeof value === "boolean") return value;
   if (typeof value === "string") {
      if (value === "true") return true;
      if (value === "false") return false;
   }
   return null;
}
