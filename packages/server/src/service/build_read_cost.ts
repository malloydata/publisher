// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { sqlLiteral } from "./db_utils";

/**
 * How much session history to admit before filtering. Set explicitly because the
 * default is 100 and applies before the `WHERE` — see {@link snowflakeCostSQL}.
 */
const SNOWFLAKE_HISTORY_LIMIT = 10_000;

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

/**
 * Cost columns selected by the query that already locates the read's result
 * table. They must ride on that query: a script's child job is reachable only
 * through its parent, so a separate lookup keyed on the child id finds nothing.
 */
export const BIGQUERY_COST_COLUMNS = `
   total_slot_time_ms,
   json_extract_string(statistics, '$.query.totalBytesProcessed') AS bytes_processed,
   json_extract_string(statistics, '$.query.totalBytesBilled')    AS bytes_billed,
   json_extract_string(statistics, '$.query.cacheHit')            AS cache_hit`;

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
 * The database to resolve `INFORMATION_SCHEMA` against.
 *
 * Emitted BARE, not quoted: quoting is case-exact while the connection parameter
 * resolves case-insensitively, so `mydb` against a stored `MYDB` would miss. A
 * name needing quotes takes the `SNOWFLAKE` fallback (measured to return the same
 * rows) rather than being interpolated raw into identifier position.
 */
function snowflakeDatabaseQualifier(
   database: string | null | undefined,
): string {
   // Snowflake's unquoted-identifier rule: a leading digit or `$` needs quotes.
   return typeof database === "string" &&
      /^[A-Za-z_][A-Za-z0-9_$]*$/.test(database)
      ? database
      : "SNOWFLAKE";
}

/**
 * The queries this build's session ran, found by the tag the build set on it.
 *
 * - `INFORMATION_SCHEMA`, not `ACCOUNT_USAGE`: the latter lags up to three hours
 *   and needs elevated grants.
 * - Keyed on the TAG, not `LAST_QUERY_ID()`: the ADBC driver issues its own
 *   `SELECT 1` probes on the session, so the last query is often a probe.
 *   {@link pickSnowflakeReadRow} picks the read out of the result.
 * - `_BY_SESSION` with an explicit `RESULT_LIMIT`: these table functions cap
 *   output (default 100) BEFORE the `WHERE` runs, so no predicate reaches past
 *   the cap. Measured: past 100 statements, the default form matched zero rows.
 * - The accounting query carries the same tag but is not filtered by text: the
 *   exact-SQL match in {@link pickSnowflakeReadRow} and `EXECUTION_STATUS`
 *   already exclude it, and a text filter would misfire on a source that models
 *   Snowflake's query history.
 * - Aliases are QUOTED: Snowflake folds bare aliases to uppercase.
 * - The tag is escaped with {@link sqlLiteral}, not quote-doubling: it is JSON,
 *   so backslashes arrive doubled and Snowflake consumes one as an escape.
 * - Always database-qualified: a database-less connection has no CURRENT
 *   database, so the unqualified form fails with `002004 (42601)`. The
 *   `SNOWFLAKE` shared database exists on every account; a role that cannot read
 *   it loses only the cost, since a failed lookup is treated as no cost.
 */
export function snowflakeCostSQL(
   queryTag: string,
   /** The connection's configured database, absent for a database-less one. */
   database?: string | null,
): string {
   const qualifier = `${snowflakeDatabaseQualifier(database)}.INFORMATION_SCHEMA`;
   return `
      SELECT QUERY_ID       AS "job_id",
             QUERY_TEXT     AS "query_text",
             BYTES_SCANNED  AS "bytes_scanned",
             EXECUTION_TIME AS "execution_time_ms",
             ROWS_PRODUCED  AS "rows_produced"
      FROM TABLE(${qualifier}.QUERY_HISTORY_BY_SESSION(
                 RESULT_LIMIT => ${SNOWFLAKE_HISTORY_LIMIT}))
      WHERE QUERY_TAG = '${sqlLiteral(queryTag, "snowflake")}'
        AND EXECUTION_STATUS = 'SUCCESS'`;
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
