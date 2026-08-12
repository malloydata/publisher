import { logger } from "../logger";
import {
   recordBuildCostLookup,
   recordCachedBuild,
} from "../materialization_metrics";
import type { SqlRunner } from "./incremental_apply";

/**
 * What a build's warehouse read actually cost, read back from the warehouse's
 * own accounting after the build has run.
 *
 * Every field is nullable and per-engine, deliberately rather than by omission:
 * the two warehouses do not measure the same things, and flattening them into a
 * shared shape would invent equivalences that do not exist. In particular there
 * is no cross-engine "cost" number — BigQuery bills bytes, Snowflake bills
 * warehouse-seconds — so a caller comparing the two must compare like for like
 * or not at all.
 */
export interface BuildCost {
   engine: "bigquery" | "snowflake";
   /** The warehouse's own id for the read, for a human to look it up. */
   jobId: string | null;
   /** Bytes the query read. BigQuery `totalBytesProcessed`; Snowflake `BYTES_SCANNED`. */
   bytesScanned: number | null;
   /**
    * Bytes the query is BILLED for, which is not the same number: BigQuery
    * rounds up to a 10MB minimum per query, so a small read bills an order of
    * magnitude above what it scanned. This is the figure that maps to money.
    * BigQuery only — Snowflake has no per-query billed quantity at all.
    */
   bytesBilled: number | null;
   /**
    * BigQuery slot-milliseconds: aggregate parallel compute, so it is NOT wall
    * clock and routinely exceeds the query's duration. Null on Snowflake, whose
    * compute is billed per warehouse-second regardless of what runs on it, and
    * which therefore has no comparable per-query figure. Deliberately not
    * merged with Snowflake's execution time, which measures something else.
    */
   slotTimeMs: number | null;
   /** Snowflake wall-clock execution ms. Null on BigQuery, where slotTimeMs is the meaningful number. */
   executionTimeMs: number | null;
   /**
    * The warehouse answered from cache, so this read was free. On a BUILD that
    * is also a finding rather than a curiosity: BigQuery invalidates the cache
    * when the underlying tables change, so a cached build means the source data
    * had not changed and the rebuild was unnecessary work.
    */
   cacheHit: boolean | null;
}

/** Why a lookup produced no cost, for the metric that tracks how well the key works. */
export type BuildCostOutcome =
   | "found"
   | "not_found"
   | "ambiguous"
   | "error"
   | "unsupported";

/**
 * Widen the search window by this much before the read started. `since` is our
 * wall clock and the warehouse stamps its own, so a locally-fast clock would
 * otherwise exclude the very job being looked for. Widening costs nothing: the
 * exact-SQL match is what identifies the job, and the window only bounds how
 * many rows are scanned to find it.
 */
const CLOCK_SKEW_MARGIN_MS = 60_000;

/** Cap the candidate set. A window this narrow holding more rows means something is wrong. */
const MAX_CANDIDATES = 50;

export interface BuildCostLookup {
   /** Runs SQL on the session that performed the build — the passthrough is reachable from it. */
   runner: SqlRunner;
   engine: "bigquery" | "snowflake";
   /** BigQuery billing/target project. Unused for Snowflake. */
   project?: string;
   /** The exact SQL handed to the passthrough — the correlation key. */
   sql: string;
   /** When the read started, by our clock. */
   since: Date;
}

/**
 * Look up what a build's warehouse read cost, by finding the warehouse's own
 * record of it.
 *
 * <b>The key is a heuristic, and the failure mode is deliberately "no number"
 * rather than "a wrong number."</b> Neither passthrough hands back a job id for
 * a rows-returning call, so the job is identified by a time window plus an
 * EXACT match on the query text the warehouse recorded. When that does not
 * resolve to exactly one row this returns null and counts why:
 *
 * <ul>
 *   <li><b>ambiguous</b> — more than one match. Never guess, and never take the
 *       most recent: two identical-SQL reads are the case where the costs
 *       diverge MOST, not least, because the second one hits the result cache
 *       and bills zero. Picking the newest would systematically under-report,
 *       and under-reporting build cost flatters the savings figure that this
 *       data exists to compute.
 *   <li><b>not_found</b> — no match. The read may not have created a job
 *       (metadata-only queries do not), or the warehouse's accounting may not
 *       be readable yet.
 * </ul>
 *
 * Ambiguity is rarer than it looks: the control plane already admits one
 * in-flight build per source, so it takes two DISTINCT sources compiling to
 * byte-identical SQL against one warehouse, overlapping in time.
 *
 * <b>Never throws.</b> This is telemetry attached to a build that has already
 * succeeded; a failure here must not turn a good build into a failed one.
 */
export async function lookupBuildCost(
   opts: BuildCostLookup,
): Promise<BuildCost | null> {
   const { engine } = opts;
   try {
      const rows =
         engine === "bigquery"
            ? await lookupBigQuery(opts)
            : await lookupSnowflake(opts);

      if (rows.length === 0) return finish(engine, "not_found", null);
      if (rows.length > 1) {
         logger.debug("Build cost lookup matched more than one query", {
            engine,
            matches: rows.length,
         });
         return finish(engine, "ambiguous", null);
      }
      const cost = rows[0];
      if (cost.cacheHit) recordCachedBuild(engine);
      return finish(engine, "found", cost);
   } catch (error) {
      // Includes the case where the warehouse refuses the accounting query
      // outright — some deployments restrict query history — which is a
      // permission answer, not a bug, and must degrade to "no number".
      logger.debug("Build cost lookup failed", {
         engine,
         error: error instanceof Error ? error.message : String(error),
      });
      return finish(engine, "error", null);
   }
}

function finish(
   engine: "bigquery" | "snowflake",
   outcome: BuildCostOutcome,
   cost: BuildCost | null,
): BuildCost | null {
   recordBuildCostLookup(engine, outcome);
   return cost;
}

/**
 * BigQuery: the Jobs API, exposed by the passthrough extension as
 * `bigquery_jobs()`. Listing one's OWN jobs needs no permission beyond the one
 * the build already used, so this adds no onboarding requirement — verified
 * against a service account holding only jobUser + readSessionUser + a
 * dataset-scoped grant.
 *
 * `job_type` is filtered in a SUBQUERY on purpose. Applied in the outer WHERE
 * alongside the JSON path, a non-QUERY job (a load, a copy) reaches the JSON
 * extraction and fails the whole statement with a cast error.
 */
async function lookupBigQuery(opts: BuildCostLookup): Promise<BuildCost[]> {
   const since = new Date(opts.since.getTime() - CLOCK_SKEW_MARGIN_MS);
   const sql = `
      SELECT job_id,
             bytes_processed,
             total_slot_time_ms,
             json_extract_string(statistics, '$.query.totalBytesBilled') AS billed,
             json_extract_string(statistics, '$.query.cacheHit')         AS cache_hit
      FROM (SELECT * FROM bigquery_jobs(${lit(opts.project ?? "")},
                                        minCreationTime := ${lit(since.toISOString())},
                                        maxResults := ${MAX_CANDIDATES})
            WHERE job_type = 'QUERY') j
      WHERE json_extract_string(configuration, '$.query.query') = ${lit(opts.sql)}`;
   const { rows } = await opts.runner(sql);
   return (rows as Record<string, unknown>[]).map((r) => ({
      engine: "bigquery" as const,
      jobId: str(r["job_id"]),
      bytesScanned: num(r["bytes_processed"]),
      bytesBilled: num(r["billed"]),
      slotTimeMs: num(r["total_slot_time_ms"]),
      executionTimeMs: null,
      cacheHit: bool(r["cache_hit"]),
   }));
}

/**
 * Snowflake: no jobs table function exists, so the account's own query history
 * is read through the passthrough as ordinary SQL.
 *
 * `INFORMATION_SCHEMA.QUERY_HISTORY` rather than `ACCOUNT_USAGE.QUERY_HISTORY`:
 * the latter lags by up to three hours and needs elevated grants, so it cannot
 * answer a question asked immediately after a build. The one used here carries no
 * such lag — measured against a live account, a query appears in it immediately.
 *
 * `QUERY_HISTORY_BY_SESSION` would filter tighter and is genuinely available: the
 * passthrough reuses a single session across calls (measured — two
 * `snowflake_query` calls report the same `CURRENT_SESSION()`). It is not used
 * here because the account-wide view plus an exact query-text match already
 * resolves to one row. That same session guarantee is what would make
 * `LAST_QUERY_ID()` usable as an EXACT key rather than the heuristic below, which
 * is the better long-term shape and deliberately not attempted here.
 *
 * <b>The cost columns are not BigQuery's, and are not translated into them.</b>
 * Snowflake bills warehouse-seconds rather than per-query bytes, so there is no
 * billed quantity to report; `EXECUTION_TIME` is wall clock on the warehouse and
 * means something different from BigQuery's slot-milliseconds. Reporting a
 * result-cache hit is inference rather than a flag: Snowflake exposes no such
 * column, and a served-from-cache query is recognisable by having scanned
 * nothing while still producing rows.
 */
async function lookupSnowflake(opts: BuildCostLookup): Promise<BuildCost[]> {
   const since = new Date(opts.since.getTime() - CLOCK_SKEW_MARGIN_MS);
   const sql = `
      SELECT QUERY_ID          AS job_id,
             BYTES_SCANNED     AS bytes_scanned,
             EXECUTION_TIME    AS execution_time_ms,
             ROWS_PRODUCED     AS rows_produced
      FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
                 END_TIME_RANGE_START => TO_TIMESTAMP_LTZ(${lit(since.toISOString())}),
                 RESULT_LIMIT => ${MAX_CANDIDATES}))
      WHERE QUERY_TEXT = ${lit(opts.sql)}
        AND EXECUTION_STATUS = 'SUCCESS'`;
   const { rows } = await opts.runner(sql);
   return (rows as Record<string, unknown>[]).map((r) => {
      const scanned = num(r["bytes_scanned"]);
      const produced = num(r["rows_produced"]);
      return {
         engine: "snowflake" as const,
         jobId: str(r["job_id"]),
         bytesScanned: scanned,
         bytesBilled: null,
         slotTimeMs: null,
         executionTimeMs: num(r["execution_time_ms"]),
         // Inferred, not reported: nothing scanned while rows came back is the
         // shape of a result-cache hit. Left null rather than guessed `false`
         // when either input is missing, so an absent column never reads as a
         // positive "this was not cached".
         cacheHit:
            scanned === null || produced === null
               ? null
               : scanned === 0 && produced > 0,
      };
   });
}

/** Single-quoted SQL literal with quotes doubled — these strings are server-built, never caller-supplied. */
function lit(value: string): string {
   return `'${value.replace(/'/g, "''")}'`;
}

function num(v: unknown): number | null {
   if (v === null || v === undefined) return null;
   const n = typeof v === "bigint" ? Number(v) : Number(v);
   return Number.isFinite(n) ? n : null;
}

function str(v: unknown): string | null {
   return typeof v === "string" && v.length > 0 ? v : null;
}

function bool(v: unknown): boolean | null {
   if (typeof v === "boolean") return v;
   if (typeof v === "string") {
      if (v === "true") return true;
      if (v === "false") return false;
   }
   return null;
}
