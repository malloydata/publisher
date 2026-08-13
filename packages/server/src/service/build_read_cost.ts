import { sqlLiteral } from "./db_utils";

/**
 * How much of the build session's history to admit before filtering. Generous
 * against a session that runs a handful of statements, and set EXPLICITLY because
 * the default is 100 and applies before the `WHERE` — see {@link snowflakeCostSQL}.
 */
const SNOWFLAKE_HISTORY_LIMIT = 10_000;

/**
 * What a `storage=` build's warehouse read cost, as the warehouse accounts for it.
 *
 * Every field is nullable and per-engine, deliberately rather than by omission:
 * the warehouses do not measure the same things, and flattening them into a
 * shared shape would invent equivalences that do not exist. There is no
 * cross-engine "cost" number here — BigQuery bills bytes, Snowflake bills
 * warehouse-seconds — so a caller comparing two engines must compare like for
 * like or not at all.
 */
export interface BuildReadCost {
   engine: "bigquery" | "snowflake";
   /** The warehouse's own id for the job that ran the read and carries these numbers. */
   jobId: string | null;
   /**
    * The job to look {@link jobId} up BY, on an engine where it is not findable
    * on its own. BigQuery runs a labelled read as a script, and a script's child
    * job is absent from the default `bigquery_jobs()` listing — so the child id
    * alone resolves to nothing, and this is the id an operator needs. Null where
    * `jobId` is directly findable, which is every other case.
    */
   parentJobId: string | null;
   /** Bytes the query read. BigQuery `totalBytesProcessed`; Snowflake `BYTES_SCANNED`. */
   bytesScanned: number | null;
   /**
    * Bytes the query is BILLED for, which is not the same number: BigQuery
    * rounds up to a 10MB minimum per query, so a small read bills an order of
    * magnitude above what it scanned, and refreshes are mostly small reads.
    * BigQuery only — Snowflake has no per-query billed quantity at all.
    */
   bytesBilled: number | null;
   /**
    * BigQuery slot-milliseconds: aggregate parallel compute, so it is NOT wall
    * clock and routinely exceeds the query's duration. Null on Snowflake, whose
    * compute is billed per warehouse-second regardless of what runs on it.
    * Deliberately not merged with Snowflake's execution time, which measures
    * something else.
    */
   slotTimeMs: number | null;
   /** Snowflake wall-clock execution ms. Null on BigQuery, where slotTimeMs is the meaningful number. */
   executionTimeMs: number | null;
   /**
    * The warehouse answered from cache, so this read was free. On a BUILD that is
    * a finding rather than a curiosity: the cache is invalidated when the
    * underlying tables change, so a cached build means the source data had not
    * moved and the rebuild did no useful work.
    */
   cacheHit: boolean | null;
}

/**
 * The cost columns to select from a BigQuery job record, for the query that is
 * already locating the read's result table. Sharing that one call is what keeps
 * cost off any separate lookup: a script's child job is not in the default
 * `bigquery_jobs()` listing and can only be reached through its parent, so a
 * later lookup keyed on the child's id finds nothing at all.
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
 * Emitted BARE rather than quoted, and only when it is a plain identifier.
 * Quoting would make it case-exact, while the connection parameter that put the
 * session on this database resolves case-insensitively — so a connection
 * configured `mydb` against a stored `MYDB` would qualify to a database that does
 * not exist. Bare resolution matches how the session got here.
 *
 * A name needing quotes takes the shared-database fallback instead of being
 * interpolated raw. That is a correct answer rather than a degraded one — the
 * fallback is measured to return the same rows — and it keeps a configured value
 * out of identifier position.
 */
function snowflakeDatabaseQualifier(
   database: string | null | undefined,
): string {
   // typeof, not a truthiness check: the API models an unset database as either
   // null or undefined, and an empty string is not a database either.
   return typeof database === "string" && /^[A-Za-z0-9_$]+$/.test(database)
      ? database
      : "SNOWFLAKE";
}

/**
 * The queries this build's session ran, found by the tag the build set on it.
 *
 * `INFORMATION_SCHEMA.QUERY_HISTORY` rather than `ACCOUNT_USAGE.QUERY_HISTORY`:
 * the latter lags by up to three hours and needs elevated grants, so it cannot
 * answer a question asked immediately after a build. The one used here carries no
 * such lag — measured against a live account, a query appears in it immediately.
 *
 * Keyed on the TAG, not on `LAST_QUERY_ID()`. The passthrough's session is not
 * exclusively ours: the ADBC driver issues its own `SELECT 1` connection probes
 * around each call, and measured against a live account `LAST_QUERY_ID()` after a
 * build-shaped read returned a probe, not the read. {@link pickSnowflakeReadRow}
 * then picks the read out of what this returns.
 *
 * <b>`_BY_SESSION`, with an explicit `RESULT_LIMIT`, and both matter.</b> These
 * table functions cap their output BEFORE the `WHERE` runs, so a predicate can
 * only filter rows the cap already admitted — no key, however unique, reaches
 * past it. Measured against a live account: with the read pushed beyond the
 * default 100, the plain account-wide form matched ZERO rows while the same
 * predicate at `RESULT_LIMIT => 10000` matched one. `_BY_SESSION` narrows the
 * candidates to this build's own session, and the explicit limit means that
 * narrowing is what bounds the set rather than a default that happens to be
 * larger than a build — the session form carries the same 100 default, and was
 * also empty in that measurement.
 *
 * The accounting query itself carries the same tag, so it excludes its own shape.
 * Aliases are QUOTED because Snowflake folds a bare alias to uppercase, which
 * would make every field below read undefined while a row still came back.
 *
 * The tag is escaped with {@link sqlLiteral}, not by doubling quotes. It is JSON,
 * so a property value containing a backslash arrives here doubled — and Snowflake
 * consumes one as an escape, leaving a comparison value that cannot equal the tag
 * actually stored. Measured against a live account: quote-doubling here matched
 * ZERO rows for a tag carrying a backslash, and reported no cost rather than an
 * error.
 *
 * <b>A connection may have no database, and then the name has to be qualified.</b>
 * `INFORMATION_SCHEMA` is per-database and an unqualified reference resolves
 * against the session's CURRENT database — which a database-less connection does
 * not have, so the statement fails to compile rather than returning nothing.
 * Measured against a live account: the unqualified form raises `002004 (42601)`
 * on such a connection while `SNOWFLAKE.INFORMATION_SCHEMA` answers normally, and
 * answers identically on a connection that does have one. The `SNOWFLAKE` shared
 * database exists on every account, which is what makes it usable as the fallback
 * qualifier; a role that cannot read it loses the cost and nothing else, since
 * the caller treats a failed lookup as no cost.
 *
 * The connection's own database is preferred when it has one, so the common shape
 * keeps resolving exactly where it did before.
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
        AND EXECUTION_STATUS = 'SUCCESS'
        AND QUERY_TEXT NOT LIKE '%INFORMATION_SCHEMA.QUERY_HISTORY%'`;
}

/**
 * Pick the build's own read out of the queries that carried its tag.
 *
 * Measured against a live account, one build-shaped read left FOUR tagged
 * queries: the read, two driver probes, and the accounting query.
 *
 * The read is identified by comparing text HERE rather than in the statement
 * sent to the warehouse, and that distinction is the point: the compiled SQL is
 * never embedded in a warehouse query, so there is nothing to escape and no
 * dialect whose literal grammar it could break. The tag has already reduced the
 * candidates to this one build's session.
 *
 * Returns null on anything other than exactly one match rather than guessing.
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
      // Inferred, not reported: Snowflake exposes no cache-hit column, and a
      // served-from-cache query is recognisable by having scanned nothing while
      // still producing rows. Left null rather than a guessed `false` when either
      // input is missing, so an absent column never reads as "not cached".
      //
      // It OVER-reports: anything producing rows without reading a table looks
      // identical. Measured against a live account, a GENERATOR returning 50,000
      // rows reports BYTES_SCANNED 0 and reads here as a cache hit. A build's read
      // scans real tables, so the shape barely arises on this path — but this is a
      // hint, not a fact, and nothing should bill against it.
      cacheHit:
         scanned === null || produced === null
            ? null
            : scanned === 0 && produced > 0,
   };
}

/**
 * Read a column case-insensitively. The quoted aliases are what make the keys
 * lowercase; this is a second line of defence, so a transport that ever folds
 * case again produces a wrong-looking number rather than a silent null.
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
   const n = typeof value === "bigint" ? Number(value) : Number(value);
   return Number.isFinite(n) ? n : null;
}

function str(value: unknown): string | null {
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
