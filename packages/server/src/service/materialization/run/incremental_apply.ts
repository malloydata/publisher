// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Generating and applying an incremental DELTA: the bounded slice of a persisted
 * source that a refresh writes into the already-serving table, instead of
 * rebuilding it.
 *
 * The delta is composed SQL, never a compiled Malloy query: the source's own
 * build SQL (the target's `buildSQL()`, the string the seed's CTAS runs)
 * wrapped in a `WHERE` on the watermark. A compiled query's SQL arrives
 * FINALIZED (see {@link deltaSelect}), and a generated source extension
 * silently DROPS its `where:`, which would rewrite every row. Pinned in
 * incremental_compiler_contract.spec.ts.
 *
 * The statements whose text differs by dialect (string literals, the probes'
 * wrapping, the transaction around a delta) are warehouse_sql.ts's.
 *
 * The range is HALF-OPEN, `[start, end)`, which makes a run idempotent: a crash
 * between the commit and the ledger advance costs a repeat, not a corruption.
 */

import { BuildCancelledError } from "../domain/build_cancelled";
import { logger } from "../../../logger";
import { errorCodeOf } from "./error_code";
import type { IncrementalStepReason } from "../../../materialization_metrics";
import type { IncrementalLedgerEntry } from "../../../storage/DatabaseInterface";
import type { IncrementalStrategy } from "../domain/types";
import { capabilitiesOf, dialectsWith } from "../domain/dialect";
import { errMessage } from "../../../utils";
import { foldedTablePathParts, quoteIdentifier } from "../../quoting";
import {
   escapeSqlString,
   inOneTransaction,
   probeSelect,
   targetColumnsProbeSql,
   transactionKeywords,
   watermarkTextSql,
   withStatementTimeout,
} from "./warehouse_sql";
import type { PairDelta } from "../domain/incremental";
import type { LateRows } from "../domain/types";
import {
   windowDeleteBefore,
   type RollingWindow,
} from "../domain/read_time_terms";

/**
 * The incremental refresh's dialect sets, from the dialect table
 * (`domain/dialect.ts`), for message text. The source and target sets differ
 * only for a `storage=` source, whose rows are computed by the SOURCE warehouse
 * and whose DML runs in the DESTINATION engine.
 *
 * Postgres, BigQuery and DuckDB take the range replace as one multi-statement
 * script. Snowflake's driver runs one statement per call, on a possibly
 * different pooled session each time, so it gets a Snowflake Scripting block
 * instead (see {@link deltaStatements}).
 */
export const INCREMENTAL_SOURCE_RANGE_DIALECTS: ReadonlySet<string> =
   dialectsWith("incrementalSourceRange");
export const INCREMENTAL_TARGET_DML_DIALECTS: ReadonlySet<string> =
   dialectsWith("incrementalTargetDml");
export const MERGE_CAPABLE_DIALECTS: ReadonlySet<string> =
   dialectsWith("merge");

/**
 * A watermark boundary, held as CANONICAL SCALAR TEXT plus the watermark's Malloy
 * type: ISO-8601 for a date/timestamp, decimal text for a number, the value
 * itself for a string. Not a rendered literal, because the ledger stores it and
 * compareBounds needs the value.
 */
export interface WatermarkBound {
   malloyType: string;
   value: string;
}

/** Malloy types a bound can be rendered for; the gate rejects everything else. */
const RENDERABLE_TYPES = new Set(["date", "timestamp", "number", "string"]);

export function isRenderableWatermarkType(malloyType: string): boolean {
   return RENDERABLE_TYPES.has(malloyType);
}

/**
 * `YYYY-MM-DD HH:MM:SS` from ISO-8601 text, truncated to whole seconds.
 *
 * Safe because the range is half-open and the ledger records the value actually
 * used: a truncated `end` leaves trailing sub-second rows for the next run. It
 * avoids depending on how each dialect spells fractional seconds.
 */
function secondsPrecision(value: string): string {
   // Anchored at BOTH ends. A zone offset is refused rather than dropped: dropping
   // `+05:30` would relabel a local bound as UTC, and east of UTC that moves
   // `covered_through` AHEAD of the table, skipping rows permanently. A bare `Z`
   // already says UTC, and so does a zero offset (`+00`, `+00:00`, `+0000`),
   // which is how a zoned timestamp's text reads in a UTC session
   // (watermarkTextSql).
   const match = value
      .trim()
      .match(
         /^(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2})(?:\.\d+)?(?:Z|\s?[+-]00(?::?00)?)?$/,
      );
   if (!match) {
      throw new Error(
         `not a UTC ISO-8601 timestamp: ${JSON.stringify(value)} (expected YYYY-MM-DDTHH:MM:SS, with no zone offset)`,
      );
   }
   return `${match[1]} ${match[2]}`;
}

/** `YYYY-MM-DD` from ISO-8601 date or timestamp text. */
function datePrecision(value: string): string {
   const match = value.trim().match(/^(\d{4}-\d{2}-\d{2})/);
   if (!match) {
      throw new Error(
         `not an ISO-8601 date: ${JSON.stringify(value)} (expected YYYY-MM-DD)`,
      );
   }
   return match[1];
}

/**
 * The bound as a SQL literal, spelled for the watermark's own type: on a strict
 * dialect, comparing a `date` column to a timestamp literal is an error.
 *
 * The timestamp spelling is timezone-naive and every bound is UTC text (see
 * canonicalBoundValue). Postgres and BigQuery read it as UTC; Snowflake and
 * DuckDB read it in session time, so their sessions are pinned to UTC (the
 * Snowflake driver does it; for DuckDB see `pinSessionToUTC`). Otherwise the
 * range would silently land on the wrong rows.
 */
export function renderSqlBound(
   bound: WatermarkBound,
   dialect?: string,
): string {
   switch (bound.malloyType) {
      case "date":
         return `DATE '${datePrecision(bound.value)}'`;
      case "timestamp":
         return `TIMESTAMP '${secondsPrecision(bound.value)}'`;
      case "number":
         return renderNumber(bound.value);
      case "string":
         return `'${escapeSqlString(bound.value, dialect)}'`;
      default:
         throw new Error(
            `watermark type '${bound.malloyType}' has no literal rendering`,
         );
   }
}

/** A finite decimal, refused rather than pasted if it is anything else. */
function renderNumber(value: string): string {
   const parsed = Number(value);
   if (!Number.isFinite(parsed)) {
      throw new Error(`not a finite number: ${JSON.stringify(value)}`);
   }
   return String(parsed);
}

/**
 * Canonical scalar text for a watermark value read back from a warehouse (a
 * `MAX(key)` probe) or produced by the run clock. Drivers return a `Date`,
 * number, string or bignum depending on dialect and column type.
 */
export function canonicalBoundValue(
   malloyType: string,
   raw: unknown,
): WatermarkBound {
   if (raw === null || raw === undefined) {
      throw new Error("watermark value is null");
   }
   if (raw instanceof Date) {
      const iso = raw.toISOString();
      return {
         malloyType,
         value: malloyType === "date" ? iso.slice(0, 10) : iso.slice(0, 19),
      };
   }
   const text = String(
      typeof raw === "object" && "value" in (raw as Record<string, unknown>)
         ? // BigQuery hands temporal values back as {value: "…"} wrappers.
           (raw as Record<string, unknown>).value
         : raw,
   );
   switch (malloyType) {
      case "date":
         return { malloyType, value: datePrecision(text) };
      case "timestamp":
         return { malloyType, value: secondsPrecision(text) };
      case "number":
         return { malloyType, value: renderNumber(text) };
      default:
         return { malloyType, value: text };
   }
}

/**
 * The run's snapshot time as a bound, for a time-typed watermark. A date
 * watermark takes the UTC date, a timestamp watermark whole seconds.
 */
export function snapshotBound(malloyType: string, now: Date): WatermarkBound {
   return canonicalBoundValue(malloyType, now);
}

/** Whether a watermark type takes its range end from the run clock. */
function isTimeWatermark(malloyType: string): boolean {
   return malloyType === "date" || malloyType === "timestamp";
}

const MS_PER_DAY = 86_400_000;

/** A date or timestamp bound as epoch milliseconds (UTC). */
function boundMillis(bound: WatermarkBound): number {
   return bound.malloyType === "date"
      ? Date.parse(`${datePrecision(bound.value)}T00:00:00Z`)
      : Date.parse(`${secondsPrecision(bound.value).replace(" ", "T")}Z`);
}

/**
 * A date or timestamp bound moved back by `seconds`. A date moves by whole
 * days, rounded UP, so a lookback shorter than a day still re-reads the
 * boundary's day and the one before it.
 */
export function boundMinusSeconds(
   bound: WatermarkBound,
   seconds: number,
): WatermarkBound {
   const ms =
      bound.malloyType === "date"
         ? Math.ceil(seconds / 86_400) * MS_PER_DAY
         : seconds * 1000;
   return canonicalBoundValue(
      bound.malloyType,
      new Date(boundMillis(bound) - ms),
   );
}

/**
 * Where a delta of a date or timestamp watermark starts reading: the newest
 * watermark the stored table holds at or below the recorded boundary, moved
 * back by `lookback=` when one is declared.
 *
 * Not the recorded boundary itself. That is the previous run's START TIME (a
 * time watermark's range ends at the run clock), and a row stamped before it
 * but landing after it would be below every later range, never written
 * (F-23). The newest stored value is the highest point the table is known to
 * hold; anything stamped at or above it is read again. The range is
 * inclusive below (`>=`), so a row stamped in the very second of the newest
 * stored one is read too: a merge matches it by key, and a range replace
 * deletes the same range before it inserts, so neither writes it twice.
 *
 * Capped at the recorded boundary, which it may lag but never lead (see the
 * build path's invariant below). With nothing stored at or below it (every
 * stored watermark NULL, or past the clock when the table was seeded), the
 * recorded boundary stands.
 */
export function deltaLowerBound(params: {
   recorded: WatermarkBound;
   /** The table's newest watermark at or below `recorded`; undefined for none. */
   stored: WatermarkBound | undefined;
   /** The declared `lookback=`, in seconds; undefined or 0 for none. */
   lookbackSeconds?: number;
}): WatermarkBound {
   const { recorded, stored } = params;
   const base =
      stored !== undefined && compareBounds(stored, recorded) < 0
         ? stored
         : recorded;
   return params.lookbackSeconds
      ? boundMinusSeconds(base, params.lookbackSeconds)
      : base;
}

/**
 * The widest window the late-row check reads below a delta, for a date or
 * timestamp watermark: the larger of the source's `lookback=` and seven days.
 * Without it, a source quiet for months (its newest stored row months old)
 * would have every refresh read months of history.
 */
export const LATE_ROWS_WINDOW_CAP_SECONDS = 7 * 86_400;

/**
 * The start of the window the late-row check reads below a delta's lower
 * bound: `start - (end - start)`, a window as wide as the delta's own range,
 * so the check reads about as much as the delta does. For a date or
 * timestamp watermark the width is capped at the larger of `lookbackSeconds`
 * and {@link LATE_ROWS_WINDOW_CAP_SECONDS}; a number watermark's range holds
 * new rows only, so it is not. Undefined for a string watermark, which has
 * no width.
 */
export function lateRowsWindowStart(
   start: WatermarkBound,
   end: WatermarkBound,
   lookbackSeconds = 0,
): WatermarkBound | undefined {
   if (start.malloyType === "number") {
      const width = Number(end.value) - Number(start.value);
      return width > 0
         ? canonicalBoundValue("number", Number(start.value) - width)
         : undefined;
   }
   if (!isTimeWatermark(start.malloyType)) return undefined;
   const cap = Math.max(lookbackSeconds, LATE_ROWS_WINDOW_CAP_SECONDS) * 1000;
   const width = Math.min(boundMillis(end) - boundMillis(start), cap);
   if (!(width > 0)) return undefined;
   return canonicalBoundValue(
      start.malloyType,
      new Date(boundMillis(start) - width),
   );
}

/**
 * The delta's SELECT for one source and range: the source's OWN build SQL
 * (the target's `buildSQL()`, the string the seed's CTAS runs), wrapped and
 * filtered on the watermark, so the delta cannot disagree with the seed about
 * what the source computes.
 *
 * Not a compiled Malloy query, because a query's SQL is FINALIZED: on Postgres
 * (the only dialect with `hasFinalStage`) it becomes
 * `SELECT row_to_json(finalStage) as row FROM …`, one JSON column, and an
 * `INSERT … SELECT "col"` off it fails. The build SQL is unfinalized, and
 * there is no supported way to ask a query for that. Pinned as the TRIPWIRE in
 * incremental_compiler_contract.spec.ts; see docs/materialization.md,
 * "Materializing on Postgres".
 *
 * `SELECT *` because the DML projects by name (see {@link deltaStatements}).
 *
 * `end` undefined leaves the range open above: a pair table's delta (see
 * {@link pairDeltaStatements}), which replaces whole parents by key and so
 * needs no upper bound to agree with its delete.
 */
export function deltaSelect(params: {
   dialect: string;
   /** The target's `buildSQL(manifest)`, resolved against the build's manifest. */
   sourceSQL: string;
   watermarkName: string;
   start: WatermarkBound;
   end: WatermarkBound | undefined;
}): string {
   const watermark = quoteIdentifier(params.watermarkName, params.dialect);
   return (
      `SELECT * FROM (${params.sourceSQL}) AS __d ` +
      `WHERE ${watermark} >= ${renderSqlBound(params.start, params.dialect)}` +
      (params.end
         ? ` AND ${watermark} < ${renderSqlBound(params.end, params.dialect)}`
         : "")
   );
}

// ─────────────────────────────────────────────────────────────────────────────
// The build path: deciding between a SEED and a DELTA, then applying one.
//
// The delta path runs only on facts it has PROVEN about the target table; every
// other outcome falls back to a SEED (a full rebuild), which is always correct and
// merely expensive. Author errors are rejected earlier, at publish
// (incremental_policy.ts).
//
// `covered_through` may LAG what the table contains but must never LEAD it. A
// lagging boundary recomputes an already-correct range (a no-op for both range
// replace and merge); a leading one would skip rows forever.
// ─────────────────────────────────────────────────────────────────────────────

/** How a probe or DML statement reaches the warehouse. */
export type SqlRunner = (
   sql: string,
   options?: {
      /** A row limit past the connection's default (as few as ten rows). */
      rowLimit?: number;
      /** The run's cancel signal, for the delta's own statement only. */
      abortSignal?: AbortSignal;
   },
) => Promise<{ rows: unknown[] }>;

/**
 * The dialects whose target table's columns the incremental refresh probes
 * by decoded path: Postgres, Snowflake, BigQuery and DuckDB (a `storage=`
 * destination's `<destination>.[<schema>.]<name>`).
 */
const DECODED_PATH_DIALECTS = new Set([
   "postgres",
   "snowflake",
   "standardsql",
   "duckdb",
]);

/**
 * The raw identifier segments of a physical table path, as its catalog stores
 * them, or undefined when the path cannot be decoded for this dialect (which
 * leads to a SEED, not a throw). The physical name arrives as SQL (bare or
 * quoted segments) while `information_schema` compares RAW identifier text,
 * so it is decoded by the one parser of a table name
 * ({@link foldedTablePathParts}): a bare segment folded as the engine folds
 * it (Postgres to lower case, Snowflake to UPPER, ASCII only; BigQuery and
 * DuckDB not at all), a quoted one kept.
 */
export function decodeTablePathSegments(
   dialect: string,
   tablePath: string,
): string[] | undefined {
   if (!DECODED_PATH_DIALECTS.has(dialect)) return undefined;
   return foldedTablePathParts(tablePath, dialect);
}

/** A probe's rows as plain objects (see {@link probeSelect}). */
function probeRows(rows: unknown[]): Record<string, unknown>[] {
   return rows.filter(
      (r): r is Record<string, unknown> => typeof r === "object" && r !== null,
   );
}

/**
 * A probe's output alias, dialect-quoted so the key read back is stable:
 * Snowflake folds a bare alias to UPPERCASE.
 */
function probeAlias(name: string, dialect: string): string {
   return quoteIdentifier(name, dialect);
}

/**
 * The target table's column names in ordinal order, or a reason it could not be
 * read. An absent table reports `columns: []` with no error, which is the SEED
 * signal.
 */
export async function probeTargetColumns(
   runner: SqlRunner,
   dialect: string,
   physicalTableName: string,
): Promise<{ columns: string[]; error?: string }> {
   const segments = decodeTablePathSegments(dialect, physicalTableName);
   if (!segments || segments.length === 0) {
      return {
         columns: [],
         error: `physical table name ${JSON.stringify(physicalTableName)} is not a decodable ${dialect} table path`,
      };
   }
   // Undefined: an unqualified BigQuery name has no dataset to look in.
   const sql = targetColumnsProbeSql(dialect, segments);
   if (sql === undefined) {
      return {
         columns: [],
         error:
            `physical table name ${JSON.stringify(physicalTableName)} is not ` +
            `dataset-qualified, so its schema cannot be read from ` +
            `INFORMATION_SCHEMA`,
      };
   }
   try {
      const result = await runner(sql);
      const columns = probeRows(result.rows)
         .map((r) => r["column_name"])
         .filter((n): n is string => typeof n === "string" && n.length > 0);
      return { columns };
   } catch (err) {
      return { columns: [], error: errMessage(err) };
   }
}

/** Whether the target holds at least one row (`LIMIT 1`, not `count(*)`). */
async function probeTargetNonEmpty(
   runner: SqlRunner,
   dialect: string,
   quotedTablePath: string,
): Promise<{ nonEmpty: boolean; error?: string }> {
   const sql = probeSelect(
      dialect,
      `SELECT 1 AS present FROM ${quotedTablePath} LIMIT 1`,
   );
   try {
      const result = await runner(sql);
      return { nonEmpty: probeRows(result.rows).length > 0 };
   } catch (err) {
      return { nonEmpty: false, error: errMessage(err) };
   }
}

/**
 * `MAX(watermark)` over a SELECT, as a canonical bound. Undefined (not an error)
 * when the result is NULL, i.e. the input is empty.
 */
export async function probeMaxWatermark(
   runner: SqlRunner,
   dialect: string,
   innerSelect: string,
   watermarkName: string,
   malloyType: string,
): Promise<{ bound?: WatermarkBound; error?: string; errorCode?: string }> {
   const column = quoteIdentifier(watermarkName, dialect);
   const sql = probeSelect(
      dialect,
      `SELECT MAX(${column}) AS ${probeAlias("watermark_max", dialect)} ` +
         `FROM (${innerSelect}) AS __w`,
   );
   try {
      const result = await runner(sql);
      const raw = probeRows(result.rows)[0]?.["watermark_max"];
      if (raw === null || raw === undefined) return {};
      return { bound: canonicalBoundValue(malloyType, raw) };
   } catch (err) {
      // The message can quote the value read (a watermark the parser
      // refused), so only the code may be logged.
      return { error: errMessage(err), errorCode: errorCodeOf(err) };
   }
}

/**
 * The newest date or timestamp watermark the stored table holds at or below
 * `atOrBelow` (the recorded boundary), for {@link deltaLowerBound}: one
 * aggregate over one column of the table. Undefined (not an error) when none
 * is: every stored watermark NULL, or past the boundary.
 *
 * Read as text ({@link watermarkTextSql}), never as the driver's value, so a
 * zone-naive timestamp cannot be read in the process's zone.
 */
export async function probeStoredWatermark(params: {
   runner: SqlRunner;
   dialect: string;
   quotedTablePath: string;
   /** The column the table holds the watermark under. */
   column: string;
   atOrBelow: WatermarkBound;
}): Promise<{
   bound?: WatermarkBound;
   error?: string;
   errorCode?: string;
   /** The value was read but is not a UTC ISO-8601 date or timestamp. */
   unparseable?: boolean;
}> {
   const { dialect, atOrBelow } = params;
   const type = atOrBelow.malloyType;
   if (type !== "date" && type !== "timestamp") {
      return { error: `a ${type} watermark has no stored boundary to read` };
   }
   const column = quoteIdentifier(params.column, dialect);
   const sql = probeSelect(
      dialect,
      `SELECT ${watermarkTextSql(dialect, type, `MAX(${column})`)} AS ` +
         `${probeAlias("watermark_max", dialect)} ` +
         `FROM ${params.quotedTablePath} ` +
         `WHERE ${column} <= ${renderSqlBound(atOrBelow, dialect)}`,
   );
   let raw: unknown;
   try {
      const result = await params.runner(sql);
      raw = probeRows(result.rows)[0]?.["watermark_max"];
   } catch (err) {
      return { error: errMessage(err), errorCode: errorCodeOf(err) };
   }
   if (raw === null || raw === undefined) return {};
   try {
      return { bound: canonicalBoundValue(type, raw) };
   } catch (err) {
      // The message quotes the value read, so only the code may be logged.
      return {
         error: errMessage(err),
         errorCode: errorCodeOf(err),
         unparseable: true,
      };
   }
}

/**
 * Postgres's `server_version_num`, for the MERGE floor: MERGE is a syntax error
 * before Postgres 15.
 */
export async function probePostgresVersion(
   runner: SqlRunner,
): Promise<number | undefined> {
   try {
      const result = await runner(
         probeSelect(
            "postgres",
            `SELECT current_setting('server_version_num') AS version_num`,
         ),
      );
      const raw = probeRows(result.rows)[0]?.["version_num"];
      const parsed = Number(raw);
      return Number.isFinite(parsed) ? parsed : undefined;
   } catch {
      return undefined;
   }
}

/** The first Postgres release with `MERGE`. */
const POSTGRES_MERGE_MIN_VERSION_NUM = 150000;

/**
 * Compare two bounds of the same type, as a comparator. Numbers compare
 * numerically; everything else as text, which is correct for fixed-width ISO-8601
 * dates and timestamps.
 */
export function compareBounds(a: WatermarkBound, b: WatermarkBound): number {
   if (a.malloyType !== b.malloyType) {
      throw new Error(
         `cannot compare a ${a.malloyType} bound to a ${b.malloyType} bound`,
      );
   }
   if (a.malloyType === "number") return Number(a.value) - Number(b.value);
   // A timestamp read back as text is spelled `YYYY-MM-DD HH:MM:SS`, one from
   // the run clock `YYYY-MM-DDTHH:MM:SS`: one spelling, so they compare.
   const text = (bound: WatermarkBound) =>
      bound.malloyType === "timestamp"
         ? bound.value.replace("T", " ")
         : bound.value;
   const [x, y] = [text(a), text(b)];
   return x < y ? -1 : x > y ? 1 : 0;
}

/**
 * The set difference between the delta's output columns and the target table's,
 * as message text, or undefined when they are the same set (order is ignored;
 * the DML names every column).
 *
 * Membership can legitimately differ: the DML names the source's compiled
 * schema, while the build SQL (and so the seed's table) also projects columns
 * the source renames or hides (`rename:`, `except:`, non-public access). Pinned
 * in incremental_compiler_contract.spec.ts. Such sources seed, because naming a
 * subset would rewrite the unnamed columns to NULL.
 */
export function shapeMismatch(
   deltaColumns: string[],
   targetColumns: string[],
): string | undefined {
   const target = new Set(targetColumns);
   const delta = new Set(deltaColumns);
   const missing = deltaColumns.filter((c) => !target.has(c));
   const extra = targetColumns.filter((c) => !delta.has(c));
   if (missing.length === 0 && extra.length === 0) return undefined;
   const parts: string[] = [];
   if (missing.length > 0) {
      parts.push(
         `absent from the table: ${missing.map((c) => `"${c}"`).join(", ")}`,
      );
   }
   if (extra.length > 0) {
      parts.push(
         `present only in the table: ${extra.map((c) => `"${c}"`).join(", ")}`,
      );
   }
   return parts.join("; ");
}

/**
 * The statements that apply one delta, in the order they must run.
 *
 *  - MERGE (merge_key= declared): match on the identity columns, update matches,
 *    insert the rest. Tolerates a row RESTATED with a new watermark value.
 *
 *  - RANGE REPLACE (no merge_key=): delete the watermark range, then insert.
 *    Correct when a row's watermark never changes.
 *
 * Both are idempotent for a fixed range, so a crash between the DML and the
 * ledger advance is survivable.
 *
 * The range replace must be one transaction, or a failure between DELETE and
 * INSERT leaves the range empty in a serving table. Postgres, BigQuery and DuckDB
 * get a BEGIN…COMMIT script in one call; Snowflake (one statement per call, each
 * possibly on a different pooled session) gets a Snowflake Scripting block.
 */
export function deltaStatements(params: {
   dialect: string;
   quotedTablePath: string;
   deltaSQL: string;
   /** Columns to write, as the delta's own output names. */
   columns: string[];
   mergeKeys: string[];
   /**
    * Columns the stripped dynamic terms constrain, added to the merge's match.
    * Empty for every source that is not caller-scoped.
    */
   scopeColumns?: string[];
   watermarkName: string;
   start: WatermarkBound;
   end: WatermarkBound;
   /**
    * Rolling-window deletes, run after the write in the same transaction:
    * each removes the rows of `column` below `before`, which no read from
    * this refresh on needs (rolling_window.ts).
    */
   windowDeletes?: readonly { column: string; before: WatermarkBound }[];
   /**
    * A pair table's parent key and parent-row flag: the delta replaces whole
    * parents by the key instead of merging rows ({@link pairDeltaStatements}).
    */
   pair?: PairDelta & { stagingTable: string };
}): string[] {
   if (params.pair) {
      return pairDeltaStatements({
         ...params,
         pair: params.pair,
      });
   }
   const {
      dialect,
      quotedTablePath,
      deltaSQL,
      columns,
      mergeKeys,
      watermarkName,
   } = params;
   const q = (name: string) => quoteIdentifier(name, dialect);
   const columnList = columns.map(q).join(", ");
   const windowDeletes = (params.windowDeletes ?? []).map(
      (d) =>
         `DELETE FROM ${quotedTablePath} WHERE ${q(d.column)} < ` +
         renderSqlBound(d.before, dialect),
   );

   if (mergeKeys.length > 0) {
      // `merge_key=` was chosen against the source AS WRITTEN, filtered to one
      // caller. The stored table is unfiltered, so a key value occurs once per
      // caller, and matching on the key alone would UPDATE other callers' rows
      // (Postgres raises "MERGE command cannot affect row a second time"; DuckDB
      // silently corrupts both). Adding the scope columns to the match restores
      // the author's relation.
      //
      // Scoping is all-or-nothing: a term contributing no column is refused
      // upstream rather than arriving here partially scoped.
      const matchNames = [
         ...mergeKeys,
         ...(params.scopeColumns ?? []).filter((c) => !mergeKeys.includes(c)),
      ];
      const keys = new Set(mergeKeys);
      // NULL-safe: with plain `=`, a NULL identity column never matches its own
      // copy, so the row would be re-inserted every run as a silent duplicate.
      const on = matchNames
         .map(
            (k) =>
               `(__t.${q(k)} = __s.${q(k)} OR (__t.${q(k)} IS NULL AND __s.${q(k)} IS NULL))`,
         )
         .join(" AND ");
      const updates = columns
         .filter((c) => !keys.has(c))
         .map((c) => `${q(c)} = __s.${q(c)}`);
      const clauses = [
         `MERGE INTO ${quotedTablePath} AS __t`,
         `USING (${deltaSQL}) AS __s`,
         `ON ${on}`,
      ];
      // When every column is an identity column there is nothing to SET, and an
      // empty SET list is a syntax error; MERGE accepts NOT MATCHED alone.
      if (updates.length > 0) {
         clauses.push(`WHEN MATCHED THEN UPDATE SET ${updates.join(", ")}`);
      }
      clauses.push(
         `WHEN NOT MATCHED THEN INSERT (${columnList}) ` +
            `VALUES (${columns.map((c) => `__s.${q(c)}`).join(", ")})`,
      );
      if (windowDeletes.length === 0) return [clauses.join("\n")];
      // The merge and the window's delete commit together, so a reader never
      // sees the delete without the rows that replaced the window's edge.
      return inOneTransaction(dialect, [clauses.join("\n"), ...windowDeletes]);
   }

   const watermark = q(watermarkName);
   const range =
      `${watermark} >= ${renderSqlBound(params.start, dialect)} AND ` +
      `${watermark} < ${renderSqlBound(params.end, dialect)}`;
   const body = [
      `DELETE FROM ${quotedTablePath} WHERE ${range}`,
      `INSERT INTO ${quotedTablePath} (${columnList}) ` +
         `SELECT ${columnList} FROM (${deltaSQL}) AS __s`,
      ...windowDeletes,
   ];
   return inOneTransaction(dialect, body);
}

/** The text a pair delta's check fails with; {@link isPairGuardFailure} reads it. */
export const PAIR_GUARD_MESSAGE =
   "pair table parent-row check: a parent does not have exactly one parent-row flag";

/** Whether an error is a pair delta's check failing (and the delta rolled back). */
export function isPairGuardFailure(err: unknown): boolean {
   return errMessage(err).includes(PAIR_GUARD_MESSAGE);
}

/**
 * A pair table's delta (an inlined join_many, `#(materialize)`): replace every
 * row of each parent the delta holds, by the parent key, so the parent-row
 * flag stays exactly one row per parent.
 *
 * The delta holds whole parents: its watermark is one value per parent (the
 * greatest of the parent's and its children's watermark fields), so a range
 * over it selects all of a parent's rows or none, and the flag the build's
 * window writes over them is the full build's. The range is open above (see
 * planIncrementalStep): a parent whose watermark is past the clock, held there
 * by one child stamped in the future, is read again on every refresh rather
 * than never. A row-by-row MERGE cannot
 * apply it: one key matches many rows, and a child that left its parent would
 * stay. So the delta is staged once (one read of the source, so the delete
 * and the insert see one snapshot), then, in one transaction, every stored
 * row of each staged parent is deleted (NULL-safe, as a full build partitions
 * the NULL key as one parent), the staged rows are inserted, the rolling
 * windows' deletes run, and a check fails the transaction unless every parent
 * left in the table has exactly one flagged row. A failed check rolls back
 * and the source is rebuilt in full (see applyIncrementalStep).
 *
 * DuckDB only: the target of every `storage=` table, and a database pair
 * table's own warehouse when that is DuckDB (a database one elsewhere has no
 * pair lineage, see incrementalLineage, and is rebuilt in full). Its `error()`
 * raises the check; the staging table is a session temporary created before
 * the transaction, so the transaction writes one database. Another target
 * dialect has no lineage (see incrementalLineage), so it is never reached.
 */
export function pairDeltaStatements(params: {
   dialect: string;
   quotedTablePath: string;
   deltaSQL: string;
   columns: string[];
   pair: PairDelta & { stagingTable: string };
   windowDeletes?: readonly { column: string; before: WatermarkBound }[];
}): string[] {
   const { dialect, quotedTablePath, pair } = params;
   if (!capabilitiesOf(dialect).pairDelta) {
      throw new Error(
         `a pair table's delta is applied in DuckDB only, not ${dialect}`,
      );
   }
   const q = (name: string) => quoteIdentifier(name, dialect);
   const staged = q(pair.stagingTable);
   const key = q(pair.parentKey);
   const columnList = params.columns.map(q).join(", ");
   const transaction = transactionKeywords(dialect);
   return [
      `CREATE OR REPLACE TEMP TABLE ${staged} AS ${params.deltaSQL}`,
      transaction.begin,
      `DELETE FROM ${quotedTablePath} AS __t WHERE EXISTS (` +
         `SELECT 1 FROM ${staged} AS __s WHERE ` +
         `(__t.${key} = __s.${key} OR (__t.${key} IS NULL AND __s.${key} IS NULL)))`,
      `INSERT INTO ${quotedTablePath} (${columnList}) ` +
         `SELECT ${columnList} FROM ${staged}`,
      ...(params.windowDeletes ?? []).map(
         (d) =>
            `DELETE FROM ${quotedTablePath} WHERE ${q(d.column)} < ` +
            renderSqlBound(d.before, dialect),
      ),
      // The uniqueness check over the whole table, two columns read: a parent
      // with no flag or two would be counted wrongly by every parent measure.
      // A path's level flags are checked the same way, each over the keys
      // down to its level.
      ...(
         pair.checks ?? [{ flag: pair.parentRow, keys: [pair.parentKey] }]
      ).map(({ flag, keys, lane }) => {
         const within =
            lane !== undefined && pair.laneColumn !== undefined
               ? ` WHERE ${q(pair.laneColumn)} = ${Number(lane)}`
               : "";
         return (
            `SELECT error('${PAIR_GUARD_MESSAGE}') FROM (` +
            `SELECT ${keys.map(q).join(", ")} FROM ${quotedTablePath}${within} ` +
            `GROUP BY ${keys.map(q).join(", ")} ` +
            `HAVING count(*) FILTER (WHERE ${q(flag)}) <> 1) AS __g LIMIT 1`
         );
      }),
      // A table of several blocks: every parent keeps its rows in each.
      ...(pair.laneColumn !== undefined && pair.lanes !== undefined
         ? [
              `SELECT error('${PAIR_GUARD_MESSAGE}') FROM (` +
                 `SELECT ${key} FROM ${quotedTablePath} GROUP BY ${key} ` +
                 `HAVING count(DISTINCT ${q(pair.laneColumn)}) <> ${Number(pair.lanes)}) AS __g LIMIT 1`,
           ]
         : []),
      transaction.commit,
      `DROP TABLE IF EXISTS ${staged}`,
   ];
}

/**
 * The delta's statements as ONE driver call. A statement-per-call loop could put
 * BEGIN and COMMIT on different pooled sessions, silently autocommitting each
 * write. (Postgres runs it via the simple query protocol; BigQuery as a
 * multi-statement script.)
 */
export function deltaScript(statements: string[]): string {
   return statements.map((s) => `${s};`).join("\n");
}

/**
 * Run a delta's statements, rolling back the session if they fail.
 *
 * Required, not belt-and-braces: a failed script leaves the SESSION in an aborted
 * transaction, and neither Postgres nor node-postgres's pool rolls back on
 * release, so every later borrower of that connection would fail. Proven in
 * incremental_dml_semantics.spec.ts. DuckDB needs it too once `BEGIN` has run; a
 * bind-time failure opens no transaction, and the rollback is harmless there.
 *
 * Rollback failures are swallowed: the caller needs the ORIGINAL error.
 */
export async function applyDeltaScript(
   runner: SqlRunner,
   dialect: string,
   statements: string[],
   /**
    * The run's cancel signal: given to the delta's script, which reads the
    * warehouse inside a transaction, never to its rollback. A cancelled
    * script fails like any other and is rolled back here. Snowflake's
    * single-statement block has no separate rollback, so its caller passes
    * none there (incremental_delta_strategy.ts).
    */
   abortSignal?: AbortSignal,
): Promise<void> {
   try {
      if (abortSignal?.aborted) throw new BuildCancelledError();
      await runner(
         deltaScript(statements),
         abortSignal ? { abortSignal } : undefined,
      );
   } catch (err) {
      if (statements.length > 1) {
         try {
            await runner(transactionKeywords(dialect).rollback);
         } catch (rollbackErr) {
            logger.warn(
               "Could not roll back a failed delta; a pooled connection may " +
                  "stay in an aborted transaction",
               { dialect, error: errMessage(rollbackErr) },
            );
         }
      }
      throw err;
   }
}

/**
 * Everything the ledger has to agree with before a delta may run: which table,
 * on which connection, computed from which SQL, under which declaration. A change
 * to any of them means the recorded boundary cannot be built on.
 */
export interface IncrementalLineage {
   /**
    * Logical (unquoted) physical table name, as the manifest records it. With
    * `connectionName` and `storageDestinationName`, the table this boundary
    * belongs to.
    */
   physicalTableName: string;
   connectionName: string;
   /**
    * The storage destination the table lives in, absent when it lives in
    * `connectionName`'s own warehouse. Compared, because adding or removing
    * `storage=` changes neither the physical name nor the content address.
    */
   storageDestinationName?: string;
   /**
    * The source's CONTENT address (a fingerprint of the build SQL, not the
    * caller-assigned `BuildInstruction.sourceEntityId`). Compared, not keyed on,
    * so an edited source seeds rather than deltaing onto a table its definition no
    * longer describes; a table's name need not change when its definition does.
    */
   sourceEntityId: string;
   watermarkName: string;
   /** The watermark's Malloy type (`date`, `timestamp`, `number`, `string`). */
   watermarkType: string;
   mergeKeys: string[];
   /**
    * Columns the source's stripped dynamic terms constrain. Added to a
    * `merge_key=` match so it cannot reach rows outside the caller's scope —
    * see {@link deltaStatements}.
    */
   scopeColumns: string[];
   strategy: IncrementalStrategy;
   /**
    * A `#(materialize)` source's rolling-window terms: each delta also deletes
    * the rows below every term's new lower bound ({@link windowDeleteBefore}).
    * Not compared with the ledger: the terms are in the build SQL, so a change
    * to one already changes {@link sourceEntityId}.
    */
   rollingWindow?: readonly RollingWindow[];
   /**
    * The declared `lookback=`, in seconds: a date or timestamp delta starts
    * this far below the newest stored watermark ({@link deltaLowerBound}),
    * and the merge key matches the rows read again. Only with `merge_key=`
    * (the publish gate). Not compared with the ledger: it moves where a
    * delta starts, never what a recorded boundary means.
    */
   lookbackSeconds?: number;
   /**
    * A pair table's parent key and parent-row flag column
    * ({@link pairDeltaStatements}): a delta replaces every row of each parent
    * it holds, by the key, and checks that each parent keeps exactly one
    * flagged row. Recorded on the ledger as a merge on the key (`mergeKeys`
    * is `[parentKey]`); not compared on its own, since the pair's build SQL
    * is in {@link sourceEntityId}.
    */
   pair?: PairDelta;
}

/**
 * A recorded boundary as the plan reads it: the value, plus the table and the
 * declaration it was established under. Both a local ledger row and a
 * caller-returned wire `LedgerEntry` fit this shape and go through the same
 * checks.
 */
export type RecordedBoundary = Pick<
   IncrementalLedgerEntry,
   | "sourceEntityId"
   | "coveredThroughValue"
   | "coveredThroughType"
   | "watermarkDimension"
   | "mergeKeyDimensions"
   | "derivedStrategy"
   | "physicalTableName"
   | "connectionName"
   | "storageDestinationName"
>;

/**
 * Why a recorded boundary cannot be built on, or undefined when it can. Each
 * mismatch means `covered_through` is a claim about something other than the
 * delta about to run, so the only sound answer is to re-seed.
 */
export function ledgerLineageMismatch(
   entry: RecordedBoundary,
   lineage: IncrementalLineage,
): { reasonCode: IncrementalStepReasonCode; reason: string } | undefined {
   // `table_renamed`, not `lineage_changed`: the boundary was measured on a
   // different TABLE, possibly under an unchanged definition. Either a host
   // assigns a generational physical name per run, or two sources share one
   // content address and fight over one ledger row (the publish gate warns, see
   // sharedAddressAdvisories in incremental_policy.ts). The reason names both,
   // since an operator cannot tell them apart. `lineage_changed` instead means
   // the source's DEFINITION moved (an in-place edit, or a refresh through a
   // different package version).
   if (entry.physicalTableName !== lineage.physicalTableName) {
      return {
         reasonCode: "table_renamed",
         reason:
            `the recorded boundary belongs to table ` +
            `"${entry.physicalTableName}", not "${lineage.physicalTableName}" — ` +
            `either this table was renamed between runs, or another source ` +
            `shares this source's content address and advanced the boundary ` +
            `against its own table`,
      };
   }
   // A different STORE is a different table, so this is also `table_renamed`.
   // Gaining or losing `storage=` keeps both name and content address, so
   // nothing else here can tell.
   if (
      (entry.storageDestinationName ?? undefined) !==
      (lineage.storageDestinationName ?? undefined)
   ) {
      return {
         reasonCode: "table_renamed",
         reason:
            `the recorded boundary belongs to a table in ` +
            `${entry.storageDestinationName ? `storage destination "${entry.storageDestinationName}"` : "the source warehouse"}` +
            `, while this refresh writes to ` +
            `${lineage.storageDestinationName ? `storage destination "${lineage.storageDestinationName}"` : "the source warehouse"}`,
      };
   }
   const changed = (reason: string) =>
      ({ reasonCode: "lineage_changed", reason }) as const;
   // A different content address means different build SQL. The declaration
   // checks below are still needed: moving `watermark=` or `merge_key=` does NOT
   // move the address (proven in incremental_compiler_contract.spec.ts).
   if (entry.sourceEntityId !== lineage.sourceEntityId) {
      return changed(
         `the source's content address is not the one the recorded boundary was ` +
            `measured under, so that boundary describes different SQL than this ` +
            `refresh would apply a delta to — either the model was edited, or ` +
            `this refresh is instructed through a different version of the ` +
            `package than the one that advanced the boundary`,
      );
   }
   if (entry.connectionName !== lineage.connectionName) {
      return changed(
         `the recorded boundary was advanced on connection ` +
            `"${entry.connectionName}", not "${lineage.connectionName}"`,
      );
   }
   if (entry.watermarkDimension !== lineage.watermarkName) {
      return changed(
         `the watermark changed from "${entry.watermarkDimension}" to ` +
            `"${lineage.watermarkName}", so the recorded value measures a ` +
            `different column`,
      );
   }
   if (entry.coveredThroughType !== lineage.watermarkType) {
      return changed(
         `the watermark's type changed from "${entry.coveredThroughType}" to ` +
            `"${lineage.watermarkType}"`,
      );
   }
   if (entry.derivedStrategy !== lineage.strategy) {
      return changed(
         `the strategy changed from ${entry.derivedStrategy} to ` +
            `${lineage.strategy}`,
      );
   }
   // Order-sensitive. A NARROWED identity (dropped key) must rebuild, since rows
   // the old key set kept apart would now collide.
   const recorded = entry.mergeKeyDimensions;
   if (
      recorded.length !== lineage.mergeKeys.length ||
      recorded.some((k, i) => k !== lineage.mergeKeys[i])
   ) {
      return changed(
         `merge_key= changed from [${recorded.join(", ")}] to ` +
            `[${lineage.mergeKeys.join(", ")}]`,
      );
   }
   return undefined;
}

/**
 * Why a refresh seeded or skipped, as a BOUNDED code for the step metric's
 * `reason` label — the free-text `reason` names specifics (columns, versions,
 * boundary values) that a metric label cannot carry.
 */
type IncrementalStepReasonCode = IncrementalStepReason;

/** What one source's refresh should do. */
export type IncrementalStep =
   | {
        mode: "seed";
        reasonCode: IncrementalStepReasonCode;
        /** Why a full rebuild, for the log. */
        reason: string;
     }
   | {
        mode: "skip";
        reasonCode: IncrementalStepReasonCode;
        reason: string;
        /**
         * The recorded boundary, still in force because nothing was applied;
         * reported on the manifest entry like an advanced one.
         */
        coveredThrough?: WatermarkBound;
     }
   | {
        mode: "delta";
        start: WatermarkBound;
        end: WatermarkBound;
        /** The boundary to record once the DML commits (always `end`). */
        coveredThrough: WatermarkBound;
        statements: string[];
     };

/**
 * The upper bound of a delta's range. A DATE or TIMESTAMP watermark uses the
 * run's snapshot time, for free. A NUMBER or STRING watermark has no clock, so
 * its frontier is read from the SOURCE (the target stops at the last boundary),
 * costing one extra pass over the source query per refresh.
 */
async function resolveDeltaEnd(
   target: DeltaTarget,
   lineage: IncrementalLineage,
   now: Date,
): Promise<{ bound?: WatermarkBound; error?: string }> {
   const type = lineage.watermarkType;
   if (type === "date" || type === "timestamp") {
      return { bound: snapshotBound(type, now) };
   }
   return target.probeSourceFrontier();
}

/**
 * The lower bound of a delta's range. A DATE or TIMESTAMP watermark starts at
 * the newest watermark the table holds, less `lookback=`
 * ({@link deltaLowerBound}), not at the recorded boundary, which is the
 * previous run's clock: one aggregate over the table's watermark column. A
 * NUMBER or STRING boundary is already the source's own frontier, read from
 * its rows, so it is where the delta starts.
 */
async function resolveDeltaStart(
   target: DeltaTarget,
   lineage: IncrementalLineage,
   recorded: WatermarkBound,
): Promise<{ bound: WatermarkBound; error?: undefined } | { error: string }> {
   if (!isTimeWatermark(recorded.malloyType)) return { bound: recorded };
   const stored = await probeStoredWatermark({
      runner: target.runner,
      dialect: target.dialect,
      quotedTablePath: target.quotedTablePath,
      column:
         target.storedColumn?.(lineage.watermarkName) ?? lineage.watermarkName,
      atOrBelow: recorded,
   });
   if (stored.error !== undefined) {
      if (stored.unparseable) {
         // Otherwise this source rebuilds in full on every refresh with no
         // sign but the seed's reason code.
         logger.warn(
            "The newest stored watermark is not a UTC ISO-8601 date or " +
               "timestamp, so this incremental source is rebuilt in full on " +
               "every refresh. Check that the connection's setupSQL does not " +
               "set a session time zone other than UTC",
            {
               physicalTableName: lineage.physicalTableName,
               watermark: lineage.watermarkName,
               errorCode: stored.errorCode,
            },
         );
      }
      return { error: stored.error };
   }
   return {
      bound: deltaLowerBound({
         recorded,
         stored: stored.bound,
         lookbackSeconds: lineage.lookbackSeconds,
      }),
   };
}

/**
 * Where a delta's DML lands, and how its rows are produced: the only facts that
 * differ between a database table and a `storage=` one (rows computed by the
 * SOURCE warehouse, DML issued in the DESTINATION engine).
 *
 * The decision order and reason codes stay in {@link planIncrementalStep} alone,
 * so there is one place for them and they cannot drift per target.
 */
export interface DeltaTarget {
   /** The dialect the DML is written in, and the probes are quoted for. */
   dialect: string;
   /** The connection holding the table: every probe and the DML run here. */
   runner: SqlRunner;
   /** The run's cancel signal, given to the delta's script alone. */
   abortSignal?: AbortSignal;
   /** The table as the CREATE quoted it, for the DML and the emptiness probe. */
   quotedTablePath: string;
   /**
    * The same table as a LOGICAL (unquoted) path, for the column metadata read.
    * Not taken from the lineage, whose `physicalTableName` omits a stored table's
    * destination catalog.
    */
   logicalTablePath: string;
   /**
    * The rows-yielding SELECT for one bounded range, executable on the TARGET
    * (`end` undefined: open above, see {@link deltaSelect}). Async because
    * producing it can issue the source read (a stored table's rows arrive
    * through a query passthrough).
    */
   deltaRows(
      start: WatermarkBound,
      end: WatermarkBound | undefined,
   ): Promise<string>;
   /**
    * The SOURCE's rows in `[start, end)`, only `columns` of them (stored
    * names), as a SELECT executable on the TARGET: what the late-row check
    * ({@link countLateRows}) compares the stored table with. Its read is not
    * the delta's, so it is not counted in the delta's read cost. Absent: the
    * check is not run.
    */
   windowRows?(
      start: WatermarkBound,
      end: WatermarkBound,
      columns: string[],
   ): Promise<string>;
   /**
    * The SOURCE's watermark frontier, for a number or string watermark (see
    * resolveDeltaEnd).
    */
   probeSourceFrontier(): Promise<{ bound?: WatermarkBound; error?: string }>;
   /**
    * The column the table and the source's SQL hold a field under, where it
    * is not the field's name (a renamed field's column keeps its own name in
    * a `storage=` table). The DML names the watermark, merge keys, scope and
    * rolling-window columns through it. Absent: every field is its column.
    */
   storedColumn?(field: string): string;
}

/**
 * The delta target for a DATABASE source: one connection and dialect answer
 * everything, and the delta's rows are the seed's SQL with a range predicate.
 */
export function warehouseDeltaTarget(params: {
   dialect: string;
   runner: SqlRunner;
   quotedTablePath: string;
   lineage: IncrementalLineage;
   /**
    * The target's `buildSQL()` resolved against the build manifest: the string
    * the seed's CTAS runs (see {@link deltaSelect}).
    */
   sourceSQL: string;
   /** The run's cancel signal, for the delta's script. */
   abortSignal?: AbortSignal;
}): DeltaTarget {
   const { dialect, runner, lineage, sourceSQL } = params;
   return {
      dialect,
      runner,
      ...(params.abortSignal ? { abortSignal: params.abortSignal } : {}),
      quotedTablePath: params.quotedTablePath,
      logicalTablePath: lineage.physicalTableName,
      deltaRows: async (start, end) =>
         deltaSelect({
            dialect,
            sourceSQL,
            watermarkName: lineage.watermarkName,
            start,
            end,
         }),
      windowRows: async (start, end, columns) =>
         `SELECT ${columns.map((c) => quoteIdentifier(c, dialect)).join(", ")} ` +
         `FROM (${deltaSelect({
            dialect,
            sourceSQL,
            watermarkName: lineage.watermarkName,
            start,
            end,
         })}) AS __c`,
      probeSourceFrontier: () =>
         probeMaxWatermark(
            runner,
            dialect,
            sourceSQL,
            lineage.watermarkName,
            lineage.watermarkType,
         ),
   };
}

/**
 * Decide what this source's refresh does, and build the delta's SQL when it is a
 * delta. Checks run in order of increasing cost: free ones first, then warehouse
 * probes.
 */
export async function planIncrementalStep(inputs: {
   /** Where the DML lands and how its rows are produced. */
   target: DeltaTarget;
   lineage: IncrementalLineage;
   /**
    * The recorded boundary this refresh may advance from: a row from the local
    * store, or a caller-returned ledger entry. Null seeds.
    */
   ledgerEntry: RecordedBoundary | null;
   forceRefresh: boolean;
   /** The run's start time, used as the range end for a time watermark. */
   now: Date;
   /** The source's compiled output columns, which the delta's DML names. */
   columns: string[];
   /** Postgres only: the server version, for the MERGE floor. */
   postgresVersionNum?: number;
}): Promise<IncrementalStep> {
   const { target, lineage, ledgerEntry } = inputs;
   const { runner, dialect } = target;

   if (inputs.forceRefresh) {
      return {
         mode: "seed",
         reasonCode: "forced",
         reason: "a full refresh was requested",
      };
   }
   if (!ledgerEntry) {
      return {
         mode: "seed",
         reasonCode: "no_boundary",
         reason: "no covered_through boundary is recorded for this source yet",
      };
   }
   const mismatch = ledgerLineageMismatch(ledgerEntry, lineage);
   if (mismatch) {
      return { mode: "seed", ...mismatch };
   }

   if (
      lineage.strategy === "merge" &&
      capabilitiesOf(dialect).mergeGatedByServerVersion &&
      (inputs.postgresVersionNum ?? 0) < POSTGRES_MERGE_MIN_VERSION_NUM
   ) {
      // Not a silent fallback to range replace: merge_key= asks for the
      // stronger guarantee.
      return {
         mode: "seed",
         reasonCode: "merge_unsupported",
         reason:
            `merge_key= needs MERGE, which this Postgres server ` +
            `(server_version_num ${inputs.postgresVersionNum ?? "unknown"}) ` +
            `does not have; MERGE requires Postgres 15`,
      };
   }

   const probe = await probeTargetColumns(
      runner,
      dialect,
      target.logicalTablePath,
   );
   if (probe.columns.length === 0) {
      return {
         mode: "seed",
         reasonCode: "table_unreadable",
         reason:
            `the target table could not be described` +
            (probe.error
               ? ` (${probe.error})`
               : ", so it is treated as absent"),
      };
   }

   // A recorded boundary means an earlier run wrote rows, so an empty table was
   // emptied externally; a delta would restore only the tail. (An empty seed
   // records no boundary, so this cannot misfire on an empty source.)
   const rows = await probeTargetNonEmpty(
      runner,
      dialect,
      target.quotedTablePath,
   );
   if (!rows.nonEmpty) {
      return {
         mode: "seed",
         reasonCode: "table_emptied",
         reason:
            `the target table is empty while a covered_through boundary of ` +
            `${ledgerEntry.coveredThroughValue} is recorded, so its rows were ` +
            `removed outside this ledger` +
            (rows.error ? ` (${rows.error})` : ""),
      };
   }

   const recorded: WatermarkBound = {
      malloyType: ledgerEntry.coveredThroughType,
      value: ledgerEntry.coveredThroughValue,
   };
   const lower = await resolveDeltaStart(target, lineage, recorded);
   if (lower.error !== undefined) {
      // Unproven where the table reaches: a delta from the recorded boundary
      // could skip rows, so rebuild.
      return {
         mode: "seed",
         reasonCode: "table_unreadable",
         reason: `the newest stored watermark could not be read (${lower.error})`,
      };
   }
   const start = lower.bound;
   const end = await resolveDeltaEnd(target, lineage, inputs.now);
   if (!end.bound) {
      return {
         mode: "skip",
         reasonCode: end.error ? "frontier_unreadable" : "not_advanced",
         reason: end.error
            ? `the watermark frontier could not be read (${end.error})`
            : "the source has no rows with a non-null watermark",
         coveredThrough: recorded,
      };
   }
   if (compareBounds(end.bound, start) <= 0) {
      return {
         mode: "skip",
         reasonCode: "not_advanced",
         reason:
            `the watermark has not advanced past ${start.value} ` +
            `(frontier: ${end.bound.value})`,
         coveredThrough: recorded,
      };
   }

   const shape = shapeMismatch(inputs.columns, probe.columns);
   if (shape) {
      // See shapeMismatch. The reason names the columns so the author can drop
      // the rename or the incremental declaration.
      return {
         mode: "seed",
         reasonCode: "shape_mismatch",
         reason:
            `the delta's columns do not match the target table's (${shape}), ` +
            `so a delta would write a different shape than the table holds`,
      };
   }

   // Built LAST: producing it can issue the source read (DeltaTarget.deltaRows),
   // which no failed check should pay for.
   const column = (field: string) => target.storedColumn?.(field) ?? field;
   const statements = deltaStatements({
      dialect,
      quotedTablePath: target.quotedTablePath,
      // A pair table's range is open above. Its watermark is a parent's
      // greatest field over all its rows, so one child stamped past the clock
      // would hold the parent above every bounded range until the clock
      // caught up, and the parent's own changes with it; replacing a parent
      // whole is idempotent, so reading it again on each refresh is safe.
      deltaSQL: await target.deltaRows(
         start,
         lineage.pair ? undefined : end.bound,
      ),
      columns: inputs.columns,
      mergeKeys: (lineage.mergeKeys ?? []).map(column),
      scopeColumns: lineage.scopeColumns?.map(column),
      watermarkName: column(lineage.watermarkName),
      start,
      end: end.bound,
      windowDeletes: (lineage.rollingWindow ?? []).map((window) => ({
         column: column(window.field),
         before: canonicalBoundValue(
            "timestamp",
            windowDeleteBefore(window, inputs.now),
         ),
      })),
      ...(lineage.pair
         ? {
              pair: {
                 ...lineage.pair,
                 stagingTable: pairStagingTable(lineage.physicalTableName),
              },
           }
         : {}),
   });
   return {
      mode: "delta",
      start,
      end: end.bound,
      coveredThrough: end.bound,
      statements,
   };
}

/**
 * The session temporary a pair delta is staged in, one per table, so two pair
 * tables refreshed on one session never share one.
 */
export function pairStagingTable(physicalTableName: string): string {
   return `__pair_delta_${physicalTableName.replace(/[^A-Za-z0-9_]/g, "_")}`;
}

/**
 * The boundary to record after a SEED, read from the freshly built TARGET (its
 * `MAX(watermark)` is the source's frontier, cheaper than re-running the source).
 *
 * A time watermark records the run's SNAPSHOT instead, because future-dated rows
 * must not make the boundary claim a range the clock has not reached.
 *
 * A NULL maximum records nothing, so the next run seeds again: a boundary from an
 * empty seed would exclude every historical row that arrives later.
 */
export async function seedCoveredThrough(
   runner: SqlRunner,
   dialect: string,
   lineage: IncrementalLineage,
   quotedTablePath: string,
   now: Date,
   /** The column the table holds the watermark under (DeltaTarget.storedColumn). */
   storedColumn?: (field: string) => string,
): Promise<{ bound?: WatermarkBound; error?: string; errorCode?: string }> {
   const watermark =
      storedColumn?.(lineage.watermarkName) ?? lineage.watermarkName;
   const frontier = await probeMaxWatermark(
      runner,
      dialect,
      `SELECT ${quoteIdentifier(watermark, dialect)} FROM ${quotedTablePath}`,
      watermark,
      lineage.watermarkType,
   );
   if (frontier.error || !frontier.bound) return frontier;
   const type = lineage.watermarkType;
   return type === "date" || type === "timestamp"
      ? { bound: snapshotBound(type, now) }
      : frontier;
}

// ─────────────────────────────────────────────────────────────────────────────
// Late rows: rows the source holds below a delta's lower bound that the stored
// table lacks. A delta never reads below that bound, so such a row (stamped
// before the newest stored row, landing after it was written) is missing from
// the served table until a rebuild, or a `lookback=` wide enough to reach it.
// The check makes that visible; it never writes.
// ─────────────────────────────────────────────────────────────────────────────

/**
 * The late-row check's one aggregate, run on the TARGET: how many of the
 * source's rows in `[from, to)` (`windowSQL`, see DeltaTarget.windowRows) the
 * stored table lacks.
 *
 * With `merge_key=` (`matchColumns`, the keys plus any scope columns), a
 * source row is present when a stored row has the same keys AND the same
 * watermark, so a restatement the delta skipped counts as well as a row it
 * never saw. A LEFT JOIN rather than a correlated `NOT EXISTS`: BigQuery
 * refuses a correlated subquery it cannot turn into a join, and the
 * watermark's equality is the equi-join every engine needs. Both sides are
 * read only inside the window. Without a merge key (a range replace, whose
 * rows' watermarks never change) the count is the source's rows in the
 * window less the table's.
 */
export function lateRowsSql(params: {
   dialect: string;
   quotedTablePath: string;
   windowSQL: string;
   /** The watermark's stored column. */
   watermark: string;
   /** Stored columns that identify a row; empty for a range replace. */
   matchColumns: string[];
   from: WatermarkBound;
   to: WatermarkBound;
}): string {
   const { dialect, quotedTablePath, windowSQL, matchColumns } = params;
   const q = (name: string) => quoteIdentifier(name, dialect);
   const w = q(params.watermark);
   const inWindow =
      `${w} >= ${renderSqlBound(params.from, dialect)} AND ` +
      `${w} < ${renderSqlBound(params.to, dialect)}`;
   const alias = probeAlias("late_rows", dialect);
   if (matchColumns.length === 0) {
      return probeSelect(
         dialect,
         `SELECT (SELECT COUNT(*) FROM (${windowSQL}) AS __s) - ` +
            `(SELECT COUNT(*) FROM ${quotedTablePath} WHERE ${inWindow}) ` +
            `AS ${alias}`,
      );
   }
   const stored = [...new Set([params.watermark, ...matchColumns])]
      .map(q)
      .join(", ");
   const on = [
      `__t.${w} = __s.${w}`,
      ...matchColumns
         .filter((c) => c !== params.watermark)
         .map(
            (c) =>
               `(__t.${q(c)} = __s.${q(c)} OR ` +
               `(__t.${q(c)} IS NULL AND __s.${q(c)} IS NULL))`,
         ),
   ].join(" AND ");
   return probeSelect(
      dialect,
      `SELECT COUNT(*) AS ${alias} FROM (${windowSQL}) AS __s ` +
         `LEFT JOIN (SELECT 1 AS __present, ${stored} FROM ` +
         `${quotedTablePath} WHERE ${inWindow}) AS __t ON ${on} ` +
         `WHERE __t.__present IS NULL`,
   );
}

/** A bound as the wire spells it: ISO-8601 with a `T` for a timestamp. */
function wireBoundValue(bound: WatermarkBound): string {
   return bound.malloyType === "timestamp"
      ? bound.value.replace(" ", "T")
      : bound.value;
}

/**
 * How long the late-row check may take before the run goes on without it.
 * The check is best-effort, so a slow read never holds up the end of a run.
 * Its statement is stopped too: on Postgres by a statement timeout of the
 * same length (withStatementTimeout), elsewhere by the abort signal (a
 * `storage=` session is interrupted, sessionRunner in incremental_storage.ts).
 */
const LATE_ROWS_CHECK_TIMEOUT_MS = 60_000;

/**
 * Run the late-row check for a delta that committed: the source's rows in a
 * window directly below its lower bound ({@link lateRowsWindowStart}, as wide
 * as its own range, capped) that the stored table lacks. One aggregate over
 * that window of the source and of the table.
 *
 * What an earlier delta found is `carried` (the bound manifest entry's
 * `lateRows`), and it stays reported until a rebuild clears it: the window is
 * checked only above `carried.to`, so no slice is counted twice, and what it
 * finds adds to the carried count. A check that does not run, finds nothing,
 * fails or times out returns the carried rows as they were.
 *
 * Nothing new for a pair table (its delta replaces whole parents, so its rows
 * have no per-row identity to compare), a string watermark (a window has no
 * width), or a target that cannot read the source's window. Never throws:
 * the delta it checks has already committed.
 */
export async function countLateRows(params: {
   target: DeltaTarget;
   lineage: IncrementalLineage;
   start: WatermarkBound;
   end: WatermarkBound;
   carried?: LateRows;
   timeoutMs?: number;
}): Promise<{ lateRows?: LateRows; error?: string; errorCode?: string }> {
   const { target, lineage, start, end, carried } = params;
   const kept = carried ? { lateRows: carried } : {};
   const windowRows = target.windowRows;
   if (lineage.pair || !windowRows) return kept;
   let from = lateRowsWindowStart(start, end, lineage.lookbackSeconds);
   if (!from) return kept;
   if (carried) {
      // Above what an earlier check counted, so a row is counted once.
      const checkedTo = { malloyType: start.malloyType, value: carried.to };
      if (compareBounds(checkedTo, from) > 0) from = checkedTo;
      if (compareBounds(from, start) >= 0) return kept;
   }
   const windowFrom = from;
   const column = (field: string) => target.storedColumn?.(field) ?? field;
   const watermark = column(lineage.watermarkName);
   const matchColumns =
      lineage.mergeKeys.length > 0
         ? [
              ...new Set(
                 [...lineage.mergeKeys, ...(lineage.scopeColumns ?? [])].map(
                    column,
                 ),
              ),
           ]
         : [];
   const timeoutMs = params.timeoutMs ?? LATE_ROWS_CHECK_TIMEOUT_MS;
   const abort = new AbortController();
   const check = (async () => {
      const windowSQL = await windowRows(windowFrom, start, [
         ...new Set([watermark, ...matchColumns]),
      ]);
      const result = await target.runner(
         withStatementTimeout(
            target.dialect,
            lateRowsSql({
               dialect: target.dialect,
               quotedTablePath: target.quotedTablePath,
               windowSQL,
               watermark,
               matchColumns,
               from: windowFrom,
               to: start,
            }),
            timeoutMs,
         ),
         { abortSignal: abort.signal },
      );
      return Number(probeRows(result.rows)[0]?.["late_rows"]);
   })();
   // The side that loses the race must not surface as an unhandled rejection.
   check.catch(() => undefined);
   let timer: ReturnType<typeof setTimeout> | undefined;
   const timedOut = new Promise<never>((_, reject) => {
      timer = setTimeout(() => {
         abort.abort();
         reject(
            Object.assign(
               new Error(`the late-row check took longer than ${timeoutMs} ms`),
               { code: "late_rows_check_timeout" },
            ),
         );
      }, timeoutMs);
   });
   try {
      const count = await Promise.race([check, timedOut]);
      if (!Number.isFinite(count) || count <= 0) return kept;
      return {
         lateRows: {
            count: count + (carried?.count ?? 0),
            from: carried?.from ?? wireBoundValue(windowFrom),
            to: wireBoundValue(start),
         },
      };
   } catch (err) {
      if (abort.signal.aborted) {
         // Interrupted: let the statement wind down, briefly, so the
         // session or connection it held is free for the run's next one.
         await Promise.race([
            check.catch(() => undefined),
            new Promise((resolve) => setTimeout(resolve, LATE_ROWS_SETTLE_MS)),
         ]);
      }
      return { ...kept, error: errMessage(err), errorCode: errorCodeOf(err) };
   } finally {
      clearTimeout(timer);
   }
}

/** How long a timed-out late-row check is given to stop once interrupted. */
const LATE_ROWS_SETTLE_MS = 5_000;
