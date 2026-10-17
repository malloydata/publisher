// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { decodeDottedTablePath } from "@malloydata/malloy";

import { logger } from "../logger";
import type {
   IncrementalLedgerEntry,
   IncrementalStrategy,
} from "../storage/DatabaseInterface";
import { errMessage } from "../utils";
import { quoteIdentifier } from "./quoting";

/**
 * Generating and applying an incremental DELTA: the bounded slice of a persisted
 * source that a refresh writes into the already-serving table, instead of
 * rebuilding it.
 *
 * The delta is composed SQL, never a compiled Malloy query: the source's own
 * build SQL (`PersistSource.getSQL()`, the string the seed's CTAS runs) wrapped
 * in a `WHERE` on the watermark. A compiled query's SQL arrives FINALIZED (see
 * {@link deltaSelect}), and a generated source extension silently DROPS its
 * `where:`, which would rewrite every row. Pinned in
 * incremental_compiler_contract.spec.ts.
 *
 * The range is HALF-OPEN, `[start, end)`, which makes a run idempotent: a crash
 * between the commit and the ledger advance costs a repeat, not a corruption.
 */

/**
 * The two dialect sets below differ only for a `storage=` source, whose rows are
 * computed by the SOURCE warehouse and whose DML runs in the DESTINATION engine.
 *
 * Both are keyed by Malloy's `dialectName`, so BigQuery is `standardsql` (as in
 * `quoting.ts`). No source's `dialectName` is ever "bigquery", so keying by the
 * friendly name would silently reject every BigQuery source.
 */

/**
 * Dialects a bounded watermark range can be rendered and probed FOR — the source
 * side. A dialect here has literal spellings for every watermark type
 * ({@link renderSqlBound}) and a decodable table path
 * ({@link decodeTablePathSegments}).
 */
export const INCREMENTAL_SOURCE_RANGE_DIALECTS: ReadonlySet<string> = new Set([
   "postgres",
   "snowflake",
   "standardsql",
]);

/**
 * Dialects whose transactional delta apply is proven: the target side, where the
 * DML lands.
 *
 * Postgres, BigQuery and DuckDB take the range replace as one multi-statement
 * script. Snowflake's driver runs one statement per call, on a possibly different
 * pooled session each time, so it gets a Snowflake Scripting block instead (see
 * {@link deltaStatements}).
 *
 * `duckdb` is here only as a `storage=` destination; a DuckDB source is never
 * materialized.
 */
export const INCREMENTAL_TARGET_DML_DIALECTS: ReadonlySet<string> = new Set([
   "postgres",
   "snowflake",
   "standardsql",
   "duckdb",
]);

/** Dialects with a `MERGE INTO` statement, required by `merge_key=`. */
export const MERGE_CAPABLE_DIALECTS: ReadonlySet<string> = new Set([
   "postgres",
   "snowflake",
   "standardsql",
   "duckdb",
]);

/** Malloy `dialectName` -> the name an author would recognize, for a message. */
const DIALECT_DISPLAY_NAMES: Record<string, string> = {
   standardsql: "standardsql (BigQuery)",
};

/** A dialect set as message text, e.g. `postgres, standardsql (BigQuery)`. */
export function describeDialects(dialects: ReadonlySet<string>): string {
   return [...dialects]
      .sort()
      .map((d) => DIALECT_DISPLAY_NAMES[d] ?? d)
      .join(", ");
}

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
 * Escape a string for a single-quoted SQL literal (doubling the quote).
 *
 * Snowflake and BigQuery treat `\` as an escape, so a trailing backslash would
 * swallow the closing quote and must be doubled. Postgres (with
 * standard_conforming_strings, the default) would read a doubled backslash as two
 * characters, so it is left alone there.
 */
function escapeSqlString(value: string, dialect?: string): string {
   const quoted = value.replace(/'/g, "''");
   return dialect === "snowflake" || dialect === "standardsql"
      ? quoted.replace(/\\/g, "\\\\")
      : quoted;
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
   // already says UTC.
   const match = value
      .trim()
      .match(/^(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2})(?:\.\d+)?Z?$/);
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

/**
 * The delta's SELECT for one source and range: the source's OWN build SQL
 * (`PersistSource.getSQL()`, the string the seed's CTAS runs), wrapped and
 * filtered on the watermark, so the delta cannot disagree with the seed about
 * what the source computes.
 *
 * Not a compiled Malloy query, because a query's SQL is FINALIZED: on Postgres
 * (the only dialect with `hasFinalStage`) it becomes
 * `SELECT row_to_json(finalStage) as row FROM …`, one JSON column, and an
 * `INSERT … SELECT "col"` off it fails. `getSQL()` is unfinalized, and there is
 * no supported way to ask a query for that. Pinned as the TRIPWIRE in
 * incremental_compiler_contract.spec.ts; see docs/materialization.md,
 * "Materializing on Postgres".
 *
 * `SELECT *` because the DML projects by name (see {@link deltaStatements}).
 */
export function deltaSelect(params: {
   dialect: string;
   /** `PersistSource.getSQL(...)`, resolved against the build's manifest. */
   sourceSQL: string;
   watermarkName: string;
   start: WatermarkBound;
   end: WatermarkBound;
}): string {
   const watermark = quoteIdentifier(params.watermarkName, params.dialect);
   return (
      `SELECT * FROM (${params.sourceSQL}) AS __d ` +
      `WHERE ${watermark} >= ${renderSqlBound(params.start, params.dialect)} ` +
      `AND ${watermark} < ${renderSqlBound(params.end, params.dialect)}`
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
export type SqlRunner = (sql: string) => Promise<{ rows: unknown[] }>;

/**
 * Per-dialect table-path decoding. The physical name arrives as SQL (bare or
 * quoted segments) while `information_schema` compares RAW identifier text, so it
 * is decoded first. Postgres folds bare segments to lower case, Snowflake to
 * UPPER, BigQuery not at all (its names are case-sensitive).
 */
const TABLE_PATH_OPTIONS: Record<
   string,
   {
      quoteChar: string;
      escapeStyle: "doubled";
      bareIdentRegex: RegExp;
      foldCase?: "lower" | "upper";
   }
> = {
   postgres: {
      quoteChar: '"',
      escapeStyle: "doubled",
      bareIdentRegex: /^[A-Za-z_][A-Za-z0-9_$]*/,
      foldCase: "lower",
   },
   snowflake: {
      quoteChar: '"',
      escapeStyle: "doubled",
      bareIdentRegex: /^[A-Za-z_][A-Za-z0-9_$]*/,
      foldCase: "upper",
   },
   standardsql: {
      quoteChar: "`",
      escapeStyle: "doubled",
      bareIdentRegex: /^[A-Za-z_][A-Za-z0-9_-]*/,
   },
   // A `storage=` destination path: `<destination>.<name>` or
   // `<destination>.<schema>.<name>`. No case folding: DuckDB stores identifiers
   // as written and the metadata read compares stored text, so folding would miss
   // a mixed-case table.
   //
   // `-` is admitted because this is a LOGICAL path built from a destination name
   // and a persist name, neither charset-validated. Rejecting it would make
   // `my-lake` build and serve fine while this probe failed, seeding every run.
   duckdb: {
      quoteChar: '"',
      escapeStyle: "doubled",
      bareIdentRegex: /^[A-Za-z_][A-Za-z0-9_$-]*/,
   },
};

/**
 * The raw identifier segments of a physical table path, or undefined when the
 * path cannot be decoded for this dialect (which leads to a SEED, not a throw).
 */
export function decodeTablePathSegments(
   dialect: string,
   tablePath: string,
): string[] | undefined {
   const opts = TABLE_PATH_OPTIONS[dialect];
   if (!opts) return undefined;
   const decoded = decodeDottedTablePath(tablePath, {
      quoteChar: opts.quoteChar,
      escapeStyle: opts.escapeStyle,
      bareIdentRegex: opts.bareIdentRegex,
      dialectName: dialect,
   });
   if (!decoded.ok) return undefined;
   return decoded.segments.map((s) => {
      if (s.quoted || !opts.foldCase) return s.value;
      return opts.foldCase === "lower"
         ? s.value.toLowerCase()
         : s.value.toUpperCase();
   });
}

/**
 * Wrap a probe SELECT so the dialect's driver hands back named columns.
 *
 * Postgres only: `PostgresConnection` rewrites every `runSQL` row as
 * `rows[i].row` (Malloy queries project one `row` JSON column), so a plain
 * `SELECT max(x) AS m` comes back as `[undefined]`. Wrapping in `row_to_json`
 * supplies that column. Pinned in incremental_apply.spec.ts.
 */
export function probeSelect(dialect: string, innerSelect: string): string {
   return dialect === "postgres"
      ? `SELECT row_to_json(__p) AS "row" FROM (${innerSelect}) AS __p`
      : innerSelect;
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
   let sql: string;
   if (dialect === "duckdb") {
      // `duckdb_columns()`: an attached DuckLake catalog has no
      // `information_schema`, and `DESCRIBE` throws on an absent table where this
      // function's contract needs an empty answer.
      const table = segments[segments.length - 1];
      // A two-segment path is in the catalog's default schema. Named explicitly
      // because two schemas can hold the same table name, and probing the wrong
      // one would describe a table this build never wrote.
      const schema =
         segments.length >= 3 ? segments[segments.length - 2] : "main";
      // The catalog is the FIRST segment (the path is `<destination>.<name>`);
      // counting from the end would misread a longer name's container.
      const database = segments[0];
      sql = `SELECT column_name AS ${probeAlias("column_name", dialect)}
              FROM duckdb_columns()
              WHERE database_name = '${escapeSqlString(database, dialect)}'
                AND schema_name = '${escapeSqlString(schema, dialect)}'
                AND table_name = '${escapeSqlString(table, dialect)}'
              ORDER BY column_index`;
   } else if (dialect === "postgres" || dialect === "snowflake") {
      const table = segments[segments.length - 1];
      // An unqualified name lives in the session's default schema, where the CTAS
      // created it. Not "any schema on the search path": two schemas can hold
      // the same table name.
      const schema =
         segments.length >= 2
            ? `'${escapeSqlString(segments[segments.length - 2], dialect)}'`
            : "current_schema()";
      // Snowflake's information_schema is per-DATABASE, so a database-qualified
      // name must probe that database's view; Postgres cannot cross databases,
      // so any leading segment is the current one by construction.
      const infoSchema =
         dialect === "snowflake" && segments.length >= 3
            ? `${quoteIdentifier(segments[segments.length - 3], dialect)}.information_schema.columns`
            : "information_schema.columns";
      sql = probeSelect(
         dialect,
         `SELECT column_name AS ${probeAlias("column_name", dialect)}
           FROM ${infoSchema}
           WHERE table_schema = ${schema}
             AND table_name = '${escapeSqlString(table, dialect)}'
           ORDER BY ordinal_position`,
      );
   } else {
      // BigQuery's INFORMATION_SCHEMA is per-dataset, so the view itself must be
      // dataset-qualified: an unqualified target has no dataset to look in.
      if (segments.length < 2) {
         return {
            columns: [],
            error:
               `physical table name ${JSON.stringify(physicalTableName)} is not ` +
               `dataset-qualified, so its schema cannot be read from ` +
               `INFORMATION_SCHEMA`,
         };
      }
      const table = segments[segments.length - 1];
      const container = segments
         .slice(0, -1)
         .map((s) => quoteIdentifier(s, dialect))
         .join(".");
      sql = `SELECT column_name AS ${probeAlias("column_name", dialect)}
              FROM ${container}.INFORMATION_SCHEMA.COLUMNS
              WHERE table_name = '${escapeSqlString(table, dialect)}'
              ORDER BY ordinal_position`;
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
): Promise<{ bound?: WatermarkBound; error?: string }> {
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
      return { error: errMessage(err) };
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
   return a.value < b.value ? -1 : a.value > b.value ? 1 : 0;
}

/**
 * The set difference between the delta's output columns and the target table's,
 * as message text, or undefined when they are the same set (order is ignored;
 * the DML names every column).
 *
 * Membership can legitimately differ: the DML names the source's compiled
 * schema, while `getSQL()` (and so the seed's table) also projects columns the
 * source renames or hides (`rename:`, `except:`, non-public access). Pinned in
 * incremental_compiler_contract.spec.ts. Such sources seed, because naming a
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
}): string[] {
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
      return [clauses.join("\n")];
   }

   const watermark = q(watermarkName);
   const range =
      `${watermark} >= ${renderSqlBound(params.start, dialect)} AND ` +
      `${watermark} < ${renderSqlBound(params.end, dialect)}`;
   const body = [
      `DELETE FROM ${quotedTablePath} WHERE ${range}`,
      `INSERT INTO ${quotedTablePath} (${columnList}) ` +
         `SELECT ${columnList} FROM (${deltaSQL}) AS __s`,
   ];
   if (dialect === "snowflake") return [snowflakeScriptingBlock(body)];
   const transaction = transactionKeywords(dialect);
   return [transaction.begin, ...body, transaction.commit];
}

/**
 * The range replace as ONE statement for Snowflake: an anonymous Snowflake
 * Scripting block in `EXECUTE IMMEDIATE $$…$$` (drivers do not accept a bare
 * block).
 *
 * The EXCEPTION handler is the rollback, INSIDE the statement because a separate
 * `ROLLBACK` call could reach a different pooled session, leaving the open
 * transaction holding locks. RAISE re-throws, so the build still fails and the
 * boundary is not advanced.
 */
export function snowflakeScriptingBlock(statements: string[]): string {
   const body = statements.map((s) => `  ${s};`).join("\n");
   // SQL containing `$$` would end the block early and run the rest as separate
   // statements. Generated deltas never contain it, so refuse rather than escape:
   // planSourceRefresh catches the throw and seeds (plan_error).
   const block = `BEGIN\n  BEGIN TRANSACTION;\n${body}\n  COMMIT;\nEXCEPTION\n  WHEN OTHER THEN\n    ROLLBACK;\n    RAISE;\nEND`;
   if (block.includes("$$")) {
      throw new Error(
         "the delta SQL contains '$$', which cannot be carried inside a " +
            "Snowflake Scripting block",
      );
   }
   return `EXECUTE IMMEDIATE $$\n${block}\n$$`;
}

function transactionKeywords(dialect: string): {
   begin: string;
   commit: string;
   rollback: string;
} {
   return dialect === "standardsql"
      ? {
           begin: "BEGIN TRANSACTION",
           commit: "COMMIT TRANSACTION",
           rollback: "ROLLBACK TRANSACTION",
        }
      : { begin: "BEGIN", commit: "COMMIT", rollback: "ROLLBACK" };
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
): Promise<void> {
   try {
      await runner(deltaScript(statements));
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
export type IncrementalStepReasonCode =
   // Seeds.
   | "forced"
   | "no_boundary"
   | "lineage_changed"
   | "table_renamed"
   | "merge_unsupported"
   | "table_unreadable"
   | "table_emptied"
   | "shape_mismatch"
   | "ledger_unreadable"
   | "plan_error"
   | "chained_storage"
   // Skips.
   | "frontier_unreadable"
   | "not_advanced";

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
 * Where a delta's DML lands, and how its rows are produced: the only facts that
 * differ between a colocated table and a `storage=` one (rows computed by the
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
   /** The table as the CREATE quoted it, for the DML and the emptiness probe. */
   quotedTablePath: string;
   /**
    * The same table as a LOGICAL (unquoted) path, for the column metadata read.
    * Not taken from the lineage, whose `physicalTableName` omits a stored table's
    * destination catalog.
    */
   logicalTablePath: string;
   /**
    * The rows-yielding SELECT for one bounded range, executable on the TARGET.
    * Async because producing it can issue the source read (a stored table's rows
    * arrive through a query passthrough).
    */
   deltaRows(start: WatermarkBound, end: WatermarkBound): Promise<string>;
   /**
    * The SOURCE's watermark frontier, for a number or string watermark (see
    * resolveDeltaEnd).
    */
   probeSourceFrontier(): Promise<{ bound?: WatermarkBound; error?: string }>;
}

/**
 * The delta target for a COLOCATED source: one connection and dialect answer
 * everything, and the delta's rows are the seed's SQL with a range predicate.
 */
export function warehouseDeltaTarget(params: {
   dialect: string;
   runner: SqlRunner;
   quotedTablePath: string;
   lineage: IncrementalLineage;
   /**
    * `PersistSource.getSQL()` resolved against the build manifest: the string the
    * seed's CTAS runs (see {@link deltaSelect}).
    */
   sourceSQL: string;
}): DeltaTarget {
   const { dialect, runner, lineage, sourceSQL } = params;
   return {
      dialect,
      runner,
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
      dialect === "postgres" &&
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

   const start: WatermarkBound = {
      malloyType: ledgerEntry.coveredThroughType,
      value: ledgerEntry.coveredThroughValue,
   };
   const end = await resolveDeltaEnd(target, lineage, inputs.now);
   if (!end.bound) {
      return {
         mode: "skip",
         reasonCode: end.error ? "frontier_unreadable" : "not_advanced",
         reason: end.error
            ? `the watermark frontier could not be read (${end.error})`
            : "the source has no rows with a non-null watermark",
         coveredThrough: start,
      };
   }
   if (compareBounds(end.bound, start) <= 0) {
      return {
         mode: "skip",
         reasonCode: "not_advanced",
         reason:
            `the watermark has not advanced past ${start.value} ` +
            `(frontier: ${end.bound.value})`,
         coveredThrough: start,
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
   const statements = deltaStatements({
      dialect,
      quotedTablePath: target.quotedTablePath,
      deltaSQL: await target.deltaRows(start, end.bound),
      columns: inputs.columns,
      mergeKeys: lineage.mergeKeys,
      scopeColumns: lineage.scopeColumns,
      watermarkName: lineage.watermarkName,
      start,
      end: end.bound,
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
): Promise<{ bound?: WatermarkBound; error?: string }> {
   const frontier = await probeMaxWatermark(
      runner,
      dialect,
      `SELECT ${quoteIdentifier(lineage.watermarkName, dialect)} FROM ${quotedTablePath}`,
      lineage.watermarkName,
      lineage.watermarkType,
   );
   if (frontier.error || !frontier.bound) return frontier;
   const type = lineage.watermarkType;
   return type === "date" || type === "timestamp"
      ? { bound: snapshotBound(type, now) }
      : frontier;
}
