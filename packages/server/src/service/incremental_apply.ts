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
 * rebuilding the whole table.
 *
 * The delta is composed SQL, never a compiled Malloy query: the source's own
 * build SQL (`PersistSource.getSQL()`, the string the seed's CTAS runs) wrapped
 * in a `WHERE` on the watermark. Why not a query is load-bearing and pinned in
 * incremental_compiler_contract.spec.ts — a compiled query's SQL arrives
 * FINALIZED (on Postgres, wrapped in `row_to_json`, which breaks a columnar
 * INSERT; see {@link deltaSelect}), and a generated source extension silently
 * DROPS its `where:`, which would turn a delta that reports a bounded range into
 * one that rewrites every row.
 *
 * The range is HALF-OPEN, `[start, end)`. That is what makes a run idempotent:
 * re-running the same range deletes and re-inserts (or re-merges) exactly the
 * rows it did the first time, so a crash between the commit and the ledger
 * advance costs a repeat, not a corruption.
 */

/**
 * The two dialect questions a delta asks, which are the same question only when
 * the table is materialized into the source's own warehouse.
 *
 * Both sets are keyed by Malloy's `dialectName`, which is why BigQuery appears as
 * `standardsql` — the same key `quoting.ts` uses for its backtick set. Keying
 * these by the friendly name instead is a silent no-op: no source's
 * `dialectName` is ever "bigquery", so every BigQuery source would fail the
 * check while the message claimed BigQuery was supported.
 *
 * A `storage=` source is the case that separates them: its rows are computed by
 * the SOURCE warehouse and its table lives in the DESTINATION engine, so the
 * range predicate is rendered for one dialect and the DML issued in another.
 * Colocated, a source's dialect satisfies both, which is why nothing about that
 * path changes.
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
 * Dialects whose transactional delta apply the publisher has proven — the target
 * side, where the DML lands.
 *
 * Postgres, BigQuery and DuckDB accept the range replace as a multi-statement
 * transactional script in one driver call. Snowflake does NOT — its driver
 * executes exactly one statement per call, each on a possibly different pooled
 * session — so its range replace is a single Snowflake Scripting block instead
 * (see {@link deltaStatements}).
 *
 * `duckdb` covers a `storage=` destination, whose table is a DuckLake or DuckDB
 * one written from a build-scoped session. A DuckDB *source* is never
 * materialized at all, so its presence here is about where a delta LANDS, never
 * about where one is read from.
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
 * type — ISO-8601 for a date/timestamp, decimal text for a number, the value
 * itself for a string.
 *
 * Deliberately not a pre-rendered literal: the ledger holds one value that
 * outlives any one statement's spelling of it, and comparing two boundaries
 * (compareBounds) needs the value, not its rendering.
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
 * Backslash handling is the dialect-sensitive part: Snowflake and BigQuery read
 * `\` in a single-quoted string as an ESCAPE character, so a value ending in a
 * backslash would swallow the closing quote — the literal has to double it.
 * Postgres (with standard_conforming_strings, its modern default) reads the
 * same doubled backslash as two literal characters, so this cannot be one
 * spelling for all three.
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
 * Truncation is safe in both directions because the range is half-open and the
 * ledger records the value actually used: a truncated `end` leaves the trailing
 * sub-second rows for the next run, which starts exactly there. It buys
 * independence from how the compiler and each dialect spell fractional seconds.
 */
function secondsPrecision(value: string): string {
   const match = value
      .trim()
      .match(/^(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2})/);
   if (!match) {
      throw new Error(
         `not an ISO-8601 timestamp: ${JSON.stringify(value)} (expected YYYY-MM-DDTHH:MM:SS)`,
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
 * The bound as a SQL literal, spelled for the watermark's own type.
 *
 * Type-matching is not cosmetic: on a strict dialect, comparing a `date` column
 * to a timestamp literal is an error rather than a coercion, so a single
 * all-timestamps renderer would fail every date-watermarked source at build time.
 *
 * The timestamp spelling is timezone-naive on purpose, and every bound is UTC
 * text (see canonicalBoundValue). Postgres and BigQuery read it as UTC outright;
 * Snowflake reads it as session time and its Malloy driver pins every session to
 * `TIMEZONE = 'UTC'`, so a comparison against a TIMESTAMP_LTZ column lands on
 * the same instant. DuckDB — the target dialect for a `storage=` table — also
 * resolves a naive literal against a TIMESTAMPTZ column in the session's zone,
 * and the build session is pinned to UTC for exactly this reason (see
 * `pinSessionToUTC`); on the host's own zone the range would silently land on the
 * wrong rows.
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
 * `MAX(key)` probe) or produced by the run clock. Drivers hand back a `Date`, a
 * number, a string or a bignum depending on dialect and column type, so this is
 * the one place that variety is collapsed.
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
 * The delta's SELECT for one source and range: the source's OWN build SQL,
 * wrapped and filtered on the watermark.
 *
 * Built from `PersistSource.getSQL()` — the exact string the seed's `CREATE TABLE
 * AS` runs — rather than by compiling a Malloy query stage over the source, and
 * that is the load-bearing choice here.
 *
 * A query's SQL is FINALIZED: Malloy appends the dialect's `sqlFinalStage`
 * wherever `Dialect.hasFinalStage` is true, and Postgres is the only such
 * dialect. Its final stage is `SELECT row_to_json(finalStage) as row FROM …`, so
 * a compiled delta query yields ONE json column named `row` and an
 * `INSERT INTO … SELECT "col", …` off it fails with `column "col" does not
 * exist`. `PersistSource.getSQL()` is the same expression compiled UNfinalized,
 * which is why the seed's CTAS materializes real columns — and there is no
 * supported way to ask a QUERY for its unfinalized SQL, hence this composition.
 * Pinned as the TRIPWIRE in incremental_compiler_contract.spec.ts; see
 * docs/materialization.md, "Materializing on Postgres".
 *
 * Two things follow from reusing the seed's own SQL, both worth having: the
 * delta writes the same shape the seed wrote BY CONSTRUCTION, and the delta
 * cannot disagree with the seed about what the source computes.
 *
 * `SELECT *` because the caller's DML projects by name (see
 * {@link deltaStatements}), so this only has to narrow the rows.
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
// One rule governs every decision below: the delta path runs only on facts it
// has PROVEN about the target table, and every other outcome falls back to a
// SEED (a full rebuild through the ordinary CTAS + staging-rename). A seed is
// always correct and merely expensive, so an unprovable fact costs cost, never
// correctness. The author's hard errors live in the publish gates
// (incremental_policy.ts), where a fix is cheap; by the time a build runs, the
// only safe move is to build the table right.
//
// The second invariant is about the ledger: `covered_through` may LAG what the
// table actually contains, but it must never LEAD it. A lagging boundary makes
// the next delta recompute a range that is already correct — the range replace
// deletes before inserting, and the merge matches on identity, so a repeat is a
// no-op either way. A leading boundary would skip rows forever.
// ─────────────────────────────────────────────────────────────────────────────

/** How a probe or DML statement reaches the warehouse. */
export type SqlRunner = (sql: string) => Promise<{ rows: unknown[] }>;

/**
 * Per-dialect table-path decoding, for the dialects on the allowlist. The
 * physical name arrives as canonical SQL — bare segments or quoted ones — and an
 * `information_schema` lookup compares against the RAW identifier text, so the
 * surface form has to be decoded first. Postgres folds a bare segment to lower
 * case, Snowflake folds it to UPPER case, and BigQuery leaves it alone (its
 * dataset and table names are case-sensitive).
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
   // A `storage=` destination's own path — `<destination>.<name>`, or
   // `<destination>.<schema>.<name>` for a schema-qualified persist name. No case
   // folding: DuckDB resolves identifiers case-INSENSITIVELY but stores them as
   // written, and the metadata read below compares against the stored text, so
   // folding a bare segment would miss a table the build created from a
   // mixed-case name.
   //
   // `-` is admitted, as it is for `standardsql` above. What is decoded here is a
   // LOGICAL path the caller assembled from a destination name and a persist name,
   // not SQL an author wrote, and neither of those is charset-validated — so
   // rejecting a hyphen would leave `my-lake` (or `name="orders-v1"`) building and
   // serving perfectly while only this probe failed, which reads as `table_
   // unreadable` and seeds every run forever.
   duckdb: {
      quoteChar: '"',
      escapeStyle: "doubled",
      bareIdentRegex: /^[A-Za-z_][A-Za-z0-9_$-]*/,
   },
};

/**
 * The raw identifier segments of a physical table path, or undefined when the
 * path cannot be decoded for this dialect. Undefined is a SEED, not a throw: an
 * undecodable name means the probe below cannot be trusted to describe the table
 * the CTAS wrote.
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
 * Wrap a probe SELECT in whatever shape the dialect's driver hands back as named
 * columns.
 *
 * This exists for Postgres alone, and it is not cosmetic: `PostgresConnection`
 * post-processes every `runSQL` result with `rows[i] = rows[i].row`, because
 * Malloy's own queries always project a single `row` JSON column. A plain
 * `SELECT max(x) AS m` therefore comes back as `[undefined]`, not as `[{m: …}]`.
 * Wrapping in `row_to_json` gives the driver the column it expects to unwrap, so
 * every allowlisted dialect yields plain row objects. Pinned in
 * incremental_apply.spec.ts. (Snowflake's case hazard is the alias's, not the
 * row shape's — see {@link probeAlias}.)
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
 * A probe's output alias, dialect-quoted. Quoting is what pins the KEY the
 * probe reads back: Snowflake folds a bare alias to UPPERCASE (`AS
 * watermark_max` comes back as `WATERMARK_MAX`), so an unquoted alias would
 * make every probe read `undefined` there while working everywhere else.
 */
function probeAlias(name: string, dialect: string): string {
   return quoteIdentifier(name, dialect);
}

/**
 * The target table's column names in ordinal order, or a reason it could not be
 * read. An absent table reports `columns: []` with no error text on the dialects
 * here, because `information_schema` answers "no such table" as an empty result
 * rather than a failure — which is exactly the SEED signal.
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
      // `duckdb_columns()`, not `information_schema.columns`: an attached DuckLake
      // catalog exposes no `information_schema` at all (the read fails with
      // "schema information_schema does not exist"), and `DESCRIBE` THROWS on an
      // absent table — which the caller would have to translate back into the
      // empty result that means "seed". This function reports an absent table as
      // `columns: []` with no error on every other dialect, and that contract is
      // what the planner's `table_unreadable` vs. no-boundary distinction rests
      // on, so the read has to be one that answers emptily.
      const table = segments[segments.length - 1];
      // A two-segment path is `<destination>.<table>`, which the CTAS created in
      // the catalog's default schema. Named explicitly rather than left
      // unconstrained for the same reason the Postgres branch pins
      // `current_schema()`: two schemas in one catalog can hold the same table
      // name, and probing the wrong one would describe a table this build never
      // wrote.
      const schema =
         segments.length >= 3 ? segments[segments.length - 2] : "main";
      // The destination catalog is the FIRST segment, because the caller builds
      // this path as `<destination>.<name>` — counting back from the end instead
      // would read a longer name's own container as the catalog and probe a
      // database that is not this destination.
      const database = segments[0];
      sql = `SELECT column_name AS ${probeAlias("column_name", dialect)}
              FROM duckdb_columns()
              WHERE database_name = '${escapeSqlString(database, dialect)}'
                AND schema_name = '${escapeSqlString(schema, dialect)}'
                AND table_name = '${escapeSqlString(table, dialect)}'
              ORDER BY column_index`;
   } else if (dialect === "postgres" || dialect === "snowflake") {
      const table = segments[segments.length - 1];
      // An unqualified physical name lives in the session's default schema —
      // the same one the CTAS created it in. Do not widen this to "any schema
      // on the search path": two schemas can hold the same table name, and
      // probing the wrong one would describe a table this build never writes.
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

/**
 * Whether the target holds at least one row. Cheap by construction (`LIMIT 1`,
 * never a `count(*)`) because the question is existence, not size.
 */
export async function probeTargetNonEmpty(
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
 * `MAX(watermark)` over a SELECT, as a canonical bound. Undefined when the
 * result is NULL — an empty input, which is a range with nothing in it rather
 * than an error.
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
 * Postgres's `server_version_num`, for the MERGE floor. `MERGE` landed in
 * Postgres 15; on 14 and older it is a syntax error, and the only way to know
 * before issuing one is to ask.
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
export const POSTGRES_MERGE_MIN_VERSION_NUM = 150000;

/**
 * Compare two bounds of the same type. Text comparison is deliberate and works
 * for every type the gate admits: an ISO-8601 date or timestamp truncated to a
 * fixed width sorts lexically, and numbers are compared numerically below.
 * Returns a negative number, zero, or a positive number like a comparator.
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
 * as message text, or undefined when the two describe the same set.
 *
 * Order is ignored on purpose (the DML names every column explicitly, so ordinal
 * position never matters), but membership is not — and it can legitimately
 * differ. The delta SELECTS the same SQL the seed's CTAS ran, so the ROWS always
 * match by construction; what can diverge is the column list the DML names it
 * with, which is the source's compiled output schema. `getSQL()` projects columns
 * that schema does not describe — ones the source renames or hides (`rename:`,
 * `except:`, non-public access modifiers) — and those land in the table the seed
 * built. Pinned in incremental_compiler_contract.spec.ts. Every such source falls
 * back to a seed, which is the safe direction: naming a subset would rewrite the
 * unnamed columns to NULL.
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
 * Two strategies, and the choice is the author's `merge_key=`:
 *
 *  - MERGE (merge_key= declared): match the delta's rows against the table's by
 *    the declared identity columns, update the matches and insert the rest. This
 *    is the strategy that tolerates a row being RESTATED with a new watermark
 *    value, because identity is independent of the watermark.
 *
 *  - RANGE REPLACE (no merge_key=): delete the watermark range, then insert the
 *    delta. Correct whenever a row's watermark value never changes, and the only
 *    option on a table with no declared identity.
 *
 * Both are idempotent for a fixed range, which is what makes a crash between the
 * DML and the ledger advance survivable: the retry recomputes the same range and
 * converges to the same rows.
 *
 * The range replace is TWO statements, so it must be one transaction — without
 * one, a failure between the delete and the insert would leave the range empty
 * in a table that is already serving. That transaction is the whole reason for
 * the dialect allowlist, and each dialect provides it the one way its driver
 * can carry it: Postgres and BigQuery as a BEGIN…COMMIT multi-statement script
 * in one call, Snowflake as a single Snowflake Scripting block (its driver runs
 * exactly one statement per call, each on a possibly different pooled session,
 * so a script or a statement-per-call loop would both silently autocommit).
 */
export function deltaStatements(params: {
   dialect: string;
   quotedTablePath: string;
   deltaSQL: string;
   /** Columns to write, as the delta's own output names. */
   columns: string[];
   mergeKeys: string[];
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
      const keys = new Set(mergeKeys);
      // A NULL-safe match, because SQL equality is not: a NULL identity column
      // would fail `=` against its own copy, so the merge would insert the row
      // again on every run instead of updating it — a silent duplicate in a
      // serving table. The declared identity columns are usually NOT NULL, in
      // which case an engine simplifies this away.
      const on = mergeKeys
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
      // Every column being an identity column makes a matched row identical to
      // its replacement, so there is nothing to SET — and an empty SET list is a
      // syntax error. Omitting the clause is the correct reading of "matched
      // rows need no change", and MERGE accepts a NOT MATCHED clause alone.
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
 * The range replace as ONE statement for Snowflake: a Snowflake Scripting
 * anonymous block, wrapped in `EXECUTE IMMEDIATE $$…$$` because that is the form
 * drivers accept (a bare block is a console-only surface).
 *
 * The EXCEPTION handler is this dialect's version of {@link applyDeltaScript}'s
 * rollback, moved INSIDE the statement because outside is too late: a follow-up
 * `ROLLBACK` call would go to whatever pooled session the driver picks next,
 * not the one holding the open transaction. A failure without the handler would
 * park an open transaction on a pooled session, holding its locks until the
 * session dies. RAISE re-throws, so the build still fails loudly and the
 * boundary is not advanced.
 */
export function snowflakeScriptingBlock(statements: string[]): string {
   const body = statements.map((s) => `  ${s};`).join("\n");
   // `$$` is the block's own delimiter; SQL that contains it would end the
   // block early and execute the remainder as separate statements. No generated
   // delta contains one (Malloy renders string literals with backslash escaping
   // on Snowflake), so refuse loudly rather than escape: the throw lands in
   // planSourceRefresh's catch and the source seeds (reasonCode plan_error).
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
 * How the delta's statements are handed to the driver: as ONE call, because the
 * transaction has to be one unit of work.
 *
 * Postgres executes a multi-statement string through the simple query protocol
 * and returns the last statement's result; BigQuery accepts the same text as a
 * multi-statement script. A statement-per-call loop would put the BEGIN and the
 * COMMIT in different sessions on a pooled connection, which is how a
 * "transaction" silently becomes two autocommitted writes.
 */
export function deltaScript(statements: string[]): string {
   return statements.map((s) => `${s};`).join("\n");
}

/**
 * Run a delta's statements, rolling back the session if they fail.
 *
 * The rollback is not belt-and-braces. A failed multi-statement script leaves the
 * SESSION inside an aborted transaction, and a pooled connection is returned to
 * the pool in exactly that state — after which every unrelated statement that
 * borrows it fails with "current transaction is aborted" until something rolls
 * back. Neither Postgres nor node-postgres's pool does it on release, so a failed
 * delta would take out the connection rather than just itself. Proven in
 * incremental_dml_semantics.spec.ts, where omitting it poisons the rest of the
 * file.
 *
 * Best-effort and swallowed: the caller needs the ORIGINAL failure, and a
 * rollback that fails (a session already reset, a dialect with nothing to roll
 * back) changes nothing.
 */
/**
 * Dialects that abandon a failed script's transaction themselves, so the rollback
 * below is not merely unnecessary but noisy: DuckDB answers a `ROLLBACK` with no
 * active transaction by RAISING, which would log the "a pooled connection may
 * stay in an aborted transaction" warning on every failed delta — sending an
 * operator after a poisoned connection that this engine cannot have, since a
 * build session is private and single-use. Measured: after a failed
 * BEGIN/…/COMMIT script the session is immediately usable and nothing was
 * applied.
 */
const SELF_ROLLING_BACK_DIALECTS: ReadonlySet<string> = new Set(["duckdb"]);

export async function applyDeltaScript(
   runner: SqlRunner,
   dialect: string,
   statements: string[],
): Promise<void> {
   try {
      await runner(deltaScript(statements));
   } catch (err) {
      if (statements.length > 1 && !SELF_ROLLING_BACK_DIALECTS.has(dialect)) {
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
 * on which connection, computed from which SQL, advanced by which declaration. A
 * change to any of them means the recorded boundary describes a different table or
 * a different meaning of "covered", so it cannot be built on.
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
    * `connectionName`'s own warehouse.
    *
    * Compared, not merely recorded, for the same reason the content address is: a
    * source keeps its physical name AND its address when `storage=` is added or
    * removed (an annotation enters neither), so nothing else distinguishes a
    * boundary measured on the stored table from one measured on the colocated
    * table of the same name.
    */
   storageDestinationName?: string;
   /**
    * The source's CONTENT address — a fingerprint of the build SQL, not the
    * caller-assigned `BuildInstruction.sourceEntityId`. Compared rather than keyed
    * on, and the comparison is what guarantees a boundary is never read against SQL
    * other than the SQL it was computed from: an edited source seeds instead of
    * deltaing onto a table its definition no longer describes. That used to be the
    * ledger's key and so was true for free; now it is checked, because a table's
    * name does not have to change when its definition does (a standalone
    * publisher's names carry no content token).
    */
   sourceEntityId: string;
   watermarkName: string;
   /** The watermark's Malloy type (`date`, `timestamp`, `number`, `string`). */
   watermarkType: string;
   mergeKeys: string[];
   strategy: IncrementalStrategy;
}

/**
 * A recorded boundary as the plan reads it: the value, plus the table and the
 * declaration contract it was established under.
 *
 * A local ledger row is one of these (it also carries audit columns no decision
 * reads), and so is a caller-returned wire `LedgerEntry` — which is why this is
 * the shape the planner takes rather than the row type. Both arrive at the same
 * checks, so a boundary the caller echoed is trusted exactly as one the
 * publisher stored itself: it IS one the publisher stored, held by the caller.
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
 * Why a recorded boundary cannot be built on, or undefined when it can.
 *
 * Each of these is a case where the ledger's `covered_through` is a claim about
 * something OTHER than the delta about to run. Re-seeding is the only sound
 * answer: the boundary cannot be translated, and applying a delta against a
 * mismatched claim writes the wrong rows into a serving table.
 */
export function ledgerLineageMismatch(
   entry: RecordedBoundary,
   lineage: IncrementalLineage,
): { reasonCode: IncrementalStepReasonCode; reason: string } | undefined {
   // Carries its own reason code, separate from every other mismatch below,
   // because its causes are different in kind from theirs and so are its
   // remedies. The others mean the source's DEFINITION is not the one the
   // boundary was measured under, which has two causes depending on who is
   // driving: a standalone author editing the model in place, or — where packages
   // are immutable and a table is shared across a package's versions — a refresh
   // instructed through a different version than the one that established the
   // boundary, which a publish that touches this source or a rollback to an
   // earlier version both produce. Neither is anyone's mistake, which is why both
   // seed instead of failing. This one means the boundary was measured on a
   // different TABLE, under a definition that may not have moved at all. Two ways
   // THAT happens, and the reason names both because an
   // operator cannot tell them apart from the code alone: an orchestrated host
   // assigning a generational physical name per run, or two sources sharing one
   // content address and so fighting over one ledger row (which the publish gate
   // now advises on — see sharedAddressAdvisories). Seeding is still the only
   // sound answer either way: the other table is not this one, and a delta
   // measured against it would write the wrong rows here.
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
   // A different STORE is a different table, whatever the name says — so this
   // reports `table_renamed` alongside the name check above rather than
   // `lineage_changed`, which is about the source's definition having moved. The
   // remedy is the same (seed), but an operator reading the reason code needs to
   // know which of the two happened: a source that just gained or lost
   // `storage=` keeps both its name and its content address, so nothing else in
   // this function can tell.
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
   // Checked before the individual declarations because it subsumes them: a
   // different content address means different build SQL, and the boundary was
   // measured over rows that SQL may no longer produce. The declaration checks
   // below catch the narrower case of a definition that moves `watermark=` or
   // `merge_key=` — those do NOT move the address (proven in
   // incremental_compiler_contract.spec.ts), which is why both layers exist.
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
   // Order-sensitive, and that is not pedantry: the merge keys are recorded so a
   // NARROWED identity (dropping a key) forces a rebuild, since rows the old key
   // set kept apart would now collide.
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
         * The recorded boundary, which stays in force because nothing was
         * applied. Reported on the manifest entry so a caller sees the same
         * coverage for a skipped source as for one that advanced, rather than a
         * gap it has to interpret.
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
 * The upper bound of a delta's range, which is where the two watermark families
 * diverge.
 *
 * A DATE or TIMESTAMP watermark uses the run's own snapshot time: free, and
 * monotone without asking the warehouse anything.
 *
 * A NUMBER or STRING watermark has no clock, so the frontier has to be read —
 * and it has to be read from the SOURCE, not the target, since the target by
 * definition stops at the last boundary. That costs one extra pass over the
 * source query per refresh, which is the price of a non-temporal watermark and
 * is why a date is the better choice when the data offers one.
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
 * Where a delta's DML lands, and how its rows are produced.
 *
 * These are the only two facts that differ between a table materialized into the
 * source's own warehouse and one materialized into a `storage=` destination, and
 * they are the reason this is an interface rather than a dialect argument: a
 * stored table's rows are computed by the SOURCE warehouse while its DML is
 * issued in the DESTINATION engine, so one dialect cannot answer both.
 *
 * Everything else about a refresh — the order of the checks, which fallbacks are
 * a seed and which a skip, and the reason code each one reports — stays in
 * {@link planIncrementalStep}, deliberately. Those codes are the only signal that
 * a source declaring incremental refresh is quietly being rebuilt every run, so a
 * second copy of that decision order for the second target would be a second
 * place for it to drift.
 */
export interface DeltaTarget {
   /** The dialect the DML is written in, and the probes are quoted for. */
   dialect: string;
   /** The connection holding the table: every probe and the DML run here. */
   runner: SqlRunner;
   /** The table as the CREATE quoted it, for the DML and the emptiness probe. */
   quotedTablePath: string;
   /**
    * The same table as a LOGICAL (unquoted) path, for the metadata read that
    * describes its columns. Carried rather than taken from the lineage because a
    * stored table's metadata lives under its destination catalog, which the
    * lineage's `physicalTableName` does not name.
    */
   logicalTablePath: string;
   /**
    * The rows-yielding SELECT for one bounded range, written so the TARGET can
    * execute it. Asynchronous because producing it can mean issuing the source
    * read (a stored table's rows arrive through a query passthrough, which is
    * where the read is attributed and costed).
    */
   deltaRows(start: WatermarkBound, end: WatermarkBound): Promise<string>;
   /**
    * The SOURCE's watermark frontier, for a watermark that has no clock to take a
    * range end from (a number or a string). Read from the source rather than the
    * target because the target by definition stops at the last boundary.
    */
   probeSourceFrontier(): Promise<{ bound?: WatermarkBound; error?: string }>;
}

/**
 * The delta target for a COLOCATED source: the table lives in the source's own
 * warehouse, so one connection and one dialect answer everything, and the delta's
 * rows are the seed's own SQL with a range predicate around it.
 */
export function warehouseDeltaTarget(params: {
   dialect: string;
   runner: SqlRunner;
   quotedTablePath: string;
   lineage: IncrementalLineage;
   /**
    * The source's own build SQL — `PersistSource.getSQL()` resolved against the
    * build manifest, the exact string the seed's CTAS runs. The delta wraps and
    * filters it (see {@link deltaSelect}), and the frontier probe for a non-time
    * watermark reads it.
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
 * delta. Every negative answer is a SEED: see the rule at the top of this
 * section.
 *
 * The order of the checks is the order of increasing cost. `forceRefresh` and a
 * missing ledger row are free, the lineage comparison is free, and only then does
 * anything touch a warehouse.
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
      // Range replace is NOT a silent substitute here: it is the weaker
      // guarantee, and merge_key= says the author needs the stronger one.
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

   // A recorded boundary means an earlier run wrote rows, so an empty table now
   // means something outside these runs emptied it — and a delta would then
   // rebuild only the tail, leaving the history silently gone. (A seed that
   // wrote nothing records no boundary at all, precisely so that this check
   // cannot misfire on a legitimately empty source.)
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
      // Legitimate, and unfixable from here: a table-backed source that renames
      // or hides a column materializes getSQL()'s column names while the DML
      // names the source's compiled schema (see shapeMismatch). The rebuild
      // keeps the table correct, and the reason names the columns so the author
      // can drop the rename or the incremental declaration.
      return {
         mode: "seed",
         reasonCode: "shape_mismatch",
         reason:
            `the delta's columns do not match the target table's (${shape}), ` +
            `so a delta would write a different shape than the table holds`,
      };
   }

   // Built LAST, after every check has passed, because producing it can issue the
   // source read (see DeltaTarget.deltaRows): a read paid for and then discarded
   // by a check below it would be the one avoidable cost on this path.
   const statements = deltaStatements({
      dialect,
      quotedTablePath: target.quotedTablePath,
      deltaSQL: await target.deltaRows(start, end.bound),
      columns: inputs.columns,
      mergeKeys: lineage.mergeKeys,
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
 * The boundary to record after a SEED, or undefined when the seed wrote nothing
 * to be a boundary for.
 *
 * Read from the TARGET, after the CTAS: the table now holds exactly the source's
 * snapshot, so its `MAX(watermark)` is the source's frontier at a fraction of the
 * cost of re-running the source query.
 *
 * A time watermark still records the run's SNAPSHOT rather than that maximum,
 * because a table may legitimately hold future-dated rows and the boundary must
 * not claim coverage of a range the clock has not reached. Those rows are above
 * the boundary, so the next delta recomputes them — the harmless direction.
 *
 * A NULL maximum records nothing at all, which leaves the next run to seed again.
 * That is the deliberate answer for an empty source: a boundary set from an empty
 * seed would silently exclude every historical row that arrives afterwards, since
 * they would all land below it.
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
