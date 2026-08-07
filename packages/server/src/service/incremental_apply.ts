import {
   decodeDottedTablePath,
   type ModelMaterializer,
} from "@malloydata/malloy";

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
 * The delta is always a QUERY STAGE over the author's own source:
 *
 *   run: `daily_revenue` -> { where: `order_date` >= @start and `order_date` < @end; select: * }
 *
 * It is never a generated source extension. `source: __d is base extend { where: … }`
 * compiles clean and silently DROPS the predicate (pinned as a canary in
 * incremental_compiler_contract.spec.ts), which would turn a delta that reports a
 * bounded range into one that rewrites every row.
 *
 * The range is HALF-OPEN, `[start, end)`. That is what makes a run idempotent:
 * re-running the same range deletes and re-inserts (or re-merges) exactly the
 * rows it did the first time, so a crash between the commit and the ledger
 * advance costs a repeat, not a corruption.
 */

/**
 * Dialects whose transactional multi-statement DML the delta apply relies on.
 *
 * Keyed by Malloy's `dialectName`, which is why BigQuery appears as
 * `standardsql` — the same key `quoting.ts` uses for its backtick set. Keying
 * these by the friendly name instead is a silent no-op: no source's
 * `dialectName` is ever "bigquery", so every BigQuery source would fail the
 * allowlist while the message claimed BigQuery was supported.
 */
export const INCREMENTAL_DIALECT_ALLOWLIST: ReadonlySet<string> = new Set([
   "postgres",
   "standardsql",
]);

/** Dialects with a `MERGE INTO` statement, required by `merge_key=`. */
export const MERGE_CAPABLE_DIALECTS: ReadonlySet<string> = new Set([
   "postgres",
   "standardsql",
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
 * Deliberately not a pre-rendered literal: the same boundary has to appear both
 * as a Malloy literal (in the delta query) and as a SQL literal (in the range
 * DELETE), and those spellings differ. Storing the canonical text keeps one
 * value in the ledger that both renderers read.
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

/** Escape a string for a single-quoted Malloy literal (mirrors filter.ts). */
function escapeMalloyString(value: string): string {
   return value.replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

/** Escape a string for a single-quoted SQL literal (doubling the quote). */
function escapeSqlString(value: string): string {
   return value.replace(/'/g, "''");
}

/** Quote a Malloy identifier so a reserved word or odd name still parses. */
export function quoteMalloyIdentifier(name: string): string {
   return "`" + name.replace(/\\/g, "\\\\").replace(/`/g, "\\`") + "`";
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
 * The bound as a Malloy literal, spelled for the watermark's own type.
 *
 * Type-matching is not cosmetic: comparing a `date` column to a timestamp
 * literal is a Malloy COMPILE ERROR ("Cannot compare a date to a timestamp"),
 * pinned in the contract spec. A single all-timestamps renderer would fail every
 * date-watermarked source, and only at build time.
 */
export function renderMalloyBound(bound: WatermarkBound): string {
   switch (bound.malloyType) {
      case "date":
         return `@${datePrecision(bound.value)}`;
      case "timestamp":
         return `@${secondsPrecision(bound.value)}`;
      case "number":
         return renderNumber(bound.value);
      case "string":
         return `'${escapeMalloyString(bound.value)}'`;
      default:
         throw new Error(
            `watermark type '${bound.malloyType}' has no literal rendering`,
         );
   }
}

/** The bound as a SQL literal for the range DELETE. */
export function renderSqlBound(bound: WatermarkBound): string {
   switch (bound.malloyType) {
      case "date":
         return `DATE '${datePrecision(bound.value)}'`;
      case "timestamp":
         return `TIMESTAMP '${secondsPrecision(bound.value)}'`;
      case "number":
         return renderNumber(bound.value);
      case "string":
         return `'${escapeSqlString(bound.value)}'`;
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
 * The delta query for one source and range: a query stage that filters the
 * source's OUTPUT rows by the watermark and selects them all.
 *
 * `select: *` rather than a named projection on purpose. The delta has to write
 * the shape the seed CTAS wrote, and the seed writes whatever the source's own
 * SQL produces — so anything enumerated here would be this module's guess at
 * that shape rather than the compiler's own answer. The two shapes are still
 * COMPARED before any DML runs, because they can legitimately differ (see
 * {@link shapeMismatch}).
 */
export function deltaQueryText(params: {
   sourceName: string;
   watermarkName: string;
   start: WatermarkBound;
   end: WatermarkBound;
}): string {
   const source = quoteMalloyIdentifier(params.sourceName);
   const watermark = quoteMalloyIdentifier(params.watermarkName);
   const start = renderMalloyBound(params.start);
   const end = renderMalloyBound(params.end);
   return (
      `run: ${source} -> {\n` +
      `  where: ${watermark} >= ${start} and ${watermark} < ${end}\n` +
      `  select: *\n` +
      `}`
   );
}

/**
 * Bounds used only to prove a delta query COMPILES. Never executed, and never
 * written to the ledger — a real run derives its own range.
 */
export function placeholderBounds(malloyType: string): {
   start: WatermarkBound;
   end: WatermarkBound;
} {
   switch (malloyType) {
      case "date":
         return {
            start: { malloyType, value: "2000-01-01" },
            end: { malloyType, value: "2000-01-02" },
         };
      case "timestamp":
         return {
            start: { malloyType, value: "2000-01-01 00:00:00" },
            end: { malloyType, value: "2000-01-02 00:00:00" },
         };
      case "number":
         return {
            start: { malloyType, value: "0" },
            end: { malloyType, value: "1" },
         };
      default:
         return {
            start: { malloyType, value: "" },
            end: { malloyType, value: "\uffff" },
         };
   }
}

/**
 * Compile — never run — the delta query for a source, returning the compile
 * error if there is one and undefined if it compiles.
 *
 * This is the publish-time proof that a source declared incremental can actually
 * produce a delta. Everything the static gates check is necessary but not
 * sufficient: the range predicate has to type-check against the watermark, and
 * the source has to admit a query stage at all. Failing that here costs a
 * compile at publish; failing it later costs a run that cannot advance.
 */
export async function trialCompileDeltaQuery(params: {
   materializer: ModelMaterializer;
   sourceName: string;
   watermarkName: string;
   watermarkType: string;
}): Promise<string | undefined> {
   const bounds = placeholderBounds(params.watermarkType);
   let query: string;
   try {
      query = deltaQueryText({
         sourceName: params.sourceName,
         watermarkName: params.watermarkName,
         start: bounds.start,
         end: bounds.end,
      });
   } catch (err) {
      return errMessage(err);
   }
   try {
      await params.materializer.loadQuery(query).getSQL();
      return undefined;
   } catch (err) {
      return errMessage(err);
   }
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
 * Per-dialect table-path decoding, for the two dialects on the allowlist. The
 * physical name arrives as canonical SQL — bare segments or quoted ones — and an
 * `information_schema` lookup compares against the RAW identifier text, so the
 * surface form has to be decoded first. Postgres folds a bare segment to lower
 * case; BigQuery leaves it alone (its dataset and table names are
 * case-sensitive).
 */
const TABLE_PATH_OPTIONS: Record<
   string,
   {
      quoteChar: string;
      escapeStyle: "doubled";
      bareIdentRegex: RegExp;
      foldsCase: boolean;
   }
> = {
   postgres: {
      quoteChar: '"',
      escapeStyle: "doubled",
      bareIdentRegex: /^[A-Za-z_][A-Za-z0-9_$]*/,
      foldsCase: true,
   },
   standardsql: {
      quoteChar: "`",
      escapeStyle: "doubled",
      bareIdentRegex: /^[A-Za-z_][A-Za-z0-9_-]*/,
      foldsCase: false,
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
   return decoded.segments.map((s) =>
      s.quoted || !opts.foldsCase ? s.value : s.value.toLowerCase(),
   );
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
 * both dialects yield plain row objects. Pinned in incremental_apply.spec.ts.
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
   if (dialect === "postgres") {
      const table = segments[segments.length - 1];
      // A Postgres physical name is often unqualified, and `current_schema()` is
      // then the schema the CTAS created it in — the same session default. Do not
      // widen this to "any schema on the search_path": two schemas can hold the
      // same table name, and probing the wrong one would describe a table this
      // build never writes.
      const schema =
         segments.length >= 2
            ? `'${escapeSqlString(segments[segments.length - 2])}'`
            : "current_schema()";
      sql = probeSelect(
         dialect,
         `SELECT column_name FROM information_schema.columns
           WHERE table_schema = ${schema}
             AND table_name = '${escapeSqlString(table)}'
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
      sql = `SELECT column_name FROM ${container}.INFORMATION_SCHEMA.COLUMNS
              WHERE table_name = '${escapeSqlString(table)}'
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
      `SELECT MAX(${column}) AS watermark_max FROM (${innerSelect}) AS __w`,
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
 * differ. The seed CTAS materializes the source's OWN SQL, while the delta is a
 * query stage over the source, and a table-backed source that renames or hides a
 * column produces different column NAMES in the two: `rename: order_date is
 * ord_dt` writes `ord_dt` in the table and emits `order_date` from the query.
 * Pinned in incremental_compiler_contract.spec.ts.
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
 * The range replace is TWO statements, so it is wrapped in an explicit
 * transaction — without one, a failure between the delete and the insert would
 * leave the range empty in a table that is already serving. That transaction is
 * the whole reason for the dialect allowlist.
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
      `${watermark} >= ${renderSqlBound(params.start)} AND ` +
      `${watermark} < ${renderSqlBound(params.end)}`;
   const body = [
      `DELETE FROM ${quotedTablePath} WHERE ${range}`,
      `INSERT INTO ${quotedTablePath} (${columnList}) ` +
         `SELECT ${columnList} FROM (${deltaSQL}) AS __s`,
   ];
   const transaction = transactionKeywords(dialect);
   return [transaction.begin, ...body, transaction.commit];
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
 * on which connection, advanced by which declaration. A change to any of them
 * means the recorded boundary describes a different table or a different meaning
 * of "covered", so it cannot be built on.
 */
export interface IncrementalLineage {
   /** Logical (unquoted) physical table name, as the manifest records it. */
   physicalTableName: string;
   connectionName: string;
   watermarkName: string;
   /** The watermark's Malloy type (`date`, `timestamp`, `number`, `string`). */
   watermarkType: string;
   mergeKeys: string[];
   strategy: IncrementalStrategy;
}

/**
 * Why a recorded boundary cannot be built on, or undefined when it can.
 *
 * Each of these is a case where the ledger's `covered_through` is a claim about
 * something OTHER than the delta about to run. Re-seeding is the only sound
 * answer: the boundary cannot be translated, and applying a delta against a
 * mismatched claim writes the wrong rows into a serving table.
 */
export function ledgerLineageMismatch(
   entry: IncrementalLedgerEntry,
   lineage: IncrementalLineage,
): string | undefined {
   if (entry.physicalTableName !== lineage.physicalTableName) {
      return (
         `the recorded boundary belongs to table ` +
         `"${entry.physicalTableName}", not "${lineage.physicalTableName}"`
      );
   }
   if (entry.connectionName !== lineage.connectionName) {
      return (
         `the recorded boundary was advanced on connection ` +
         `"${entry.connectionName}", not "${lineage.connectionName}"`
      );
   }
   if (entry.watermarkDimension !== lineage.watermarkName) {
      return (
         `the watermark changed from "${entry.watermarkDimension}" to ` +
         `"${lineage.watermarkName}", so the recorded value measures a ` +
         `different column`
      );
   }
   if (entry.coveredThroughType !== lineage.watermarkType) {
      return (
         `the watermark's type changed from "${entry.coveredThroughType}" to ` +
         `"${lineage.watermarkType}"`
      );
   }
   if (entry.derivedStrategy !== lineage.strategy) {
      return (
         `the strategy changed from ${entry.derivedStrategy} to ` +
         `${lineage.strategy}`
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
      return (
         `merge_key= changed from [${recorded.join(", ")}] to ` +
         `[${lineage.mergeKeys.join(", ")}]`
      );
   }
   return undefined;
}

/** What one source's refresh should do. */
export type IncrementalStep =
   | {
        mode: "seed";
        /** Why a full rebuild, for the log and the metric label. */
        reason: string;
     }
   | {
        mode: "skip";
        reason: string;
     }
   | {
        mode: "delta";
        start: WatermarkBound;
        end: WatermarkBound;
        /** The boundary to record once the DML commits (always `end`). */
        coveredThrough: WatermarkBound;
        statements: string[];
        script: string;
        columns: string[];
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
   runner: SqlRunner,
   dialect: string,
   lineage: IncrementalLineage,
   sourceSQL: string,
   now: Date,
): Promise<{ bound?: WatermarkBound; error?: string }> {
   const type = lineage.watermarkType;
   if (type === "date" || type === "timestamp") {
      return { bound: snapshotBound(type, now) };
   }
   return probeMaxWatermark(
      runner,
      dialect,
      sourceSQL,
      lineage.watermarkName,
      type,
   );
}

/**
 * Decide what this source's refresh does, and build the delta's SQL when it is a
 * delta. Every negative answer is a SEED: see the rule at the top of this
 * section.
 *
 * The order of the checks is the order of increasing cost. `forceRefresh` and a
 * missing ledger row are free, the lineage comparison is free, and only then does
 * anything touch the warehouse.
 */
export async function planIncrementalStep(inputs: {
   runner: SqlRunner;
   dialect: string;
   /** The target as the CREATE quoted it, for the DML and the probes. */
   quotedTablePath: string;
   lineage: IncrementalLineage;
   ledgerEntry: IncrementalLedgerEntry | null;
   forceRefresh: boolean;
   /** The run's start time, used as the range end for a time watermark. */
   now: Date;
   /** The source's own build SQL, for the frontier probe (non-time watermarks). */
   sourceSQL: string;
   /** Compile the delta query for a range into SQL plus its output columns. */
   compileDelta: (
      start: WatermarkBound,
      end: WatermarkBound,
   ) => Promise<{ sql: string; columns: string[] }>;
   /** Postgres only: the server version, for the MERGE floor. */
   postgresVersionNum?: number;
}): Promise<IncrementalStep> {
   const { runner, dialect, lineage, ledgerEntry } = inputs;

   if (inputs.forceRefresh) {
      return { mode: "seed", reason: "a full refresh was requested" };
   }
   if (!ledgerEntry) {
      return {
         mode: "seed",
         reason: "no covered_through boundary is recorded for this source yet",
      };
   }
   const mismatch = ledgerLineageMismatch(ledgerEntry, lineage);
   if (mismatch) return { mode: "seed", reason: mismatch };

   if (
      lineage.strategy === "merge" &&
      dialect === "postgres" &&
      (inputs.postgresVersionNum ?? 0) < POSTGRES_MERGE_MIN_VERSION_NUM
   ) {
      // Range replace is NOT a silent substitute here: it is the weaker
      // guarantee, and merge_key= says the author needs the stronger one.
      return {
         mode: "seed",
         reason:
            `merge_key= needs MERGE, which this Postgres server ` +
            `(server_version_num ${inputs.postgresVersionNum ?? "unknown"}) ` +
            `does not have; MERGE requires Postgres 15`,
      };
   }

   const probe = await probeTargetColumns(
      runner,
      dialect,
      lineage.physicalTableName,
   );
   if (probe.columns.length === 0) {
      return {
         mode: "seed",
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
      inputs.quotedTablePath,
   );
   if (!rows.nonEmpty) {
      return {
         mode: "seed",
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
   const end = await resolveDeltaEnd(
      runner,
      dialect,
      lineage,
      inputs.sourceSQL,
      inputs.now,
   );
   if (!end.bound) {
      return {
         mode: "skip",
         reason: end.error
            ? `the watermark frontier could not be read (${end.error})`
            : "the source has no rows with a non-null watermark",
      };
   }
   if (compareBounds(end.bound, start) <= 0) {
      return {
         mode: "skip",
         reason:
            `the watermark has not advanced past ${start.value} ` +
            `(frontier: ${end.bound.value})`,
      };
   }

   const delta = await inputs.compileDelta(start, end.bound);
   const shape = shapeMismatch(delta.columns, probe.columns);
   if (shape) {
      // Legitimate, and unfixable from here: a table-backed source that renames
      // or hides a column materializes its OWN column names while a query stage
      // over it emits the source's names. The rebuild keeps the table correct,
      // and the reason names the columns so the author can drop the rename or the
      // incremental declaration.
      return {
         mode: "seed",
         reason:
            `the delta's columns do not match the target table's (${shape}), ` +
            `so a delta would write a different shape than the table holds`,
      };
   }

   const statements = deltaStatements({
      dialect,
      quotedTablePath: inputs.quotedTablePath,
      deltaSQL: delta.sql,
      columns: delta.columns,
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
      script: deltaScript(statements),
      columns: delta.columns,
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
