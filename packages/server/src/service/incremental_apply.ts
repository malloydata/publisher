import type { ModelMaterializer } from "@malloydata/malloy";

import { errMessage } from "../utils";

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

/** Dialects whose transactional multi-statement DML the delta apply relies on. */
export const INCREMENTAL_DIALECT_ALLOWLIST: ReadonlySet<string> = new Set([
   "bigquery",
   "postgres",
]);

/** Dialects with a `MERGE INTO` statement, required by `merge_key=`. */
export const MERGE_CAPABLE_DIALECTS: ReadonlySet<string> = new Set([
   "bigquery",
   "postgres",
]);

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
 * `select: *` rather than a named projection on purpose — the delta's shape must
 * match whatever the seed CTAS wrote, and naming columns here would silently
 * drop any the model's atomic view does not enumerate.
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
