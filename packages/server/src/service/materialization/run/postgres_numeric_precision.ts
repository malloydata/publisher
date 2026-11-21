// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Keep a Postgres `numeric`'s exact value in a `storage=` table.
 *
 * DuckDB's Postgres passthrough reads a `numeric` with no typmod (an
 * unconstrained column, and every numeric expression: `a + b`, `sum(x)`, a
 * window sum, a rollup's partial) and a `numeric(p,s)` with p > 38 as DOUBLE,
 * so `sum` of 0.1 and 0.2 would serve `0.30000000000000004` where live answers
 * `0.3`. A `numeric(p,s)` with p <= 38 already arrives as DECIMAL(p,s).
 *
 * The build therefore reads each such column as Postgres's own text, which is
 * exact, stages the rows in the build session, and stores the column as the
 * DECIMAL(38, s) that holds every value it read: `s` the widest scale seen,
 * trailing zeros dropped, provided the widest integer part still fits in 38
 * digits. A column that does not fit (or holds `NaN` / `Infinity`) is stored as
 * that text, which the serve side's read plan answers live and reports under
 * `storage-column-type-mismatch`, rather than rounded.
 *
 * Which columns: the ones the session describes as DOUBLE whose Postgres type
 * (a domain's base type) is `numeric`, read from a probe Postgres plans but
 * does not run (a `LIMIT 0` the probe left-joins to one row, so `pg_typeof`
 * sees each column's type). A `double precision` stays DOUBLE: live sums it as
 * a float too.
 *
 * What it costs: every full Postgres build describes its passthrough (Postgres
 * prepares the statement; it replaces the describe the declared-type cast
 * made for a source with a `string` column), and runs the type probe when any
 * column reads as DOUBLE. A build that has a numeric column to keep writes
 * the whole result twice: once to the session's local stage (spilling to the
 * build's temp directory), once to the destination. The warehouse is read once.
 */

import type { DuckDBConnection } from "@malloydata/db-duckdb";
import { logger } from "../../../logger";
import { escapeSQL } from "../../duckdb_session_setup";
import { quoteIdentifier } from "../../quoting";
import { STORED_COLUMN_TYPE_MISMATCH_CODE } from "../stored_column_types";
import { STORAGE_DIALECT } from "../domain/dialect";
import { errorCodeOf } from "../../../error_code";
import { fromSubselect } from "../subselect";
import { errMessage } from "../../../utils";

/** DuckDB's widest DECIMAL. */
const DECIMAL_MAX_PRECISION = 38;

/** What one numeric column's staged text holds. */
interface NumericTextStats {
   column: string;
   /** Digits left of the point, leading zeros dropped; null with no value. */
   maxIntegerDigits: number | null;
   /** Digits right of the point, as Postgres wrote them; null with no value. */
   maxScale: number | null;
   /** Values that are not a finite decimal (`NaN`, `Infinity`). */
   nonFinite: number;
}

/** How a numeric column is stored. */
type NumericStorage =
   | { column: string; decimal: string; scale: number }
   | {
        column: string;
        /** Stored as Postgres's text, which the serve side answers live. */
        text: true;
        reason: "non_finite" | "exceeds_decimal_38";
     };

/**
 * The DuckDB type that holds every value `stats` describes exactly:
 * DECIMAL(38, s) with `s` the widest scale seen (38 keeps the most room for
 * the integer part, and DuckDB sums any DECIMAL as DECIMAL(38, s) anyway). A
 * column with no value is DECIMAL(38, 0): it has nothing to round, and an
 * incremental delta that brings a wider scale re-seeds
 * ({@link isDecimalDeltaGuardFailure}).
 */
export function numericStorageFor(stats: NumericTextStats): NumericStorage {
   if (stats.nonFinite > 0) {
      return { column: stats.column, text: true, reason: "non_finite" };
   }
   const scale = stats.maxScale ?? 0;
   const integerDigits = stats.maxIntegerDigits ?? 0;
   if (integerDigits + scale > DECIMAL_MAX_PRECISION) {
      return { column: stats.column, text: true, reason: "exceeds_decimal_38" };
   }
   return {
      column: stats.column,
      decimal: `DECIMAL(${DECIMAL_MAX_PRECISION},${scale})`,
      scale,
   };
}

/** Postgres's text for a finite numeric: an optional sign, digits, a fraction. */
const FINITE = "^-?[0-9]+(\\.[0-9]*)?$";

/** The DuckDB aggregates {@link NumericTextStats} reads, over a text column. */
function statsSelect(column: string, index: number): string {
   const c = quoteIdentifier(column, STORAGE_DIALECT);
   const finite = `regexp_full_match(${c}, '${FINITE}')`;
   const integerPart = `split_part(ltrim(${c}, '-'), '.', 1)`;
   return [
      `max(CASE WHEN ${finite} THEN length(ltrim(${integerPart}, '0')) END) AS i${index}`,
      // Trailing zeros dropped: `2.50` needs scale 1, and a narrower scale
      // leaves DuckDB's DECIMAL arithmetic more room.
      `max(CASE WHEN ${finite} THEN length(rtrim(split_part(${c}, '.', 2), '0')) END) AS s${index}`,
      `count(*) FILTER (WHERE ${c} IS NOT NULL AND NOT ${finite}) AS n${index}`,
   ].join(", ");
}

/**
 * A Postgres SELECT returning `innerSQL`'s columns in order, each of
 * `numericColumns` as its text.
 */
export function numericAsTextSQL(
   innerSQL: string,
   columns: readonly string[],
   numericColumns: readonly string[],
   sourceDialect: string,
): string {
   const numeric = new Set(numericColumns);
   const list = columns.map((name) => {
      const c = quoteIdentifier(name, sourceDialect);
      return numeric.has(name) ? `${c}::text AS ${c}` : c;
   });
   return `SELECT ${list.join(", ")} ${fromSubselect(innerSQL, "__numeric_text")}`;
}

/**
 * The columns of `innerSQL` that DuckDB would read as DOUBLE although Postgres
 * types them `numeric`, with every column's name in order. Nothing runs in
 * Postgres: the session describes the passthrough (Postgres prepares it), and
 * the type probe reads a `LIMIT 0`.
 *
 * A describe or probe that fails throws, and the build fails: without the
 * types, any DOUBLE column may be an exact `numeric` the passthrough would
 * round, and a rounded key or gate column stored past 2^53 matches its
 * neighbours (two tenants' ids read as one). Storing it as DOUBLE anyway would
 * serve rows live never returns, so the source is not built and answers live.
 */
export async function postgresNumericColumns(
   session: DuckDBConnection,
   handle: string,
   innerSQL: string,
   sourceDialect: string,
): Promise<{
   columns: string[];
   numeric: string[];
   /** Each column as the session reads it, for the declared-type casts; absent when the describe failed. */
   described?: { name: string; type: string }[];
}> {
   const passthrough = (sql: string) =>
      `SELECT * FROM postgres_query('${escapeSQL(handle)}', '${escapeSQL(sql)}')`;
   try {
      const described = rowsOf(
         await session.runSQL(`DESCRIBE (${passthrough(innerSQL)})`),
      ).map((r) => ({
         name: String(r.column_name),
         type: String(r.column_type),
      }));
      const columns = described.map((c) => c.name);
      const doubles = described
         .filter((c) => c.type.toUpperCase() === "DOUBLE")
         .map((c) => c.name);
      if (doubles.length === 0) return { columns, numeric: [], described };
      const probe =
         `SELECT ${doubles
            .map(
               (name, i) =>
                  // A domain reads as its base type.
                  `(SELECT coalesce(nullif(typbasetype, 0), oid)::regtype::text FROM pg_type WHERE oid = pg_typeof(q.${quoteIdentifier(name, sourceDialect)})) AS t${i}`,
            )
            .join(", ")} ` +
         `FROM (SELECT 1) AS one LEFT JOIN ` +
         `(SELECT * ${fromSubselect(innerSQL, "b")} LIMIT 0) AS q ON true`;
      const [types] = rowsOf(await session.runSQL(passthrough(probe)));
      const numeric = doubles.filter((_, i) => types?.[`t${i}`] === "numeric");
      return { columns, numeric, described };
   } catch (err) {
      logger.warn(
         "Could not read a Postgres storage build's column types; the build fails rather than store a numeric column as DOUBLE",
         {
            code: STORED_COLUMN_TYPE_MISMATCH_CODE,
            errorCode: errorCodeOf(err),
         },
      );
      // The cause keeps the driver's own error, which says whether a retry
      // could succeed (`build_failure_class.ts`).
      throw new Error(
         `Could not read the Postgres column types this storage build needs to store a numeric column exactly: ${errMessage(err)}`,
         { cause: err },
      );
   }
}

/** The local temporary table a build's text-read rows are staged in. */
const STAGE = "__numeric_text_stage";

/**
 * Stage `selectSQL`'s rows (the passthrough reading each numeric column as
 * text) in the build session, and return the SELECT that reads them back with
 * each column at the type that holds it ({@link numericStorageFor}), plus a
 * `drop` to run once the table is written. One warehouse read: the stage is
 * local, and the build (and its cast retry) reads the stage.
 */
export async function stageNumericColumns(
   session: DuckDBConnection,
   selectSQL: string,
   numericColumns: readonly string[],
   context: { table: string },
): Promise<{
   selectSQL: string;
   storage: NumericStorage[];
   drop: () => Promise<void>;
}> {
   const stage = `temp.main.${quoteIdentifier(STAGE, STORAGE_DIALECT)}`;
   await session.runSQL(
      `CREATE OR REPLACE TEMP TABLE ${quoteIdentifier(STAGE, STORAGE_DIALECT)} AS ${selectSQL}`,
   );
   const drop = async () => {
      try {
         await session.runSQL(`DROP TABLE IF EXISTS ${stage}`);
      } catch {
         // The session is disposed with the build, and the stage with it.
      }
   };
   try {
      const [row] = rowsOf(
         await session.runSQL(
            `SELECT ${numericColumns.map(statsSelect).join(", ")} FROM ${stage}`,
         ),
      );
      const storage = numericColumns.map((column, i) =>
         numericStorageFor({
            column,
            maxIntegerDigits: nullableNumber(row?.[`i${i}`]),
            maxScale: nullableNumber(row?.[`s${i}`]),
            nonFinite: Number(row?.[`n${i}`] ?? 0),
         }),
      );
      const kept = storage.filter(
         (s): s is Extract<NumericStorage, { text: true }> => "text" in s,
      );
      if (kept.length > 0) {
         logger.warn(
            "Storage build kept Postgres numeric columns as text: no DECIMAL holds their values exactly, so queries reading them are answered live",
            {
               code: STORED_COLUMN_TYPE_MISMATCH_CODE,
               table: context.table,
               columns: kept.map((k) => ({
                  column: k.column,
                  reason: k.reason,
               })),
            },
         );
      }
      const casts = storage.flatMap((s) =>
         "decimal" in s
            ? [
                 `CAST(${quoteIdentifier(s.column, STORAGE_DIALECT)} AS ${s.decimal}) AS ${quoteIdentifier(s.column, STORAGE_DIALECT)}`,
              ]
            : [],
      );
      return {
         selectSQL:
            casts.length > 0
               ? `SELECT * REPLACE (${casts.join(", ")}) FROM ${stage}`
               : `SELECT * FROM ${stage}`,
         storage,
         drop,
      };
   } catch (err) {
      await drop();
      throw err;
   }
}

/** The text a delta's precision guard fails with; {@link isDecimalDeltaGuardFailure} reads it. */
const DECIMAL_GUARD_MARKER = "storage-decimal-delta-does-not-fit";

/** Whether `err` is a delta's precision guard refusing a value its column cannot hold. */
export function isDecimalDeltaGuardFailure(err: unknown): boolean {
   const message = err instanceof Error ? err.message : String(err);
   return message.includes(DECIMAL_GUARD_MARKER);
}

/** `DECIMAL(p,s)` as DuckDB's DESCRIBE writes it, or undefined. */
export function decimalTypeOf(
   storedType: string,
): { precision: number; scale: number } | undefined {
   const m = /^DECIMAL\((\d+),\s*(\d+)\)$/i.exec(storedType.trim());
   return m ? { precision: Number(m[1]), scale: Number(m[2]) } : undefined;
}

/**
 * Wrap a delta's DuckDB SELECT (the passthrough reading each of `columns` as
 * Postgres's text) so each text value is cast to the stored DECIMAL when it fits
 * exactly, and the delta fails with {@link DECIMAL_GUARD_MARKER} when one does
 * not (a wider scale or integer part than the seed saw), which re-seeds the
 * table rather than rounding the value into it. A column stored as text keeps
 * the text.
 */
export function guardDeltaDecimals(
   selectSQL: string,
   columns: readonly { column: string; storedType: string }[],
): string {
   const casts = columns.flatMap(({ column, storedType }) => {
      const decimal = decimalTypeOf(storedType);
      if (!decimal) return [];
      const c = quoteIdentifier(column, STORAGE_DIALECT);
      const integerDigits = decimal.precision - decimal.scale;
      const fits =
         `regexp_full_match(${c}, '^-?0*[0-9]{0,${integerDigits}}` +
         // Trailing zeros are not digits the column must hold.
         `(\\.[0-9]{0,${decimal.scale}}0*)?$')`;
      return [
         `CASE WHEN ${c} IS NULL THEN NULL WHEN ${fits} ` +
            `THEN CAST(${c} AS DECIMAL(${decimal.precision},${decimal.scale})) ` +
            `ELSE error('${DECIMAL_GUARD_MARKER}: ${escapeSQL(column)}') END AS ${c}`,
      ];
   });
   if (casts.length === 0) return selectSQL;
   return `SELECT * REPLACE (${casts.join(", ")}) ${fromSubselect(selectSQL, "__delta_decimals")}`;
}

function nullableNumber(value: unknown): number | null {
   if (value === null || value === undefined) return null;
   const n = Number(value);
   return Number.isFinite(n) ? n : null;
}

function rowsOf(result: unknown): Record<string, unknown>[] {
   if (Array.isArray(result)) return result as Record<string, unknown>[];
   return ((result as { rows?: unknown[] })?.rows ?? []) as Record<
      string,
      unknown
   >[];
}
