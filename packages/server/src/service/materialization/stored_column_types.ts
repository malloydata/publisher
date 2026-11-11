// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Stored column types against the types Malloy declares (ms2data/service#6642).
 *
 * A `storage=` table's column types are whatever the warehouse returned, which
 * need not be the type Malloy declares for the field: Postgres answers a date
 * truncation with a timestamp, DuckDB answers `date + 1 month` with one, and a
 * `sql_boolean` over an integer stays an integer. The serve shape types each
 * column from its STORED type, so such a column serves as a different type than
 * live: a date compared with a date column no longer compiles, and a boolean
 * groups as 0 and 1.
 *
 * Two halves, one table:
 *
 * - At build, a column whose declared type has a single faithful DuckDB type
 *   ({@link BUILD_CAST_TARGETS}) is CAST to it in the build's SELECT, so the
 *   table stores the declared type whatever the warehouse returned.
 * - At serve, a column whose stored type does not serve as its declared type
 *   ({@link storedTypeServesAs}) is read through a cast to its declared type
 *   when one is faithful ({@link serveTypePlan}), so a table an older build
 *   typed still serves every field built on it. A column no cast can make
 *   faithful (a number stored as text) is left off the shape when nothing on
 *   the shape names it, so only queries reading it are answered live; when a
 *   refinement names it, that one binding is withheld instead, so the failure
 *   stays with that source and never degrades the rest of the shape.
 * - An incremental source whose stored table holds a type a rebuild would cast
 *   re-seeds on its next run ({@link rebuildWouldRecast}), since a delta never
 *   reaches the CTAS that casts.
 */

import { quoteIdentifier } from "../quoting";
import { fromSubselect } from "./subselect";
import { duckdbTypeToMalloyType } from "./serve/materialization_serve_transform";

/**
 * The `code` of the package warning, and of the build and serve log lines, for
 * a stored column whose type does not serve as its declared type.
 */
export const STORED_COLUMN_TYPE_MISMATCH_CODE = "storage-column-type-mismatch";

/** A column as Malloy declares it: its name and Malloy basic type. */
export interface DeclaredColumn {
   name: string;
   /** A Malloy type (`string`, `number`, `date`, `timestamp`, …). */
   type: string;
}

/** One stored column whose type does not serve as the type Malloy declares. */
export interface StoredColumnTypeMismatch {
   /** The column, under the author's spelling. */
   column: string;
   /** The destination's type for it, as `DESCRIBE` reports it. */
   storedType: string;
   /** The Malloy type the source declares for it. */
   declaredType: string;
}

/**
 * The DuckDB type a declared Malloy type is CAST to at build. Only types with
 * one faithful DuckDB representation, whose cast cannot lose a value Malloy
 * means: a timestamp truncates to its date, an integer flag becomes the
 * boolean it encodes, and anything renders as its text.
 *
 * Deliberately absent:
 * - `number`: Malloy's number spans integers, decimals and floats, and every
 *   DuckDB numeric type serves it, so there is no one type to cast to. A
 *   number stored as something non-numeric is caught at serve instead.
 * - `timestamp` / `timestamptz`: both serve faithfully from either DuckDB
 *   timestamp type (build and serve sessions both run in UTC), so recasting
 *   would rewrite every table's column type for nothing.
 * - `json`, `sql native`, arrays and records: no faithful scalar target.
 */
export const BUILD_CAST_TARGETS: Readonly<Record<string, string>> = {
   string: "VARCHAR",
   boolean: "BOOLEAN",
   date: "DATE",
};

/** The Malloy types the serve-side check compares; anything else is not checked. */
const CHECKED_TYPES = new Set([
   "string",
   "number",
   "boolean",
   "date",
   "timestamp",
   "timestamptz",
]);

/**
 * Whether a column stored as `storedType` serves as `declaredType`, or
 * `undefined` when the declared type is not one this check compares (json,
 * sql native, arrays, records), which keeps today's behaviour for them.
 *
 * The rule is the serve shape's own mapping ({@link duckdbTypeToMalloyType}):
 * the type the shape will declare must be the type the author declared, with
 * one widening: the shape declares every DuckDB timestamp as `timestamp`, which
 * serves a declared `timestamptz` faithfully in a UTC session.
 *
 * | Declared      | Faithful stored types                                    |
 * | ------------- | -------------------------------------------------------- |
 * | `string`      | VARCHAR (TEXT, CHAR, BPCHAR, STRING), UUID, TIME         |
 * | `number`      | every integer width, signed or not, FLOAT, DOUBLE, DECIMAL |
 * | `boolean`     | BOOLEAN                                                  |
 * | `date`        | DATE                                                     |
 * | `timestamp`   | TIMESTAMP (any precision), TIMESTAMP WITH TIME ZONE      |
 * | `timestamptz` | TIMESTAMP WITH TIME ZONE, TIMESTAMP (any precision)      |
 */
export function storedTypeServesAs(
   storedType: string,
   declaredType: string,
): boolean | undefined {
   if (!CHECKED_TYPES.has(declaredType)) return undefined;
   const mapped = duckdbTypeToMalloyType(storedType);
   if (!("type" in mapped)) return false;
   if (declaredType === "timestamptz") return mapped.type === "timestamp";
   return mapped.type === declaredType;
}

/**
 * Find a declared column by a stored name: exactly first, then by a unique
 * case-insensitive match (a warehouse may fold the case DESCRIBE reports, as
 * {@link narrowSchemaToPublic} allows). An ambiguous fold matches nothing.
 */
function declaredFor(
   name: string,
   declared: readonly DeclaredColumn[],
): DeclaredColumn | undefined {
   const exact = declared.find((d) => d.name === name);
   if (exact) return exact;
   const folded = declared.filter(
      (d) => d.name.toLowerCase() === name.toLowerCase(),
   );
   return folded.length === 1 ? folded[0] : undefined;
}

/**
 * The stored columns whose type does not serve as their declared type. A
 * column with no declared counterpart, or whose declared type is not checked,
 * is never reported.
 */
export function storedColumnTypeMismatches(
   schema: readonly { name: string; type: string }[],
   declared: readonly DeclaredColumn[],
): StoredColumnTypeMismatch[] {
   const out: StoredColumnTypeMismatch[] = [];
   for (const column of schema) {
      const field = declaredFor(column.name, declared);
      if (!field) continue;
      if (storedTypeServesAs(column.type, field.type) === false) {
         out.push({
            column: field.name,
            storedType: column.type,
            declaredType: field.type,
         });
      }
   }
   return out;
}

/**
 * The scalar columns a compiled source's field list declares, for the checks
 * above. Reads compiled `FieldDef`s (`{ name, type }`) as the serve side holds
 * them and the API's `AtomicField`s (whose `type` is the same string) alike;
 * anything without a string name and type (a join, a view) is skipped.
 */
export function declaredColumnsOf(
   fields: readonly unknown[] | undefined,
): DeclaredColumn[] {
   const out: DeclaredColumn[] = [];
   for (const f of fields ?? []) {
      const { name, type } = (f ?? {}) as { name?: unknown; type?: unknown };
      if (typeof name === "string" && typeof type === "string") {
         out.push({ name, type });
      }
   }
   return out;
}

/**
 * The declared columns of a persist source being built, or none when its field
 * list cannot be read (the build then runs uncast, as it did before, and the
 * serve-side check still guards the result).
 */
export function persistSourceDeclaredColumns(persistSource: {
   _explore: { intrinsicFields: readonly unknown[] };
}): DeclaredColumn[] {
   try {
      return declaredColumnsOf(
         persistSource._explore.intrinsicFields.filter((f) =>
            (f as { isAtomicField?: () => boolean }).isAtomicField?.(),
         ),
      );
   } catch {
      return [];
   }
}

/**
 * Wrap a DuckDB SELECT so every column whose declared type is in
 * {@link BUILD_CAST_TARGETS} is CAST to that type, in place and under its own
 * name; every other column passes through unchanged and in order. Returns the
 * SELECT untouched when there is nothing to cast.
 *
 * Runs in the destination's DuckDB session, never the warehouse, so one
 * spelling serves every source dialect. A CAST is a no-op on a column that is
 * already the target type. A value the cast cannot convert (a boolean the
 * warehouse returned as 'Y'/'N') makes the build retry without the casts
 * ({@link isCastConversionError}), storing what the warehouse returned: never
 * TRY_CAST, whose NULLs would be wrong answers. The serve side then decides
 * that column.
 */
export function castToDeclaredTypes(
   selectSQL: string,
   declared: readonly DeclaredColumn[],
   options: BuildCastOptions = {},
): string {
   const casts = declared.flatMap((d) => {
      const target = buildCastTarget(d.type, options);
      if (!target) return [];
      const column = quoteIdentifier(d.name, "duckdb");
      return [`CAST(${column} AS ${target}) AS ${column}`];
   });
   if (casts.length === 0) return selectSQL;
   return `SELECT * REPLACE (${casts.join(", ")}) ${fromSubselect(selectSQL, "__declared_types")}`;
}

/** What decides a build cast beyond the declared type. */
export interface BuildCastOptions {
   /**
    * The source's query time zone. When set, a `date` is not cast at build: if
    * the warehouse returned an instant (TIMESTAMP WITH TIME ZONE), DuckDB's
    * cast would take its date in the build session's UTC, not in this zone, and
    * could store the day before. The serve side decides such a column instead.
    */
   queryTimezone?: string;
}

/** The DuckDB type a declared Malloy type is cast to at build, or undefined. */
export function buildCastTarget(
   declaredType: string,
   options: BuildCastOptions = {},
): string | undefined {
   if (declaredType === "date" && options.queryTimezone) return undefined;
   return BUILD_CAST_TARGETS[declaredType];
}

/**
 * The query time zone a compiled source declares, when it declares one:
 * Malloy records a query's `timezone:` on its pipeline stage, so a query-shaped
 * source carries it on `query.pipeline[i].queryTimezone` (the last stage that
 * sets one wins), or on the source itself.
 */
export function queryTimezoneOf(structDef: unknown): string | undefined {
   const def = structDef as
      | {
           queryTimezone?: unknown;
           query?: { pipeline?: readonly { queryTimezone?: unknown }[] };
        }
      | undefined;
   let tz: unknown = def?.queryTimezone;
   for (const stage of def?.query?.pipeline ?? []) {
      if (typeof stage?.queryTimezone === "string") tz = stage.queryTimezone;
   }
   return typeof tz === "string" && tz !== "" ? tz : undefined;
}

/** {@link queryTimezoneOf} for a persist source being built. */
export function persistSourceQueryTimezone(persistSource: {
   _explore: unknown;
}): string | undefined {
   try {
      const explore = persistSource._explore as {
         _structDef?: unknown;
         structDef?: unknown;
      };
      return queryTimezoneOf(explore._structDef ?? explore.structDef);
   } catch {
      return undefined;
   }
}

/**
 * What a storage build of `persistSource` casts to: its declared columns, and
 * the options that decide each cast. The one derivation every build path uses
 * (a recompute, a chained build, a same-catalog build), so they cannot drift.
 */
export function storageBuildCasts(persistSource: {
   _explore: { intrinsicFields: readonly unknown[] };
}): { declaredColumns: DeclaredColumn[]; castOptions: BuildCastOptions } {
   return {
      declaredColumns: persistSourceDeclaredColumns(persistSource),
      castOptions: { queryTimezone: persistSourceQueryTimezone(persistSource) },
   };
}

/** Whether `err` is DuckDB refusing a value a cast cannot convert. */
export function isCastConversionError(err: unknown): boolean {
   const message = err instanceof Error ? err.message : String(err);
   return /Conversion Error|Could not convert|Invalid Input Error: .*cast/i.test(
      message,
   );
}

/** A stored column read through a cast to its declared type at serve. */
export interface ReadCast {
   column: string;
   /** The DuckDB type it is read as. */
   castTo: string;
}

const INTEGER_TYPES =
   /^(TINYINT|SMALLINT|INTEGER|INT|INT1|INT2|INT4|INT8|BIGINT|HUGEINT|UTINYINT|USMALLINT|UINTEGER|UBIGINT|UHUGEINT|SHORT|LONG|SIGNED)$/;
const TIMESTAMPTZ_TYPES = /^(TIMESTAMP WITH TIME ZONE|TIMESTAMPTZ)$/;
const NAIVE_TIMESTAMP_TYPES =
   /^(TIMESTAMP|TIMESTAMP_S|TIMESTAMP_MS|TIMESTAMP_NS|TIMESTAMP_US|DATETIME)$/;

/**
 * The DuckDB type a mismatched column can be CAST to with no value able to fail
 * the cast and no value changed from what live gives, or undefined. One rule
 * for the serve side's read cast ({@link serveTypePlan}) and the incremental
 * re-seed ({@link rebuildWouldRecast}), so the two never disagree:
 *
 * - `boolean` from any integer width: the flag the integer encodes.
 * - `date` from a naive timestamp: its wall-clock date. From an instant
 *   (TIMESTAMP WITH TIME ZONE) only when the source has no query time zone,
 *   since the session takes the date in UTC.
 *
 * Nothing else: a number or boolean stored as text can hold a value the cast
 * refuses, and text-to-number has no single right type.
 */
export function faithfulCastTarget(
   m: StoredColumnTypeMismatch,
   options: BuildCastOptions = {},
): string | undefined {
   const base = m.storedType
      .replace(/\(.*\)/, "")
      .trim()
      .toUpperCase();
   if (m.declaredType === "boolean" && INTEGER_TYPES.test(base)) {
      return "BOOLEAN";
   }
   if (
      m.declaredType === "date" &&
      (NAIVE_TIMESTAMP_TYPES.test(base) ||
         (TIMESTAMPTZ_TYPES.test(base) && !options.queryTimezone))
   ) {
      return "DATE";
   }
   return undefined;
}

/**
 * The stored columns a rebuild would cast to their declared type: a table an
 * older build typed, which a delta (an INSERT into the existing columns) can
 * never correct, so its incremental source re-seeds.
 *
 * Only when the rebuild's casts are sure to succeed: every mismatched column
 * the build casts must be one {@link faithfulCastTarget} casts, or a declared
 * `string` (any value renders as text). One that is neither, such as a boolean
 * the warehouse returned as text, is what a build leaves when its cast could
 * not convert a value and it stored the warehouse's types instead
 * ({@link isCastConversionError}). The build casts every column in one
 * statement, so a re-seed would fail the same way and store the same types,
 * and re-seed again on the next run, a full read every time. None is returned
 * then: the table keeps advancing by delta, and the serve side reads its
 * castable columns through a cast and answers the rest live.
 */
export function rebuildWouldRecast(
   schema: readonly { name: string; type: string }[],
   declared: readonly DeclaredColumn[],
   options: BuildCastOptions = {},
): StoredColumnTypeMismatch[] {
   const recast = storedColumnTypeMismatches(schema, declared).filter(
      (m) => buildCastTarget(m.declaredType, options) !== undefined,
   );
   const sure = recast.every(
      (m) =>
         m.declaredType === "string" ||
         faithfulCastTarget(m, options) !== undefined,
   );
   return sure ? recast : [];
}

/**
 * How the serve shape handles each mismatched column: read it through a cast
 * when {@link faithfulCastTarget} names one, since that cast gives the value
 * live gives and no value can fail it; otherwise report it unservable, so only
 * the queries reading it are answered live.
 */
export function serveTypePlan(
   mismatches: readonly StoredColumnTypeMismatch[],
   options: BuildCastOptions = {},
): { casts: ReadCast[]; unservable: StoredColumnTypeMismatch[] } {
   const casts: ReadCast[] = [];
   const unservable: StoredColumnTypeMismatch[] = [];
   for (const m of mismatches) {
      const castTo = faithfulCastTarget(m, options);
      if (castTo) {
         casts.push({ column: m.column, castTo });
      } else {
         unservable.push(m);
      }
   }
   return { casts, unservable };
}
