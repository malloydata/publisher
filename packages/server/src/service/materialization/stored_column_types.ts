// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A stored column's type, both directions, in one module.
 *
 * - What a field is stored as: the type a build writes for each column
 *   ({@link storedColumnsOfOutput}), with the casts that make the table hold
 *   the declared type whatever the warehouse returned ({@link
 *   BUILD_CAST_TARGETS}, {@link storageBuildCasts}).
 * - What a stored column is declared as when served: one declaration rule
 *   ({@link storedMalloyType}, {@link storedMalloyTypeFor}), and per
 *   column a verdict: declared as its stored type,
 *   read through a faithful cast ({@link serveTypePlan}), read as its native
 *   type (a big integer, {@link bigIntegerReads}), or left out, so a query
 *   reading it answers live and every other query is served.
 * - The schema a bound entry carries ({@link boundSchemaOf}, {@link
 *   boundNativeSchemaOf}) and a dry run's prediction of it ({@link
 *   previewNativeSchema}).
 *
 * The warehouse need not return the type Malloy declares: Postgres answers a
 * date truncation with a timestamp, DuckDB answers `date + 1 month` with one,
 * and a `sql_boolean` over an integer stays an integer. An incremental source
 * whose stored table holds a type a rebuild would cast re-seeds on its next
 * run ({@link rebuildWouldRecast}), since a delta never reaches the CTAS that
 * casts. The capture of a database table's native types stays in
 * `run/database_native_types.ts`.
 *
 * Pure: the build and the served copy apply what it decides.
 */

import { queryTimezoneOf } from "../governance/ir/compiled_model";
import { exploreOf } from "../governance/ir/compiled_model";
import { getDialect } from "@malloydata/malloy";
import { capabilitiesOf, STORAGE_DIALECT } from "./domain/dialect";
import { liveRoundsBigIntegersAsPostgres } from "./domain/live_big_integers";
import { identifier } from "../malloy_text";
import { quoteIdentifier } from "../quoting";
import { fromSubselect } from "./subselect";
import type { Binding } from "./domain/binding";
import type { StoredColumn } from "./domain/table_plans";

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
const BUILD_CAST_TARGETS: Readonly<Record<string, string>> = {
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
 * sql native, arrays, records), which is never a mismatch.
 *
 * The rule is the DuckDB type mapping ({@link duckdbTypeToMalloyType}): the
 * type it declares must be the type the author declared, with one widening:
 * it declares every DuckDB timestamp as `timestamp`, which serves a declared
 * `timestamptz` faithfully in a UTC session.
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
 * `storedColumnsWithAccess` allows). An ambiguous fold matches nothing.
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
 * The declared columns of a persist source being built, each under the name
 * its build's SELECT outputs it as, or none when its field list cannot be read
 * (the build then runs uncast, and the serve-side check
 * still guards the result).
 *
 * A field a trailing `extend` renames (`rename: kind is sub`) is the query's
 * `sub` column under another name: the SELECT, and so the stored table,
 * hold `sub`, and the serve side renames it again. Its definition keeps the
 * output name (`name`) beside the new one (`as`), so the column is declared
 * as `sub`. A dimension the `extend` computes is no column of the SELECT;
 * the explore's intrinsic fields already leave it out.
 */
export function persistSourceDeclaredColumns(persistSource: {
   _explore: { intrinsicFields: readonly unknown[] };
}): DeclaredColumn[] {
   try {
      return declaredColumnsOf(
         exploreOf(persistSource).intrinsicFields.flatMap((f) => {
            const field = f as {
               isAtomicField?: () => boolean;
               name?: unknown;
               type?: unknown;
               fieldTypeDef?: { name?: unknown };
            };
            if (!field.isAtomicField?.()) return [];
            const def = field.fieldTypeDef;
            const output =
               typeof def?.name === "string" ? def.name : field.name;
            return [{ name: output, type: field.type }];
         }),
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
   const cast = new Set<string>();
   const casts = declared.flatMap((d) => {
      const target = buildCastTarget(d.type, options);
      if (!target) return [];
      cast.add(d.name.toLowerCase());
      const column = quoteIdentifier(d.name, STORAGE_DIALECT);
      return [`CAST(${column} AS ${target}) AS ${column}`];
   });
   for (const name of options.exactIntegers ?? []) {
      if (cast.has(name.toLowerCase())) continue;
      const column = quoteIdentifier(name, STORAGE_DIALECT);
      casts.push(
         `CAST(${column} AS ${EXACT_INTEGER_STORAGE_TYPE}) AS ${column}`,
      );
   }
   if (casts.length === 0) return selectSQL;
   return `SELECT * REPLACE (${casts.join(", ")}) ${fromSubselect(selectSQL, "__declared_types")}`;
}

/**
 * Whether the build keeps a column's warehouse type rather than casting it to
 * its declared `string`: a binary or an interval value. Postgres's dialect
 * declares `bytea` and `interval` as `string`, and live answers them in
 * Postgres's own text (`\x0102`, `3 mons`), which DuckDB's cast to VARCHAR does
 * not write (`\x01\x02`, `3 months`). Stored as BLOB or INTERVAL, the column
 * has no faithful declaration ({@link storedTypeServesAs}), so it is left out
 * and a query reading it is answered live. Decided from the type the
 * destination's session reports for the source's column, never from a value.
 */
export function keepsWarehouseType(
   warehouseType: string,
   declaredType: string,
): boolean {
   if (declaredType !== "string") return false;
   const base = warehouseType
      .replace(/\(.*\)/, "")
      .trim()
      .toUpperCase();
   return base === "BLOB" || base === "BYTEA" || base === "INTERVAL";
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
   /**
    * Output columns the build computed in DuckDB as a 128-bit integer
    * ({@link wideIntegerColumns}): each is stored as `DECIMAL(38,0)`, which
    * DuckLake's Parquet keeps exactly, where it would store the 128-bit type
    * as a DOUBLE ({@link ducklakeStoresAsDouble}).
    */
   exactIntegers?: readonly string[];
}

/** The type a 128-bit integer column is stored as, every digit kept. */
const EXACT_INTEGER_STORAGE_TYPE = "DECIMAL(38,0)";

/**
 * The columns of a described SELECT that DuckLake would store as a DOUBLE: a
 * 128-bit integer, such as DuckDB's sum of BIGINTs. A build stores each as
 * {@link EXACT_INTEGER_STORAGE_TYPE}; a value past 38 digits fails that cast,
 * and the build stores the warehouse's types instead
 * ({@link isCastConversionError}), which the serve side answers live.
 */
export function wideIntegerColumns(
   described: readonly { name?: string; type?: string }[],
): string[] {
   return described.flatMap((c) =>
      c.name && c.type && ducklakeStoresAsDouble(c.type) ? [c.name] : [],
   );
}

/** The DuckDB type a declared Malloy type is cast to at build, or undefined. */
function buildCastTarget(
   declaredType: string,
   options: BuildCastOptions = {},
): string | undefined {
   if (declaredType === "date" && options.queryTimezone) return undefined;
   return BUILD_CAST_TARGETS[declaredType];
}

/** {@link queryTimezoneOf} for a persist source being built. */
export function persistSourceQueryTimezone(persistSource: {
   _explore: unknown;
}): string | undefined {
   try {
      const explore = exploreOf(persistSource) as {
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
   /**
    * Read as that SQL type itself (`::"HUGEINT"`), which Malloy types from
    * the dialect (a `bigint` number), rather than as the Malloy type it maps
    * to (`::number`, which would round it). For a big-integer column
    * ({@link bigIntegerReads}).
    */
   native?: true;
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
function faithfulCastTarget(
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
      (m) =>
         buildCastTarget(m.declaredType, options) !== undefined &&
         // Stored as the warehouse typed it on purpose: a rebuild keeps it.
         !keepsWarehouseType(m.storedType, m.declaredType),
   );
   const sure = recast.every(
      (m) =>
         m.declaredType === "string" ||
         faithfulCastTarget(m, options) !== undefined,
   );
   return sure ? recast : [];
}

/**
 * How the served copy handles each mismatched column: read it through a cast
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

/**
 * How one database table's column that moved since its build is read now,
 * the rule every reader of a database table and its warning share
 * ({@link serveTypePlan}): `left_out` when the bind left it out (gone, a
 * type Malloy reads as no basic type, a timestamp that gained or lost its
 * zone off DuckDB) or no faithful cast reads its type as declared; `cast`
 * (to a DuckDB type, `BOOLEAN` or `DATE`) where one does; else `as_is`, a
 * type that still reads as declared.
 */
type DriftedColumnRead =
   | { read: "as_is" }
   | { read: "cast"; castTo: string }
   | { read: "left_out" };

export function driftedColumnRead(
   column: { column: string; observedType?: string; leftOut?: true },
   declaredType: string,
   options: BuildCastOptions = {},
): DriftedColumnRead {
   if (column.leftOut || !column.observedType) return { read: "left_out" };
   const [cast] = serveTypePlan(
      [
         {
            column: column.column,
            storedType: column.observedType,
            declaredType,
         },
      ],
      options,
   ).casts;
   if (cast) return { read: "cast", castTo: cast.castTo };
   return storedTypeServesAs(column.observedType, declaredType) !== false
      ? { read: "as_is" }
      : { read: "left_out" };
}

/**
 * The Malloy type a moved column must read as: its build source's declared
 * one, else the one its build wrote.
 */
export function driftDeclaredType(
   declared: readonly { name?: string; type?: string }[] | undefined,
   column: string,
   builtType: string,
): string {
   return (
      declared?.find((c) => c.name === column)?.type ??
      storedMalloyType(builtType) ??
      builtType
   );
}

/** A declared column, with the author's field when its big-integer read is asked. */
export interface DeclaredRead extends DeclaredColumn {
   /** The compiled field: its `bigint` subtype decides a native read. */
   field?: unknown;
}

/** How a stored table's columns read as their declared types. */
interface StoredColumnReadPlan {
   /** Every stored column whose type does not serve as declared. */
   mismatches: StoredColumnTypeMismatch[];
   /**
    * Each column read through a cast: a faithful cast to its declared type
    * ({@link serveTypePlan}) in schema order, then each big integer's native
    * read (`native`, {@link bigIntegerReadOf}) in declared order.
    */
   casts: ReadCast[];
   /** The mismatches no faithful cast reads as declared: left out, or refused. */
   leftOut: StoredColumnTypeMismatch[];
   /** The big integers stored where no cast keeps live's digits. */
   refusals: { column: string; storedType: string }[];
}

/**
 * The one read plan for a stored table: which columns are cast, which no
 * cast reads as declared, and which big integers no read keeps exact. Every
 * serve-side reader of a stored table (the served copy's v1 and
 * `#(materialize)` columns, a persisted result, a rollup) and the type
 * warning build it here, so they never disagree.
 *
 * A declared column's big-integer read is asked only when it carries its
 * `field`; the stored column it reads is matched by exact name.
 */
export function storedColumnReadPlan(
   schema: readonly { name: string; type: string }[],
   declared: readonly DeclaredRead[],
   options: BuildCastOptions &
      Partial<BigIntegerReadOptions> & {
         /**
          * The table is in DuckLake (a `storage=` destination): a 128-bit
          * integer column holds a DOUBLE's digits
          * ({@link ducklakeStoresAsDouble}), so it is left out whatever its
          * field's type, never read or re-aggregated.
          */
         ducklake?: boolean;
      } = {},
): StoredColumnReadPlan {
   const lossy: StoredColumnTypeMismatch[] = [];
   if (options.ducklake) {
      for (const column of schema) {
         if (!ducklakeStoresAsDouble(column.type)) continue;
         const d = declaredFor(column.name, declared);
         if (d) {
            lossy.push({
               column: d.name,
               storedType: column.type,
               declaredType: d.type || "number",
            });
         }
      }
   }
   const lost = new Set(lossy.map((m) => m.column));
   const mismatches = storedColumnTypeMismatches(schema, declared).filter(
      (m) => !lost.has(m.column),
   );
   const { casts, unservable } = serveTypePlan(mismatches, options);
   const refusals: StoredColumnReadPlan["refusals"] = [];
   const cast = new Set(casts.map((c) => c.column));
   for (const d of declared) {
      if (!("field" in d) || cast.has(d.name) || lost.has(d.name)) continue;
      const stored = schema.find((s) => s.name === d.name);
      if (!stored) continue;
      const big = bigIntegerReadOf(d.field, stored, {
         ...options,
         dialect: options.dialect,
      });
      if (!big) continue;
      if (big.castTo) {
         casts.push({ column: d.name, castTo: big.castTo, native: true });
         cast.add(d.name);
      } else {
         refusals.push({ column: d.name, storedType: stored.type });
      }
   }
   return {
      mismatches: [...lossy, ...mismatches],
      casts,
      leftOut: [...lossy, ...unservable],
      refusals,
   };
}

/**
 * The stored columns a build source's compiled output declares, each in the
 * DuckDB spelling a serve binding's schema carries (`VARCHAR`, `BIGINT`,
 * `STRUCT(a VARCHAR)[]`), which the served copy reads back as Malloy types.
 *
 * For a database `#(materialize)` table (no `storage=`), whose build writes
 * into the source's own warehouse and captures no schema: its columns are the
 * build source's output, a table at one content address holds exactly that
 * build's columns, and Malloy reads each back as the basic type it built.
 *
 * A field whose type has no faithful spelling here (`timestamptz`, `json`, a
 * `sql native` type, a nested type off DuckDB) is left out, so the served copy
 * finds the column missing and the source serves live: never declared as a
 * type its table does not hold.
 */
export function storedColumnsOfOutput(
   fields: readonly unknown[] | undefined,
   options: {
      nested: boolean;
      /**
       * Store a `bigint` (Malloy's numberType for a 64-bit integer read back
       * from a table) as BIGINT rather than DOUBLE: for a schema read off a
       * v1 table. Off for a `#(materialize)` plan's compiled output.
       */
      bigint?: boolean;
      /**
       * Store a `timestamptz` as TIMESTAMP WITH TIME ZONE rather than leaving
       * the column out: for a v1 table's schema, whose every column the
       * served copy must declare. Off otherwise.
       */
      timestamptz?: boolean;
   },
): StoredColumn[] {
   const out: StoredColumn[] = [];
   for (const f of fields ?? []) {
      const name =
         (f as { as?: unknown; name?: unknown })?.as ??
         (f as { name?: unknown })?.name;
      if (typeof name !== "string") continue;
      const type = storedTypeOfField(
         f,
         options.nested,
         0,
         options.bigint,
         options.timestamptz,
      );
      if (type !== undefined) out.push({ name, type });
   }
   return out;
}

/**
 * A warehouse column's type (as its catalog names it) in the spelling a
 * build's schema capture writes ({@link storedColumnsOfOutput}), or undefined
 * when Malloy reads it as no basic type, which has no spelling. An array is
 * one, though a dialect maps `DATE[]` to its element's `date`.
 */
export function spelledNativeType(
   nativeType: string,
   dialect: string,
): string | undefined {
   if (isArrayType(nativeType)) return undefined;
   const [spelled] = storedColumnsOfOutput(
      [{ name: "c", ...getDialect(dialect).sqlTypeToMalloyType(nativeType) }],
      { nested: false, bigint: true, timestamptz: true },
   );
   return spelled?.type;
}

/** Whether Malloy reads a column of native type `nativeType` as a string. */
export function isStringNativeType(
   nativeType: string,
   dialect: string,
): boolean {
   try {
      return (
         getDialect(dialect).sqlTypeToMalloyType(nativeType).type === "string"
      );
   } catch {
      return false;
   }
}

function isArrayType(type: string): boolean {
   return /\[\s*\d*\s*\]\s*$/.test(type) || /^\s*ARRAY\b/i.test(type);
}

/** One output field's type in DuckDB's spelling, or undefined (see above). */
function storedTypeOfField(
   field: unknown,
   nested: boolean,
   depth: number,
   bigint = false,
   timestamptz = false,
): string | undefined {
   if (depth > 16 || field === null || typeof field !== "object") {
      return undefined;
   }
   const f = field as {
      type?: unknown;
      numberType?: unknown;
      elementTypeDef?: unknown;
      fields?: unknown[];
   };
   switch (f.type) {
      case "string":
         return "VARCHAR";
      case "number":
         // A schema read back from a table says `bigint` for a 64-bit integer.
         return f.numberType === "integer" ||
            (bigint && f.numberType === "bigint")
            ? "BIGINT"
            : "DOUBLE";
      case "boolean":
         return "BOOLEAN";
      case "date":
         return "DATE";
      case "timestamp":
         return "TIMESTAMP";
      case "timestamptz":
         return timestamptz ? "TIMESTAMP WITH TIME ZONE" : undefined;
      case "record":
         return nested
            ? structOf(f.fields, depth, bigint, timestamptz)
            : undefined;
      case "array": {
         if (!nested) return undefined;
         const element = f.elementTypeDef as { type?: unknown } | undefined;
         // An array of records (a repeated record) lists its fields on the
         // array; an array of a scalar names the scalar's type.
         const inner =
            element?.type === "record_element"
               ? structOf(f.fields, depth, bigint, timestamptz)
               : storedTypeOfField(
                    element,
                    nested,
                    depth + 1,
                    bigint,
                    timestamptz,
                 );
         return inner === undefined ? undefined : `${inner}[]`;
      }
      default:
         return undefined;
   }
}

function structOf(
   fields: unknown[] | undefined,
   depth: number,
   bigint = false,
   timestamptz = false,
): string | undefined {
   const parts: string[] = [];
   for (const f of fields ?? []) {
      const name =
         (f as { as?: unknown; name?: unknown })?.as ??
         (f as { name?: unknown })?.name;
      if (typeof name !== "string") return undefined;
      const type = storedTypeOfField(f, true, depth + 1, bigint, timestamptz);
      if (type === undefined) return undefined;
      parts.push(`"${name.replace(/"/g, '""')}" ${type}`);
   }
   return parts.length === 0 ? undefined : `STRUCT(${parts.join(", ")})`;
}

/**
 * Whether live answers a source's 64-bit integers with every digit: false for
 * a dialect Malloy says returns its results through JSON
 * (`supportsBigIntPrecision`), where live reads 9007199254740993 as
 * 9007199254740992 and a stored read must round it the same way (Postgres:
 * measured by the r4 big-integer spec). DuckDB inherits `false` from its
 * Postgres base, yet its driver keeps every digit (measured too), so it is
 * read as true. An unknown dialect counts as exact: a stored read then keeps
 * every digit too, or leaves the column live.
 */
export function liveKeepsBigIntegers(dialect: unknown): boolean {
   if (
      typeof dialect !== "string" ||
      capabilitiesOf(dialect).driverKeepsBigIntegers
   ) {
      return true;
   }
   try {
      return (
         (getDialect(dialect) as { supportsBigIntPrecision?: boolean })
            .supportsBigIntPrecision !== false
      );
   } catch {
      return true;
   }
}

/**
 * Whether a `storage=` table's big-integer columns are read through their
 * native type: where live keeps every digit, or rounds them as Postgres does
 * ({@link liveRoundsBigIntegersAsPostgres}), the answer then rounded.
 */
function storageReadsBigIntegers(dialect: unknown): boolean {
   return (
      liveKeepsBigIntegers(dialect) || liveRoundsBigIntegersAsPostgres(dialect)
   );
}

/**
 * How a stored read gives one big-integer column the value live gives: read
 * through `castTo`, a native SQL type of the stored table's dialect that Malloy
 * types as a `bigint` number, so the result keeps every digit; or, with no
 * cast that can be exact, left out (`castTo` undefined), so only a query
 * reading it is answered live.
 */
export interface BigIntegerRead {
   /** The stored column, as the schema spells it. */
   column: string;
   /** The native type it is read as (`HUGEINT` in DuckDB), or none: left out. */
   castTo?: string;
}

/** DuckDB integer types (and a DECIMAL with no scale) HUGEINT holds exactly. */
const HUGEINT_EXACT =
   /^(TINYINT|SMALLINT|INTEGER|INT|INT1|INT2|INT4|INT8|BIGINT|HUGEINT|INT128|UTINYINT|USMALLINT|UINTEGER|UBIGINT|SHORT|LONG|SIGNED|(DECIMAL|NUMERIC)\(\s*\d+\s*,\s*0\s*\))$/;

/**
 * The stored columns live answers with more digits than a plain `number`
 * keeps, and how a stored read keeps them: each a
 * column whose author field Malloy types as a `bigint` number (DuckDB
 * BIGINT, HUGEINT and UBIGINT; BigQuery INT64; Snowflake NUMBER(p,0); a sum
 * or max over one). A served `type:` can declare such a column only as a
 * `number`, which Malloy reads through a JavaScript number, so
 * 9007199254740993 would come back as 9007199254740992, and as a JSON number
 * where live's answer is a string.
 *
 * Decided from Malloy's own type for the field, the type live answers with,
 * never from the stored values: `count()` and a sum of 32-bit integers are
 * `integer`, and a DECIMAL is `float`, all read through a number live too, so
 * they are not touched. Served must equal live in value and in type, the
 * `bigint` subtype included (it decides that an answer past 2^53 is a JSON
 * string). A database table is read through the warehouse's own bigint
 * type, on live's own data path, which rounds where live rounds. A
 * `storage=` table is read in DuckDB, which keeps every digit, so on
 * Postgres, whose live answer rounds ({@link liveRoundsBigIntegersAsPostgres}),
 * the served answer is rounded as live's is, after it runs
 * (`domain/live_big_integers.ts`): the read itself stays exact, since a sum
 * over rounded values is not live's rounded sum. On another dialect whose
 * live answer rounds (unmeasured) a `storage=` table is not touched, since
 * its plain `number` rounds the same way, though as a JSON number.
 *
 * - A `storage=` table (`database` false) is in a DuckLake destination and
 *   its schema is what DESCRIBE reported: a 64-bit or narrower integer
 *   column, or a DECIMAL with no scale, is read as HUGEINT, which holds every
 *   value exactly; one stored as a float has lost digits already, and so has
 *   a 128-bit integer, which DuckLake writes to Parquet as a DOUBLE
 *   ({@link ducklakeStoresAsDouble}): both are left out.
 * - A database table is in the source's own warehouse, and its column can
 *   be wider than any type Malloy names for a `bigint` number (Postgres
 *   stores a sum of BIGINTs as `numeric`, SQL Server's `decimal(38,0)` is
 *   typed `bigint`, DuckDB's UHUGEINT holds values past HUGEINT). It is read
 *   through a cast to exactly the type its build captured from the
 *   warehouse's catalog (`nativeTypeOf`, `ManifestEntry.nativeSchema`), when
 *   Malloy types that type as a `bigint` number, so nothing is narrowed and
 *   the value keeps live's representation. A column of any other type (a
 *   `numeric`, which Malloy reads as a float), or one whose type its build
 *   did not capture, is left out, except on BigQuery, whose only integer
 *   type is INT64 ({@link exactWarehouseCast}).
 */
export function bigIntegerReads(
   schema: readonly { name: string; type: string }[],
   fields: readonly unknown[] | undefined,
   options: BigIntegerReadOptions,
): BigIntegerRead[] {
   // A `storage=` table is read in DuckDB, which keeps every digit: only for
   // a source whose live answer does too, or whose rounding is measured.
   if (!options.database && !storageReadsBigIntegers(options.dialect)) {
      return [];
   }
   // How a field is matched to its stored column depends on how the table
   // stores it. A v1 query-shaped table stores each column under its
   // query's output name: a field a trailing `extend` renames (`rename:
   // amount is total`) is stored as `total`, and that name is its own
   // whatever another field is now called, so the output name is matched
   // first, then a field the `extend` computes, and only then a renamed
   // field's new name. A `#(materialize)` table stores the source after its
   // renames, under each field's own name (`as`, else `name`), matched
   // first; the output name only where no field goes by it.
   const byName = new Map<string, unknown>();
   const declared: DeclaredColumn[] = [];
   const fieldOf = (f: unknown) =>
      (f ?? {}) as {
         name?: unknown;
         as?: unknown;
         type?: unknown;
         e?: unknown;
      };
   const claim = (key: unknown, f: unknown) => {
      const { type } = fieldOf(f);
      if (typeof key !== "string" || typeof type !== "string") return;
      if (byName.has(key)) return;
      byName.set(key, f);
      declared.push({ name: key, type });
   };
   const ownName = (f: unknown) => {
      const { name, as } = fieldOf(f);
      return typeof as === "string" ? as : name;
   };
   if (options.storesOutputNames) {
      for (const f of fields ?? []) {
         if (fieldOf(f).e === undefined) claim(fieldOf(f).name, f);
      }
      for (const f of fields ?? []) {
         if (fieldOf(f).e !== undefined) claim(ownName(f), f);
      }
      for (const f of fields ?? []) {
         const { as, e } = fieldOf(f);
         if (e === undefined && typeof as === "string") claim(as, f);
      }
   } else {
      for (const f of fields ?? []) claim(ownName(f), f);
      for (const f of fields ?? []) {
         if (typeof fieldOf(f).as === "string") claim(fieldOf(f).name, f);
      }
   }
   const out: BigIntegerRead[] = [];
   for (const column of schema) {
      const d = declaredFor(column.name, declared);
      const read = d && bigIntegerReadOf(byName.get(d.name), column, options);
      if (read) out.push(read);
   }
   return out;
}

/**
 * {@link bigIntegerReads} for one stored column and the field it holds (a
 * joined source's field, read through a record), or undefined when it needs
 * nothing.
 */
export function bigIntegerReadOf(
   field: unknown,
   column: { name: string; type: string },
   options: BigIntegerReadOptions,
): BigIntegerRead | undefined {
   if (!options.database && !storageReadsBigIntegers(options.dialect)) {
      return undefined;
   }
   if (!isBigIntegerField(field)) return undefined;
   const castTo = options.database
      ? exactWarehouseCast(options.dialect, options.nativeTypeOf?.(column.name))
      : duckdbBigIntegerCast(column.type);
   return { column: column.name, ...(castTo ? { castTo } : {}) };
}

/** Where a stored table is, and what is known of its columns' types. */
export interface BigIntegerReadOptions {
   /** The author's source's dialect: the one live answers on. */
   dialect: unknown;
   /** A database table, in the source's own warehouse (else `storage=`). */
   database?: boolean;
   /**
    * For a database table: a stored column's type as the warehouse's catalog
    * names it, as its build captured it, or undefined when it did not.
    */
   nativeTypeOf?: (column: string) => string | undefined;
   /**
    * The table stores each column under its query's output name, before the
    * source's trailing `extend` renames it: a v1 query-shaped (or
    * refinement-rooted) table. A `#(materialize)` table stores the source
    * after its renames, under each field's own name. Decides which name a
    * stored column is matched to a field by first ({@link bigIntegerReads}).
    */
   storesOutputNames?: boolean;
}

/**
 * A database column's captured warehouse type, as the cast that reads it
 * exactly, when Malloy types that type as a `bigint` number (`bigint`,
 * `NUMBER(38,0)`, `decimal(38,0)`, `UHUGEINT`); undefined for any other type
 * (`numeric`, read as a float), a type the dialect's CAST does not take by
 * that name (MySQL's `bigint`), or none captured: no cast keeps both the
 * value and live's representation, so the column is left out.
 *
 * BigQuery is the one exception to "none captured": it has exactly one
 * integer type, INT64, and it is the only BigQuery type Malloy reads as a
 * `bigint` number, so the field's own type names the column's exactly. A
 * table bound before its build captured the catalog's types is read through
 * INT64 there; no other dialect is inferred.
 */
function exactWarehouseCast(
   dialect: unknown,
   nativeType: string | undefined,
): string | undefined {
   if (typeof dialect !== "string") return undefined;
   if (!nativeType?.trim()) {
      return capabilitiesOf(dialect).onlyBigIntegerType;
   }
   // MySQL's CAST names no integer type (`CAST(x AS bigint)` is a syntax
   // error; its SIGNED and UNSIGNED are not numbers to Malloy), so only a
   // DECIMAL is a type the column can be read through.
   if (
      !capabilitiesOf(dialect).castsToIntegerType &&
      !/^decimal\b/i.test(nativeType.trim())
   ) {
      return undefined;
   }
   try {
      return isBigIntegerField(
         getDialect(dialect).sqlTypeToMalloyType(nativeType),
      )
         ? nativeType.trim()
         : undefined;
   } catch {
      return undefined;
   }
}

/**
 * A lookup of a stored column's captured warehouse type, by name, exactly and
 * then by a unique case fold (a warehouse may fold the names its catalog
 * reports).
 */
export function nativeTypeLookup(
   nativeSchema: readonly { name: string; type: string }[] | undefined,
): (column: string) => string | undefined {
   const columns = nativeSchema ?? [];
   return (column) => {
      const exact = columns.find((c) => c.name === column);
      if (exact) return exact.type;
      const folded = columns.filter(
         (c) => c.name.toLowerCase() === column.toLowerCase(),
      );
      return folded.length === 1 ? folded[0].type : undefined;
   };
}

/** Whether a compiled field is a number Malloy types with the `bigint` subtype. */
function isBigIntegerField(field: unknown): boolean {
   const f = (field ?? {}) as { type?: unknown; numberType?: unknown };
   return f.type === "number" && f.numberType === "bigint";
}

/**
 * The DuckDB type a `storage=` column is read as to keep every digit, or none.
 *
 * A storage destination is a DuckLake catalog, and DuckLake (DuckDB 1.5)
 * writes a 128-bit integer column (`HUGEINT`, `UHUGEINT`) to Parquet as a
 * DOUBLE while its catalog still reports the 128-bit type, so a value past
 * 2^53 comes back rounded (4530000000000225187 as 4530000000000225280). Only
 * rows still inlined in the catalog keep every digit, and a reader cannot
 * tell which it has, so such a column has no exact read: left out, so only a
 * query reading it is answered live ({@link ducklakeStoresAsDouble}).
 */
function duckdbBigIntegerCast(storedType: string): string | undefined {
   const base = storedType.trim().toUpperCase();
   if (ducklakeStoresAsDouble(base)) return undefined;
   return HUGEINT_EXACT.test(base) ? "HUGEINT" : undefined;
}

/**
 * Whether DuckLake stores a column of this type, as its catalog reports it,
 * as a DOUBLE in its Parquet files: a 128-bit integer, which Parquet has no
 * type for. Measured on DuckDB 1.5.5 with ducklake; a plain DuckDB table
 * keeps every digit of the same column, and a `DECIMAL(38,0)` keeps them in
 * DuckLake too.
 */
export function ducklakeStoresAsDouble(storedType: string): boolean {
   return /^(HUGEINT|INT128|UHUGEINT)$/i.test(storedType.trim());
}

/**
 * Whether a stored column of this type holds a 128-bit integer anywhere,
 * a list's element or a struct's field included: each such leaf DuckLake
 * holds as a DOUBLE too.
 */
export function holdsDucklakeDouble(storedType: string): boolean {
   return /\b(HUGEINT|INT128|UHUGEINT)\b/i.test(storedType);
}

/**
 * The Malloy basic type a DuckDB column type (as reported by `DESCRIBE`) maps
 * onto, or `undefined` for a type with no faithful mapping: a collection
 * (`array`) or anything unrecognized (nested, struct, enum, blob). Pure; see
 * {@link storedMalloyType} for how a stored column is declared.
 */
export function duckdbTypeToMalloyType(
   duckdbType: string,
): { type: string } | { unmapped: "array" | "unrecognized" } {
   const raw = duckdbType.trim();
   if (/\[\s*\]/.test(raw)) return { unmapped: "array" };
   // DECIMAL(18,2) -> DECIMAL
   const base = raw
      .replace(/\(.*\)/, "")
      .trim()
      .toUpperCase();

   if (base === "DATE") return { type: "date" };
   if (
      base.startsWith("TIMESTAMP") ||
      base === "DATETIME" ||
      base === "TIMESTAMPTZ"
   ) {
      return { type: "timestamp" };
   }
   if (base === "TIME") {
      // Malloy has no time-of-day scalar; carry as string to preserve the value.
      return { type: "string" };
   }

   switch (base) {
      case "TINYINT":
      case "SMALLINT":
      case "INTEGER":
      case "INT":
      case "INT4":
      case "BIGINT":
      case "INT8":
      case "HUGEINT":
      case "UTINYINT":
      case "USMALLINT":
      case "UINTEGER":
      case "UBIGINT":
      case "UHUGEINT":
      case "DOUBLE":
      case "FLOAT":
      case "FLOAT4":
      case "FLOAT8":
      case "REAL":
      case "DECIMAL":
      case "NUMERIC":
         return { type: "number" };
      case "VARCHAR":
      case "TEXT":
      case "CHAR":
      case "BPCHAR":
      case "STRING":
      case "UUID":
         return { type: "string" };
      case "BOOLEAN":
      case "BOOL":
         return { type: "boolean" };
      default:
         return { unmapped: "unrecognized" };
   }
}

/**
 * The Malloy type a stored DuckDB column (as `DESCRIBE` reports it) is
 * declared as in a `type:`, or undefined when no declaration reads its values
 * back as live would. A scalar is {@link duckdbTypeToMalloyType}'s mapping; a
 * list and a struct (what a persisted `nest:` stores, `STRUCT(status VARCHAR,
 * n BIGINT)[]`, or a table's own `INTEGER[]`) are written as Malloy's array
 * and record types (`{ status::string, n::number }[]`), so their rows read
 * back as they do live. Anything else (a MAP, a UNION, an ENUM, BLOB,
 * INTERVAL, JSON, or a list or struct holding one) has no faithful
 * declaration: Malloy refuses `json` in a `type:`, and a quoted native type
 * would change how its values come back. Such a column is left out of the
 * declaration ({@link declarableColumns}), so a query reading it is answered
 * live.
 *
 * The one rule for every stored table's `type:`: the chained build's parents
 * and the served copy.
 */
export function storedMalloyType(stored: string): string | undefined {
   const parsed = parseStoredType(stored);
   return parsed && renderStoredType(parsed);
}

/**
 * {@link storedMalloyType} for a stored list or struct, fitted to the field
 * live answers it as, or undefined when no declaration reads it back as live
 * does, so it is left out and a query reading it is answered live. Every
 * level must be live's shape: an array where live has one, a record with
 * live's fields by name, and each leaf live's type. A leaf live types as a
 * `timestamptz` stored with its zone is declared `timestamptz`, as a top-level
 * column is. A leaf that differs otherwise is refused: a warehouse type Malloy
 * reads as `sql native` (a Postgres composite stored as a STRUCT), and a
 * `bigint` number where live keeps every digit (`keepsBigIntegers`), which
 * Malloy reads through a declared `number` as a JavaScript number, rounding it
 * past 2^53. A nested value has no cast that reads it exactly, as a top-level
 * BIGINT has.
 */
export function storedMalloyTypeFor(
   stored: string,
   field: unknown,
   keepsBigIntegers: boolean,
): string | undefined {
   const parsed = parseStoredType(stored);
   return parsed && fitStoredType(parsed, field, keepsBigIntegers);
}

/** A stored DuckDB type: a scalar with its Malloy type, a struct or a list. */
type StoredType =
   | { kind: "scalar"; raw: string; malloyType: string }
   | { kind: "record"; fields: { name: string; type: StoredType }[] }
   | { kind: "array"; element: StoredType };

function parseStoredType(stored: string): StoredType | undefined {
   const parsed = parseNestedType(stored.trim());
   if (parsed === undefined || parsed.rest.trim() !== "") return undefined;
   return parsed.type;
}

/** A stored type as Malloy type text, from the stored type alone. */
function renderStoredType(t: StoredType): string {
   switch (t.kind) {
      case "scalar":
         return t.malloyType;
      case "array":
         return `${renderStoredType(t.element)}[]`;
      case "record":
         return `{ ${t.fields
            .map((f) => `${identifier(f.name)}::${renderStoredType(f.type)}`)
            .join(", ")} }`;
   }
}

/** {@link storedMalloyTypeFor}'s walk of a stored type beside live's field. */
function fitStoredType(
   t: StoredType,
   field: unknown,
   keepsBigIntegers: boolean,
): string | undefined {
   const live = (field ?? {}) as {
      type?: unknown;
      numberType?: unknown;
      fields?: unknown;
      elementTypeDef?: unknown;
   };
   switch (t.kind) {
      case "array": {
         if (live.type !== "array") return undefined;
         const element = live.elementTypeDef as { type?: unknown } | undefined;
         // An array of records keeps its records' fields on the array.
         const inner = fitStoredType(
            t.element,
            element?.type === "record_element"
               ? { type: "record", fields: live.fields }
               : element,
            keepsBigIntegers,
         );
         return inner && `${inner}[]`;
      }
      case "record": {
         if (live.type !== "record" || !Array.isArray(live.fields)) {
            return undefined;
         }
         const liveFields = live.fields as { name?: unknown; as?: unknown }[];
         if (liveFields.length !== t.fields.length) return undefined;
         const parts: string[] = [];
         for (const f of t.fields) {
            const match = liveFields.find((l) => (l.as ?? l.name) === f.name);
            const inner =
               match && fitStoredType(f.type, match, keepsBigIntegers);
            if (!inner) return undefined;
            parts.push(`${identifier(f.name)}::${inner}`);
         }
         return `{ ${parts.join(", ")} }`;
      }
      case "scalar":
         if (live.type === "number") {
            return t.malloyType === "number" &&
               !(keepsBigIntegers && live.numberType === "bigint")
               ? "number"
               : undefined;
         }
         if (
            live.type === "timestamptz" &&
            t.malloyType === "timestamp" &&
            /^(TIMESTAMP WITH TIME ZONE|TIMESTAMPTZ)$/i.test(t.raw.trim())
         ) {
            return "timestamptz";
         }
         return live.type === t.malloyType ? t.malloyType : undefined;
   }
}

/**
 * One DuckDB type at the start of `s`: a scalar, `STRUCT(<name> <type>, …)`,
 * or either followed by `[]`. Undefined when it is no such type.
 */
function parseNestedType(
   s: string,
   depth = 0,
): { type: StoredType; rest: string } | undefined {
   if (depth > 16) return undefined;
   let type: StoredType;
   let rest: string;
   const struct = /^STRUCT\s*\(/i.exec(s);
   if (struct) {
      rest = s.slice(struct[0].length);
      const fields: { name: string; type: StoredType }[] = [];
      for (;;) {
         const name = /^\s*("(?:[^"]|"")*"|[A-Za-z_][A-Za-z0-9_]*)\s+/.exec(
            rest,
         );
         if (!name) return undefined;
         const fieldName = name[1].startsWith('"')
            ? name[1].slice(1, -1).replace(/""/g, '"')
            : name[1];
         const inner = parseNestedType(rest.slice(name[0].length), depth + 1);
         if (!inner) return undefined;
         fields.push({ name: fieldName, type: inner.type });
         rest = inner.rest.trimStart();
         if (rest.startsWith(",")) {
            rest = rest.slice(1);
            continue;
         }
         if (!rest.startsWith(")")) return undefined;
         rest = rest.slice(1);
         break;
      }
      type = { kind: "record", fields };
   } else {
      // A scalar, with its own parenthesised arguments (`DECIMAL(18,2)`).
      const scalar =
         /^[A-Za-z_][A-Za-z0-9_ ]*?(\([0-9,\s]*\))?(?=\s*(\[|,|\)|$))/.exec(s);
      if (!scalar) return undefined;
      const mapped = duckdbTypeToMalloyType(scalar[0]);
      if (!("type" in mapped)) return undefined;
      type = { kind: "scalar", raw: scalar[0], malloyType: mapped.type };
      rest = s.slice(scalar[0].length);
   }
   for (;;) {
      const list = /^\s*\[\s*\]/.exec(rest);
      if (!list) break;
      type = { kind: "array", element: type };
      rest = rest.slice(list[0].length);
   }
   return { type, rest };
}

/** A stored column, by name and DuckDB type, that no `type:` can declare. */
export interface UndeclarableColumn {
   name: string;
   type: string;
}

/**
 * Split a stored table's schema into the columns a `type:` declares, each
 * with its Malloy type ({@link storedMalloyType}), and the ones it leaves out.
 * A left-out column is never declared as a type that would change its values:
 * a query reading it fails to compile against the shape and is answered live,
 * and every other query still reads the table. Pure; a caller emitting a
 * shape reports what it left out ({@link reportLeftOutColumns}).
 */
export function declarableColumns(
   schema: readonly { name: string; type: string }[],
): {
   declared: { name: string; type: string; malloyType: string }[];
   leftOut: UndeclarableColumn[];
} {
   const declared: { name: string; type: string; malloyType: string }[] = [];
   const leftOut: UndeclarableColumn[] = [];
   for (const c of schema) {
      const malloyType = storedMalloyType(c.type);
      if (malloyType !== undefined) declared.push({ ...c, malloyType });
      else leftOut.push({ name: c.name, type: c.type.trim() });
   }
   return { declared, leftOut };
}

/** `name TYPE, …` for an error or log line: names and types, never values. */

/**
 * A wire entry's captured `schema` as a bound entry carries it: only columns
 * with both a name and a type, and undefined when none is left.
 */
export function boundSchemaOf(
   schema: readonly { name?: string; type?: string }[] | undefined,
): { schema: { name: string; type: string }[] } | Record<string, never> {
   const columns = (schema ?? []).flatMap((c) =>
      typeof c.name === "string" && typeof c.type === "string"
         ? [{ name: c.name, type: c.type }]
         : [],
   );
   return columns.length > 0 ? { schema: columns } : {};
}

/** {@link boundSchemaOf} for a wire entry's `nativeSchema`. */
export function boundNativeSchemaOf(
   schema: readonly { name?: string; type?: string }[] | undefined,
): { nativeSchema: { name: string; type: string }[] } | Record<string, never> {
   const bound = boundSchemaOf(schema);
   return "schema" in bound ? { nativeSchema: bound.schema } : {};
}

/**
 * A dry run's database binding with the warehouse types a build would
 * capture (`ManifestEntry.nativeSchema`), so it predicts the cast a big-integer
 * column is read through: each column as the dialect's own type for a
 * `bigint` number. A preview has no table to read; the real build's capture
 * can only be as wide or wider, and a type Malloy does not read as `bigint`
 * answers that column live, which no dry run can foresee.
 */
export function previewNativeSchema(
   binding: Binding,
   sources: Readonly<Record<string, { dialect?: string }>>,
): Binding {
   const dialect = sources[binding.sourceIDs?.[0] ?? ""]?.dialect;
   if (binding.tier !== "database" || !dialect) return binding;
   let type: string;
   try {
      type = getDialect(dialect).malloyTypeToSQLType({
         type: "number",
         numberType: "bigint",
      });
   } catch {
      return binding;
   }
   return {
      ...binding,
      nativeSchema: binding.schema.map((c) => ({ name: c.name, type })),
   };
}

/**
 * The stored columns to rename to the name the build source declares: each
 * whose name matches no declared name exactly and exactly one by a case fold,
 * when no other stored column folds to it. A warehouse that folds quoted
 * identifiers (Snowflake with `QUOTED_IDENTIFIERS_IGNORE_CASE = TRUE`)
 * returns Malloy's lower-case aliases upper-cased; stored under the declared
 * names, every reader finds them by exact name, as live does.
 */
export function canonicalRenames(
   stored: readonly { name?: string }[],
   declared: readonly string[],
): { from: string; to: string }[] {
   const exact = new Set(declared);
   const fold = (n: string) => n.toLowerCase();
   const names = stored.flatMap((c) => (c.name ? [c.name] : []));
   const storedFolds = new Map<string, number>();
   for (const name of names) {
      storedFolds.set(fold(name), (storedFolds.get(fold(name)) ?? 0) + 1);
   }
   return names.flatMap((name) => {
      if (exact.has(name) || storedFolds.get(fold(name)) !== 1) return [];
      const matches = declared.filter((d) => fold(d) === fold(name));
      return matches.length === 1 ? [{ from: name, to: matches[0] }] : [];
   });
}

/**
 * A stored schema with each column named as `names` spells it where it
 * matches only by a unique case fold ({@link canonicalRenames}): what a
 * reader matching by exact name reads, for a table stored before its build
 * renamed such columns. DuckDB resolves a column name case-insensitively, so
 * the renamed name still reads the stored column.
 */
export function canonicalStoredSchema<C extends { name: string }>(
   schema: readonly C[],
   names: readonly string[],
): C[] {
   const renamed = new Map(
      canonicalRenames(schema, names).map((r) => [r.from, r.to]),
   );
   return schema.map((c) =>
      renamed.has(c.name) ? { ...c, name: renamed.get(c.name)! } : c,
   );
}

/**
 * Whether a stored column's native type reads as the Malloy type the plan's
 * stand-in declared it as. A `storage=` build casts a string, boolean or
 * date to its declared type and keeps a number's warehouse type
 * ({@link castToDeclaredTypes}), so a number stood in as BIGINT or DOUBLE may be
 * stored as any integer, float or decimal type: every numeric type matches
 * every other (a Malloy `bigint` stood in as a DOUBLE and stored as the
 * warehouse's BIGINT among them), compared by class, never by spelling.
 */
export function sameStoredTypeClass(stored: string, standIn: string): boolean {
   const a = storedClass(stored);
   const b = storedClass(standIn);
   return a === b || (NUMERIC_CLASSES.has(a) && NUMERIC_CLASSES.has(b));
}

const NUMERIC_CLASSES = new Set(["integer", "float", "decimal"]);

/** A native type's class, case, spacing and parameters aside. */
function storedClass(nativeType: string): string {
   const spelled = nativeType.replace(/\s+/g, " ").trim().toUpperCase();
   const base = spelled.replace(/\(.*\)/, "").trim();
   if (/^(VARCHAR|TEXT|STRING|CHAR|BPCHAR)/.test(base)) return "string";
   if (base === "BOOLEAN" || base === "BOOL") return "boolean";
   if (base === "DATE") return "date";
   if (base === "TIMESTAMPTZ" || base === "TIMESTAMP WITH TIME ZONE") {
      return "timestamptz";
   }
   if (base.startsWith("TIMESTAMP")) return "timestamp";
   if (/^U?(TINYINT|SMALLINT|INTEGER|INT|BIGINT|HUGEINT)$/.test(base)) {
      return "integer";
   }
   if (/^(FLOAT|REAL|DOUBLE)/.test(base)) return "float";
   if (base === "DECIMAL" || base === "NUMERIC") return "decimal";
   return spelled;
}
