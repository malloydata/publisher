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
 *   ({@link storedTypeServesAs}) is left off the serve shape, so a query that
 *   reads it is answered live and every other query still reads the table.
 *   That covers tables built before the cast, and the types no cast can safely
 *   produce (a number stored as text).
 */

import { quoteIdentifier } from "../quoting";
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
 * already the target type. A value the cast cannot convert (a date column the
 * warehouse returned as unparseable text) fails the build rather than storing
 * a value live would not produce.
 */
export function castToDeclaredTypes(
   selectSQL: string,
   declared: readonly DeclaredColumn[],
): string {
   const casts = declared
      .filter((d) => BUILD_CAST_TARGETS[d.type] !== undefined)
      .map((d) => {
         const column = quoteIdentifier(d.name, "duckdb");
         return `CAST(${column} AS ${BUILD_CAST_TARGETS[d.type]}) AS ${column}`;
      });
   if (casts.length === 0) return selectSQL;
   // A trailing terminator would end the subselect early, and a trailing `--`
   // comment would swallow the closing paren, hence its own line.
   const inner = selectSQL.replace(/[\s;]+$/, "");
   return `SELECT * REPLACE (${casts.join(", ")}) FROM (\n${inner}\n) AS __declared_types`;
}
