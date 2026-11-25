// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { Result } from "@malloydata/malloy";

/**
 * Give every record in a result each key its declared type names, as null
 * where the row omits it. Does nothing when Malloy's internal result shape is
 * not the one read here.
 *
 * A record's declared keys are not always in every row. Malloy infers the
 * keys of a Snowflake OBJECT (or a VARIANT holding objects) by sampling the
 * column, so the type is the union of the keys the sample held, and
 * Snowflake's `OBJECT_CONSTRUCT` drops a key whose value is NULL. Malloy's
 * result wrapper reads a missing key as `undefined`, which it accepts only as
 * null for no type: a number throws `Cannot convert undefined to number`.
 * Only the result's struct says which keys a record declares; a connection
 * never sees it, so this runs on the result, for every dialect.
 *
 * Records nested in records, records in arrays and arrays of arrays are all
 * walked, and a value where a record is declared that is not one is null.
 * The rows are changed in place, so the full and the compact answer both
 * carry the keys.
 */
export function fillDeclaredRecordKeys(result: Result): void {
   const structs = result._queryResult?.structs as unknown as
      | ResultType[]
      | undefined;
   const root = structs?.[structs.length - 1];
   if (root?.fields === undefined) return;
   const rows: unknown[] = result.data.rawData;
   for (const row of rows) fillFields(row, root.fields, false);
}

/** A result field or element type, as much of Malloy's definition as this reads. */
interface ResultType {
   type: string;
   name?: string;
   as?: string;
   fields?: ResultType[];
   elementTypeDef?: ResultType;
}

/**
 * Walk `record`'s declared fields. `fill` adds the keys the record omits; a
 * top-level row is only walked, since its columns are the query's own.
 */
function fillFields(
   record: unknown,
   fields: ResultType[],
   fill: boolean,
): void {
   if (!isPlainObject(record)) return;
   const values = record as Record<string, unknown>;
   for (const field of fields) {
      if (field.type === "turtle") continue;
      const key = field.as ?? field.name;
      if (key === undefined) continue;
      if (!Object.hasOwn(values, key)) {
         if (fill) values[key] = null;
         continue;
      }
      values[key] = fillValue(values[key], field);
   }
}

/**
 * `value` with its records' keys filled, as `type` declares it. A value
 * where a record is declared that is not one (a scalar a Snowflake VARIANT
 * holds in some rows) is null: Malloy's wrapper would read each declared
 * key of it as `undefined` and throw on a date or a number.
 */
function fillValue(value: unknown, type: ResultType): unknown {
   if (value === null || value === undefined) return value;
   if (type.type === "record" && type.fields) {
      if (!isPlainObject(value)) return null;
      fillFields(value, type.fields, true);
   } else if (type.type === "array" && Array.isArray(value)) {
      const element = type.elementTypeDef;
      for (let i = 0; i < value.length; i++) {
         if (element?.type === "record_element" && type.fields) {
            if (isPlainObject(value[i]))
               fillFields(value[i], type.fields, true);
            else if (value[i] !== null && value[i] !== undefined)
               value[i] = null;
         } else if (element) {
            value[i] = fillValue(value[i], element);
         }
      }
   }
   return value;
}

function isPlainObject(value: unknown): boolean {
   if (value === null || typeof value !== "object" || Array.isArray(value)) {
      return false;
   }
   const proto = Object.getPrototypeOf(value);
   return proto === Object.prototype || proto === null;
}
