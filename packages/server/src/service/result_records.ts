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
 * walked. The rows are changed in place, so the full and the compact answer
 * both carry the keys.
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
      fillValue(values[key], field);
   }
}

function fillValue(value: unknown, type: ResultType): void {
   if (value === null || value === undefined) return;
   if (type.type === "record" && type.fields) {
      fillFields(value, type.fields, true);
   } else if (type.type === "array" && Array.isArray(value)) {
      const element = type.elementTypeDef;
      if (element?.type === "record_element" && type.fields) {
         for (const item of value) fillFields(item, type.fields, true);
      } else if (element) {
         for (const item of value) fillValue(item, element);
      }
   }
}

function isPlainObject(value: unknown): boolean {
   if (value === null || typeof value !== "object" || Array.isArray(value)) {
      return false;
   }
   const proto = Object.getPrototypeOf(value);
   return proto === Object.prototype || proto === null;
}
