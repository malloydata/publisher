// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A stored answer's big integers, as live gives them on a dialect whose live
 * answer rounds them (known limitation 12).
 *
 * Live on Postgres reads its rows through JSON, so an integer past 2^53
 * becomes the nearest JavaScript number, and Malloy gives a field it types
 * as a `bigint` number that value as a BigInt: 9007199254740993 answers as
 * 9007199254740992n, which a caller receives as the JSON string
 * `"9007199254740992"`, and 9223372036854775807 as `"9223372036854776000"`
 * (the double's shortest text, not its exact integer). From 1e21 that number's text is exponential
 * (`1.8e+21`), and Malloy's normaliser keeps such a value a number, so a
 * caller receives the JSON number `1.8e+21`. A `storage=` table is read in DuckDB, through a cast
 * that keeps the field's `bigint` subtype and every digit
 * (`bigIntegerReads`), so its answer has live's type but more digits than
 * live's. Rounding each value of the answer as live's JSON stage rounds it
 * gives live's answer exactly: the rounding is of the answer, as live's is,
 * never of the stored values a query aggregates, since a sum of rounded
 * values is not the rounded sum.
 *
 * Query serving calls {@link roundBigIntegersAsLive} on every answer a leg
 * ran whose live dialect is Postgres ({@link liveRoundsBigIntegersAsPostgres});
 * it rounds in place. Pure but for that.
 */

import type { QueryData, QueryRecord } from "@malloydata/malloy";
import { capabilitiesOf } from "./dialect";

/**
 * Whether live rounds a source's big integers through JSON in the one way
 * measured (Postgres, by the R4 big-integer and rollup parity specs): to the
 * nearest double, as a BigInt below 1e21 and a number from it. A `storage=`
 * answer on such a source is read exactly and rounded after it runs as
 * live's is ({@link roundBigIntegersAsLive}). The other dialects whose live
 * answer rounds (`liveKeepsBigIntegers` false: Trino, Presto, Databricks,
 * SQL Server) are unmeasured, so their `storage=` big integers stay a plain
 * `number` and their answers are not rounded.
 */
export function liveRoundsBigIntegersAsPostgres(dialect: unknown): boolean {
   return capabilitiesOf(dialect).liveRoundsBigIntegersMeasured;
}

/** A field of a result's struct, as far as this reads it. */
interface ResultField {
   name: string;
   as?: string;
   type: string;
   numberType?: string;
   fields?: ResultField[];
   elementTypeDef?: ResultField;
}

/** A run's result, as far as this reads it: its rows and their struct. */
export interface ResultRows {
   _queryResult: {
      result: QueryData;
      structs: readonly { fields: readonly unknown[] }[];
   };
}

/**
 * A big integer as live's JSON stage and Malloy's normaliser give it: the
 * nearest double, read back from that double's shortest text (`String`), as
 * the normaliser reads it, never from the double's exact integer. From about
 * 2^55 the two differ: 9223372036854775807 is 9223372036854776000 to live,
 * not 9223372036854775808. A BigInt (a string, where it ran as one), or the
 * number itself where that text is exponential (from 1e21), which the
 * normaliser keeps a number.
 */
function roundedAsLive(value: unknown): unknown {
   const exact =
      typeof value === "bigint" ||
      (typeof value === "string" && /^-?\d+$/.test(value));
   if (!exact) return value;
   const nearest = Number(value);
   if (/e/i.test(String(nearest))) return nearest;
   return typeof value === "bigint" ? BigInt(String(nearest)) : String(nearest);
}

function roundValue(value: unknown, field: ResultField): unknown {
   if (value === null || value === undefined) return value;
   if (field.type === "number") {
      return field.numberType === "bigint" ? roundedAsLive(value) : value;
   }
   if (field.type === "array" && Array.isArray(value)) {
      if (field.fields) {
         for (const item of value) roundRecord(item as QueryRecord, field);
      } else if (field.elementTypeDef) {
         const element = field.elementTypeDef;
         return value.map((item) => roundValue(item, element));
      }
      return value;
   }
   if (field.type === "record" && field.fields && typeof value === "object") {
      roundRecord(value as QueryRecord, field);
   }
   return value;
}

/** Whether a struct holds a `bigint` number field at any depth. */
function holdsBigInteger(field: {
   numberType?: string;
   fields?: readonly ResultField[];
   elementTypeDef?: ResultField;
}): boolean {
   return (
      field.numberType === "bigint" ||
      (field.fields ?? []).some(holdsBigInteger) ||
      (field.elementTypeDef !== undefined &&
         holdsBigInteger(field.elementTypeDef))
   );
}

function roundRecord(
   row: QueryRecord,
   struct: { fields?: readonly ResultField[] },
): void {
   for (const field of struct.fields ?? []) {
      const name = field.as ?? field.name;
      if (name in row) row[name] = roundValue(row[name], field) as never;
   }
}

/**
 * Round every `bigint` value of a result, at any depth, as live's JSON stage
 * rounds it, in place. On an answer already rounded (a database table read
 * on live's own connection) it changes nothing.
 */
export function roundBigIntegersAsLive(result: ResultRows): void {
   const { structs, result: rows } = result._queryResult;
   const struct = structs[structs.length - 1] as
      | { fields?: readonly ResultField[] }
      | undefined;
   if (!struct || !holdsBigInteger(struct)) return;
   for (const row of rows) roundRecord(row, struct);
}
