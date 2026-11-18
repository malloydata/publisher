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
 * `"9007199254740992"`. A `storage=` table is read in DuckDB, through a cast
 * that keeps the field's `bigint` subtype and every digit
 * (`bigIntegerReads`), so its answer has live's type but more digits than
 * live's. Rounding each value of the answer as live's JSON stage rounds it
 * gives live's answer exactly: the rounding is of the answer, as live's is,
 * never of the stored values a query aggregates, since a sum of rounded
 * values is not the rounded sum.
 *
 * {@link roundStoredAnswerAsLive} is the one entry, called by query
 * serving on every answer a leg ran; it rounds in place.
 */

import type {
   QueryData,
   QueryMaterializer,
   QueryRecord,
} from "@malloydata/malloy";
import { liveKeepsBigIntegers } from "../stored_column_types";

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

/** A big integer as live's JSON stage gives it: the nearest double's integer. */
function roundedAsLive(value: unknown): unknown {
   if (typeof value === "bigint") return BigInt(Number(value));
   if (typeof value === "string" && /^-?\d+$/.test(value)) {
      return String(BigInt(Number(value)));
   }
   return value;
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
   if (!struct) return;
   for (const row of rows) roundRecord(row, struct);
}

/**
 * A stored answer's big integers rounded as live rounds them, when the
 * query's live dialect does ({@link liveKeepsBigIntegers} false). A live
 * compile that cannot say its dialect leaves the answer as it ran.
 */
export async function roundStoredAnswerAsLive(
   result: unknown,
   liveRunnable: QueryMaterializer,
): Promise<void> {
   let dialect: string;
   try {
      dialect = (await liveRunnable.getPreparedQuery()).dialect;
   } catch {
      return;
   }
   if (liveKeepsBigIntegers(dialect)) return;
   roundBigIntegersAsLive(result as ResultRows);
}
