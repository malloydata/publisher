// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import {
   decimalTypeOf,
   guardDeltaDecimals,
   isDecimalDeltaGuardFailure,
   numericAsTextSQL,
   numericStorageFor,
   stageNumericColumns,
} from "./postgres_numeric_precision";

describe("numericStorageFor", () => {
   const stats = (
      maxIntegerDigits: number | null,
      maxScale: number | null,
      nonFinite = 0,
   ) => ({ column: "c", maxIntegerDigits, maxScale, nonFinite });

   it("chooses DECIMAL(38, widest scale seen)", () => {
      expect(numericStorageFor(stats(1, 3))).toEqual({
         column: "c",
         decimal: "DECIMAL(38,3)",
         scale: 3,
      });
      expect(numericStorageFor(stats(20, 10))).toMatchObject({
         decimal: "DECIMAL(38,10)",
      });
   });

   it("chooses DECIMAL(38,0) for integers and for a column with no value", () => {
      expect(numericStorageFor(stats(12, 0))).toMatchObject({
         decimal: "DECIMAL(38,0)",
      });
      expect(numericStorageFor(stats(null, null))).toMatchObject({
         decimal: "DECIMAL(38,0)",
      });
   });

   it("fits exactly 38 digits, and keeps a 39th as text", () => {
      expect(numericStorageFor(stats(28, 10))).toMatchObject({
         decimal: "DECIMAL(38,10)",
      });
      expect(numericStorageFor(stats(29, 10))).toEqual({
         column: "c",
         text: true,
         reason: "exceeds_decimal_38",
      });
      expect(numericStorageFor(stats(0, 39))).toMatchObject({
         reason: "exceeds_decimal_38",
      });
   });

   it("keeps NaN and Infinity as text", () => {
      expect(numericStorageFor(stats(1, 1, 2))).toEqual({
         column: "c",
         text: true,
         reason: "non_finite",
      });
   });
});

describe("numericAsTextSQL", () => {
   it("reads only the numeric columns as text, in order, quoted for Postgres", () => {
      expect(
         numericAsTextSQL(
            "SELECT 1",
            ["id", 'a"b', "Total"],
            ['a"b', "Total"],
            "postgres",
         ),
      ).toBe(
         `SELECT "id", "a""b"::text AS "a""b", "Total"::text AS "Total" FROM (SELECT 1) AS __numeric_text`,
      );
   });
});

describe("decimalTypeOf", () => {
   it("reads DESCRIBE's DECIMAL spelling, and nothing else", () => {
      expect(decimalTypeOf("DECIMAL(38,3)")).toEqual({
         precision: 38,
         scale: 3,
      });
      expect(decimalTypeOf("decimal(10, 2)")).toEqual({
         precision: 10,
         scale: 2,
      });
      expect(decimalTypeOf("DOUBLE")).toBeUndefined();
      expect(decimalTypeOf("VARCHAR")).toBeUndefined();
   });
});

describe("staging and guarding in DuckDB", () => {
   let db: DuckDBConnection;
   beforeAll(() => {
      db = new DuckDBConnection("numeric-precision-spec", ":memory:");
   });
   afterAll(async () => {
      await db.close();
   });

   const rows = async (sql: string) =>
      ((await db.runSQL(sql)).rows ?? []) as Record<string, unknown>[];

   it("stages Postgres's text and reads it back exactly at the widest scale", async () => {
      const staged = await stageNumericColumns(
         db,
         `SELECT * FROM (VALUES (1, '0.1', '-12.50', 'NaN', NULL::VARCHAR),
                                (2, '0.2', '3', '1', NULL::VARCHAR)) t(id, u, v, n, z)`,
         ["u", "v", "n", "z"],
         { table: "t" },
      );
      expect(staged.storage).toEqual([
         { column: "u", decimal: "DECIMAL(38,1)", scale: 1 },
         { column: "v", decimal: "DECIMAL(38,2)", scale: 2 },
         { column: "n", text: true, reason: "non_finite" },
         { column: "z", decimal: "DECIMAL(38,0)", scale: 0 },
      ]);
      const [sum] = await rows(
         `SELECT sum(u)::varchar AS su, sum(v)::varchar AS sv, typeof(any_value(n)) AS tn FROM (${staged.selectSQL})`,
      );
      expect(sum).toEqual({ su: "0.3", sv: "-9.50", tn: "VARCHAR" });
      await staged.drop();
   });

   it("casts a delta value that fits its column, and refuses one that would round", async () => {
      const guard = (value: string) =>
         guardDeltaDecimals(`SELECT '${value}' AS c`, [
            { column: "c", storedType: "DECIMAL(38,2)" },
         ]);
      expect(
         await rows(`SELECT c::varchar AS c FROM (${guard("-1.5")})`),
      ).toEqual([{ c: "-1.50" }]);
      for (const value of ["0.125", "NaN", "1" + "0".repeat(36)]) {
         let err: unknown;
         try {
            await db.runSQL(guard(value));
         } catch (e) {
            err = e;
         }
         expect(isDecimalDeltaGuardFailure(err)).toBe(true);
      }
   });

   it("leaves a delta column stored as text, or not as DECIMAL, alone", () => {
      const sql = "SELECT '1' AS c";
      expect(
         guardDeltaDecimals(sql, [{ column: "c", storedType: "VARCHAR" }]),
      ).toBe(sql);
   });
});
