// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import {
   PackageMaterialization,
   type PackageMaterializationOwner,
} from "./package_materialization";
import {
   castToDeclaredTypes,
   STORED_COLUMN_TYPE_MISMATCH_CODE,
   declaredColumnsOf,
   persistSourceDeclaredColumns,
   isCastConversionError,
   queryTimezoneOf,
   rebuildWouldRecast,
   serveTypePlan,
   storedColumnTypeMismatches,
   storedTypeServesAs,
} from "./stored_column_types";

describe("storedTypeServesAs", () => {
   const cases: [string, string, boolean | undefined][] = [
      ["VARCHAR", "string", true],
      ["UUID", "string", true],
      ["INTEGER", "string", false],
      ["DECIMAL(18,3)", "number", true],
      ["HUGEINT", "number", true],
      ["UBIGINT", "number", true],
      ["DOUBLE", "number", true],
      ["VARCHAR", "number", false],
      ["BOOLEAN", "boolean", true],
      ["INTEGER", "boolean", false],
      ["DATE", "date", true],
      ["TIMESTAMP", "date", false],
      ["TIMESTAMP WITH TIME ZONE", "date", false],
      ["TIMESTAMP", "timestamp", true],
      ["TIMESTAMP_NS", "timestamp", true],
      ["TIMESTAMP WITH TIME ZONE", "timestamp", true],
      ["DATE", "timestamp", false],
      ["TIMESTAMP WITH TIME ZONE", "timestamptz", true],
      ["TIMESTAMP", "timestamptz", true],
      ["DATE", "timestamptz", false],
      // Unmapped stored types serve as json, which is no checked type.
      ["INTEGER[]", "number", false],
      ["STRUCT(a INTEGER)", "string", false],
      // Not compared: today's behaviour is kept.
      ["VARCHAR", "json", undefined],
      ["INTEGER[]", "array", undefined],
      ["BLOB", "sql native", undefined],
   ];
   for (const [stored, declared, expected] of cases) {
      it(`${stored} as ${declared}: ${expected}`, () => {
         expect(storedTypeServesAs(stored, declared)).toBe(expected);
      });
   }
});

describe("storedColumnTypeMismatches", () => {
   it("reports a mismatch under the author's spelling, matching case-insensitively", () => {
      expect(
         storedColumnTypeMismatches(
            [
               { name: "ORDER_MONTH", type: "TIMESTAMP" },
               { name: "amount", type: "DECIMAL(18,2)" },
               { name: "hidden", type: "VARCHAR" },
            ],
            [
               { name: "order_month", type: "date" },
               { name: "amount", type: "number" },
            ],
         ),
      ).toEqual([
         {
            column: "order_month",
            storedType: "TIMESTAMP",
            declaredType: "date",
         },
      ]);
   });

   it("matches nothing on an ambiguous fold", () => {
      expect(
         storedColumnTypeMismatches(
            [{ name: "aB", type: "TIMESTAMP" }],
            [
               { name: "ab", type: "date" },
               { name: "AB", type: "date" },
            ],
         ),
      ).toEqual([]);
   });
});

describe("declaredColumnsOf", () => {
   it("keeps named, typed fields and skips the rest", () => {
      expect(
         declaredColumnsOf([
            { name: "a", type: "date" },
            { name: "j", type: "join" },
            { name: 3, type: "number" },
            null,
            { type: "string" },
         ]),
      ).toEqual([
         { name: "a", type: "date" },
         { name: "j", type: "join" },
      ]);
   });

   it("reads a persist source's atomic fields, and none when they cannot be read", () => {
      const atomic = (name: string, type: string) => ({
         name,
         type,
         isAtomicField: () => true,
      });
      expect(
         persistSourceDeclaredColumns({
            _explore: {
               intrinsicFields: [
                  atomic("d", "date"),
                  { name: "v", type: "turtle", isAtomicField: () => false },
               ],
            },
         }),
      ).toEqual([{ name: "d", type: "date" }]);
      expect(
         persistSourceDeclaredColumns({
            get _explore(): { intrinsicFields: unknown[] } {
               throw new Error("no explore");
            },
         }),
      ).toEqual([]);
   });
});

describe("castToDeclaredTypes", () => {
   let duckdb: DuckDBConnection;
   beforeAll(() => {
      duckdb = new DuckDBConnection("cast_spec", ":memory:");
   });
   afterAll(async () => {
      await duckdb.close();
   });

   it("leaves the SELECT alone when nothing is castable", () => {
      const sql = "SELECT 1 AS n";
      expect(
         castToDeclaredTypes(sql, [
            { name: "n", type: "number" },
            { name: "t", type: "timestamp" },
         ]),
      ).toBe(sql);
   });

   it("casts string, boolean and date columns in place, keeping order and the rest", async () => {
      const sql = castToDeclaredTypes(
         `SELECT TIMESTAMP '2026-02-01 00:00:00' AS "Month", 1 AS flag,
                 42 AS code, 1.5 AS amount, TIMESTAMP '2026-02-01 10:00:00' AS ts -- a trailing comment
          ;`,
         [
            { name: "Month", type: "date" },
            { name: "flag", type: "boolean" },
            { name: "code", type: "string" },
            { name: "amount", type: "number" },
            { name: "ts", type: "timestamp" },
         ],
      );
      const described = (await duckdb.runSQL(`DESCRIBE ${sql}`)).rows as {
         column_name: string;
         column_type: string;
      }[];
      expect(described.map((c) => [c.column_name, c.column_type])).toEqual([
         ["Month", "DATE"],
         ["flag", "BOOLEAN"],
         ["code", "VARCHAR"],
         ["amount", "DECIMAL(2,1)"],
         ["ts", "TIMESTAMP"],
      ]);
      const [row] = (await duckdb.runSQL(sql)).rows;
      expect(row).toMatchObject({ flag: true, code: "42" });
   });

   it("quotes a name that needs it", () => {
      expect(
         castToDeclaredTypes("SELECT 1", [{ name: 'a"b', type: "date" }]),
      ).toContain('CAST("a""b" AS DATE) AS "a""b"');
   });
});

describe("PackageMaterialization.storedColumnTypeWarnings", () => {
   it("warns once per source and column, keyed by the source's sourceID", () => {
      const of = (sourceID: string, handling: "cast" | "live") => [
         {
            sourceName: "orders",
            sourceID,
            mismatches: [
               {
                  column: "order_month",
                  storedType: "TIMESTAMP",
                  declaredType: "date",
                  handling,
               },
            ],
         },
      ];
      const models = new Map<string, unknown>([
         // a.malloy declares orders; b.malloy imports the same source.
         [
            "a.malloy",
            { storedColumnTypeMismatches: () => of("orders@a", "cast") },
         ],
         [
            "b.malloy",
            { storedColumnTypeMismatches: () => of("orders@a", "cast") },
         ],
         // c.malloy declares an orders of its own: reported too.
         [
            "c.malloy",
            { storedColumnTypeMismatches: () => of("orders@c", "live") },
         ],
         ["d.malloy", {}],
      ]);
      const materialization = new PackageMaterialization({
         models: () => models,
      } as unknown as PackageMaterializationOwner);
      const warnings = materialization.storedColumnTypeWarnings();
      expect(warnings.map((w) => w.model)).toEqual(["a.malloy", "c.malloy"]);
      expect(warnings[0]).toMatchObject({
         subject: "orders",
         code: STORED_COLUMN_TYPE_MISMATCH_CODE,
         severity: "warn",
      });
      expect(warnings[0].message).toContain("`order_month` as TIMESTAMP");
      expect(warnings[0].message).toContain("read through a cast");
      expect(warnings[1].message).toContain("answered live");
   });
});

describe("the serve-side cast plan", () => {
   const m = (column: string, storedType: string, declaredType: string) => ({
      column,
      storedType,
      declaredType,
   });

   it("casts an integer flag to boolean and a naive timestamp to date", () => {
      expect(
         serveTypePlan([
            m("f", "INTEGER", "boolean"),
            m("d", "TIMESTAMP", "date"),
         ]),
      ).toEqual({
         casts: [
            { column: "f", castTo: "BOOLEAN" },
            { column: "d", castTo: "DATE" },
         ],
         unservable: [],
      });
   });

   it("dates an instant only without a query time zone", () => {
      const tz = m("d", "TIMESTAMP WITH TIME ZONE", "date");
      expect(serveTypePlan([tz]).casts).toEqual([
         { column: "d", castTo: "DATE" },
      ]);
      expect(serveTypePlan([tz], { queryTimezone: "Asia/Tokyo" })).toEqual({
         casts: [],
         unservable: [tz],
      });
   });

   it("casts nothing that could fail a query or has no single right type", () => {
      const text = [
         m("n", "VARCHAR", "number"),
         m("b", "VARCHAR", "boolean"),
         m("s", "INTEGER", "string"),
      ];
      expect(serveTypePlan(text)).toEqual({ casts: [], unservable: text });
   });
});

describe("the build cast under a query time zone", () => {
   it("leaves a date uncast, and still casts a boolean", () => {
      const sql = castToDeclaredTypes(
         "SELECT 1",
         [
            { name: "d", type: "date" },
            { name: "b", type: "boolean" },
         ],
         { queryTimezone: "Asia/Tokyo" },
      );
      expect(sql).toContain('CAST("b" AS BOOLEAN)');
      expect(sql).not.toContain("AS DATE");
   });

   it("reads the zone from the query stage that sets it", () => {
      expect(
         queryTimezoneOf({
            type: "query_source",
            query: { pipeline: [{ queryTimezone: "Asia/Tokyo" }, {}] },
         }),
      ).toBe("Asia/Tokyo");
      expect(queryTimezoneOf({ type: "table" })).toBeUndefined();
   });
});

describe("rebuildWouldRecast", () => {
   it("names only what a rebuild would cast", () => {
      expect(
         rebuildWouldRecast(
            [
               { name: "d", type: "TIMESTAMP" },
               { name: "n", type: "VARCHAR" },
            ],
            [
               { name: "d", type: "date" },
               { name: "n", type: "number" },
            ],
         ).map((x) => x.column),
      ).toEqual(["d"]);
   });
});

describe("isCastConversionError", () => {
   it("recognizes DuckDB refusing a value, and nothing else", () => {
      expect(
         isCastConversionError(
            new Error(
               "Conversion Error: Could not convert string 'open' to BOOL",
            ),
         ),
      ).toBe(true);
      expect(isCastConversionError(new Error("Binder Error: x"))).toBe(false);
   });
});
