// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import {
   PackageMaterialization,
   type PackageMaterializationOwner,
} from "./package_materialization";
import {
   bigIntegerReads,
   castToDeclaredTypes,
   liveKeepsBigIntegers,
   nativeTypeLookup,
   STORED_COLUMN_TYPE_MISMATCH_CODE,
   declaredColumnsOf,
   persistSourceDeclaredColumns,
   isCastConversionError,
   queryTimezoneOf,
   keepsWarehouseType,
   rebuildWouldRecast,
   serveTypePlan,
   storedColumnsOfOutput,
   storedColumnTypeMismatches,
   storageBuildCasts,
   storedTypeServesAs,
} from "./stored_column_types";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
} from "@malloydata/malloy";
import { plannedSources } from "./plan/build_plan";
import { mkdtempSync, rmSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";

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

describe("keepsWarehouseType", () => {
   it.each([
      ["BLOB", "string", true],
      ["INTERVAL", "string", true],
      [" interval ", "string", true],
      ["VARCHAR", "string", false],
      ["UUID", "string", false],
      ["INTEGER", "string", false],
      ["TIME", "string", false],
      // Only a declared `string` is cast to text in the first place.
      ["BLOB", "sql native", false],
      ["INTERVAL", "number", false],
   ])("%s declared %s: %p", (stored, declared, kept) => {
      expect(keepsWarehouseType(stored, declared)).toBe(kept);
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

   it("re-seeds for a string column whatever it was stored as, since any value renders as text", () => {
      expect(
         rebuildWouldRecast(
            [{ name: "code", type: "INTEGER" }],
            [{ name: "code", type: "string" }],
         ).map((x) => x.column),
      ).toEqual(["code"]);
   });

   // A Postgres `bytea` or `interval` the build keeps as the warehouse typed
   // it: a rebuild stores the same type, so re-seeding would never end.
   it("does not re-seed for a string the build keeps as a BLOB or an INTERVAL", () => {
      expect(
         rebuildWouldRecast(
            [
               { name: "bin", type: "BLOB" },
               { name: "iv", type: "INTERVAL" },
            ],
            [
               { name: "bin", type: "string" },
               { name: "iv", type: "string" },
            ],
         ),
      ).toEqual([]);
   });

   // The table a build leaves when its cast could not convert a value: every
   // column stored as the warehouse returned it. A re-seed would fail the same
   // cast and store the same table, a full read on every run.
   it("names none when a column's cast could fail, even beside castable ones", () => {
      const declared = [
         { name: "d", type: "date" },
         { name: "flag", type: "boolean" },
         { name: "is_open", type: "boolean" },
      ];
      expect(
         rebuildWouldRecast([{ name: "is_open", type: "VARCHAR" }], declared),
      ).toEqual([]);
      expect(
         rebuildWouldRecast(
            [
               { name: "d", type: "TIMESTAMP" },
               { name: "flag", type: "INTEGER" },
               { name: "is_open", type: "VARCHAR" },
            ],
            declared,
         ),
      ).toEqual([]);
      expect(
         rebuildWouldRecast([{ name: "d", type: "VARCHAR" }], declared),
      ).toEqual([]);
   });

   it("re-seeds for exactly the boolean and date columns the serve side reads through a cast", () => {
      const schema = [
         { name: "flag", type: "BIGINT" },
         { name: "d", type: "TIMESTAMP_MS" },
         { name: "day_of", type: "TIMESTAMP WITH TIME ZONE" },
      ];
      const declared = [
         { name: "flag", type: "boolean" },
         { name: "d", type: "date" },
         { name: "day_of", type: "date" },
      ];
      const served = serveTypePlan(storedColumnTypeMismatches(schema, declared))
         .casts.map((c) => c.column)
         .sort();
      expect(
         rebuildWouldRecast(schema, declared)
            .map((m) => m.column)
            .sort(),
      ).toEqual(served);
      expect(served).toEqual(["d", "day_of", "flag"]);
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

// Every storage build path takes its casts from storageBuildCasts, so this pins
// what a real build of a real persist source casts, from the compiler's output.
describe("storageBuildCasts over a compiled persist source", () => {
   // File-backed: db-duckdb shares one in-memory instance between connections.
   const dir = mkdtempSync(join(tmpdir(), "build-casts-"));
   const duckdb = new DuckDBConnection("duckdb", join(dir, "casts.duckdb"));
   afterAll(async () => {
      await duckdb.close();
      rmSync(dir, { recursive: true, force: true });
   });

   async function persisted(model: string) {
      const runtime = new Runtime({
         urlReader: new InMemoryURLReader(
            new Map([["file:///m.malloy", model]]),
         ),
         connections: new FixedConnectionMap(
            new Map([["duckdb", duckdb]]),
            "duckdb",
         ),
      });
      const compiled = await runtime
         .loadModel(new URL("file:///m.malloy"))
         .getModel();
      return plannedSources(await runtime.getBuildTargets(compiled));
   }

   it("declares each output column with its Malloy type, and the query time zone", async () => {
      await duckdb.runSQL(
         "CREATE OR REPLACE TABLE ev AS SELECT 1 AS id, DATE '2026-01-31' AS d, TIMESTAMPTZ '2026-01-31 23:00:00+00' AS tz",
      );
      const [zoned, plain] =
         await persisted(`##! experimental { persistence sql_functions }
source: ev is duckdb.table('ev')

#@ persist name="zoned" storage="lake"
source: zoned is ev -> {
  timezone: 'Asia/Tokyo'
  select: id, d_next is d + 1 month, flag is sql_boolean("\${id} = 1"), day_of is sql_date("\${tz}")
} extend {
  measure: n is count()
  view: by_flag is { group_by: flag; aggregate: n }
}

#@ persist name="plain" storage="lake"
source: plain is ev -> { select: id, label is concat('x', id::string) }
`);
      const casts = storageBuildCasts(zoned);
      expect(casts.declaredColumns).toEqual([
         { name: "id", type: "number" },
         { name: "d_next", type: "date" },
         { name: "flag", type: "boolean" },
         { name: "day_of", type: "date" },
      ]);
      expect(casts.castOptions).toEqual({ queryTimezone: "Asia/Tokyo" });
      // Under the zone a date is not cast at build; the boolean still is.
      expect(
         castToDeclaredTypes(
            "SELECT 1",
            casts.declaredColumns,
            casts.castOptions,
         ),
      ).toBe(
         'SELECT * REPLACE (CAST("flag" AS BOOLEAN) AS "flag") FROM (\nSELECT 1\n) AS __declared_types',
      );

      const plainCasts = storageBuildCasts(plain);
      expect(plainCasts.declaredColumns).toEqual([
         { name: "id", type: "number" },
         { name: "label", type: "string" },
      ]);
      expect(plainCasts.castOptions).toEqual({ queryTimezone: undefined });
   });
});

describe("bigIntegerReads (known limitation 12)", () => {
   const big = { name: "id", type: "number", numberType: "bigint" };
   const fields = [
      big,
      { name: "n", type: "number", numberType: "integer" },
      { name: "amt", type: "number", numberType: "float" },
      { name: "label", type: "string" },
   ];
   const schema = (idType: string) => [
      { name: "id", type: idType },
      { name: "n", type: "BIGINT" },
      { name: "amt", type: "DECIMAL(38,0)" },
      { name: "label", type: "VARCHAR" },
   ];

   it("reads a bigint field stored as any DuckDB integer, or a DECIMAL with no scale, as HUGEINT", () => {
      for (const t of [
         "BIGINT",
         "HUGEINT",
         "UBIGINT",
         "INTEGER",
         "DECIMAL(38,0)",
         "NUMERIC(20, 0)",
      ]) {
         expect(
            bigIntegerReads(schema(t), fields, { dialect: "duckdb" }),
         ).toEqual([{ column: "id", castTo: "HUGEINT" }]);
      }
      expect(
         bigIntegerReads(schema("UHUGEINT"), fields, { dialect: "duckdb" }),
      ).toEqual([{ column: "id", castTo: "UHUGEINT" }]);
   });

   it("leaves out a bigint field stored as a float, whose digits are gone", () => {
      for (const t of ["DOUBLE", "FLOAT", "DECIMAL(18,2)", "VARCHAR"]) {
         expect(
            bigIntegerReads(schema(t), fields, { dialect: "duckdb" }),
         ).toEqual([{ column: "id" }]);
      }
   });

   it("never touches a field live reads through a number: count(), a 32-bit sum, a DECIMAL", () => {
      const reads = bigIntegerReads(schema("BIGINT"), fields, {
         dialect: "duckdb",
      });
      expect(reads.map((r) => r.column)).toEqual(["id"]);
   });

   it("matches a stored column to its field by a unique case fold", () => {
      expect(
         bigIntegerReads([{ name: "ID", type: "BIGINT" }], [big], {
            dialect: "duckdb",
         }),
      ).toEqual([{ column: "ID", castTo: "HUGEINT" }]);
   });

   it("keeps a storage= table of a source whose live answer rounds as it is: served equals live", () => {
      expect(liveKeepsBigIntegers("postgres")).toBe(false);
      expect(
         bigIntegerReads(schema("BIGINT"), fields, { dialect: "postgres" }),
      ).toEqual([]);
   });

   const colocated = (dialect: string, nativeType: string | undefined) =>
      bigIntegerReads(schema("DOUBLE"), fields, {
         dialect,
         colocated: true,
         nativeTypeOf: nativeTypeLookup(
            nativeType === undefined ? [] : [{ name: "id", type: nativeType }],
         ),
      });

   it("reads a colocated column through a cast to exactly the warehouse type its build captured", () => {
      for (const [dialect, nativeType] of [
         ["postgres", "bigint"],
         ["duckdb", "BIGINT"],
         ["duckdb", "HUGEINT"],
         // Past HUGEINT: never narrowed to it (review of #1714).
         ["duckdb", "UHUGEINT"],
         ["snowflake", "NUMBER(38,0)"],
         ["standardsql", "INT64"],
         // Past BIGINT: never narrowed to it.
         ["sqlserver", "decimal(38,0)"],
         ["mysql", "decimal(38,0)"],
      ] as const) {
         expect(colocated(dialect, nativeType)).toEqual([
            { column: "id", castTo: nativeType },
         ]);
      }
   });

   it("leaves a colocated column out when no cast is exact in live's representation", () => {
      // Postgres stores a sum of BIGINTs as `numeric`, which can hold values
      // past 2^63 (a cast back to BIGINT failed) and which Malloy reads as a
      // float (a cast to it changes the representation): answered live.
      expect(colocated("postgres", "numeric")).toEqual([{ column: "id" }]);
      expect(colocated("duckdb", "DECIMAL(38,0)")).toEqual([{ column: "id" }]);
      expect(colocated("standardsql", "BIGNUMERIC")).toEqual([
         { column: "id" },
      ]);
      // MySQL's CAST takes no integer type by name (only SIGNED or
      // UNSIGNED, which Malloy does not read as a number): a cast to the
      // captured `bigint` is a syntax error, so the column answers live.
      expect(colocated("mysql", "bigint")).toEqual([{ column: "id" }]);
      expect(colocated("mysql", "bigint unsigned")).toEqual([{ column: "id" }]);
      // No type captured (a table built before the capture, a host manifest
      // without it): nothing is known to be exact.
      expect(colocated("postgres", undefined)).toEqual([{ column: "id" }]);
      expect(colocated("duckdb", undefined)).toEqual([{ column: "id" }]);
   });

   it("matches a captured type to its column by a unique case fold", () => {
      const lookup = nativeTypeLookup([
         { name: "ID", type: "NUMBER(38,0)" },
         { name: "x", type: "a" },
         { name: "X", type: "b" },
      ]);
      expect(lookup("id")).toBe("NUMBER(38,0)");
      expect(lookup("x")).toBe("a");
      expect(lookup("missing")).toBeUndefined();
   });

   it("reads DuckDB as keeping every digit, though Malloy's flag says otherwise", () => {
      expect(liveKeepsBigIntegers("duckdb")).toBe(true);
      expect(liveKeepsBigIntegers("snowflake")).toBe(true);
      expect(liveKeepsBigIntegers(undefined)).toBe(true);
   });
});

describe("storedColumnsOfOutput: a bigint read back from a table", () => {
   it("is a BIGINT when asked (a schema read off a table), and a DOUBLE as before otherwise", () => {
      // Malloy reads a 64-bit integer column back as numberType `bigint`
      // (a captured colocated schema, the v1 cutover); a compiled output's
      // integer says `integer`. A #(materialize) plan's compiled output keeps
      // its mapping (`bigint` as DOUBLE).
      const fields = [
         { name: "a", type: "number", numberType: "bigint" },
         { name: "b", type: "number", numberType: "integer" },
         { name: "c", type: "number", numberType: "float" },
      ];
      expect(storedColumnsOfOutput(fields, { nested: true })).toEqual([
         { name: "a", type: "DOUBLE" },
         { name: "b", type: "BIGINT" },
         { name: "c", type: "DOUBLE" },
      ]);
      expect(
         storedColumnsOfOutput(fields, { nested: true, bigint: true }),
      ).toEqual([
         { name: "a", type: "BIGINT" },
         { name: "b", type: "BIGINT" },
         { name: "c", type: "DOUBLE" },
      ]);
   });
});
