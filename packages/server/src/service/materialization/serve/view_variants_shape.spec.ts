// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   resultKinds,
   stageShape,
   storedColumnCasts,
   viewDeclaration,
   type ViewShape,
} from "./view_variants";

/**
 * The view leg's pure decisions about a stored result: which columns live
 * reports as measures or nests, the last stage's columns and ordering, the
 * casts a `storage=` read needs (or why none is faithful), and the view
 * redeclared over the stored rows (or why it cannot be).
 */

type Column = ViewShape["columns"][number];

function col(
   name: string,
   kind: Column["kind"] = "dimension",
   type = "string",
   extra: Partial<Column> = {},
): Column {
   return { name, type, kind, notes: [], field: { name, type }, ...extra };
}

function shape(over: Partial<ViewShape> = {}): ViewShape {
   return {
      columns: [col("region"), col("total", "measure", "number")],
      orderBy: [{ name: "region", desc: false }],
      notes: [],
      stageType: "reduce",
      ...over,
   };
}

describe("resultKinds", () => {
   it("reads a measure from an atomic field that was one, and a nest from an explore field", () => {
      const field = (
         name: string,
         atomic: boolean,
         measure?: boolean,
      ): unknown => ({
         name,
         isAtomicField: () => atomic,
         isExploreField: () => !atomic,
         ...(measure !== undefined ? { sourceWasMeasure: () => measure } : {}),
      });
      const kinds = resultKinds({
         allFields: [
            field("region", true, false),
            field("total", true, true),
            field("legacy", true),
            field("by_month", false),
         ],
      });
      expect([...kinds.measures]).toEqual(["total"]);
      expect([...kinds.nests]).toEqual(["by_month"]);
   });
});

describe("stageShape", () => {
   const none = { measures: new Set<string>(), nests: new Set<string>() };

   it("names each column by its `as`, else its name, and drops one with neither", () => {
      const { columns } = stageShape(
         {
            outputStruct: {
               fields: [
                  { name: "carrier", as: "c", type: "string" },
                  { name: "n", type: "number" },
                  { type: "number" },
               ],
            },
         },
         none,
      );
      expect(columns.map((c) => [c.name, c.type])).toEqual([
         ["c", "string"],
         ["n", "number"],
      ]);
   });

   it("keeps a timeframe Malloy truncates by, and drops any other", () => {
      const { columns } = stageShape(
         {
            outputStruct: {
               fields: [
                  { name: "m", type: "date", timeframe: "month" },
                  { name: "x", type: "date", timeframe: "fortnight" },
               ],
            },
         },
         none,
      );
      expect(columns[0].timeframe).toBe("month");
      expect(columns[1].timeframe).toBeUndefined();
   });

   it("gives each column live's kind, a nest before a measure", () => {
      const { columns } = stageShape(
         {
            outputStruct: {
               fields: [
                  { name: "a", type: "string" },
                  { name: "b", type: "number" },
                  { name: "c", type: "array" },
               ],
            },
         },
         { measures: new Set(["b", "c"]), nests: new Set(["c"]) },
      );
      expect(columns.map((c) => c.kind)).toEqual([
         "dimension",
         "measure",
         "nest",
      ]);
   });

   it("reads an ordering by output position or by name, and drops one that names neither", () => {
      const { orderBy } = stageShape(
         {
            outputStruct: {
               fields: [
                  { name: "a", type: "string" },
                  { name: "b", type: "number" },
               ],
            },
            orderBy: [
               { field: 2, dir: "desc" },
               { field: "a", dir: "asc" },
               { field: 9 },
               { field: { not: "a name" } },
            ],
         },
         none,
      );
      expect(orderBy).toEqual([
         { name: "b", desc: true },
         { name: "a", desc: false },
      ]);
   });

   it("gives no columns and no ordering for no stage", () => {
      expect(stageShape(undefined, none)).toEqual({ columns: [], orderBy: [] });
   });
});

describe("storedColumnCasts", () => {
   it("needs no cast when every stored column serves as declared", () => {
      const out = storedColumnCasts(
         [
            { name: "region", type: "VARCHAR" },
            { name: "total", type: "DOUBLE" },
         ],
         shape(),
      );
      expect(out).toEqual({ casts: new Map() });
   });

   it("refuses a column live answers as a warehouse-native or JSON value", () => {
      expect(
         storedColumnCasts(
            [],
            shape({ columns: [col("t", "dimension", "sql native")] }),
         ),
      ).toEqual({
         refusal:
            "live answers `t` as a warehouse-native value, which no stored read gives exactly",
      });
      expect(
         storedColumnCasts(
            [],
            shape({ columns: [col("j", "dimension", "json")] }),
         ),
      ).toEqual({
         refusal:
            "live answers `j` as a JSON value, which no stored read gives exactly",
      });
   });

   it("reads a date stored as a timestamp, and a boolean stored as an integer, through a cast", () => {
      const out = storedColumnCasts(
         [
            { name: "d", type: "TIMESTAMP" },
            { name: "b", type: "INTEGER" },
         ],
         shape({
            columns: [
               col("d", "dimension", "date"),
               col("b", "dimension", "boolean"),
            ],
         }),
      );
      expect("casts" in out && [...out.casts]).toEqual([
         ["d", { castTo: "DATE" }],
         ["b", { castTo: "BOOLEAN" }],
      ]);
   });

   it("refuses a date stored as an instant when the view has a query time zone", () => {
      const out = storedColumnCasts(
         [{ name: "d", type: "TIMESTAMP WITH TIME ZONE" }],
         shape({
            columns: [col("d", "dimension", "date")],
            queryTimezone: "America/Denver",
         }),
      );
      expect(out).toEqual({
         refusal:
            "the stored `d` (TIMESTAMP WITH TIME ZONE) cannot be read as live's date",
      });
   });

   it("refuses a column no faithful cast reads as live's type", () => {
      const out = storedColumnCasts(
         [{ name: "total", type: "VARCHAR" }],
         shape(),
      );
      expect(out).toEqual({
         refusal:
            "the stored `total` (VARCHAR) cannot be read as live's number",
      });
   });

   it("leaves a nest and an untyped column out of the type check", () => {
      const out = storedColumnCasts(
         [
            { name: "n", type: "VARCHAR" },
            { name: "u", type: "VARCHAR" },
         ],
         shape({
            columns: [col("n", "nest", "array"), col("u", "dimension", "")],
         }),
      );
      expect(out).toEqual({ casts: new Map() });
   });

   it("reads a big integer as HUGEINT, and refuses one stored as a float", () => {
      const big = col("n", "measure", "number", {
         field: { name: "n", type: "number", numberType: "bigint" },
      });
      const exact = storedColumnCasts(
         [{ name: "n", type: "BIGINT" }],
         shape({ columns: [big], dialect: "duckdb" }),
      );
      expect("casts" in exact && [...exact.casts]).toEqual([
         ["n", { nativeCast: "HUGEINT" }],
      ]);
      expect(
         storedColumnCasts(
            [{ name: "n", type: "DOUBLE" }],
            shape({ columns: [big], dialect: "duckdb" }),
         ),
      ).toEqual({
         refusal: "the stored `n` (DOUBLE) cannot hold live's integer exactly",
      });
   });

   it("reads a big integer as HUGEINT on a dialect whose live answer rounds, a float-stored one too", () => {
      const big = col("n", "measure", "number", {
         field: { name: "n", type: "number", numberType: "bigint" },
      });
      for (const type of ["DOUBLE", "BIGINT"]) {
         const out = storedColumnCasts(
            [{ name: "n", type }],
            shape({ columns: [big], dialect: "postgres" }),
         );
         expect("casts" in out && [...out.casts]).toEqual([
            ["n", { nativeCast: "HUGEINT" }],
         ]);
      }
   });

   it("does not read a column it already casts as a big integer too", () => {
      const flag = col("b", "dimension", "boolean", {
         field: { name: "b", type: "number", numberType: "bigint" },
      });
      const out = storedColumnCasts(
         [{ name: "b", type: "BIGINT" }],
         shape({ columns: [flag], dialect: "duckdb" }),
      );
      expect("casts" in out && [...out.casts]).toEqual([
         ["b", { castTo: "BOOLEAN" }],
      ]);
   });
});

describe("viewDeclaration", () => {
   it("selects a dimension-only result's stored columns, in order, with its ordering", () => {
      const out = viewDeclaration(
         "by_region",
         shape({
            columns: [col("region"), col("city")],
            orderBy: [{ name: "city", desc: true }],
         }),
      );
      expect(out).toEqual({
         text: "  view: by_region is { select: region, city; order_by: city desc }",
      });
   });

   it("writes the view's notes and own time zone on a select", () => {
      const out = viewDeclaration(
         "v",
         shape({
            columns: [col("region")],
            orderBy: [],
            notes: ["# bar_chart"],
            viewTimezone: "America/Denver",
         }),
      );
      expect(out).toEqual({
         text: "  # bar_chart\n  view: v is { timezone: 'America/Denver'; select: region }",
      });
   });

   it("groups a result with a measure again, each measure re-aggregating its one stored value", () => {
      const out = viewDeclaration("by_region", shape());
      expect(out).toEqual({
         text: "  view: by_region is {\n    group_by: region\n    aggregate:\n      total is total.max()\n    order_by: region asc\n  }",
      });
   });

   it("re-aggregates a boolean measure through the number it picks, NULL kept NULL", () => {
      const out = viewDeclaration(
         "v",
         shape({
            columns: [col("region"), col("any_late", "measure", "boolean")],
            orderBy: [],
         }),
      );
      expect("text" in out && out.text).toContain(
         "any_late is max(pick null when any_late is null pick 1 when any_late else 0) = 1",
      );
   });

   it("writes a measure's own notes on its aggregate, and the time zone on a regroup", () => {
      const out = viewDeclaration(
         "v",
         shape({
            columns: [
               col("region"),
               col("total", "measure", "number", { notes: ["# currency"] }),
            ],
            orderBy: [],
            viewTimezone: "UTC",
         }),
      );
      expect("text" in out && out.text).toBe(
         "  view: v is {\n    timezone: 'UTC'\n    group_by: region\n    aggregate:\n      # currency\n      total is total.max()\n  }",
      );
   });

   it("opens the statement with the head a named query passes", () => {
      const out = viewDeclaration(
         "q",
         shape({ columns: [col("region")], orderBy: [] }),
         "query: q is orders ->",
      );
      expect(out).toEqual({
         text: "  query: q is orders -> { select: region }",
      });
   });

   it("refuses an index, whose columns a stored read would annotate", () => {
      expect(viewDeclaration("v", shape({ stageType: "index" }))).toEqual({
         refusal:
            "it is an `index:`, whose columns live writes without the annotations a stored read gives them",
      });
   });

   it("refuses to regroup a stage that does not group", () => {
      expect(viewDeclaration("v", shape({ stageType: "project" }))).toEqual({
         refusal:
            "its last stage is a project that does not group, so its rows have no key",
      });
      expect(viewDeclaration("v", shape({ stageType: undefined }))).toEqual({
         refusal:
            "its last stage is a stage that does not group, so its rows have no key",
      });
   });

   it("refuses to rebuild a nest", () => {
      expect(
         viewDeclaration(
            "v",
            shape({
               columns: [col("region"), col("by_city", "nest", "array")],
            }),
         ),
      ).toEqual({
         refusal:
            "it nests `by_city`, whose stored rows cannot be rebuilt exactly",
      });
   });

   it("refuses a result that groups by no dimension", () => {
      expect(
         viewDeclaration(
            "v",
            shape({
               columns: [col("total", "measure", "number")],
               orderBy: [],
            }),
         ),
      ).toEqual({
         refusal:
            "it groups by no dimension, so an empty result would come back as one row",
      });
   });

   it("refuses a measure of a type max() does not take", () => {
      expect(
         viewDeclaration(
            "v",
            shape({
               columns: [col("region"), col("tags", "measure", "array")],
               orderBy: [],
            }),
         ),
      ).toEqual({
         refusal:
            "the measure `tags` is a array that max() cannot re-aggregate",
      });
      expect(
         viewDeclaration(
            "v",
            shape({
               columns: [col("region"), col("x", "measure", "")],
               orderBy: [],
            }),
         ),
      ).toEqual({
         refusal: "the measure `x` is a value that max() cannot re-aggregate",
      });
   });
});
