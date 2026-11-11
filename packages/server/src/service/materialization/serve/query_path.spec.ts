// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
} from "@malloydata/malloy";
import { afterAll, describe, expect, it } from "bun:test";
import {
   readPreparedQuery,
   resolveStructRefSource,
} from "../../governance/ir/prepared_query";
import {
   buildQueryPath,
   MAX_NOT_CONSIDERED,
   type MaterializedTableCatalog,
   type PlannedTable,
   type QueryPathInput,
   queryShapeOf,
   sourceChainOf,
   type StorageTable,
} from "./query_path";

const shape = {
   sourceName: "daily",
   groupedBy: ["region"],
   filteredBy: [],
   measures: ["total"],
};

const planned = (
   name: string,
   origin: PlannedTable["origin"] = "persist",
): PlannedTable => ({
   sourceID: `${name}@file:///m.malloy`,
   name,
   sourceEntityId: `id_${name}`,
   origin,
});

function catalog(
   tables: PlannedTable[],
   bound: Record<string, string> = {},
): MaterializedTableCatalog {
   return {
      planned: new Map(tables.map((t) => [t.sourceID, t])),
      bound: Object.fromEntries(
         Object.entries(bound).map(([id, tableName]) => [id, { tableName }]),
      ),
   };
}

const storageOf = (name: string): StorageTable => ({
   sourceName: name,
   sourceIDs: [`${name}@file:///m.malloy`],
   origin: "persist",
   tablePath: `lake.t_${name}`,
});

function input(overrides: Partial<QueryPathInput>): QueryPathInput {
   return {
      shape,
      chain: ["daily@file:///m.malloy", "orders@file:///m.malloy"],
      catalog: undefined,
      storage: { all: [], fresh: [] },
      rollups: [],
      considered: [],
      sql: undefined,
      chains: new Map(),
      ...overrides,
   };
}

describe("buildQueryPath", () => {
   it("answers live, considering nothing, for a source with no table", () => {
      expect(buildQueryPath(input({}))).toEqual({
         query: shape,
         servedBy: { kind: "live" },
         considered: [],
         notConsidered: [],
      });
   });

   it("skips every applicable table behind a row-level gate as CONTRACT", () => {
      const daily = storageOf("daily");
      const report = buildQueryPath(
         input({
            catalog: catalog([planned("daily")]),
            storage: { all: [daily], fresh: [daily] },
            considered: [
               {
                  kind: "storage",
                  outcome: "skipped",
                  reason: "row_level_gate",
               },
            ],
         }),
      );
      expect(report.servedBy).toEqual({ kind: "live" });
      expect(report.considered).toEqual([
         {
            tableId: "id_daily",
            kind: "materialized",
            outcome: "skipped",
            reason: "CONTRACT",
         },
      ]);
   });

   it("lists storage before colocated, and a colocated table only when no stored one answered", () => {
      const daily = storageOf("daily");
      const tables = [planned("daily"), planned("orders")];
      const answered = buildQueryPath(
         input({
            catalog: catalog(tables, { id_orders: "orders_built" }),
            storage: { all: [daily], fresh: [daily] },
            considered: [{ kind: "storage", outcome: "used" }],
            storageTablesRead: [daily],
            sql: "SELECT * FROM lake.t_daily",
         }),
      );
      expect(answered.considered).toEqual([
         { tableId: "id_daily", kind: "materialized", outcome: "used" },
      ]);

      const missed = buildQueryPath(
         input({
            catalog: catalog(tables, { id_orders: "orders_built" }),
            storage: { all: [daily], fresh: [daily] },
            considered: [
               {
                  kind: "storage",
                  outcome: "skipped",
                  reason: "does_not_answer",
                  detail: "no field x",
                  errorName: "Error",
               },
            ],
            sql: "SELECT region FROM orders_built",
         }),
      );
      expect(missed.servedBy).toEqual({
         kind: "materialized",
         tableId: "id_orders",
      });
      expect(missed.considered).toEqual([
         {
            tableId: "id_daily",
            kind: "materialized",
            outcome: "skipped",
            reason: "MISSING_FIELD",
            detail: "no field x",
         },
         { tableId: "id_orders", kind: "materialized", outcome: "used" },
      ]);
   });

   it("names a storage answer's table even when the chain does not place it", () => {
      const other = storageOf("other");
      const report = buildQueryPath(
         input({
            catalog: catalog([planned("other")]),
            storage: { all: [other], fresh: [other] },
            considered: [{ kind: "storage", outcome: "used" }],
            storageTablesRead: [other],
         }),
      );
      expect(report.servedBy).toEqual({
         kind: "materialized",
         tableId: "id_other",
      });
   });

   it("reads a colocated table only as a whole identifier in the SQL", () => {
      const report = buildQueryPath(
         input({
            catalog: catalog([planned("daily")], { id_daily: "daily_t" }),
            sql: "SELECT * FROM daily_t_old",
         }),
      );
      // Bound, fresh and not read: nearer tables would have answered, so it is
      // not reported as tried.
      expect(report.servedBy).toEqual({ kind: "live" });
      expect(report.considered).toEqual([]);
   });

   it("maps a rollup coverage refusal to COUNT_SEMANTICS, or MISSING_FIELD past the rollup's fields", () => {
      const rollup = planned("orders_rollup", "preaggregate");
      const base = {
         catalog: catalog([rollup], { id_orders_rollup: "rollup_t" }),
         rollups: [
            {
               rollupSourceName: "orders_rollup",
               baseSourceName: "orders",
               baseSourceID: "orders@file:///m.malloy",
            },
         ],
      };
      const reasonFor = (detail: string) =>
         buildQueryPath(
            input({
               ...base,
               considered: [
                  {
                     kind: "rollup",
                     outcome: "skipped",
                     reason: "does_not_answer",
                     detail,
                     errorName: "RollupCoverageError",
                  },
               ],
            }),
         ).considered[0]?.reason;
      expect(reasonFor("orders_rollup: aggregate count() is computed")).toBe(
         "COUNT_SEMANTICS",
      );
      expect(
         reasonFor("orders_rollup: reference a.b reaches past the rollup"),
      ).toBe("MISSING_FIELD");
   });

   it("lists at most ten nearby tables, nearest first", () => {
      const tables = Array.from({ length: 12 }, (_, i) =>
         planned(`below_${String(i).padStart(2, "0")}`),
      );
      const chains = new Map(
         tables.map((t, i) => [
            t.sourceID,
            [
               t.sourceID,
               ...(i % 2 ? ["mid@file:///m.malloy"] : []),
               "daily@file:///m.malloy",
            ],
         ]),
      );
      const report = buildQueryPath(
         input({ catalog: catalog(tables), chains }),
      );
      expect(report.notConsidered).toHaveLength(MAX_NOT_CONSIDERED);
      // Direct extensions (distance 1) before the ones a level further down.
      expect(report.notConsidered.slice(0, 6).map((n) => n.sourceName)).toEqual(
         [
            "below_00",
            "below_02",
            "below_04",
            "below_06",
            "below_08",
            "below_10",
         ],
      );
      expect(new Set(report.notConsidered.map((n) => n.relationship))).toEqual(
         new Set(["below"]),
      );
   });
});

describe("sourceChainOf", () => {
   it("follows an extend, a rename and a query source's base, nearest first", () => {
      const orders = { type: "table", sourceID: "orders@m" };
      const daily = {
         type: "query_source",
         sourceID: "daily@m",
         query: { structRef: "orders" },
      };
      const ext = {
         type: "query_source",
         sourceID: "ext@m",
         extends: "daily@m",
      };
      const renamed = {
         type: "query_source",
         sourceID: "renamed@m",
         referenceID: "ext@m",
      };
      const modelDef = {
         contents: { orders, daily, ext, renamed },
         sourceRegistry: {
            "orders@m": {
               entry: { type: "source_registry_reference", name: "orders" },
            },
            "daily@m": {
               entry: { type: "source_registry_reference", name: "daily" },
            },
            "ext@m": {
               entry: { type: "source_registry_reference", name: "ext" },
            },
         },
      };
      expect(sourceChainOf(renamed, modelDef)).toEqual([
         "renamed@m",
         "ext@m",
         "daily@m",
         "orders@m",
      ]);
   });

   it("stops on a cycle, and names a hop it cannot resolve", () => {
      const a: Record<string, unknown> = { sourceID: "a@m", extends: "b@m" };
      const b: Record<string, unknown> = { sourceID: "b@m", extends: "a@m" };
      const modelDef = {
         contents: { a, b },
         sourceRegistry: {
            "a@m": { entry: { type: "source_registry_reference", name: "a" } },
            "b@m": { entry: { type: "source_registry_reference", name: "b" } },
         },
      };
      expect(sourceChainOf(a, modelDef)).toEqual(["a@m", "b@m"]);
      expect(
         sourceChainOf(
            { sourceID: "x@m", extends: "gone@m" },
            { contents: {} },
         ),
      ).toEqual(["x@m", "gone@m"]);
   });
});

describe("queryShapeOf, on compiled queries", () => {
   const duckdb = new DuckDBConnection("duckdb");
   afterAll(async () => {
      await duckdb.close();
   });
   const model = `
source: regions is duckdb.sql("SELECT 'east' AS code, 'East' AS label")
source: orders is duckdb.sql("""
  SELECT 1 AS id, 'east' AS region, 2.5 AS amount, DATE '2026-01-01' AS d
""") extend {
  join_one: r is regions on r.code = region
  dimension: big is amount > 2
  measure: total is amount.sum()
}
`;
   async function shapeOf(query: string) {
      const runtime = new Runtime({
         urlReader: new InMemoryURLReader(
            new Map([["file:///m.malloy", model]]),
         ),
         connections: new FixedConnectionMap(
            new Map([["duckdb", duckdb]]),
            "duckdb",
         ),
      });
      const prepared = await readPreparedQuery(
         runtime.loadModel(new URL("file:///m.malloy")).loadQuery(query),
      );
      return queryShapeOf(
         prepared.query,
         resolveStructRefSource(prepared.structRef, prepared.modelDef),
         { sourceName: "orders" },
      );
   }

   it("splits groups from measures, through joins and nests", async () => {
      expect(
         await shapeOf(
            "run: orders -> { group_by: r.label; aggregate: total; nest: by_day is { group_by: d } }",
         ),
      ).toEqual({
         sourceName: "orders",
         groupedBy: ["r.label", "d"],
         filteredBy: [],
         measures: ["total"],
      });
   });

   it("lists what an inline field reads, and the fields a filter reads", async () => {
      expect(
         await shapeOf(
            "run: orders -> { timezone: 'Asia/Tokyo'; where: big and r.code = 'east'; group_by: up is upper(region); aggregate: s is amount.sum() }",
         ),
      ).toEqual({
         sourceName: "orders",
         groupedBy: ["region"],
         filteredBy: ["big", "r.code"],
         measures: ["amount"],
         timeZone: "Asia/Tokyo",
      });
   });
});
