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
         warnings: [],
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
               },
            ],
            colocatedRead: new Set(["id_orders"]),
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
            // The compiler's own message stays in the logs.
            reason: "MISSING_FIELD",
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

   it("names the colocated table the run read from the manifest, whatever the SQL says", () => {
      const tables = catalog([planned("daily"), planned("orders")], {
         id_daily: "daily_t",
         id_orders: "orders_t",
      });
      // Bound, fresh and not read: not reported as tried.
      const unread = buildQueryPath(
         input({ catalog: tables, colocatedRead: new Set() }),
      );
      expect(unread.servedBy).toEqual({ kind: "live" });
      expect(unread.considered).toEqual([]);
      // The run substituted orders' table (daily's own is not in the manifest
      // it read), so orders answered.
      const read = buildQueryPath(
         input({ catalog: tables, colocatedRead: new Set(["id_orders"]) }),
      );
      expect(read.servedBy).toEqual({
         kind: "materialized",
         tableId: "id_orders",
      });
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
      const skipFor = (code: string | undefined) =>
         buildQueryPath(
            input({
               ...base,
               considered: [
                  {
                     kind: "rollup",
                     outcome: "skipped",
                     reason: "does_not_answer",
                     // Internal text, which never decides the reason.
                     detail: "orders_rollup: reaches past, or not",
                     ...(code
                        ? { code, callerDetail: "aggregate count()" }
                        : {}),
                  },
               ],
            }),
         ).considered[0];
      expect(skipFor("rollup_count_semantics")).toMatchObject({
         reason: "COUNT_SEMANTICS",
         detail: "aggregate count()",
      });
      expect(skipFor("rollup_missing_field")?.reason).toBe("MISSING_FIELD");
      // Unexplained (a compile error): MISSING_FIELD, and no detail at all.
      expect(skipFor(undefined)).toEqual({
         tableId: "id_orders_rollup",
         kind: "rollup",
         outcome: "skipped",
         reason: "MISSING_FIELD",
      });
   });

   it("reports a run-time storage failure without its error text", () => {
      const daily = storageOf("daily");
      const report = buildQueryPath(
         input({
            catalog: catalog([planned("daily")]),
            storage: { all: [daily], fresh: [daily] },
            considered: [{ kind: "storage", outcome: "used" }],
            storageTablesRead: [daily],
            runFailed: true,
         }),
      );
      expect(report.considered).toEqual([
         {
            tableId: "id_daily",
            kind: "materialized",
            outcome: "skipped",
            reason: "RUN_FAILED",
         },
      ]);
   });

   it("never names a storage table by its physical path", () => {
      // The plan failed to compute: the binding has no content address.
      const daily = storageOf("daily");
      const report = buildQueryPath(
         input({
            catalog: catalog([]),
            storage: { all: [daily], fresh: [daily] },
            considered: [{ kind: "storage", outcome: "used" }],
            storageTablesRead: [daily],
         }),
      );
      expect(report.servedBy).toEqual({ kind: "materialized" });
      expect(report.considered).toEqual([]);
      expect(JSON.stringify(report)).not.toContain("lake.t_daily");
   });

   it("with the storage tier not routed, reports its table WARNED and still names the colocated table", () => {
      const daily = storageOf("daily");
      const tables = catalog([planned("daily")], { id_daily: "daily_t" });
      const warned = buildQueryPath(
         input({
            catalog: tables,
            storage: { all: [daily], fresh: [daily] },
            storageWarned: true,
            colocatedRead: new Set(["id_daily"]),
         }),
      );
      // The same content address in two places: the storage copy is not
      // routed, and the warehouse's own copy answered.
      expect(warned.considered).toEqual([
         {
            tableId: "id_daily",
            kind: "materialized",
            outcome: "skipped",
            reason: "WARNED",
         },
         { tableId: "id_daily", kind: "materialized", outcome: "used" },
      ]);
      expect(warned.servedBy).toEqual({
         kind: "materialized",
         tableId: "id_daily",
      });
      // No warning (a destination missing while the tier is on): the storage
      // table is left out, and an unbuilt colocated twin is not invented.
      const absent = buildQueryPath(
         input({
            catalog: catalog([planned("daily")]),
            storage: { all: [daily], fresh: [daily] },
         }),
      );
      expect(absent.considered).toEqual([]);
   });

   it("matches a rollup to its own model's plan, and names none when ambiguous", () => {
      const rollupIn = (modelPath: string, id: string): PlannedTable => ({
         sourceID: `orders_rollup@file:///${modelPath}#preaggregate`,
         name: "orders_rollup",
         sourceEntityId: id,
         origin: "preaggregate",
         modelPath,
      });
      const rollups = [
         {
            rollupSourceName: "orders_rollup",
            baseSourceName: "orders",
            baseSourceID: "orders@file:///m.malloy",
         },
      ];
      const tables = catalog(
         [rollupIn("a.malloy", "id_a"), rollupIn("b.malloy", "id_b")],
         { id_a: "rollup_a", id_b: "rollup_b" },
      );
      const ownModel = buildQueryPath(
         input({ modelPath: "b.malloy", catalog: tables, rollups }),
      );
      // Not routed to (no rollup decision): only its NOT_BUILT state would
      // show; it is bound, so nothing is listed, but nearby lookups use b's.
      expect(
         buildQueryPath(
            input({
               modelPath: "b.malloy",
               catalog: tables,
               rollups,
               considered: [{ kind: "rollup", outcome: "used" }],
               rollupMember: "orders_rollup",
            }),
         ).servedBy,
      ).toEqual({ kind: "rollup", tableId: "id_b" });
      expect(ownModel.considered).toEqual([]);
      // A third model with the same name: ambiguous, so no table is named.
      expect(
         buildQueryPath(
            input({
               modelPath: "c.malloy",
               catalog: tables,
               rollups,
               considered: [{ kind: "rollup", outcome: "used" }],
               rollupMember: "orders_rollup",
            }),
         ).servedBy,
      ).toEqual({ kind: "live" });
   });

   it("never names a source the caller cannot see among the nearby tables", () => {
      const hidden = planned("orders_pii");
      const shown = planned("orders_public");
      const chains = new Map([
         [hidden.sourceID, [hidden.sourceID, "orders@file:///m.malloy"]],
         [shown.sourceID, [shown.sourceID, "orders@file:///m.malloy"]],
      ]);
      const report = buildQueryPath(
         input({
            catalog: catalog([hidden, shown]),
            chains,
            isVisibleSource: (name) => name !== "orders_pii",
         }),
      );
      expect(report.notConsidered.map((n) => n.sourceName)).toEqual([
         "orders_public",
      ]);
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

describe("buildQueryPath, reading the serve verdicts", () => {
   const verdicts = (
      entries: Record<string, { reason: string; sourceName: string }>,
   ) => new Map(Object.entries(entries));

   it("reports a table the bind refuses as WARNED with its reason, not NOT_BUILT", () => {
      const tables = {
         ...catalog([planned("daily")]),
         liveVerdicts: verdicts({
            "colocated:id_daily": { reason: "refused", sourceName: "daily" },
         }),
      };
      const report = buildQueryPath(input({ catalog: tables }));
      expect(report.considered).toEqual([
         {
            tableId: "id_daily",
            kind: "materialized",
            outcome: "skipped",
            reason: "WARNED",
            code: "refused",
         },
      ]);
      expect(report.warnings).toEqual([
         {
            code: "refused",
            tableId: "id_daily",
            sourceName: "daily",
            message: expect.stringContaining("refuses to serve this table"),
         },
      ]);
   });

   it("reports a rollup every entry point gates as WARNED row_level_gate, in place of CONTRACT", () => {
      const daily = storageOf("daily");
      const report = buildQueryPath(
         input({
            catalog: {
               ...catalog([planned("daily")]),
               liveVerdicts: verdicts({
                  "storage:id_daily": {
                     reason: "row_level_gate",
                     sourceName: "daily",
                  },
               }),
            },
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
      expect(report.considered).toEqual([
         {
            tableId: "id_daily",
            kind: "materialized",
            outcome: "skipped",
            reason: "WARNED",
            code: "row_level_gate",
         },
      ]);
      expect(report.warnings?.map((w) => w.code)).toEqual(["row_level_gate"]);
   });

   it("reports a gated source the build refused as WARNED refused, in place of CONTRACT, on either tier", () => {
      // storage=: the router skipped the stored table for the entry point's
      // gate; the verdict knows the build refused the gated source outright.
      const daily = storageOf("daily");
      const storageReport = buildQueryPath(
         input({
            catalog: {
               ...catalog([planned("daily")]),
               liveVerdicts: verdicts({
                  "storage:id_daily": {
                     reason: "refused",
                     sourceName: "daily",
                  },
               }),
            },
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
      expect(storageReport.considered).toEqual([
         {
            tableId: "id_daily",
            kind: "materialized",
            outcome: "skipped",
            reason: "WARNED",
            code: "refused",
         },
      ]);
      expect(storageReport.warnings?.map((w) => w.code)).toEqual(["refused"]);

      // Colocated: a rollup skipped for the gate, whose verdict is a refusal.
      const rollup = planned("orders_rollup", "preaggregate");
      const rollupInput = (isVisibleSource?: (name: string) => boolean) =>
         input({
            catalog: {
               ...catalog([rollup], { id_orders_rollup: "rollup_t" }),
               liveVerdicts: verdicts({
                  "colocated:id_orders_rollup": {
                     reason: "refused",
                     sourceName: "orders_rollup",
                  },
               }),
            },
            rollups: [
               {
                  rollupSourceName: "orders_rollup",
                  baseSourceName: "orders",
                  baseSourceID: "orders@file:///m.malloy",
               },
            ],
            considered: [
               { kind: "rollup", outcome: "skipped", reason: "row_level_gate" },
            ],
            ...(isVisibleSource ? { isVisibleSource } : {}),
         });
      const colocated = buildQueryPath(rollupInput());
      expect(colocated.considered[0]).toMatchObject({
         reason: "WARNED",
         code: "refused",
      });
      expect(colocated.warnings?.map((w) => w.code)).toEqual(["refused"]);
      // A source the caller cannot list keeps its code but is never named.
      const hidden = buildQueryPath(
         rollupInput((name) => name !== "orders_rollup"),
      );
      expect(hidden.considered[0]).toMatchObject({ code: "refused" });
      expect(hidden.warnings).toEqual([]);
   });

   it("keeps what only this query shows: a table that answered, STALE and RUN_FAILED", () => {
      const daily = storageOf("daily");
      const liveVerdicts = verdicts({
         "colocated:id_daily": { reason: "unreachable", sourceName: "daily" },
         "storage:id_daily": { reason: "unreachable", sourceName: "daily" },
      });
      const usedReport = buildQueryPath(
         input({
            catalog: {
               ...catalog([planned("daily")], { id_daily: "daily_t" }),
               liveVerdicts,
            },
            colocatedRead: new Set(["id_daily"]),
         }),
      );
      expect(usedReport.considered).toEqual([
         { tableId: "id_daily", kind: "materialized", outcome: "used" },
      ]);
      expect(usedReport.warnings).toEqual([]);
      const stale = buildQueryPath(
         input({
            catalog: {
               ...catalog([planned("daily")], { id_daily: "daily_t" }),
               liveVerdicts,
            },
            freshColocated: new Set(),
         }),
      );
      expect(stale.considered.map((c) => c.reason)).toEqual(["STALE"]);
      const failed = buildQueryPath(
         input({
            catalog: { ...catalog([planned("daily")]), liveVerdicts },
            storage: { all: [daily], fresh: [daily] },
            considered: [{ kind: "storage", outcome: "used" }],
            storageTablesRead: [daily],
            runFailed: true,
         }),
      );
      expect(failed.considered[0]).toMatchObject({ reason: "RUN_FAILED" });
      expect(failed.considered[0]).not.toHaveProperty("code");
   });

   it("codes a hidden source's skip but never names it in a warning", () => {
      const report = buildQueryPath(
         input({
            catalog: {
               ...catalog([planned("daily")]),
               liveVerdicts: verdicts({
                  "colocated:id_daily": {
                     reason: "unreachable",
                     sourceName: "daily",
                  },
               }),
            },
            isVisibleSource: (name) => name !== "daily",
         }),
      );
      expect(report.considered[0]).toMatchObject({
         reason: "WARNED",
         code: "unreachable",
      });
      expect(report.warnings).toEqual([]);
      expect(JSON.stringify(report.warnings)).not.toContain("daily");
   });

   it("gives no warning for a table whose colocated copy answered beside an unrouted storage copy", () => {
      const daily = storageOf("daily");
      const report = buildQueryPath(
         input({
            catalog: {
               ...catalog([planned("daily")], { id_daily: "daily_t" }),
               liveVerdicts: verdicts({
                  "storage:id_daily": {
                     reason: "storage_mode",
                     sourceName: "daily",
                  },
               }),
            },
            storage: { all: [daily], fresh: [daily] },
            storageWarned: true,
            colocatedRead: new Set(["id_daily"]),
         }),
      );
      expect(report.considered).toEqual([
         {
            tableId: "id_daily",
            kind: "materialized",
            outcome: "skipped",
            reason: "WARNED",
            code: "storage_mode",
         },
         { tableId: "id_daily", kind: "materialized", outcome: "used" },
      ]);
      expect(report.warnings).toEqual([]);
   });
   it("keeps a skip the router reached by trying the table, with its detail", () => {
      const daily = storageOf("daily");
      const report = buildQueryPath(
         input({
            catalog: {
               ...catalog([planned("daily")]),
               liveVerdicts: verdicts({
                  "storage:id_daily": {
                     reason: "unreachable",
                     sourceName: "daily",
                  },
               }),
            },
            storage: { all: [daily], fresh: [daily] },
            considered: [
               {
                  kind: "storage",
                  outcome: "skipped",
                  reason: "does_not_answer",
                  code: "rollup_missing_field",
                  callerDetail: "region",
               },
            ],
         }),
      );
      expect(report.considered[0]).toMatchObject({
         reason: "MISSING_FIELD",
         detail: "region",
      });
      expect(report.considered[0]).not.toHaveProperty("code");
      expect(report.warnings).toEqual([]);
   });

   it("keeps CONTRACT when the verdict names a different reason than the gate", () => {
      const daily = storageOf("daily");
      const report = buildQueryPath(
         input({
            catalog: {
               ...catalog([planned("daily")]),
               liveVerdicts: verdicts({
                  "storage:id_daily": {
                     reason: "unreachable",
                     sourceName: "daily",
                  },
               }),
            },
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
      expect(report.considered[0]).toMatchObject({ reason: "CONTRACT" });
      expect(report.considered[0]).not.toHaveProperty("code");
   });

   it("reads the verdict of the tier that was skipped, never its twin's", () => {
      const daily = storageOf("daily");
      const report = buildQueryPath(
         input({
            catalog: {
               ...catalog([planned("daily")]),
               // Only the colocated copy is refused; the storage copy is
               // merely unrouted, which this verdict does not explain.
               liveVerdicts: verdicts({
                  "colocated:id_daily": {
                     reason: "refused",
                     sourceName: "daily",
                  },
               }),
            },
            storage: { all: [daily], fresh: [daily] },
            storageWarned: true,
         }),
      );
      expect(report.considered).toEqual([
         {
            tableId: "id_daily",
            kind: "materialized",
            outcome: "skipped",
            reason: "WARNED",
         },
      ]);
      expect(report.warnings).toEqual([]);
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
  view: by_region is { where: big; group_by: region; aggregate: total }
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

   it("reads a nested named view's fields", async () => {
      expect(
         await shapeOf("run: orders -> { group_by: d; nest: by_region }"),
      ).toEqual({
         sourceName: "orders",
         groupedBy: ["d", "region"],
         filteredBy: ["big"],
         measures: ["total"],
      });
   });
});
