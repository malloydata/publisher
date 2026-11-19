// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The path report (materialization v2, D1) for the `storage=` tier, over the
// R4 harness: real builds into a DuckDB destination, real serve shapes. The
// harness model has no package, so a table is named by its stored path rather
// than its plan's content address (query_path.component.spec.ts covers that).
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import { buildScenario, disposeScenarios, type R4Fixture } from "./harness";
import { startMetricsHarness } from "../../../src/test_helpers/metrics_harness";
import { resetMaterializationTelemetryForTesting } from "../../../src/materialization_metrics";

let restoreSettings: () => void;
beforeAll(() => {
   restoreSettings = installSettings({
      env: { PERSIST_STORAGE_MODE: "on" },
   }).restore;
});
afterAll(async () => {
   restoreSettings();
   await disposeScenarios();
});

describe("the path report over storage=", () => {
   let fixture: R4Fixture;
   let daily: string;
   beforeAll(async () => {
      fixture = await buildScenario({
         files: {
            "m.malloy": `##! experimental.persistence
source: orders is duckdb.table('orders') extend { primary_key: id }

#@ persist name="daily" storage="lake"
source: daily is orders -> {
  group_by: order_date, region
  aggregate: total is amount.sum()
}
`,
         },
         seedSql: [
            `CREATE TABLE orders AS SELECT * FROM (VALUES
               (1, DATE '2026-01-01', 'east', 100.5),
               (2, DATE '2026-01-02', 'west', 3.5)
             ) AS t(id, order_date, region, amount)`,
         ],
      });
      // Its content address, never the physical table path.
      daily = "h0";
   });

   it("names the stored table that answered, by its content address", async () => {
      fixture.setBindings(fixture.bindings);
      const { path, servedFrom } = await fixture.runAsIs(
         "run: daily -> { group_by: region; aggregate: t is total.sum() }",
      );
      expect(servedFrom).toBe("storage");
      // The served copy answers a v1 `storage=` table (the v1 cutover).
      expect(path?.servedBy).toEqual({
         kind: "materialized",
         origin: "persist",
         tier: "storage",
         tableId: daily,
         via: "served_copy",
      });
      expect(path?.considered).toEqual([
         {
            tableId: daily,
            kind: "materialized",
            origin: "persist",
            outcome: "used",
         },
      ]);
      expect(JSON.stringify(path)).not.toContain(fixture.bindings[0].tablePath);
   });

   it("skips the stored table for a query it cannot compile, with the compiler's reason", async () => {
      fixture.setBindings(fixture.bindings);
      const { path, servedFrom } = await fixture.runAsIs(
         "run: daily extend { join_one: o is orders on o.id = 1 } -> { group_by: o.region; aggregate: t is total.sum() }",
      );
      expect(servedFrom).toBeNull();
      expect(path?.servedBy).toEqual({ kind: "live" });
      expect(path?.considered).toEqual([
         {
            tableId: daily,
            kind: "materialized",
            origin: "persist",
            outcome: "skipped",
            // The compiler's message stays in the logs: it names the
            // synthesized serve-shape sources.
            reason: "MISSING_FIELD",
         },
      ]);
   });

   it("reports a stored table past its freshness window as STALE", async () => {
      fixture.setBindings(
         fixture.bindings.map((b) => ({
            ...b,
            freshAsOf: "2020-01-01T00:00:00.000Z",
            freshnessWindowSeconds: 60,
            freshnessFallback: "live" as const,
         })),
      );
      const { path } = await fixture.runAsIs(
         "run: daily -> { group_by: region; aggregate: t is total.sum() }",
      );
      expect(path?.servedBy).toEqual({ kind: "live" });
      expect(path?.considered).toEqual([
         {
            tableId: daily,
            kind: "materialized",
            origin: "persist",
            outcome: "skipped",
            reason: "STALE",
         },
      ]);
   });

   it("counts the served copy's decline of a stale table as STALE, as the path report says", async () => {
      const harness = await startMetricsHarness();
      resetMaterializationTelemetryForTesting();
      try {
         fixture.setBindings(
            fixture.bindings.map((b) => ({
               ...b,
               freshAsOf: "2020-01-01T00:00:00.000Z",
               freshnessWindowSeconds: 60,
               freshnessFallback: "live" as const,
            })),
         );
         await fixture.runAsIs(
            "run: daily -> { group_by: region; aggregate: t is total.sum() }",
         );
         expect(
            await harness.collectCounter("publisher_query_route_total", {
               leg: "materialized",
               outcome: "skipped",
               reason: "STALE",
            }),
         ).toBe(1);
         expect(
            await harness.collectCounter("publisher_query_route_total", {
               leg: "materialized",
               outcome: "skipped",
               reason: "NOT_SERVED",
            }),
         ).toBe(0);
      } finally {
         resetMaterializationTelemetryForTesting();
         await harness.shutdown();
      }
   });

   it("reports a chosen table whose read failed as RUN_FAILED, answered live", async () => {
      const missing = "lake.no_such_table";
      fixture.setBindings(
         fixture.bindings.map((b) => ({
            ...b,
            tablePath: missing,
            freshnessFallback: "live" as const,
         })),
      );
      const { path, servedFrom } = await fixture.runAsIs(
         "run: daily -> { group_by: region; aggregate: t is total.sum() }",
      );
      expect(servedFrom).toBe("live_fallback");
      expect(path?.servedBy).toEqual({ kind: "live" });
      expect(path?.considered).toEqual([
         {
            tableId: daily,
            kind: "materialized",
            origin: "persist",
            outcome: "skipped",
            reason: "READ_FAILED",
         },
      ]);
      // The store's error (here a table path) is never on the wire.
      expect(JSON.stringify(path)).not.toContain("no_such_table");
   });

   // A worker can lose a destination's catalog (a DuckLake catalog missing
   // from its locked instance). The stored table is an optimisation and the
   // fault is the worker's, so the query answers live whatever the binding's
   // fallback says, rather than a 400 the caller cannot fix.
   it("answers live, as READ_FAILED, when the destination's catalog is missing, whatever the fallback", async () => {
      for (const freshnessFallback of [undefined, "fail" as const]) {
         fixture.setBindings(
            fixture.bindings.map((b) => {
               const { freshnessFallback: _, ...rest } = b;
               return {
                  ...rest,
                  tablePath: "lost_catalog.main.daily",
                  ...(freshnessFallback ? { freshnessFallback } : {}),
               };
            }),
         );
         const { rows, path, servedFrom } = await fixture.runAsIs(
            "run: daily -> { group_by: region; aggregate: t is total.sum() }",
         );
         expect(servedFrom).toBe("live_fallback");
         expect(rows.length).toBe(2);
         expect(path?.servedBy).toEqual({ kind: "live" });
         expect(path?.considered).toEqual([
            {
               tableId: daily,
               kind: "materialized",
               origin: "persist",
               outcome: "skipped",
               reason: "READ_FAILED",
            },
         ]);
         expect(JSON.stringify(path)).not.toContain("lost_catalog");
      }
   });

   it("still fails a missing table, not catalog, whose fallback does not allow live", async () => {
      fixture.setBindings(
         fixture.bindings.map((b) => {
            const { freshnessFallback: _, ...rest } = b;
            return { ...rest, tablePath: "lake.no_such_table" };
         }),
      );
      await expect(
         fixture.runAsIs(
            "run: daily -> { group_by: region; aggregate: t is total.sum() }",
         ),
      ).rejects.toThrow();
   });

   it("reports nothing considered for a source with no table", async () => {
      fixture.setBindings(fixture.bindings);
      const { path } = await fixture.runAsIs(
         "run: orders -> { group_by: region; aggregate: n is count() }",
      );
      expect(path?.servedBy).toEqual({ kind: "live" });
      expect(path?.considered).toEqual([]);
      expect(path?.query).toEqual({
         sourceName: "orders",
         groupedBy: ["region"],
         filteredBy: [],
         measures: [],
      });
   });
});
