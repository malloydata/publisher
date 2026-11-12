// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What-if (materialization v2, T3) on the storage tier: the compile-only route
 * what-if takes (`Model.routeWithoutRunning`) must report the path a real run
 * reports, over a table really built; and a table declared ahead of the build
 * from its columns' Malloy types (`storedSchemaOf`, what what-if binds) must
 * route the same as the table the build wrote.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { resetMaterializationTelemetryForTesting } from "../../../src/materialization_metrics";
import { startMetricsHarness } from "../../../src/test_helpers/metrics_harness";
import { installSettings } from "../../../src/test_helpers/settings_env";
import { duckdbTypeToMalloy } from "../../../src/service/materialization/serve/materialization_serve_transform";
import { storedSchemaOf } from "../../../src/service/materialization/what_if";
import { buildScenario, disposeScenarios, type R4Fixture } from "./harness";

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

const QUERIES = [
   "run: daily -> { group_by: order_date; aggregate: t is total.sum() }",
   "run: daily -> { where: region = 'east'; aggregate: n is order_count.sum() }",
   "run: daily_ext -> { group_by: big_day; aggregate: grand_total }",
   // The base, below the stored source: no stored table can answer it.
   "run: orders -> { group_by: region; aggregate: c is count() }",
];

describe("what-if routes as a run does, on the storage tier", () => {
   let fixture: R4Fixture;
   beforeAll(async () => {
      fixture = await buildScenario({
         files: {
            "m.malloy": `##! experimental.persistence
source: orders is duckdb.table('orders') extend { primary_key: id }

#@ persist name="daily" storage="lake"
source: daily is orders -> {
  group_by: order_date, region
  aggregate: total is amount.sum(), order_count is count()
}

source: daily_ext is daily extend {
  dimension: big_day is total > 100
  measure: grand_total is total.sum()
}
`,
         },
         seedSql: [
            `CREATE TABLE orders AS SELECT * FROM (VALUES
               (1, DATE '2026-01-01', 'east', 100.5),
               (2, DATE '2026-01-01', 'west', 50.25),
               (3, DATE '2026-01-02', 'east', 200.0),
               (5, DATE '2026-01-03', 'west', 300.0)
             ) AS t(id, order_date, region, amount)`,
         ],
      });
   });

   for (const query of QUERIES) {
      it(`reports the run's path, compiled only: ${query}`, async () => {
         fixture.setBindings(fixture.bindings);
         const ran = await fixture.runAsIs(query);
         const routed = await fixture.routeOnly(query);
         expect(routed).toEqual(ran.path!);
      });
   }

   it("serves from storage where the run does", async () => {
      fixture.setBindings(fixture.bindings);
      const routed = await fixture.routeOnly(QUERIES[0]);
      expect(routed.servedBy.kind).toBe("materialized");
      const live = await fixture.routeOnly(QUERIES[3]);
      expect(live.servedBy.kind).toBe("live");
   });

   it("records no routing metric for a hypothetical route", async () => {
      const harness = await startMetricsHarness();
      resetMaterializationTelemetryForTesting();
      try {
         fixture.setBindings(fixture.bindings);
         for (const query of QUERIES) await fixture.routeOnly(query);
         expect(
            await harness.collectCounter(
               "publisher_query_variant_routing_total",
            ),
         ).toBe(0);
         expect(
            await harness.collectCounter(
               "publisher_storage_serve_routing_total",
            ),
         ).toBe(0);
         // A real run of the same query does record them.
         await fixture.runAsIs(QUERIES[0]);
         expect(
            await harness.collectCounter(
               "publisher_query_variant_routing_total",
            ),
         ).toBeGreaterThan(0);
      } finally {
         resetMaterializationTelemetryForTesting();
         await harness.shutdown();
      }
   });

   for (const query of QUERIES) {
      it(`routes a table declared from Malloy types as the built one: ${query}`, async () => {
         fixture.setBindings(fixture.bindings);
         const built = await fixture.routeOnly(query);
         // The table what-if binds before anything is built: the build plan
         // names each column's Malloy type, which a build stores as below.
         fixture.setBindings(
            fixture.bindings.map((binding) => ({
               ...binding,
               schema: storedSchemaOf(
                  binding.schema.map((column) => ({
                     name: column.name,
                     type: duckdbTypeToMalloy(column.type),
                  })),
               ),
            })),
         );
         const declared = await fixture.routeOnly(query);
         expect(declared).toEqual(built);
      });
   }
});
