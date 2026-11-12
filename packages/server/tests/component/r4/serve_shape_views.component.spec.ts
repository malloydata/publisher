// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// A stored source's view that the serve shape cannot reproduce goes live
// alone. Before, one such view dropped the view category from the whole shape,
// so every view of every stored source in the model answered live.
import { afterAll, beforeAll, describe, expect, it, spyOn } from "bun:test";
import { logger } from "../../../src/logger";
import { StorageServeShape } from "../../../src/service/materialization/serve/model_serve_shape";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
   type R4Query,
} from "./harness";

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

async function expectServed(
   fixture: R4Fixture,
   query: R4Query,
   from: "storage" | "live",
) {
   const run = await fixture.run(query);
   if (from === "storage") expect(run.servedFrom).toBe("storage");
   else expect(run.servedFrom).not.toBe("storage");
   expect(rowDifference(run.live, run.served)).toBeUndefined();
}

describe("a view the serve shape cannot reproduce", () => {
   let fixture: R4Fixture;
   const warn = spyOn(logger, "warn");
   const loads = spyOn(StorageServeShape.prototype, "loadServeShapeQuery");
   const droppedLogs = () =>
      warn.mock.calls.filter((call) =>
         String(call[0]).includes("left off views that do not compile"),
      ).length;
   beforeAll(async () => {
      fixture = await buildScenario({
         files: {
            // `regions` is not materialized, so the join to it is not carried
            // onto the shape, and `by_region_name`, which reaches through it,
            // cannot compile there. `daily`'s other views, and `weekly`'s, can.
            "m.malloy": `##! experimental.persistence
source: orders is duckdb.table('orders') extend { primary_key: id }
source: regions is duckdb.table('regions') extend { primary_key: code }

#@ persist name="daily" storage="lake"
source: daily is orders -> {
  group_by: order_date, region
  aggregate: total is amount.sum()
} extend {
  join_one: r is regions on region = r.code
  measure: grand is total.sum()
  view: totals is { aggregate: grand }
  view: by_region_name is { group_by: r.name; aggregate: grand }
  view: by_region is { group_by: region; aggregate: grand }
}

#@ persist name="weekly" storage="lake"
source: weekly is orders -> {
  group_by: region
  aggregate: n is count()
} extend {
  view: counts is { group_by: region; aggregate: c is n.sum() }
}
`,
         },
         seedSql: [
            `CREATE TABLE orders AS SELECT * FROM (VALUES
               (1, DATE '2026-01-01', 'e', 100.5),
               (2, DATE '2026-01-01', 'w', 20.0),
               (3, DATE '2026-01-02', 'e', 7.25)
             ) AS t(id, order_date, region, amount)`,
            `CREATE TABLE regions AS SELECT * FROM (VALUES
               ('e', 'East'), ('w', 'West')
             ) AS t(code, name)`,
         ],
      });
   });

   it("answers that view live", async () => {
      await expectServed(
         fixture,
         { name: "the view", malloy: "run: daily -> by_region_name" },
         "live",
      );
   });

   it("keeps serving the source's other views from storage", async () => {
      for (const view of ["totals", "by_region"]) {
         await expectServed(
            fixture,
            { name: view, malloy: `run: daily -> ${view}` },
            "storage",
         );
      }
   });

   it("keeps serving another stored source's views from storage", async () => {
      await expectServed(
         fixture,
         { name: "weekly", malloy: "run: weekly -> counts" },
         "storage",
      );
   });

   it("logs the dropped view once, by source and name, not per query", async () => {
      const dropped = warn.mock.calls.filter((call) =>
         String(call[0]).includes("left off views that do not compile"),
      );
      expect(dropped.length).toBeGreaterThan(0);
      const meta = (dropped[0] as unknown[])[1] as {
         dropped: { source: string; view: string }[];
      };
      expect(meta.dropped.map((d) => [d.source, d.view])).toEqual([
         ["daily", "by_region_name"],
      ]);
      // Every query above ran on the same bindings: compiled, and logged,
      // once per bind.
      const before = dropped.length;
      await fixture.runAsIs("run: daily -> totals");
      expect(
         warn.mock.calls.filter((call) =>
            String(call[0]).includes("left off views that do not compile"),
         ).length,
      ).toBe(before);
   });

   it("builds the shape once for queries that miss the cache together", async () => {
      const before = droppedLogs();
      // A bind drops the memo; both queries then arrive before it is rebuilt.
      fixture.setBindings(fixture.bindings);
      await Promise.all([
         fixture.runAsIs("run: daily -> totals"),
         fixture.runAsIs("run: weekly -> counts"),
         fixture.runAsIs("run: daily -> by_region"),
      ]);
      expect(droppedLogs()).toBe(before + 1);
   });

   it("forgets the views it left off when the bindings change", async () => {
      await fixture.runAsIs("run: daily -> totals");
      const shape = loads.mock.contexts.at(-1) as StorageServeShape;
      expect([...shape.prunedViews("daily").keys()]).toEqual([
         "by_region_name",
      ]);
      fixture.setBindings(fixture.bindings);
      expect(shape.prunedViews("daily").size).toBe(0);
      await fixture.runAsIs("run: daily -> totals");
      expect([...shape.prunedViews("daily").keys()]).toEqual([
         "by_region_name",
      ]);
   });
});

describe("a shape that fails for a reason other than a view", () => {
   let fixture: R4Fixture;
   const prune = spyOn(
      StorageServeShape.prototype as unknown as {
         pruneUncompilableViews: () => Promise<unknown>;
      },
      "pruneUncompilableViews",
   );
   beforeAll(async () => {
      fixture = await buildScenario({
         files: {
            // The filter reaches through the join to `regions`, which is not
            // materialized, so the shape fails even with no view at all (a
            // filter is never thinned); the derived `daily_plus` is a lift, so
            // both prune sites are reached (a lift is a query-shaped source over a
            // bound one).
            "m.malloy": `##! experimental.persistence
source: orders is duckdb.table('orders') extend { primary_key: id }
source: regions is duckdb.table('regions') extend { primary_key: code }

#@ persist name="daily" storage="lake"
source: daily is orders -> {
  group_by: order_date, region
  aggregate: total is amount.sum()
} extend {
  join_one: r is regions on region = r.code
  where: r.name = 'East'
  view: totals is { aggregate: t is total.sum() }
}

source: daily_plus is daily -> { group_by: order_date; aggregate: t is total.sum() }
`,
         },
         seedSql: [
            `CREATE TABLE orders AS SELECT * FROM (VALUES
               (1, DATE '2026-01-01', 'e', 100.5),
               (2, DATE '2026-01-02', 'w', 20.0)
             ) AS t(id, order_date, region, amount)`,
            `CREATE TABLE regions AS SELECT * FROM (VALUES
               ('e', 'East'), ('w', 'West')
             ) AS t(code, name)`,
         ],
      });
   });

   it("tries to prune views once per build, not again at the ladder", async () => {
      prune.mockClear();
      fixture.setBindings(fixture.bindings);
      await fixture.runAsIs("run: daily -> { aggregate: t is total.sum() }");
      expect(prune).toHaveBeenCalledTimes(1);
   });
});

describe("a bad view beside a lifted entry point", () => {
   let fixture: R4Fixture;
   const warn = spyOn(logger, "warn");
   beforeAll(async () => {
      fixture = await buildScenario({
         files: {
            // `by_region_name` reaches through the join to the unmaterialized
            // `regions`; `daily_by_day` is a lift over `daily` that compiles.
            "m.malloy": `##! experimental.persistence
source: orders is duckdb.table('orders') extend { primary_key: id }
source: regions is duckdb.table('regions') extend { primary_key: code }

#@ persist name="daily" storage="lake"
source: daily is orders -> {
  group_by: order_date, region
  aggregate: total is amount.sum()
} extend {
  join_one: r is regions on region = r.code
  view: by_region_name is { group_by: r.name; aggregate: t is total.sum() }
  view: by_region is { group_by: region; aggregate: t is total.sum() }
}

source: daily_by_day is daily -> { group_by: order_date; aggregate: t is total.sum() }
`,
         },
         seedSql: [
            `CREATE TABLE orders AS SELECT * FROM (VALUES
               (1, DATE '2026-01-01', 'e', 100.5),
               (2, DATE '2026-01-02', 'w', 20.0)
             ) AS t(id, order_date, region, amount)`,
            `CREATE TABLE regions AS SELECT * FROM (VALUES
               ('e', 'East'), ('w', 'West')
             ) AS t(code, name)`,
         ],
      });
   });

   it("keeps the lift and says nothing about lifts when the view was the cause", async () => {
      warn.mockClear();
      fixture.setBindings(fixture.bindings);
      await expectServed(
         fixture,
         { name: "the lift", malloy: "run: daily_by_day -> { select: * }" },
         "storage",
      );
      await expectServed(
         fixture,
         { name: "the good view", malloy: "run: daily -> by_region" },
         "storage",
      );
      expect(
         warn.mock.calls.filter((call) =>
            String(call[0]).includes("with its lifted entry points"),
         ),
      ).toEqual([]);
   });
});
