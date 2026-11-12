// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// A stored source's view that the serve shape cannot reproduce goes live
// alone. Before, one such view dropped the view category from the whole shape,
// so every view of every stored source in the model answered live.
import { afterAll, beforeAll, describe, expect, it, spyOn } from "bun:test";
import { logger } from "../../../src/logger";
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
      const meta = dropped[0][1] as {
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
});
