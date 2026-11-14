// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for the served copy (materialization v2, phase 3 PR 3b): with
 * `materialize.enabled` on, a query on a stored source, its extensions,
 * importing files and query-shaped sources built on it is answered from the
 * served copy (the author's own files with the stored source re-rooted on its
 * table), and the answer equals live. With the setting off, nothing changes:
 * today's serve shape answers.
 */

import { afterAll, beforeAll, describe, expect, it, spyOn } from "bun:test";
import type { ModelMaterializer } from "@malloydata/malloy";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
   type R4Query,
} from "./harness";

const seedSql = [
   `CREATE TABLE ev AS SELECT * FROM (VALUES
      (1, 'east', 10.5, DATE '2026-01-01'),
      (2, 'east', 20.0, DATE '2026-01-02'),
      (3, 'west', 5.5, DATE '2026-01-02'),
      (4, 'west', 1.0, DATE '2026-01-03')
    ) AS t(id, region, amt, d)`,
];

const model = `##! experimental { persistence }
source: ev is duckdb.table('ev')

#@ persist name="daily" storage="lake"
source: daily is ev -> {
  group_by: region, d
  aggregate: total is amt.sum(), n is count()
} extend {
  dimension: big is total > 10
  measure: grand is total.sum()
  view: by_region is { group_by: region; aggregate: grand }
}

source: daily_ext is daily extend {
  measure: avg_total is total.avg()
}

#@ -persist
source: daily_optout is daily extend {
  dimension: doubled is total * 2
}

source: weekly is daily -> {
  group_by: region
  aggregate: s is total.sum()
}
`;

async function expectServedCopy(fixture: R4Fixture, query: R4Query) {
   const run = await fixture.run(query);
   expect(run.servedFrom).toBe("storage");
   expect(run.servedVia).toBe("served_copy");
   expect(rowDifference(run.live, run.served)).toBeUndefined();
   return run;
}

const queries: R4Query[] = [
   {
      name: "the stored source with its extend block",
      malloy: "run: daily -> { group_by: region, big; aggregate: grand }",
   },
   {
      name: "a view the extend block declares",
      malloy: "run: daily -> by_region",
   },
   {
      name: "an extension in the same file",
      malloy: "run: daily_ext -> { group_by: region; aggregate: avg_total }",
   },
   {
      name: "a query-shaped source built on it",
      malloy: "run: weekly -> { group_by: region; aggregate: t is s.sum() }",
   },
];

describe("R4: the served copy answers equal to live", () => {
   let restore: () => void;
   beforeAll(() => {
      restore = installSettings({
         env: {
            PERSIST_STORAGE_MODE: "on",
            PUBLISHER_MATERIALIZE: "true",
         },
      }).restore;
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   describe("in the declaring file", () => {
      let fixture: R4Fixture;
      beforeAll(async () => {
         fixture = await buildScenario({
            files: { "m.malloy": model },
            seedSql,
         });
      });
      for (const query of queries) {
         it(query.name, async () => {
            await expectServedCopy(fixture, query);
         });
      }
   });

   describe("a reader that opts out with #@ -persist", () => {
      let fixture: R4Fixture;
      beforeAll(async () => {
         fixture = await buildScenario({
            files: { "m.malloy": model },
            seedSql,
         });
      });
      it("is declined by the served copy and recomputed", async () => {
         const run = await fixture.run({
            name: "opt-out",
            malloy:
               "run: daily_optout -> { group_by: region; aggregate: d is doubled.sum() }",
         });
         expect(run.servedVia).toBeUndefined();
         expect(run.servedFrom).toBeNull();
         expect(rowDifference(run.live, run.served)).toBeUndefined();
      });
   });

   describe("a query that reads no stored table (decision 6)", () => {
      let fixture: R4Fixture;
      beforeAll(async () => {
         fixture = await buildScenario({
            files: { "m.malloy": model },
            seedSql,
         });
      });
      it("is declined once per compiled copy, then without compiling again", async () => {
         const servedCopy = fixture.servedCopy();
         await servedCopy.compiledModelDef();
         const materializer = (
            servedCopy as unknown as {
               compiledLanes: Map<number, { materializer: ModelMaterializer }>;
            }
         ).compiledLanes.get(0)!.materializer;
         const compiles = spyOn(materializer, "loadRestrictedQuery");
         const miss = "run: ev -> { group_by: region }";
         for (let i = 0; i < 3; i++) {
            await expect(servedCopy.loadQuery(miss)).rejects.toThrow(
               "the query reads no re-rooted table",
            );
         }
         expect(compiles).toHaveBeenCalledTimes(1);
         // The opt-out decline is remembered the same way.
         const optOut = "run: daily_optout -> { group_by: region }";
         await expect(servedCopy.loadQuery(optOut)).rejects.toThrow("opts out");
         await expect(servedCopy.loadQuery(optOut)).rejects.toThrow("opts out");
         expect(compiles).toHaveBeenCalledTimes(2);
         // A query that reads the table still answers.
         const hit = await servedCopy.loadQuery(
            "run: daily -> { group_by: region }",
         );
         expect(hit.tablesRead.map((b) => b.sourceName)).toEqual(["daily"]);
         // And through the router, the decline still falls through to live.
         const run = await fixture.run({ name: "miss", malloy: miss });
         expect(run.servedVia).toBeUndefined();
         expect(rowDifference(run.live, run.served)).toBeUndefined();
      });

      it("compiles once with no fresh binding, not on every query", async () => {
         const servedCopy = fixture.servedCopy();
         servedCopy.setBindings([]);
         const compile = spyOn(
            servedCopy as unknown as { compile(): Promise<unknown> },
            "compile",
         );
         for (let i = 0; i < 3; i++) await servedCopy.compiledModelDef();
         expect(compile).toHaveBeenCalledTimes(1);
         compile.mockRestore();
      });

      it("forgets its declines when the copy is compiled again", async () => {
         const servedCopy = fixture.servedCopy();
         await servedCopy.compiledModelDef();
         servedCopy.invalidate();
         await servedCopy.compiledModelDef();
         const materializer = (
            servedCopy as unknown as {
               compiledLanes: Map<number, { materializer: ModelMaterializer }>;
            }
         ).compiledLanes.get(0)!.materializer;
         const compiles = spyOn(materializer, "loadRestrictedQuery");
         await expect(
            servedCopy.loadQuery("run: ev -> { group_by: region }"),
         ).rejects.toThrow();
         expect(compiles).toHaveBeenCalledTimes(1);
      });
   });

   describe("through a file that imports and extends it", () => {
      let fixture: R4Fixture;
      beforeAll(async () => {
         fixture = await buildScenario({
            files: {
               "index.malloy": `##! experimental { persistence }
import { daily } from "m.malloy"

source: daily_plus is daily extend {
  dimension: tiny is total < 2
}
`,
               "m.malloy": model,
            },
            seedSql,
         });
      });
      it("a re-exported stored source", async () => {
         await expectServedCopy(fixture, {
            name: "re-exported",
            malloy: "run: daily -> { group_by: region; aggregate: grand }",
         });
      });
      it("an extension declared in the importing file", async () => {
         await expectServedCopy(fixture, {
            name: "imported extension",
            malloy:
               "run: daily_plus -> { group_by: tiny; aggregate: n2 is n.sum() }",
         });
      });
   });
});
