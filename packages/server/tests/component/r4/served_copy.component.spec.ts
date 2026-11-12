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

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
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

describe("R4: with the served copy off, today's serve shape answers", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on" },
      }).restore;
      fixture = await buildScenario({ files: { "m.malloy": model }, seedSql });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });
   it("serves from storage without the served copy", async () => {
      const run = await fixture.run({
         name: "off",
         malloy: "run: daily -> { group_by: region; aggregate: grand }",
      });
      expect(run.servedFrom).toBe("storage");
      expect(run.servedVia).toBeUndefined();
      expect(rowDifference(run.live, run.served)).toBeUndefined();
   });
});
