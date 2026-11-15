// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Colocated persisted views and persisted queries, of both kinds together,
 * that compile to one build but name different tables (`name=`), through the real package path (load,
 * build, bind, query). Each names its own table, so each has its own address
 * (foldDistinctTableNames) and its own table; Malloy files all four under one
 * key (the build's `buildId`), so the view and query legs substitute each
 * leg's own table under that key (readStoredVariant's `compilerKey`). Each
 * is answered by its leg from its own table, equal to live.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { unrecordedAudit } from "../../../src/service/access_record";
import { QueryPrivileges } from "../../../src/service/model_query_request";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   freshLoadPool,
   type LoadedFixture,
   loadFixture,
} from "./conformance/package_harness";
import { rowDifference } from "./harness";

const MODEL = `##! experimental { persistence }
source: orders is duckdb.sql("""
  select * from (values (1, 'east', 10.0), (2, 'west', 20.0), (3, 'east', 5.0))
  as t(id, region, amount)
""") extend {
  measure: total is amount.sum()
  #@ persist name="by_region_a"
  view: by_region_a is { group_by: region; aggregate: total; order_by: region }
  #@ persist name="by_region_b"
  view: by_region_b is { group_by: region; aggregate: total; order_by: region }
}

#@ persist name="q_region_a"
query: q_region_a is orders -> { group_by: region; aggregate: total; order_by: region }

#@ persist name="q_region_b"
query: q_region_b is orders -> { group_by: region; aggregate: total; order_by: region }
`;

/** Each call, the leg that answers it, and its build source's plan name. */
const CALLS: [string, "view" | "query", string][] = [
   ["run: orders -> by_region_a", "view", "orders__by_region_a__view"],
   ["run: orders -> by_region_b", "view", "orders__by_region_b__view"],
   ["run: q_region_a", "query", "q_region_a__query"],
   ["run: q_region_b", "query", "q_region_b__query"],
];

describe("colocated persisted views and queries of one build, each under its own name=", () => {
   let restore: () => void = () => {};
   let loader: Awaited<ReturnType<typeof loadFixture>>;
   let fixture: LoadedFixture;
   const live = new Map<string, Record<string, unknown>[]>();

   async function ask(malloy: string) {
      const result = await fixture.pkg
         .getModel("m.malloy")!
         .runQuery(
            unrecordedAudit(),
            { query: malloy } as never,
            QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
         );
      return {
         rows: result.compactResult as Record<string, unknown>[],
         servedBy: result.path?.servedBy as
            | { kind?: string; tableId?: string }
            | undefined,
      };
   }

   beforeAll(async () => {
      restore = installSettings({
         env: { PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      await freshLoadPool();
      loader = await loadFixture({ "m.malloy": MODEL });
      for (const [malloy] of CALLS) {
         const out = await loader.run("m.malloy", malloy);
         if ("error" in out) throw new Error(`${malloy}: ${out.error}`);
         live.set(malloy, out.answer.rows);
      }
      fixture = await loader.bind();
   }, 120_000);

   afterAll(async () => {
      restore();
      await loader?.dispose();
      await freshLoadPool();
   });

   it("plans a table at its own address for each name=", () => {
      const sources = Object.values(fixture.pkg.getBuildPlan()?.sources ?? {});
      const addresses = CALLS.map(
         ([, , name]) => sources.find((s) => s.name === name)?.sourceEntityId,
      );
      expect(addresses.every((a) => a !== undefined)).toBe(true);
      expect(new Set(addresses).size).toBe(CALLS.length);
      // One build: the views and the queries, across kinds, compile to the
      // same SQL, which alone would be one address.
      const sql = CALLS.map(
         ([, , name]) => sources.find((s) => s.name === name)?.sql,
      );
      expect(sql[0]).toBeDefined();
      expect(new Set(sql).size).toBe(1);
      expect(fixture.buildErrors).toEqual({});
   });

   for (const [malloy, leg, name] of CALLS) {
      it(`answers ${malloy} by the ${leg} leg from its own table, equal to live`, async () => {
         const plan = Object.values(fixture.pkg.getBuildPlan()?.sources ?? {});
         const own = plan.find((s) => s.name === name)?.sourceEntityId;
         const served = await ask(malloy);
         expect(served.servedBy?.kind).toBe(leg);
         expect(served.servedBy?.tableId).toBe(own);
         expect(rowDifference(live.get(malloy)!, served.rows)).toBeUndefined();
      });
   }
});
