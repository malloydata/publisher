// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A join the served copy moves across connections is left out alone where
 * it can be, and its declaring source whole only where it cannot (a
 * `rename:` reads it): per source, not per model. Before, one declaration
 * that had to be left out whole took every refused join's declaration in the
 * model with it, so a source whose join could be left out alone answered
 * live too ("Reference to undefined object 'd_one'").
 *
 * Through a real package (plan, build, bind, router), on DuckDB with a DuckDB
 * `lake` destination; served rows are compared with live rows as text.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../src/test_helpers/settings_env";
import {
   freshLoadPool,
   LAKE,
   type LoadedFixture,
   loadFixture,
   type Outcome,
} from "../../src/test_helpers/conformance/package_runner";

const DATA = {
   "accounts.csv":
      "id,account_name,region\n1,acme,east\n2,globex,west\n3,initech,east\n",
   "deals.csv":
      "deal_id,account_id,amount\n10,1,100.0\n11,1,5.0\n12,2,300.0\n13,3,50.0\n14,9,75.0\n",
};

const CRM = `#@ persist name="crm_current" storage="${LAKE}"
source: crm_current is duckdb.table('accounts.csv') -> { select: * } extend {
  primary_key: id
}
`;

// Its join is read by a `rename:`, which no definition cut can place.
const UNCUTTABLE = `#@ persist name="d_stored"
source: d_stored is duckdb.table('deals.csv') -> { select: * } extend {
  primary_key: deal_id
  join_one: cur is crm_current with account_id
  dimension: cur_name is cur.account_name
  rename: account_name is cur_name
  measure: deal_total is amount.sum()
}
`;

const CUTTABLE = `#@ persist name="d_one"
source: d_one is duckdb.table('deals.csv') -> { select: * } extend {
  primary_key: deal_id
  join_one: other is crm_current with account_id
  measure: deal_total is amount.sum()
}
`;

const QUERIES = {
   d_one: "run: d_one -> { group_by: account_id; aggregate: deal_total; order_by: account_id }",
   d_stored:
      "run: d_stored -> { group_by: account_id; aggregate: deal_total; order_by: account_id }",
};

function answer(o: Outcome | undefined): { servedBy: string; rows: string } {
   if (!o) return { servedBy: "missing", rows: "" };
   if ("error" in o) return { servedBy: "error", rows: o.error };
   return {
      servedBy: o.answer.servedBy,
      rows: JSON.stringify(o.answer.rows, (_k, v: unknown) =>
         typeof v === "bigint" ? v.toString() : v,
      ),
   };
}

const H = "##! experimental { persistence }\n";

for (const [order, model] of [
   ["the uncuttable source first", H + CRM + UNCUTTABLE + CUTTABLE],
   ["the cuttable source first", H + CRM + CUTTABLE + UNCUTTABLE],
] as const) {
   describe(`a join left out alone beside one left out whole (${order})`, () => {
      let restore: () => void;
      let loader: Awaited<ReturnType<typeof loadFixture>> | undefined;
      let fixture: LoadedFixture | undefined;
      const live: Record<string, Outcome> = {};
      const served: Record<string, Outcome> = {};
      beforeAll(async () => {
         restore = installSettings({
            env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
         }).restore;
         await freshLoadPool();
         loader = await loadFixture({ "m.malloy": model, ...DATA });
         for (const [name, q] of Object.entries(QUERIES)) {
            live[name] = await loader.run("m.malloy", q);
         }
         fixture = await loader.bind();
         for (const [name, q] of Object.entries(QUERIES)) {
            served[name] = await fixture.run("m.malloy", q);
         }
      }, 600_000);
      afterAll(async () => {
         await fixture?.dispose();
         await loader?.dispose().catch(() => {});
         restore?.();
         await freshLoadPool();
      });

      it("serves the source whose join is left out alone, equal to live", () => {
         expect(fixture?.buildErrors).toEqual({});
         expect(answer(served.d_one).servedBy).toBe("materialized");
         expect(answer(served.d_one).rows).toBe(answer(live.d_one).rows);
      });

      it("answers the source left out whole live, equal to live", () => {
         expect(answer(served.d_stored).servedBy).toBe("live");
         expect(answer(served.d_stored).rows).toBe(answer(live.d_stored).rows);
      });

      it("warns that only the uncuttable source is left out whole", () => {
         const problems = [
            ...(fixture?.pkg.getModel("m.malloy")?.servedCopyProblems() ?? []),
         ];
         expect(
            problems
               .filter((p) => p.reason === "cross_connection_join")
               .map((p) => [p.sourceName, p.join, p.leftOutWhole === true])
               .sort(),
         ).toEqual([
            ["d_one", "other", false],
            ["d_stored", "cur", true],
         ]);
      });
   });
}
