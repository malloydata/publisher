// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4: a stored source the served copy re-roots onto its destination moves an
 * unpersisted source's join to it across connections, and that join cannot
 * be left out as one definition. Before, the copy then failed to compile and
 * every source of the model, and of every model importing it, answered live
 * under one model-level `cross_connection_unbuilt` warning. Now only the
 * declaring source is left out (and what reads it), with a per-source
 * warning; every other stored source still serves, equal to live.
 *
 * Two ways the join could not be left out alone:
 * - it shares its `join_one:` statement with a second refused join, and
 *   blanking both left the keyword with nothing after it;
 * - what reads it is a `rename:` (or an `except:`), which is no definition a
 *   cut can place, so the whole declaring source is left out instead.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { useMaterializationOn } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
} from "./harness";

const seedSql = [
   `CREATE TABLE accounts AS SELECT * FROM (VALUES
      (1, 'acme', 'east'), (2, 'globex', 'west'), (3, 'initech', 'east')
    ) AS t(id, account_name, region)`,
   `CREATE TABLE deals AS SELECT * FROM (VALUES
      (10, 1, 100.0), (11, 1, 5.0), (12, 2, 300.0), (13, 3, 50.0), (14, 9, 75.0)
    ) AS t(deal_id, account_id, amount)`,
   `CREATE TABLE carriers AS SELECT * FROM (VALUES
      ('AA', 'east'), ('UA', 'west'), ('DL', 'east')
    ) AS t(code, region)`,
];

// `crm_current` is stored in `lake`; `d_raw` stays on the origin and joins
// it, so the re-root moves that join across connections.
const head = `##! experimental { persistence }
#@ persist name="crm_current" storage="lake"
source: crm_current is duckdb.table('accounts') -> { select: * } extend {
  primary_key: id
}
`;

const tail = `
#@ persist name="carriers_copy" storage="lake"
source: carriers_copy is duckdb.table('carriers') -> { select: * } extend {
  measure: carrier_count is count()
}
`;

const shapes = {
   "two refused joins in one statement": `
source: d_raw is duckdb.table('deals') extend {
  primary_key: deal_id
  join_one: cur is crm_current with account_id, prev is crm_current with account_id
  measure: deal_total is amount.sum()
}

source: d11_incl is d_raw -> {
  select: deal_id, amount, account_name is cur.account_name
}

#@ persist name="deals11" storage="lake"
source: deals11 is d11_incl extend {
  where: amount > 10
  measure: deal_count is count()
}
`,
   "a join read by a rename": `
source: d_raw is duckdb.table('deals') extend {
  primary_key: deal_id
  join_one: cur is crm_current with account_id
  dimension: cur_name is cur.account_name
  rename: account_name is cur_name
  measure: deal_total is amount.sum()
}

source: d11_incl is d_raw -> { select: deal_id, amount, account_name }

#@ persist name="deals11" storage="lake"
source: deals11 is d11_incl extend {
  where: amount > 10
  measure: deal_count is count()
}
`,
} as const;

const importer = `##! experimental { persistence }
import "deals.malloy"

source: carriers_more is carriers_copy extend {
  measure: east_count is carrier_count { where: region = 'east' }
}
`;

for (const [shape, body] of Object.entries(shapes)) {
   const deals = head + body + tail;

   describe(`R4: ${shape}, moved across connections by a re-root`, () => {
      let fixture: R4Fixture;
      useMaterializationOn();
      beforeAll(async () => {
         fixture = await buildScenario({
            files: { "deals.malloy": deals },
            seedSql,
         });
      });
      afterAll(async () => {
         await disposeScenarios();
      });

      it("serves an unrelated stored source in the same file, equal to live", async () => {
         const run = await fixture.run({
            name: "unrelated",
            malloy:
               "run: carriers_copy -> { group_by: region; aggregate: carrier_count }",
         });
         expect(run.servedVia).toBe("served_copy");
         expect(rowDifference(run.live, run.served)).toBeUndefined();
      });

      it("serves the stored source built over it from its table, equal to live", async () => {
         const run = await fixture.run({
            name: "built over",
            malloy:
               "run: deals11 -> { group_by: account_name; aggregate: deal_count }",
         });
         expect(run.servedVia).toBe("served_copy");
         expect(rowDifference(run.live, run.served)).toBeUndefined();
      });

      it("answers the source declaring the join live, equal to live", async () => {
         const run = await fixture.run({
            name: "declaring",
            malloy:
               "run: d_raw -> { group_by: account_id; aggregate: deal_total }",
         });
         expect(run.servedVia).toBeUndefined();
         expect(rowDifference(run.live, run.served)).toBeUndefined();
      });

      it("names the declaring source in a per-source warning, not the model", async () => {
         const problems = await fixture.servedCopyProblems();
         expect(problems.length).toBeGreaterThan(0);
         for (const p of problems) {
            expect([p.sourceName, p.reason]).toEqual([
               "d_raw",
               "cross_connection_join",
            ]);
         }
         expect(problems.map((p) => p.join)).toContain("cur");
         // Left out whole only where the join could not be left out alone.
         expect(problems.some((p) => p.leftOutWhole)).toBe(
            shape === "a join read by a rename",
         );
      });
   });

   describe(`R4: an importer of a file with ${shape}`, () => {
      let fixture: R4Fixture;
      useMaterializationOn();
      beforeAll(async () => {
         fixture = await buildScenario({
            files: { "importer.malloy": importer, "deals.malloy": deals },
            seedSql,
         });
      });
      afterAll(async () => {
         await disposeScenarios();
      });

      it("still serves its stored source, equal to live", async () => {
         const run = await fixture.run({
            name: "importer",
            malloy:
               "run: carriers_more -> { group_by: region; aggregate: carrier_count, east_count }",
         });
         expect(run.servedVia).toBe("served_copy");
         expect(rowDifference(run.live, run.served)).toBeUndefined();
      });
   });
}
