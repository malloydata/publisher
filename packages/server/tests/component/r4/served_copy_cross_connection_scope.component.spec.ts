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

// gtm-funnel finding 2 as found: the model's own leave-out-able join
// (`d_raw`) and a second one in a file it imports (`crm_including_deleted`).
// The imported file's refusal failed the import, so the first compile
// reported the importer's own join only as what follows from that failure;
// those follow-ons were then never left out, and the whole copy failed.
const importedSeed = [
   `CREATE TABLE orders AS SELECT * FROM (VALUES
      ('a', 'east', 'shipped', 10.0), ('a', 'east', 'cancelled', 5.0),
      ('b', 'west', 'shipped', 7.0), ('b', 'east', 'shipped', 3.0)
    ) AS t(category, region, status, amount)`,
];

const importedCrm = `##! experimental { persistence }
source: crm_raw is duckdb.table('orders')

#@ persist storage="lake" name="gtmval_crm_current"
source: crm_current is crm_raw -> {
  group_by: category, region
  aggregate: amt is amount.sum()
}

source: crm_including_deleted is crm_raw extend {
  join_one: cur is crm_current on category = cur.category and region = cur.region
  measure: order_count is count()
}
`;

const importingDeals = `##! experimental { persistence }
import "crm.malloy"

source: d_raw is duckdb.table('orders') extend {
  join_one: cur is crm_current on category = cur.category and region = cur.region
  measure: deal_total is amount.sum()
}

source: d11_incl is d_raw -> {
  group_by: category, region, is_deleted is status = 'cancelled', cur_amt is cur.amt
  aggregate: amt is amount.sum()
}

#@ persist storage="lake" name="gtmval_deals11"
source: deals11 is d11_incl extend { where: not is_deleted }

#@ persist storage="lake" name="gtmval_d_unrelated"
source: d_unrelated is duckdb.table('orders') -> {
  group_by: region
  aggregate: n is count()
}
`;

const pipeline = `##! experimental { persistence }
import "deals.malloy"
`;

describe("R4: a cross-connection join in the model and another in a file it imports", () => {
   let fixture: R4Fixture;
   useMaterializationOn();
   beforeAll(async () => {
      fixture = await buildScenario({
         files: { "deals.malloy": importingDeals, "crm.malloy": importedCrm },
         seedSql: importedSeed,
      });
   });
   afterAll(async () => {
      await disposeScenarios();
   });

   const served = [
      [
         "an unrelated stored source",
         "run: d_unrelated -> { group_by: region; aggregate: t is n.sum() }",
      ],
      [
         "the stored source over the joining stage",
         "run: deals11 -> { group_by: category; aggregate: a is amt.sum() }",
      ],
      [
         "the imported stored source both joins read",
         "run: crm_current -> { group_by: category; aggregate: a is amt.sum() }",
      ],
   ] as const;
   for (const [name, malloy] of served) {
      it(`serves ${name}, equal to live`, async () => {
         const run = await fixture.run({ name, malloy });
         expect(run.servedVia).toBe("served_copy");
         expect(rowDifference(run.live, run.served)).toBeUndefined();
      });
   }

   const live = [
      [
         "the importer's joining source",
         "run: d_raw -> { group_by: category; aggregate: deal_total }",
      ],
      [
         "the imported joining source",
         "run: crm_including_deleted -> { group_by: region; aggregate: order_count }",
      ],
   ] as const;
   for (const [name, malloy] of live) {
      it(`answers ${name} live, equal to live`, async () => {
         const run = await fixture.run({ name, malloy });
         expect(run.servedVia).toBeUndefined();
         expect(rowDifference(run.live, run.served)).toBeUndefined();
      });
   }

   it("warns of each left-out join on its own source", async () => {
      const problems = await fixture.servedCopyProblems();
      expect(
         problems.map((p) => [p.sourceName, p.reason, p.join]).sort(),
      ).toEqual([
         ["crm_including_deleted", "cross_connection_join", "cur"],
         ["d_raw", "cross_connection_join", "cur"],
      ]);
   });
});

describe("R4: an importer of a model whose imported file also refuses a join", () => {
   let fixture: R4Fixture;
   useMaterializationOn();
   beforeAll(async () => {
      fixture = await buildScenario({
         files: {
            "pipeline.malloy": pipeline,
            "deals.malloy": importingDeals,
            "crm.malloy": importedCrm,
         },
         seedSql: importedSeed,
      });
   });
   afterAll(async () => {
      await disposeScenarios();
   });

   it("serves the unrelated stored source, equal to live", async () => {
      const run = await fixture.run({
         name: "pipeline",
         malloy:
            "run: d_unrelated -> { group_by: region; aggregate: t is n.sum() }",
      });
      expect(run.servedVia).toBe("served_copy");
      expect(rowDifference(run.live, run.served)).toBeUndefined();
   });
});
