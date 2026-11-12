// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for tenant isolation in the served copy: a `where:` that reads a given is
 * never in a stored table, so it is applied per caller when the table is read.
 * The re-root replaces a declaration's base, so a term written there (a
 * parenthesised head, an inline extend) would be gone from the served source,
 * and every caller would read every caller's rows. Each placement is checked
 * for two callers with different givens against live; a source whose term the
 * served copy cannot keep is left as written, with a coded warning.
 *
 * The harness builds each table with its SQL as compiled. A `#@ persist`
 * source's given is in its extend block, never in the build; a
 * `#(materialize)` source's given term is left out of its build by the
 * generator's build copy (P2), so its table holds every caller's rows and only
 * the served copy's re-applied term scopes a read. A served copy that dropped
 * the term would answer each caller with every caller's rows.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
} from "./harness";

const seedSql = [
   `CREATE TABLE ev AS SELECT * FROM (VALUES
      (1, 'acme', 10.0, DATE '2026-01-01'),
      (2, 'acme', 20.0, DATE '2026-01-02'),
      (3, 'globex', 5.0, DATE '2026-01-01'),
      (4, 'globex', 7.0, DATE '2026-01-03')
    ) AS t(id, org, amt, d)`,
];

const persistModel = `##! experimental { persistence givens }
given: ORG :: string is 'acme'

source: ev is duckdb.table('ev')

#@ persist name="trailing" storage="lake"
source: trailing is ev -> {
  group_by: org, d
  aggregate: total is amt.sum()
} extend {
  where: org = $ORG
}

#@ persist name="in_parens" storage="lake"
source: in_parens is (ev -> {
  group_by: org, d
  aggregate: total is amt.sum()
} extend {
  where: org = $ORG
})

#@ persist name="parens_then_extend" storage="lake"
source: parens_then_extend is (ev -> {
  group_by: org, d
  aggregate: total is amt.sum()
} extend {
  where: org = $ORG
}) extend {
  measure: grand is total.sum()
}
`;

const materializeModel = `##! experimental.givens
given: ORG :: string is 'acme'

source: ev is duckdb.table('ev')

#(materialize) storage="lake"
source: head_extend is (ev extend { where: org = $ORG }) extend {
  measure: total is amt.sum()
}

#(materialize) storage="lake"
source: trailing_where is ev extend {
  where: org = $ORG
  measure: total is amt.sum()
}
`;

const CALLERS = [{ ORG: "acme" }, { ORG: "globex" }];

async function expectEachCallerLive(
   fixture: R4Fixture,
   name: string,
   malloy: string,
) {
   const runs = [];
   for (const givens of CALLERS) {
      const run = await fixture.run({
         name: `${name} as ${givens.ORG}`,
         malloy,
         givens,
      });
      expect(rowDifference(run.live, run.served)).toBeUndefined();
      runs.push(run);
   }
   // The two callers see different rows live, so a served answer that ignored
   // the given could not match both.
   expect(runs[0].live).not.toEqual(runs[1].live);
   return runs;
}

describe("R4: a given-reading where: is applied per caller in the served copy", () => {
   let restore: () => void;
   beforeAll(() => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   describe("#@ persist storage=", () => {
      let fixture: R4Fixture;
      beforeAll(async () => {
         fixture = await buildScenario({
            files: { "m.malloy": persistModel },
            seedSql,
         });
      });

      it("serves a term in the trailing extend from the served copy, per caller", async () => {
         const runs = await expectEachCallerLive(
            fixture,
            "trailing",
            "run: trailing -> { group_by: org; aggregate: t is total.sum() }",
         );
         for (const run of runs) expect(run.servedVia).toBe("served_copy");
      });

      for (const source of ["in_parens", "parens_then_extend"]) {
         it(`leaves ${source} (its term in a parenthesised head) to another path, per caller`, async () => {
            const runs = await expectEachCallerLive(
               fixture,
               source,
               `run: ${source} -> { group_by: org; aggregate: t is total.sum() }`,
            );
            for (const run of runs) expect(run.servedVia).toBeUndefined();
         });
      }
   });

   describe("#(materialize)", () => {
      let fixture: R4Fixture;
      beforeAll(async () => {
         fixture = await buildScenario({
            files: { "m.malloy": materializeModel },
            seedSql,
            materialize: true,
         });
      });

      it("does not build a source whose term is in an inline extend in its head, and answers each caller live", async () => {
         // The generator refuses it (the re-root would drop the term), so no
         // table exists for it and the served copy has nothing to re-root.
         expect(
            fixture.bindings
               .flatMap((b) => b.sourceIDs ?? [])
               .map((id) => id.split("@")[0]),
         ).toEqual(["trailing_where"]);
         const runs = await expectEachCallerLive(
            fixture,
            "head_extend",
            "run: head_extend -> { group_by: org; aggregate: total }",
         );
         for (const run of runs) expect(run.servedVia).toBeUndefined();
      });

      it("builds a term in the source's own extend out of the table and re-applies it per caller from the served copy", async () => {
         // The build left the term out: the table holds every caller's rows.
         const stored = await fixture.storedSQL(
            (table) => `SELECT * FROM ${table("trailing_where")}`,
         );
         expect([...new Set(stored.map((r) => r.org))].sort()).toEqual([
            "acme",
            "globex",
         ]);
         const problems = await fixture.servedCopyProblems();
         expect(
            problems.filter((p) => p.sourceName === "trailing_where"),
         ).toEqual([]);
         for (const malloy of [
            "run: trailing_where -> { group_by: org; aggregate: total }",
            "run: trailing_where -> { group_by: d; aggregate: total, n is count() }",
         ]) {
            const runs = await expectEachCallerLive(
               fixture,
               "trailing_where",
               malloy,
            );
            for (const run of runs) expect(run.servedVia).toBe("served_copy");
         }
      });
   });
});
