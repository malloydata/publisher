// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Second review of #1703, G8: a #(materialize) re-root keeps the blocks of its
 * same-file bases that declare a measure, view or join. Two callers per shape,
 * interleaved (#1637's leak needed the second caller after the first), with a
 * DEFAULTED given so a value baked into the build would show as a wrong answer
 * rather than a failed build; gates on the base; multi-block bases.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { AccessDeniedError } from "../../../src/errors";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
   type R4Run,
} from "./harness";

const seedSql = [
   `CREATE TABLE ev AS SELECT * FROM (VALUES
      (1, 'acme', 10.0, 'gold'),
      (2, 'acme', 20.0, 'tin'),
      (3, 'globex', 5.0, 'gold'),
      (4, 'globex', 7.0, 'tin'),
      (5, 'initech', 3.0, 'gold')
    ) AS t(id, org, amt, tier)`,
   `CREATE TABLE labels AS SELECT * FROM (VALUES
      ('acme', 'A'), ('globex', 'G'), ('initech', 'I')
    ) AS t(org, letter)`,
];

const model = `##! experimental { givens access_modifiers }
given:
  ORG :: string is 'acme'
  GROUPS :: string[] is ['acme']
  ROLE :: string is 'viewer'

// G8's trigger: a base with a measure, and a given-reading dimension but
// no given-reading where: (so no kept base term).
source: b_dim is duckdb.table('ev') extend {
  dimension: mine is org = $ORG
  dimension: tag is pick 'me' when org = $ORG else 'other'
  measure: n is count()
}
#(materialize) storage="lake"
source: m_dim is b_dim extend { measure: total is amt.sum() }

// A measure filtered by a given, on the base only.
source: b_meas is duckdb.table('ev') extend {
  measure: mine_total is amt.sum() { where: org = $ORG }
  view: v is { group_by: org; aggregate: mine_total }
}
#(materialize) storage="lake"
source: m_meas is b_meas extend { dimension: z is upper(org) }

// A row-level gate on the base, which declares a measure.
#(access_filter) org in $GROUPS
source: b_gate is duckdb.table('ev') extend {
  measure: n is count()
}
#(materialize) storage="lake"
source: m_gate is b_gate extend { measure: total is amt.sum() }

// A source gate on the base, which declares a view.
#(authorize) $ROLE = 'admin'
source: b_auth is duckdb.table('ev') extend {
  measure: n is count()
  view: by_org is { group_by: org; aggregate: n }
}
#(materialize) storage="lake"
source: m_auth is b_auth extend { measure: total is amt.sum() }

// extend / include / extend on the base.
source: b_multi is duckdb.table('ev') extend {
  measure: t is amt.sum()
} include { public: *; internal: tier } extend {
  view: v is { group_by: org; aggregate: t }
  dimension: gold is tier = 'gold'
}
#(materialize) storage="lake"
source: m_multi is b_multi extend { dimension: zz is upper(org) }

// A base declaring a join and a measure through it.
source: b_join is duckdb.table('ev') extend {
  join_one: l is duckdb.table('labels') on l.org = org
  measure: n is count()
}
#(materialize) storage="lake"
source: m_join is b_join extend { measure: total is amt.sum() }

// A measure two steps up the chain.
source: b_top is duckdb.table('ev') extend { measure: n is count() }
source: b_mid is b_top extend { dimension: big is amt > 6 }
#(materialize) storage="lake"
source: m_chain is b_mid extend { measure: total is amt.sum() }

// A constant where: on a base that also declares a measure (re-applied over
// a table already filtered by it).
source: b_where is duckdb.table('ev') extend {
  where: amt > 4
  measure: n is count()
}
#(materialize) storage="lake"
source: m_where is b_where extend { measure: total is amt.sum() }

// A base that renames a column and declares a measure.
source: b_ren is duckdb.table('ev') extend {
  rename: company is org
  measure: n is count()
}
#(materialize) storage="lake"
source: m_ren is b_ren extend { measure: total is amt.sum() }

// A base that drops a column and declares a measure.
source: b_exc is duckdb.table('ev') extend {
  except: tier
  measure: n is count()
}
#(materialize) storage="lake"
source: m_exc is b_exc extend { measure: total is amt.sum() }
`;

const ORG_CALLERS = [{ ORG: "acme" }, { ORG: "globex" }];

async function eachCaller(
   fixture: R4Fixture,
   malloy: string,
   callers: Record<string, string | string[]>[],
): Promise<R4Run[]> {
   const runs: R4Run[] = [];
   for (const givens of [...callers, ...callers]) {
      const run = await fixture.run({
         name: `${malloy} as ${JSON.stringify(givens)}`,
         malloy,
         givens,
      });
      expect({
         malloy,
         givens,
         diff: rowDifference(run.live, run.served),
      }).toEqual({ malloy, givens, diff: undefined });
      runs.push(run);
   }
   return runs;
}

for (const gated of [false, true]) {
   describe(`review 1703b G8: kept base blocks, two callers (gated sources ${gated ? "on" : "off"})`, () => {
      let restore: () => void;
      let fixture: R4Fixture;
      beforeAll(async () => {
         restore = installSettings({
            env: {
               PERSIST_STORAGE_MODE: "on",
               PUBLISHER_MATERIALIZE: "true",
               ...(gated
                  ? { PUBLISHER_MATERIALIZE_GATED_SOURCES: "true" }
                  : {}),
            },
         }).restore;
         fixture = await buildScenario({
            files: { "m.malloy": model },
            seedSql,
            materialize: true,
         });
      });
      afterAll(async () => {
         restore();
         await disposeScenarios();
      });

      it("reports what it built and the served copy's problems", async () => {
         const built = fixture.bindings
            .flatMap((b) => b.sourceIDs ?? [])
            .map((id) => id.split("@")[0])
            .sort();
         expect(built.length).toBeGreaterThan(0);
      });

      const orgCases = [
         "run: m_dim -> { group_by: org, mine, tag; aggregate: total, n }",
         "run: m_dim -> { where: mine; aggregate: total }",
         "run: m_meas -> { group_by: z; aggregate: mine_total }",
         "run: m_meas -> v",
      ];
      for (const malloy of orgCases) {
         it(`${malloy}: each caller gets live's rows`, async () => {
            const runs = await eachCaller(fixture, malloy, ORG_CALLERS);
            expect(runs[0].live).not.toEqual(runs[1].live);
         });
      }

      it("the access-filtered base: each caller gets live's rows only", async () => {
         const malloy = "run: m_gate -> { group_by: org; aggregate: total, n }";
         const runs = await eachCaller(fixture, malloy, [
            { GROUPS: ["acme"] },
            { GROUPS: ["globex", "initech"] },
         ]);
         expect(runs[0].served.map((r) => r.org)).toEqual(["acme"]);
         expect(runs[1].served.map((r) => r.org).sort()).toEqual([
            "globex",
            "initech",
         ]);
      });

      it("the authorized base: the admin gets live's rows, the viewer is denied", async () => {
         const malloy = "run: m_auth -> by_org";
         const run = await fixture.run({
            name: "admin",
            malloy,
            givens: { ROLE: "admin" },
         });
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         let refusal: unknown;
         try {
            await fixture.run({
               name: "viewer",
               malloy,
               givens: { ROLE: "viewer" },
            });
         } catch (e) {
            refusal = e;
         }
         expect(refusal).toBeInstanceOf(AccessDeniedError);
         // and with the default
         let refusal2: unknown;
         try {
            await fixture.run({ name: "default", malloy });
         } catch (e) {
            refusal2 = e;
         }
         expect(refusal2).toBeInstanceOf(AccessDeniedError);
      });

      for (const malloy of [
         "run: m_multi -> v",
         "run: m_multi -> { group_by: zz, gold; aggregate: t }",
         "run: m_join -> { group_by: l.letter; aggregate: total, n }",
         "run: m_chain -> { group_by: big; aggregate: total, n }",
         "run: m_where -> { group_by: org; aggregate: total, n }",
         "run: m_ren -> { group_by: company; aggregate: total, n }",
         "run: m_exc -> { group_by: org; aggregate: total, n }",
      ]) {
         it(`${malloy}: equal to live`, async () => {
            const run = await fixture.run({ name: malloy, malloy });
            expect(rowDifference(run.live, run.served)).toBeUndefined();
         });
      }

      it("the multi-block base's internal column stays hidden", async () => {
         for (const malloy of [
            "run: m_multi -> { group_by: tier; aggregate: t }",
            "run: m_multi -> { where: tier = 'gold'; aggregate: t }",
         ]) {
            let refusal: unknown;
            try {
               await fixture.run({ name: malloy, malloy });
            } catch (e) {
               refusal = e;
            }
            expect({ malloy, refused: refusal !== undefined }).toEqual({
               malloy,
               refused: true,
            });
         }
      });
   });
}
