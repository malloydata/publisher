// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for tenant isolation through a base: the common governed shape, a base
 * that scopes its rows by a given (`where: org = $ORG`) or declares a
 * given-reading dimension, and `#(materialize)` extensions over it.
 *
 * Only a materialized source's own given-reading `where:` (and its own
 * given-reading dimensions) is left out of the build and applied at read:
 * the served copy keeps only that source's declaration. So every extension
 * here that inherits its term or dimension from a base is refused at plan,
 * is not built, and answers live, under the term. This is the shape of
 * #1637's cross-tenant leak (a re-root dropped a given-reading `where:` and a
 * second caller saw the first caller's rows), so every case runs two callers
 * with different givens, interleaved and repeated, each equal to live, each
 * holding only its own rows. The fix the refusal names (the term moved onto
 * the materialized source, and extensions of it that declare no
 * `#(materialize)`) is served from its table, per caller.
 *
 * The harness builds each generated source with its SQL as compiled and does
 * not run the eligibility gates, so a source the generator wrongly planned
 * would be built and could be answered from its table: the guard under test
 * is the generator's and the served copy's, not the gate's.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { AccessDeniedError } from "../../../src/errors";
import {
   installSettings,
   useMaterializationOn,
} from "../../../src/test_helpers/settings_env";
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
      (5, 'globex', 9.0, 'gold'),
      (6, 'initech', 3.0, 'gold')
    ) AS t(id, org, amt, tier)`,
   `CREATE TABLE tiers AS SELECT * FROM (VALUES
      ('acme', 'gold', 'A-gold'),
      ('acme', 'tin', 'A-tin'),
      ('globex', 'gold', 'G-gold'),
      ('globex', 'tin', 'G-tin'),
      ('initech', 'gold', 'I-gold')
    ) AS t(org, tier, label)`,
];

const model = `##! experimental { givens access_modifiers }
given:
  ORG :: string
  MIN_AMT :: number is 0

source: ev is duckdb.table('ev')
// Stored in the same destination, so a join to it stays a join at read.
#(materialize) storage="lake"
source: tiers is duckdb.table('tiers')

source: base is ev extend {
  where: org = $ORG
  dimension: mine is org = $ORG
  dimension: label is pick 'mine' when mine else 'theirs'
  dimension: up is upper(org)
  measure: base_total is amt.sum()
}

#(materialize) storage="lake"
source: orders is base extend {
  measure: total is amt.sum()
  view: by_label is { group_by: label; aggregate: total }
}

source: mid is base extend {
  dimension: big is amt > 6
}

#(materialize) storage="lake"
source: deep is mid extend {
  measure: total is amt.sum()
}

source: through_dim is ev extend {
  dimension: org_key is pick org when org is not null else 'none'
  dimension: is_mine is org_key = $ORG
  where: org_key = $ORG
}

#(materialize) storage="lake"
source: by_dimension is through_dim extend {
  measure: total is amt.sum()
}

source: joined_base is ev extend {
  join_one: t is tiers on t.org = org and t.tier = tier and t.org = $ORG
  dimension: tier_label is t.label
}

#(materialize) storage="lake"
source: by_join is joined_base extend {
  measure: total is amt.sum()
}

#(materialize) storage="lake"
source: both is base extend {
  where: amt >= $MIN_AMT
  measure: total is amt.sum()
}

source: renaming_base is ev extend {
  where: org = $ORG
}

#(materialize) storage="lake"
source: renames is renaming_base extend {
  rename: company is org
  dimension: org is 'x'
  measure: total is amt.sum()
}

// The fix the refusals name: the term and the dimension on the materialized
// source itself, and an extension of it that declares no #(materialize).
#(materialize) storage="lake"
source: own_scoped is ev extend {
  where: org = $ORG
  dimension: mine is org = $ORG
  dimension: label is pick 'mine' when mine else 'theirs'
  measure: total is amt.sum()
}

source: own_ext is own_scoped extend {
  dimension: big is amt > 6
  view: by_label is { group_by: label; aggregate: total }
}
`;

const CALLERS = [{ ORG: "acme" }, { ORG: "globex" }];

/** Two callers, interleaved and repeated: acme, globex, acme, globex. */
const INTERLEAVED = [...CALLERS, ...CALLERS];

async function asEachCaller(
   fixture: R4Fixture,
   name: string,
   malloy: string,
   options: {
      served?: boolean;
      givens?: Record<string, string | number>[];
      orgsOf?: (row: Record<string, unknown>) => unknown;
   } = {},
): Promise<R4Run[]> {
   const served = options.served ?? true;
   const runs: R4Run[] = [];
   for (const givens of options.givens ?? INTERLEAVED) {
      const run = await fixture.run({
         name: `${name} as ${JSON.stringify(givens)}`,
         malloy,
         givens,
      });
      expect(rowDifference(run.live, run.served)).toBeUndefined();
      expect(run.servedVia).toBe(served ? "served_copy" : undefined);
      if (served) expect(run.servedFrom).toBe("storage");
      if (options.orgsOf) {
         for (const row of run.served) {
            expect(options.orgsOf(row)).toBe(givens.ORG);
         }
      }
      runs.push(run);
   }
   // Different callers see different answers live, so a served answer that
   // read one caller's rows, or stored one caller's value, could not match
   // both; repeated, so an answer cached from the first caller would not.
   expect(runs[0].live).not.toEqual(runs[1].live);
   if (runs.length >= 4) {
      expect(runs[2].served).toEqual(runs[0].served);
      expect(runs[3].served).toEqual(runs[1].served);
   }
   return runs;
}

const ORGS = ["acme", "globex", "initech"];

describe("R4: a base's given-reading where: and dimensions, re-applied at read through the served copy", () => {
   let fixture: R4Fixture;
   useMaterializationOn();
   beforeAll(async () => {
      fixture = await buildScenario({
         files: { "m.malloy": model },
         seedSql,
         materialize: true,
      });
   });
   afterAll(async () => {
      await disposeScenarios();
   });

   it("builds no source that inherits a given-reading where: or dimension, and builds the source that writes its own", async () => {
      const built = fixture.bindings
         .flatMap((b) => b.sourceIDs ?? [])
         .map((id) => id.split("@")[0])
         .sort();
      expect(built).toEqual(["own_scoped", "tiers"]);
      const orgs = await fixture.storedSQL(
         (t) => `SELECT DISTINCT org FROM ${t("own_scoped")} ORDER BY org`,
      );
      expect(orgs.map((r) => r.org)).toEqual(ORGS);
      const [row] = await fixture.storedSQL(
         (t) => `SELECT * FROM ${t("own_scoped")} LIMIT 1`,
      );
      expect(Object.keys(row)).not.toContain("mine");
      expect(Object.keys(row)).not.toContain("label");
   });

   describe("an inherited where: term and a base's given-reading dimension", () => {
      for (const [name, malloy] of [
         [
            "grouped by a stored column",
            "run: orders -> { group_by: org, tier; aggregate: total, n is count(); order_by: org, tier }",
         ],
         [
            "grouped by the base's given-reading dimension",
            "run: orders -> { group_by: mine, org; aggregate: total }",
         ],
         [
            "grouped by one that reads it through another",
            "run: orders -> { group_by: label, org; aggregate: total }",
         ],
         ["the source's own view over it", "run: orders -> by_label"],
         [
            "the base's measure",
            "run: orders -> { group_by: org; aggregate: base_total }",
         ],
         [
            "a stored computed dimension of the base",
            "run: orders -> { group_by: up, org; aggregate: total }",
         ],
      ] as const) {
         it(`answers each caller live, with its own rows: ${name}`, async () => {
            await asEachCaller(fixture, name, malloy, {
               served: false,
               orgsOf:
                  malloy.includes("org;") || malloy.includes("org,")
                     ? (r) => r.org
                     : undefined,
            });
         });
      }

      it("answers an extension of an extension live, per caller", async () => {
         await asEachCaller(
            fixture,
            "deep",
            "run: deep -> { group_by: org, big, label; aggregate: total; order_by: big, label }",
            { served: false, orgsOf: (r) => r.org },
         );
      });

      it("answers a term and a dimension that reach the tenant column through another dimension live, per caller", async () => {
         await asEachCaller(
            fixture,
            "by_dimension",
            "run: by_dimension -> { group_by: org, org_key, is_mine; aggregate: total, n is count() }",
            { served: false, orgsOf: (r) => r.org },
         );
      });

      it("answers a dimension that reads its given through a join condition live, per caller", async () => {
         await asEachCaller(
            fixture,
            "by_join",
            "run: by_join -> { group_by: tier_label; aggregate: total; order_by: tier_label }",
            { served: false },
         );
      });

      it("answers a base's term and the source's own term together live, per caller and per threshold", async () => {
         await asEachCaller(
            fixture,
            "both",
            "run: both -> { group_by: org; aggregate: total, n is count() }",
            {
               givens: [
                  { ORG: "acme", MIN_AMT: 15 },
                  { ORG: "globex", MIN_AMT: 6 },
                  { ORG: "acme", MIN_AMT: 15 },
                  { ORG: "globex", MIN_AMT: 6 },
               ],
               served: false,
               orgsOf: (r) => r.org,
            },
         );
         // A lower threshold answers differently, live.
         const low = await fixture.run({
            name: "both low",
            malloy: "run: both -> { aggregate: n is count() }",
            givens: { ORG: "globex", MIN_AMT: 0 },
         });
         const high = await fixture.run({
            name: "both high",
            malloy: "run: both -> { aggregate: n is count() }",
            givens: { ORG: "globex", MIN_AMT: 8 },
         });
         for (const run of [low, high]) {
            expect(run.servedVia).toBeUndefined();
            expect(rowDifference(run.live, run.served)).toBeUndefined();
         }
         expect(low.served).toEqual([{ n: 3 }]);
         expect(high.served).toEqual([{ n: 1 }]);
      });

      it("does not build a source that renames the base's field, and denies it as live does", async () => {
         // The base's `where: org = $ORG` is inherited, so the generator
         // refuses the source (not built, above); live's bind guard denies it
         // (the base's term would read the source's own new `org`), and so
         // does the served path.
         for (const givens of CALLERS) {
            let refusal: unknown;
            try {
               await fixture.run({
                  name: `renames as ${givens.ORG}`,
                  malloy:
                     "run: renames -> { group_by: company; aggregate: total }",
                  givens,
               });
            } catch (error) {
               refusal = error;
            }
            expect(refusal).toBeInstanceOf(AccessDeniedError);
         }
      });

      it("answers a caller who supplies no given as live does", async () => {
         const attempt = async (bindings: typeof fixture.bindings) => {
            fixture.setBindings(bindings);
            try {
               return {
                  rows: (
                     await fixture.runAsIs(
                        "run: own_scoped -> { group_by: org, mine; aggregate: total }",
                     )
                  ).rows,
               };
            } catch (error) {
               return { error: String(error) };
            }
         };
         const live = await attempt([]);
         const served = await attempt(fixture.bindings);
         fixture.setBindings(fixture.bindings);
         // Whatever live does with an unset given (an error, or no rows), the
         // stored table must do the same: never every caller's rows.
         expect(served).toEqual(live);
         if ("rows" in live) expect(live.rows).toEqual([]);
      });

      it("serves the source that writes its own term, and an extension of it, from its table, per caller", async () => {
         for (const malloy of [
            "run: own_scoped -> { group_by: org, label; aggregate: total }",
            "run: own_ext -> { group_by: org, big, label; aggregate: total; order_by: big, label }",
         ]) {
            await asEachCaller(fixture, malloy, malloy, {
               orgsOf: (r) => r.org,
            });
         }
         await asEachCaller(
            fixture,
            "own_ext view",
            "run: own_ext -> by_label",
         );
      });

      // Last: it empties the origin, so only the stored table can answer.
      it("answers from the stored table once the origin is emptied, each caller its own rows", async () => {
         const truth = async (org: string) =>
            fixture.storedSQL(
               (t) =>
                  `SELECT org, sum(amt)::DOUBLE AS total FROM ${t("own_scoped")}
                   WHERE org = '${org}' GROUP BY 1`,
            );
         const expected = {
            acme: await truth("acme"),
            globex: await truth("globex"),
         };
         await fixture.storedSQL(() => "DELETE FROM ev");
         for (const givens of INTERLEAVED) {
            const run = await fixture.run({
               name: `after the origin emptied as ${givens.ORG}`,
               malloy: "run: own_ext -> { group_by: org; aggregate: total }",
               givens,
            });
            expect(run.live).toEqual([]);
            expect(run.servedVia).toBe("served_copy");
            expect(
               rowDifference(
                  expected[givens.ORG as "acme" | "globex"],
                  run.served,
               ),
            ).toBeUndefined();
         }
      });
   });
});

const gatedModel = `##! experimental { givens access_modifiers }
given:
  ORG :: string
  TIERS :: string[]

source: base is duckdb.table('ev') extend {
  where: org = $ORG
}

#(access_filter) tier in $TIERS
#(materialize) storage="lake"
source: gated is base extend {
  dimension: mine is org = $ORG
  measure: total is amt.sum()
}

// The same gate over the term written on the materialized source itself.
#(access_filter) tier in $TIERS
#(materialize) storage="lake"
source: gated_own is duckdb.table('ev') extend {
  where: org = $ORG
  dimension: mine is org = $ORG
  measure: total is amt.sum()
}
`;

describe("R4: a gated source with an inherited given term (gated serving on): not built, live", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: {
            PUBLISHER_PERSIST_STORAGE_MODE: "on",
            PUBLISHER_MATERIALIZE_ENABLED: "true",
            PUBLISHER_MATERIALIZE_GATED_SOURCES: "true",
         },
      }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": gatedModel },
         seedSql,
         materialize: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("builds only the source that writes its own term", () => {
      expect(
         fixture.bindings
            .flatMap((b) => b.sourceIDs ?? [])
            .map((id) => id.split("@")[0]),
      ).toEqual(["gated_own"]);
   });

   it("answers each caller live with its own rows inside its gate", async () => {
      const callers = [
         { ORG: "acme", TIERS: ["gold"] },
         { ORG: "globex", TIERS: ["gold", "tin"] },
         { ORG: "acme", TIERS: ["gold"] },
         { ORG: "globex", TIERS: ["gold", "tin"] },
      ];
      const runs: R4Run[] = [];
      for (const givens of callers) {
         const run = await fixture.run({
            name: `gated as ${JSON.stringify(givens)}`,
            malloy:
               "run: gated -> { group_by: org, tier, mine; aggregate: total; order_by: tier }",
            givens,
         });
         expect(run.servedVia).toBeUndefined();
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         for (const row of run.served) {
            expect(row.org).toBe(givens.ORG);
            expect(givens.TIERS).toContain(row.tier as string);
         }
         runs.push(run);
      }
      expect(runs[0].live).not.toEqual(runs[1].live);
      expect(runs[2].served).toEqual(runs[0].served);
      expect(runs[3].served).toEqual(runs[1].served);
   });

   it("serves the one that writes its own term from its table, each caller its own rows inside its gate", async () => {
      const callers = [
         { ORG: "acme", TIERS: ["gold"] },
         { ORG: "globex", TIERS: ["gold", "tin"] },
         { ORG: "acme", TIERS: ["gold"] },
         { ORG: "globex", TIERS: ["gold", "tin"] },
      ];
      const runs: R4Run[] = [];
      for (const givens of callers) {
         const run = await fixture.run({
            name: `gated_own as ${JSON.stringify(givens)}`,
            malloy:
               "run: gated_own -> { group_by: org, tier, mine; aggregate: total; order_by: tier }",
            givens,
         });
         expect(run.servedVia).toBe("served_copy");
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         for (const row of run.served) {
            expect(row.org).toBe(givens.ORG);
            expect(givens.TIERS).toContain(row.tier as string);
         }
         runs.push(run);
      }
      expect(runs[0].live).not.toEqual(runs[1].live);
      expect(runs[2].served).toEqual(runs[0].served);
      expect(runs[3].served).toEqual(runs[1].served);
   });

   it("denies a caller who supplies no gate value, as live does", async () => {
      let refusal: unknown;
      try {
         await fixture.run({
            name: "no tiers",
            malloy: "run: gated -> { group_by: org; aggregate: total }",
            givens: { ORG: "acme" },
         });
      } catch (error) {
         refusal = error;
      }
      expect(refusal).toBeInstanceOf(AccessDeniedError);
   });
});
