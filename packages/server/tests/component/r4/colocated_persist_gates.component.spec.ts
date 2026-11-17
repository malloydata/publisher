// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for colocated v1 `#@ persist` tables past a row-level gate and across
 * column shapes, with the gated-sources switch off (G3), held to the real
 * package path. Each source is built and bound twice: by the R4 harness, and
 * by a real package (conformance/package_runner.ts: its build plan, its
 * colocated bind, the router and `govern()`). For every caller, interleaved:
 *
 * - the harness answers by the route the package answers by, the served copy
 *   or live, so R4 never proves a route no run takes;
 * - the harness's served rows are the package's, and both are live's;
 * - an entry point under its own `#(access_filter)` is served from its table
 *   with the gate kept, and each caller gets exactly their own rows;
 * - a source whose gate is inherited or reached through a join gets no table
 *   of its own from the colocated tier, in the harness as in the package; it
 *   is answered by the served copy over the gated source's table, with that
 *   source's gate kept.
 *
 * The data is inline SQL, so the harness and the package read the same rows.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   freshLoadPool,
   type LoadedFixture,
   loadFixture,
   type Outcome,
} from "../../../src/test_helpers/conformance/package_runner";
import {
   buildScenario,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
   type Row,
} from "./harness";

const MODEL = `##! experimental { persistence givens access_modifiers }
given: ORGS :: string[]

source: raw is duckdb.sql("""
  SELECT * FROM (VALUES
    (1, 'acme', 10.0, 9007199254740993, TIMESTAMPTZ '2026-01-03 10:00:00+00', 'x'),
    (2, 'acme', 20.0, 2, TIMESTAMPTZ '2026-01-04 23:30:00+00', 'y'),
    (3, 'globex', 40.0, 3, TIMESTAMPTZ '2026-02-01 00:00:00+00', 'x'),
    (4, 'initech', 80.0, 4, TIMESTAMPTZ '2026-02-14 12:00:00+00', 'z')
  ) AS t(id, org, amt, big, ts, tag)
""")

source: orgs is duckdb.sql("""
  SELECT * FROM (VALUES ('acme', 'east'), ('globex', 'west'), ('initech', 'east'))
    AS t(org, coast)
""") extend { primary_key: org }

#(access_filter) org in $ORGS
#@ persist name="r4p_gated"
source: gated is raw -> { select: id, org, amt } extend {
  measure: total is amt.sum(), n is count()
}

#@ persist name="r4p_inherits"
source: inherits is gated -> { select: id, org, amt } extend {
  measure: total is amt.sum()
}

#@ persist name="r4p_joiner"
source: joiner is orgs -> { select: org, coast } extend {
  join_many: g is gated on g.org = org
  measure: n_orgs is count()
}

#@ persist name="r4p_shaped"
source: shaped is raw -> { select: id, org, amt, big, ts, tag } extend {
  rename: amount is amt
  except: tag
  private dimension: secret is amount * 3
  dimension: doubled is amount * 2
  measure: total is amount.sum(), secret_total is secret.sum()
}
`;

const COLOCATED = ["gated", "inherits", "joiner", "shaped"];

interface Case {
   name: string;
   malloy: string;
   /** The route both the harness and the package answer by. */
   via: "served_copy" | "live";
   /** Every caller's rows, exactly, when the gate decides them. */
   rows?: (givens: Givens) => Row[];
}

type Givens = Record<string, string[]>;

const ACME = { ORGS: ["acme"] };
const GLOBEX_INITECH = { ORGS: ["globex", "initech"] };
const NOBODY = { ORGS: [] as string[] };
/** Interleaved, then repeated, so a graft or cache shared across callers shows. */
const ORDER = [ACME, GLOBEX_INITECH, NOBODY, GLOBEX_INITECH, ACME];

const TOTALS: Record<string, number> = { acme: 30, globex: 40, initech: 80 };

const CASES: Case[] = [
   {
      name: "an entry point under its own row-level gate",
      malloy:
         "run: gated -> { group_by: org; aggregate: total, n; order_by: org }",
      via: "served_copy",
      rows: (g) =>
         g.ORGS.slice()
            .sort()
            .map((org) => ({
               org,
               total: TOTALS[org],
               n: org === "acme" ? 2 : 1,
            })),
   },
   {
      name: "a persisted source over the gated one, its gate inherited",
      malloy:
         "run: inherits -> { group_by: org; aggregate: total; order_by: org }",
      via: "served_copy",
      rows: (g) =>
         g.ORGS.slice()
            .sort()
            .map((org) => ({ org, total: TOTALS[org] })),
   },
   {
      name: "a persisted source joining the gated one",
      malloy:
         "run: joiner -> { group_by: coast; aggregate: n_orgs, g.total; order_by: coast }",
      via: "served_copy",
   },
   {
      name: "rename, except, a private field and a computed dimension",
      malloy:
         "run: shaped -> { group_by: org; aggregate: total, secret_total, d is doubled.sum(); order_by: org }",
      via: "served_copy",
   },
   {
      name: "a big integer and a zoned timestamp, read back",
      malloy: "run: shaped -> { select: id, big, ts, amount; order_by: id }",
      via: "served_copy",
   },
];

const G3_ONLY = {
   PUBLISHER_PERSIST_STORAGE_MODE: "on",
   PUBLISHER_MATERIALIZE_ENABLED: "true",
   PUBLISHER_MATERIALIZE_GATED_SOURCES: "false",
};

/** `v` as a number when it reads as one, so DECIMAL, BIGINT and DOUBLE compare. */
function plain(rows: Row[]): Row[] {
   return rows.map((r) =>
      Object.fromEntries(
         Object.entries(r).map(([k, v]) => [
            k,
            typeof v === "bigint"
               ? String(v)
               : v instanceof Date
                 ? v.toISOString()
                 : v,
         ]),
      ),
   );
}

function answered(o: Outcome): { rows: Row[]; via: string } {
   if ("error" in o) throw new Error(o.error);
   return { rows: o.answer.rows, via: o.answer.via ?? o.answer.servedBy };
}

describe("R4: colocated #@ persist past its own row-level gate, and its column shapes, as a package serves them (gated sources off)", () => {
   let restore: () => void = () => {};
   let r4: R4Fixture;
   let pkg: LoadedFixture;
   const live = new Map<string, Outcome>();
   const key = (kase: Case, givens: object) =>
      `${kase.name} ${JSON.stringify(givens)}`;

   beforeAll(async () => {
      restore = installSettings({ env: G3_ONLY }).restore;
      await freshLoadPool();
      const loader = await loadFixture({ "m.malloy": MODEL });
      for (const kase of CASES) {
         for (const givens of [ACME, GLOBEX_INITECH, NOBODY]) {
            live.set(
               key(kase, givens),
               await loader.run("m.malloy", kase.malloy, givens),
            );
         }
      }
      pkg = await loader.bind();
      r4 = await buildScenario({
         files: { "m.malloy": MODEL },
         seedSql: [],
         colocated: COLOCATED,
      });
   }, 120_000);

   afterAll(async () => {
      await pkg?.dispose();
      await disposeScenarios();
      restore();
      await freshLoadPool();
   });

   it("the harness binds a table only where the package's colocated tier admits one", () => {
      expect(pkg.buildErrors).toEqual({});
      expect(pkg.built.map((b) => `${b.name}:${b.tier}`).sort()).toEqual([
         "gated:colocated",
         "shaped:colocated",
      ]);
      // The package's colocated eligibility refuses the inherited gate and
      // the joined one, so neither is built or bound.
      const eligibility = (
         pkg.pkg.materialization() as unknown as {
            plan: {
               colocatedSourceEligibility?: { refused: Record<string, string> };
            };
         }
      ).plan.colocatedSourceEligibility;
      const refusedNames = Object.values(eligibility?.refused ?? {})
         .map((why) => /Source '([^']+)'/.exec(why)?.[1])
         .sort();
      expect(refusedNames).toEqual(["inherits", "joiner"]);
      expect(
         r4.bindings.map((b) => `${b.sourceName}:${b.tier}`).sort(),
      ).toEqual(["gated:colocated", "shaped:colocated"]);
   });

   for (const kase of CASES) {
      it(`${kase.name}: every caller gets the package's route and rows, and live's`, async () => {
         for (const givens of ORDER) {
            const real = answered(
               await pkg.run("m.malloy", kase.malloy, givens),
            );
            const realLive = answered(live.get(key(kase, givens))!);
            const harness = await r4.run({
               name: `${kase.name} as ${JSON.stringify(givens)}`,
               malloy: kase.malloy,
               givens,
            });
            expect(real.via).toBe(kase.via);
            expect(harness.servedVia ?? "live").toBe(kase.via);
            expect(
               rowDifference(plain(realLive.rows), plain(real.rows)),
            ).toBeUndefined();
            expect(
               rowDifference(plain(harness.live), plain(harness.served)),
            ).toBeUndefined();
            expect(
               rowDifference(plain(real.rows), plain(harness.served)),
            ).toBeUndefined();
            if (kase.rows) {
               expect(plain(harness.served)).toEqual(kase.rows(givens));
            }
         }
      });
   }
});
