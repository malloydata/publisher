// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * g-mixed for rollups (question 14): a gated query source reads an UNGATED
 * joined source's `#(preaggregate)` rollup through its join (A7), with the
 * gated-sources switch off, on the database tier and in a `storage=`
 * destination alike.
 *
 * The branch answers past the gate only when the gate reads the query
 * source's own plain columns (none through a join), the joined base is
 * ungated, the author's gate notes survive onto the variant (the same gate
 * set `govern()` re-proves), and, in a storage destination, the query
 * source's own table there is itself a G3 table. The gate is grafted on the
 * variant's query source, whose rows are the author's, so it filters the
 * parents exactly as live does before the re-pointed join reads the rollup.
 *
 * Pinned, against live, for every caller (two allowed carriers, one with no
 * rows, a missing given, a lock that refuses):
 *
 * - a rollup at the join key (re-pointed as a join_one) and one at the key
 *   and a joined dimension (re-pointed as a join_many);
 * - a parent with no joined rows;
 * - a caller `all()` over the joined measure, never the global total;
 * - a gate that reads through the re-pointed join, or a computed dimension,
 *   declines (`CONTRACT`) and answers live;
 *
 * In a `storage=` destination the branch declines past a gate (`CONTRACT`)
 * and the query answers live: its variant is declared over the served
 * copy's files, not in an overlay of the query source's declaring file, so
 * the gate would not land on it. Every caller still gets live's answer.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import type { GivenValue } from "@malloydata/malloy";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   DESTINATION,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
} from "./harness";

const seedSql = [
   `CREATE TABLE flights_raw AS SELECT * FROM (VALUES
      (1, 'AA', 100), (2, 'AA', 200), (3, 'BB', 300), (4, 'CC', 400), (5, 'BB', 500)
    ) AS t(id, carrier, distance)`,
   // Flight 4 has no segments; flight 5's one segment has no miles; segment
   // 17 belongs to no flight.
   `CREATE TABLE segs_raw AS SELECT * FROM (VALUES
      (10, 1, 'p', 2.0), (11, 1, 'q', 3.0), (12, 2, 'p', 4.0), (13, 2, 'p', 1.5),
      (14, 3, 'q', 1.0), (15, 5, 'p', NULL), (16, 3, 'p', 7.0), (17, 9, 'p', 50.0)
    ) AS t(seg_id, flight_id, kind, miles)`,
   `CREATE TABLE crew_raw AS SELECT * FROM (VALUES
      (1, 3, 'AA'), (3, 2, 'BB'), (5, 4, 'BB')
    ) AS t(flight_id, size, base)`,
];

/**
 * The query sources, stored or not, by placement. The computed-dimension
 * gate is a model of its own: with all four gated sources in one model, live
 * itself denies `flights` (a row-level graft that does not land), which is
 * not what these specs pin.
 */
function model(storage: boolean, computed = false): string {
   const S = storage ? ` storage="${DESTINATION}"` : "";
   // In a destination the query source is read from its own table there;
   // on the database tier, live from its warehouse.
   const stored = storage ? `#(materialize)${S}\n` : "";
   return `##! experimental { givens }
given: CARRIER :: string
given: ROLE :: string
given: BASE :: string

source: segments is duckdb.table('segs_raw') extend {
  primary_key: seg_id
  #(preaggregate) grain=["flight_id"]${S}
  #(preaggregate) grain=["flight_id", "kind"]${S}
  measure: seg_miles is miles.sum()
}

source: crew is duckdb.table('crew_raw') extend {
  primary_key: flight_id
  #(preaggregate) grain=["flight_id"]${S}
  measure: crew_size is size.sum()
}

#(access_filter) carrier = $CARRIER
${stored}source: flights is duckdb.table('flights_raw') extend {
  primary_key: id
  join_many: segments on segments.flight_id = id
  join_one: crew with id
  measure: flight_count is count()
}

#(authorize) $ROLE = 'admin'
#(access_filter) carrier = $CARRIER
${stored}source: locked_flights is duckdb.sql("SELECT * FROM flights_raw") extend {
  primary_key: id
  join_many: segments on segments.flight_id = id
  measure: flight_count is count()
}

// A gate through the re-pointed join: never answered from the rollup.
#(access_filter) crew.base = $BASE
${stored}source: by_crew_base is duckdb.sql("SELECT id, carrier FROM flights_raw") extend {
  primary_key: id
  join_one: crew with id
  measure: flight_count is count()
}

${
   computed
      ? `// A gate over a computed dimension: never answered from the rollup.
#(access_filter) lower_carrier = $CARRIER
${stored}source: computed_flights is duckdb.sql("SELECT id, carrier, distance FROM flights_raw") extend {
  primary_key: id
  join_many: segments on segments.flight_id = id
  dimension: lower_carrier is lower(carrier)
  measure: flight_count is count()
}`
      : ""
}
`;
}

type Givens = Record<string, GivenValue>;

const AA: Givens = { CARRIER: "AA", ROLE: "admin", BASE: "AA" };
const BB: Givens = { CARRIER: "BB", ROLE: "viewer", BASE: "BB" };
const NOBODY_ROWS: Givens = { CARRIER: "ZZ", ROLE: "admin", BASE: "ZZ" };
const NOBODY: Givens = {};

interface Case {
   name: string;
   malloy: string;
   /** Answered from the rollup (default), or live. */
   rollup: boolean;
   /** Run against the computed-dimension model. */
   computed?: true;
   callers: Givens[];
}

const ALLOWED = [AA, BB, NOBODY_ROWS];
// The gate admits no row for NOBODY_ROWS: an empty answer, never a denial.

const CASES: Case[] = [
   {
      name: "a join_one re-pointed at the rollup at the join key",
      malloy:
         "run: flights -> { group_by: carrier; aggregate: crew.crew_size, flight_count }",
      rollup: true,
      callers: ALLOWED,
   },
   {
      name: "a join_many at the key, a parent with no joined rows included",
      malloy:
         "run: flights -> { group_by: carrier; aggregate: segments.seg_miles, flight_count }",
      rollup: true,
      callers: ALLOWED,
   },
   {
      name: "a rollup at the key and a joined dimension, re-pointed as a join_many",
      malloy:
         "run: flights -> { group_by: segments.kind; aggregate: segments.seg_miles, flight_count }",
      rollup: true,
      callers: ALLOWED,
   },
   {
      name: "a grand total",
      malloy: "run: flights -> { aggregate: segments.seg_miles, flight_count }",
      rollup: true,
      callers: ALLOWED,
   },
   {
      name: "a lock with a row filter",
      malloy:
         "run: locked_flights -> { group_by: carrier; aggregate: segments.seg_miles }",
      rollup: true,
      callers: [AA, NOBODY_ROWS],
   },
   {
      name: "a gate through the re-pointed join",
      malloy:
         "run: by_crew_base -> { group_by: carrier; aggregate: crew.crew_size, flight_count }",
      rollup: false,
      callers: [AA, BB, NOBODY_ROWS],
   },
   {
      name: "a gate over a computed dimension",
      malloy:
         "run: computed_flights -> { group_by: carrier; aggregate: segments.seg_miles }",
      rollup: false,
      computed: true,
      callers: [{ CARRIER: "aa" }, { CARRIER: "bb" }],
   },
];

const G3_ONLY = {
   PERSIST_STORAGE_MODE: "on",
   PUBLISHER_MATERIALIZE: "true",
   PUBLISHER_MATERIALIZE_GATED_SOURCES: "false",
};

for (const storage of [false, true]) {
   const tier = storage ? "storage=" : "database";
   describe(`g-mixed for rollups (${tier}): a gated query source reads an ungated joined source's rollup`, () => {
      let restore: () => void;
      let fixture: R4Fixture;
      let main: R4Fixture;
      let computed: R4Fixture;

      /** Live, then served; a denial is compared as its message. */
      const both = async (malloy: string, givens: Givens) => {
         try {
            const run = await fixture.run({ name: malloy, malloy, givens });
            return { run };
         } catch (error) {
            const live = error instanceof Error ? error.message : String(error);
            let served: string;
            try {
               fixture.setBindings(fixture.bindings);
               await fixture.runAsIs(malloy, givens);
               served = "answered";
            } catch (e) {
               served = e instanceof Error ? e.message : String(e);
            }
            return { live, served };
         }
      };

      beforeAll(async () => {
         restore = installSettings({ env: G3_ONLY }).restore;
         const build = async (withComputed: boolean) => {
            const built = await buildScenario({
               files: { "m.malloy": model(storage, withComputed) },
               seedSql,
               preaggregate: true,
               rollupKeyChecks: true,
               ...(storage ? { materialize: true } : {}),
            });
            // As the bind marks them: a query source's `#(materialize)`
            // table under its own row-level gate is admitted past it (G3),
            // and a rollup over an ungated base holds only ungated rows.
            for (const b of built.bindings) {
               if (b.kind === "rollup") b.holdsOnlyUngatedRows = true;
               else if (b.tier === "storage") b.ownGateKept = true;
            }
            return built;
         };
         main = await build(false);
         computed = await build(true);
         fixture = main;
      }, 240_000);
      afterAll(async () => {
         restore();
         await disposeScenarios();
      });

      for (const kase of CASES) {
         it(`${kase.name}: every caller gets live's answer${kase.rollup && !storage ? ", from the rollup" : ", live"}`, async () => {
            fixture = kase.computed ? computed : main;
            for (const givens of [...kase.callers, ...kase.callers]) {
               const out = await both(kase.malloy, givens);
               // Every caller here is admitted: a denial is a defect.
               if (!("run" in out)) throw new Error(out.live);
               const { run } = out;
               expect(rowDifference(run.live, run.served)).toBeUndefined();
               const fromRollup = kase.rollup && !storage;
               expect([kase.name, run.servedBy?.kind]).toEqual([
                  kase.name,
                  fromRollup ? "rollup" : run.servedBy?.kind,
               ]);
               if (!fromRollup) {
                  expect(run.servedBy?.kind).not.toBe("rollup");
               }
            }
         });
      }

      it("denies a caller with no givens, and one a lock refuses, exactly as live", async () => {
         fixture = main;
         for (const [malloy, givens] of [
            ["run: flights -> { aggregate: segments.seg_miles }", NOBODY],
            ["run: locked_flights -> { aggregate: segments.seg_miles }", BB],
         ] as const) {
            const out = await both(malloy, givens);
            expect("live" in out).toBe(true);
            if ("live" in out) expect(out.served).toBe(out.live);
         }
      });

      it("a caller's all() over the joined measure is the caller's total, never the global one", async () => {
         fixture = main;
         const malloy =
            "run: flights -> { group_by: carrier; aggregate: segments.seg_miles, t is all(segments.seg_miles) }";
         for (const givens of [AA, BB]) {
            const out = await both(malloy, givens);
            if (!("run" in out)) throw new Error(out.live);
            expect(rowDifference(out.run.live, out.run.served)).toBeUndefined();
         }
         const aa = await both(malloy, AA);
         if (!("run" in aa)) throw new Error(aa.live);
         // AA's flights 1 and 2 have 2 + 3 + 4 + 1.5 miles.
         expect(aa.run.served.map((r) => r.t)).toEqual([10.5]);
      });

      it("each answer holds only the caller's parents", async () => {
         fixture = main;
         const out = await both(
            "run: flights -> { group_by: carrier; aggregate: segments.seg_miles }",
            BB,
         );
         if (!("run" in out)) throw new Error(out.live);
         expect(out.run.servedBy?.kind === "rollup").toBe(!storage);
         expect(out.run.served.map((r) => r.carrier)).toEqual(["BB"]);
      });
   });
}
