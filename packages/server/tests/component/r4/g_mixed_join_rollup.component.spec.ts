// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * g-mixed for rollups (question 14): a gated query source reads an UNGATED
 * joined source's `#(rollup)` rollup through its join (A7), with the
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
 *   declines (`ROW_LEVEL_GATE`) and answers live;
 *
 * In a `storage=` destination the query source is read from its own table
 * there and the rollup from its table beside it. The variant is declared in
 * an overlay of the served copy's own copy of the query source's declaring
 * file, so it keeps the author's `sourceID` and the gate grafts on it. The
 * answer keeps the gate only when every table read is a G3 table: the query
 * source's (here admitted past its own gate) and the rollup's (holding only
 * ungated rows); either unmarked, it answers live.
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
   type Row,
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

/** The query sources, stored or not, by placement. */
function model(storage: boolean): string {
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
  #(rollup) grain=["flight_id"]${S}
  #(rollup) grain=["flight_id", "kind"]${S}
  measure: seg_miles is miles.sum()
}

source: crew is duckdb.table('crew_raw') extend {
  primary_key: flight_id
  #(rollup) grain=["flight_id"]${S}
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

// A gate over a computed dimension: never answered from the rollup.
#(access_filter) lower_carrier = $CARRIER
${stored}source: computed_flights is duckdb.sql("SELECT id, carrier, distance FROM flights_raw") extend {
  primary_key: id
  join_many: segments on segments.flight_id = id
  dimension: lower_carrier is lower(carrier)
  measure: flight_count is count()
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

      /**
       * One query live (no bindings, so no stored table), then served (every
       * binding): each run on its own, so a denial is reported on the leg
       * that gave it.
       */
      type Both =
         | {
              run: { live: Row[]; served: Row[]; servedBy?: { kind: string } };
              live?: undefined;
              served?: undefined;
           }
         | { run?: undefined; live: string; served: string };
      const both = async (malloy: string, givens: Givens): Promise<Both> => {
         type Attempt =
            | Awaited<ReturnType<R4Fixture["runAsIs"]>>
            | { error: string };
         const attempt = async (served: boolean): Promise<Attempt> => {
            fixture.setBindings(served ? fixture.bindings : []);
            try {
               return await fixture.runAsIs(malloy, givens);
            } catch (error) {
               return {
                  error: error instanceof Error ? error.message : String(error),
               };
            }
         };
         const live = await attempt(false);
         const served = await attempt(true);
         if ("error" in live || "error" in served) {
            return {
               live: "error" in live ? live.error : "answered",
               served: "error" in served ? served.error : "answered",
            };
         }
         return {
            run: {
               live: live.rows,
               served: served.rows,
               servedBy: served.path?.servedBy,
            },
         };
      };

      beforeAll(async () => {
         restore = installSettings({ env: G3_ONLY }).restore;
         fixture = await buildScenario({
            files: { "m.malloy": model(storage) },
            seedSql,
            preaggregate: true,
            rollupKeyChecks: true,
            ...(storage ? { materialize: true } : {}),
         });
         // As the bind marks them: a query source's `#(materialize)` table
         // under its own row-level gate is admitted past it (G3), and a
         // rollup over an ungated base holds only ungated rows.
         for (const b of fixture.bindings) {
            if (b.tier !== "storage") continue;
            if (b.kind === "rollup") b.holdsOnlyUngatedRows = true;
            else b.ownGateKept = true;
         }
      }, 240_000);
      afterAll(async () => {
         restore();
         await disposeScenarios();
      });

      for (const kase of CASES) {
         it(`${kase.name}: every caller gets live's answer${kase.rollup ? ", from the rollup" : ", live"}`, async () => {
            for (const givens of [...kase.callers, ...kase.callers]) {
               const out = await both(kase.malloy, givens);
               // Every caller here is admitted: a denial is a defect.
               if (out.run === undefined) {
                  throw new Error(`live: ${out.live}; served: ${out.served}`);
               }
               const { run } = out;
               expect(rowDifference(run.live, run.served)).toBeUndefined();
               const fromRollup = kase.rollup;
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
         for (const [malloy, givens] of [
            ["run: flights -> { aggregate: segments.seg_miles }", NOBODY],
            ["run: locked_flights -> { aggregate: segments.seg_miles }", BB],
         ] as const) {
            const out = await both(malloy, givens);
            expect(out.run).toBeUndefined();
            expect(out.served).toBe(out.live);
         }
      });

      it("a caller's all() over the joined measure is the caller's total, never the global one", async () => {
         const malloy =
            "run: flights -> { group_by: carrier; aggregate: segments.seg_miles, t is all(segments.seg_miles) }";
         for (const givens of [AA, BB]) {
            const out = await both(malloy, givens);
            if (out.run === undefined) {
               throw new Error(`live: ${out.live}; served: ${out.served}`);
            }
            expect(rowDifference(out.run.live, out.run.served)).toBeUndefined();
         }
         const aa = await both(malloy, AA);
         if (aa.run === undefined) throw new Error(aa.live);
         // AA's flights 1 and 2 have 2 + 3 + 4 + 1.5 miles.
         expect(aa.run.served.map((r) => r.t)).toEqual([10.5]);
      });

      it("each answer holds only the caller's parents", async () => {
         const out = await both(
            "run: flights -> { group_by: carrier; aggregate: segments.seg_miles }",
            BB,
         );
         if (out.run === undefined) {
            throw new Error(`live: ${out.live}; served: ${out.served}`);
         }
         expect(out.run.servedBy?.kind).toBe("rollup");
         expect(out.run.served.map((r) => r.carrier)).toEqual(["BB"]);
      });

      if (storage) {
         for (const [what, unmark] of [
            [
               "the query source's own table there",
               (b: R4Fixture["bindings"][number]) => {
                  if (b.kind !== "rollup") b.ownGateKept = undefined;
               },
            ],
            [
               "the rollup's table",
               (b: R4Fixture["bindings"][number]) => {
                  if (b.kind === "rollup") b.holdsOnlyUngatedRows = undefined;
               },
            ],
         ] as const) {
            it(`answers live, equal to live, when ${what} is not a G3 table`, async () => {
               const saved = fixture.bindings.map((b) => ({ ...b }));
               for (const b of fixture.bindings) unmark(b);
               try {
                  for (const givens of [AA, BB]) {
                     const out = await both(
                        "run: flights -> { group_by: carrier; aggregate: segments.seg_miles }",
                        givens,
                     );
                     if (out.run === undefined) {
                        throw new Error(
                           `live: ${out.live}; served: ${out.served}`,
                        );
                     }
                     expect(out.run.servedBy?.kind).not.toBe("rollup");
                     expect(
                        rowDifference(out.run.live, out.run.served),
                     ).toBeUndefined();
                  }
               } finally {
                  fixture.bindings.forEach((b, i) =>
                     Object.assign(b, saved[i]),
                  );
               }
            });
         }
      }
   });
}
