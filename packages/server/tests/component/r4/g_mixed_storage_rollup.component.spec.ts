// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The g-mixed `storage=` rollup cell (question 14), served against live: a
 * gated query source, stored in a `storage=` destination, reads an ungated
 * joined source's rollup in the same destination through its join (A7).
 *
 * The variant is declared in an overlay of the served copy's own copy of the
 * query source's declaring file, so its run target keeps the author's
 * `sourceID` and `govern()` grafts the gate on it. The answer keeps the gate
 * only when every table it reads is a G3 table, each holding the gate's
 * columns exactly (#1855's rules).
 *
 * Every case is run live, then served, for every caller (two allowed
 * carriers, interleaved and repeated; one with no rows; one a lock refuses;
 * one with no givens), with the gated-sources switch off and on, in six
 * zones, each as the query's `timezone:` and as the query source's own:
 * served must equal live, rows and denial alike. A query zone is read
 * through the join only when nothing it reads there was computed in the
 * build's zone (A5); a gate over a zoned day, and a query source with its
 * own `timezone:`, answer live.
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
      (1, 'AA', 100, TIMESTAMP '2026-01-04 04:00:00'),
      (2, 'AA', 200, TIMESTAMP '2026-01-04 20:00:00'),
      (3, 'BB', 300, TIMESTAMP '2026-01-05 06:00:00'),
      (4, 'CC', 400, TIMESTAMP '2026-01-05 23:30:00'),
      (5, 'BB', 500, TIMESTAMP '2026-01-06 12:00:00')
    ) AS t(id, carrier, distance, dep_ts)`,
   // Flight 4 has no segments; flight 5's one segment has no miles; segment
   // 17 belongs to no flight.
   `CREATE TABLE segs_raw AS SELECT * FROM (VALUES
      (10, 1, 'p', 2.0), (11, 1, 'q', 3.0), (12, 2, 'p', 4.0), (13, 2, 'p', 1.5),
      (14, 3, 'q', 1.0), (15, 5, 'p', NULL), (16, 3, 'p', 7.0), (17, 9, 'p', 50.0)
    ) AS t(seg_id, flight_id, kind, miles)`,
];

const ZONES = [
   "UTC",
   "America/New_York",
   "Asia/Kolkata",
   "Pacific/Chatham",
   "Pacific/Kiritimati",
   "America/Adak",
];

const S = ` storage="${DESTINATION}"`;

const MODEL = `##! experimental { givens }
given: CARRIER :: string
given: ROLE :: string
given: ON_DAY :: timestamp

source: segments is duckdb.table('segs_raw') extend {
  primary_key: seg_id
  #(rollup) grain=["flight_id"]${S}
  #(rollup) grain=["flight_id", "kind"]${S}
  measure: seg_miles is miles.sum()
}

#(access_filter) carrier = $CARRIER
#(materialize)${S}
source: flights is duckdb.table('flights_raw') extend {
  primary_key: id
  join_many: segments on segments.flight_id = id
  measure: flight_count is count()
}

#(authorize) $ROLE = 'admin'
#(access_filter) carrier = $CARRIER
#(materialize)${S}
source: locked_flights is duckdb.sql("SELECT id, carrier, distance, dep_ts FROM flights_raw") extend {
  primary_key: id
  join_many: segments on segments.flight_id = id
  measure: flight_count is count()
}

// A gate over a day: a computed dimension, so the join branch declines
// (ROW_LEVEL_GATE) and every zone answers live.
#(access_filter) dep_day = $ON_DAY
#(materialize)${S}
source: by_day is duckdb.sql("SELECT id, carrier, dep_ts FROM flights_raw") extend {
  primary_key: id
  join_many: segments on segments.flight_id = id
  dimension: dep_day is dep_ts.day
  measure: flight_count is count()
}
${ZONES.map(
   (z, i) => `
source: flights_z${i} is flights extend { timezone: '${z}' }`,
).join("")}
`;

type Givens = Record<string, GivenValue>;

const CALLERS: Record<string, Givens> = {
   aa: { CARRIER: "AA", ROLE: "admin", ON_DAY: "2026-01-04 00:00:00" },
   bb: { CARRIER: "BB", ROLE: "admin", ON_DAY: "2026-01-05 00:00:00" },
   none: { CARRIER: "ZZ", ROLE: "admin", ON_DAY: "2026-02-01 00:00:00" },
   locked: { CARRIER: "AA", ROLE: "viewer", ON_DAY: "2026-01-04 00:00:00" },
   missing: {},
};
/** Interleaved, then repeated, so a graft or cache shared across callers shows. */
const ORDER = ["aa", "bb", "none", "locked", "missing", "bb", "aa", "missing"];

interface Case {
   name: string;
   malloy: string;
   /** Served from the rollup for an admitted caller (default: live). */
   rollup?: true;
}

const BASE: Case[] = [
   {
      name: "a join_many at the key",
      malloy:
         "run: flights -> { group_by: carrier; aggregate: segments.seg_miles, flight_count }",
      rollup: true,
   },
   {
      name: "the key and a joined dimension",
      malloy:
         "run: flights -> { group_by: segments.kind; aggregate: segments.seg_miles, flight_count }",
      rollup: true,
   },
   {
      name: "a grand total",
      malloy: "run: flights -> { aggregate: segments.seg_miles }",
      rollup: true,
   },
   {
      // The rollup at the join key has no fan-out dimension, so an
      // ungrouped aggregate reads it; the gate filters the parents first,
      // so `all()` is the caller's own total, never the global one.
      name: "a caller all()",
      malloy:
         "run: flights -> { group_by: carrier; aggregate: segments.seg_miles, t is all(segments.seg_miles) }",
      rollup: true,
   },
   {
      name: "a lock with a row filter",
      malloy:
         "run: locked_flights -> { group_by: carrier; aggregate: segments.seg_miles }",
      rollup: true,
   },
   {
      name: "a computed day gate",
      malloy:
         "run: by_day -> { group_by: carrier; aggregate: segments.seg_miles }",
   },
];

const CASES: Case[] = [
   ...BASE,
   ...ZONES.flatMap((z, i): Case[] => [
      {
         // Nothing read through the join, nor the grain it meets, was
         // computed in the build's zone (A5), so the rollup answers.
         name: `the query's timezone: ${z}`,
         malloy: `run: flights -> { timezone: '${z}'; group_by: carrier; aggregate: segments.seg_miles }`,
         rollup: true,
      },
      {
         name: `a day gate read in ${z}`,
         malloy: `run: by_day -> { timezone: '${z}'; group_by: carrier; aggregate: segments.seg_miles }`,
      },
      {
         name: `the query source's own timezone: ${z}`,
         malloy: `run: flights_z${i} -> { group_by: carrier; aggregate: segments.seg_miles }`,
      },
   ]),
];

type Attempt = Awaited<ReturnType<R4Fixture["runAsIs"]>> | { error: string };

for (const gatedSources of [false, true]) {
   describe(`g-mixed storage= rollup, served against live (gated sources ${gatedSources ? "on" : "off"})`, () => {
      let restore: () => void;
      let fixture: R4Fixture;
      const diffs: string[] = [];
      const fromRollup = new Map<string, number>();

      const attempt = async (
         malloy: string,
         givens: Givens,
         served: boolean,
      ): Promise<Attempt> => {
         fixture.setBindings(served ? fixture.bindings : []);
         try {
            return await fixture.runAsIs(malloy, givens);
         } catch (error) {
            return {
               error: error instanceof Error ? error.message : String(error),
            };
         }
      };

      beforeAll(async () => {
         restore = installSettings({
            env: {
               PERSIST_STORAGE_MODE: "on",
               PUBLISHER_MATERIALIZE: "true",
               PUBLISHER_MATERIALIZE_GATED_SOURCES: String(gatedSources),
            },
         }).restore;
         fixture = await buildScenario({
            files: { "m.malloy": MODEL },
            seedSql,
            preaggregate: true,
            rollupKeyChecks: true,
            materialize: true,
         });
         // As the bind marks them: a query source's `#(materialize)` table
         // under its own row-level gate is admitted past it (G3), and a
         // rollup over an ungated base holds only ungated rows.
         for (const b of fixture.bindings) {
            if (b.tier !== "storage") continue;
            if (b.kind === "rollup") b.holdsOnlyUngatedRows = true;
            else b.ownGateKept = true;
         }
         for (const kase of CASES) {
            for (const who of ORDER) {
               const givens = CALLERS[who];
               const live = await attempt(kase.malloy, givens, false);
               const served = await attempt(kase.malloy, givens, true);
               const tag = `${kase.name} [${who}]`;
               if ("error" in live || "error" in served) {
                  const l = "error" in live ? live.error : "answered";
                  const s = "error" in served ? served.error : "answered";
                  if (l !== s) diffs.push(`${tag}: live ${l} | served ${s}`);
                  continue;
               }
               const d = rowDifference(
                  live.rows as Row[],
                  served.rows as Row[],
               );
               if (d) diffs.push(`${tag}: ${d}`);
               if (served.path?.servedBy?.kind === "rollup") {
                  fromRollup.set(
                     kase.name,
                     (fromRollup.get(kase.name) ?? 0) + 1,
                  );
               }
            }
         }
      }, 600_000);
      afterAll(async () => {
         restore();
         await disposeScenarios();
      });

      it("every caller, in every zone, gets live's answer: no difference", () => {
         expect(diffs).toEqual([]);
      });

      it("answers from the rollup where the branch admits the gate, and never for a zoned gate or a zoned source", () => {
         // The callers admitted with rows or none: aa, bb (twice each), none
         // (once). Every case at once, so one mismatch hides no other.
         expect(
            CASES.map((kase) => [
               kase.name,
               (fromRollup.get(kase.name) ?? 0) > 0,
            ]),
         ).toEqual(CASES.map((kase) => [kase.name, kase.rollup === true]));
      });
   });
}
