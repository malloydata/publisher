// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Adversarial R4 for upper-bound rolling windows (materialization v2; plan,
 * Rolling windows, question 2, option A): the shapes where leaving the term's
 * text out of the build copy could reach the wrong text, or reach more than
 * the source's own rows. Each source is read at a pinned clock *b* and later
 * with no rebuild, served equal to live, or answered live where it is refused.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
} from "./harness";

const B = "2026-11-10 00:00:00";
// b, mid-interval, 20 days on, and past every two-sided window's rows. US
// daylight saving time ends between b - 30 days and b.
const READS = [
   "2026-11-10 00:00:00",
   "2026-11-10 12:30:00",
   "2026-11-30 00:00:00",
   "2027-02-08 00:00:00",
];

const seedSql = [
   `CREATE TABLE events AS
      SELECT ts, row_number() OVER (ORDER BY ts) AS amount,
             CASE WHEN hour(ts) % 2 = 0 THEN 'acme' ELSE 'globex' END AS org
      FROM range(TIMESTAMP '${B}' - INTERVAL 120 DAY, TIMESTAMP '${B}' + INTERVAL 2 DAY, INTERVAL 1 HOUR) t(ts)`,
];

const model = (upper: number) => `##! experimental.givens
given: GROUPS :: string[] is ['acme']

source: events is duckdb.table('events')

source: ev is events extend { primary_key: amount; where: ts < now - ${upper} days }

// An inlined join to the base the upper bound is inherited from: the build
// copy would take the term out of the join as well.
#(materialize) storage="lake" inline=["nxt"]
source: joined is ev extend {
  dimension: next_key is amount + 1
  join_one: nxt is ev with next_key
  dimension: nxt_ts is nxt.ts
  measure: n is count(), with_next is count(nxt.amount)
}

#(materialize) storage="lake"
source: older is events extend {
  where: ts < now - ${upper} days
  measure: n is count()
}

#(materialize) storage="lake"
source: between is events extend {
  where: ts >= now - 60 days and ts < now - ${upper} days
  measure: n is count()
}

#(materialize) storage="lake"
source: twice is ev extend {
  where: ts < now - ${upper} days
  measure: n is count()
}

#(materialize) storage="lake"
source: decoy is events extend {
  // where: ts < now - ${upper} days
  dimension: label is 'ts < now - ${upper} days'
  where: ts < now - ${upper} days
  measure: n is count()
}

#(materialize) storage="lake"
source: multi_line is events extend {
  where:
    ts >= now - 60 days
    and ts < now - ${upper} days
  measure: n is count()
}

#(materialize) storage="lake"
source: mirrored is events extend {
  where: now - 60 days < ts and now - ${upper} days >= ts
  measure: n is count()
}

#(materialize) storage="lake"
source: ranged is events extend {
  where: ts ? now - 60 days to now - ${upper} days
  measure: n is count()
}

#(materialize) storage="lake"
source: fixed_conjunct is events extend {
  where: ts < now - ${upper} days and org = 'acme'
  measure: n is count()
}

#(materialize) storage="lake"
source: mixed_range is events extend {
  where: ts ? now - 60 days to @2026-10-20
  measure: n is count()
}

source: ev_renamed is ev
#(materialize) storage="lake"
source: renamed is ev_renamed extend { measure: n is count() }

source: ev_low is events extend { where: ts >= now - 60 days }
#(materialize) storage="lake"
source: own_upper is ev_low extend { where: ts < now - ${upper} days; measure: n is count() }
#(materialize) storage="lake"
source: own_lower is ev extend { where: ts >= now - 60 days; measure: n is count() }

#(materialize) storage="lake"
source: ext1 is events extend { where: ts < now - ${upper} days; measure: n is count() }
#(materialize) storage="lake"
source: ext2 is ext1 extend { where: ts >= now - 60 days; dimension: big is amount > 1000 }
#(materialize) storage="lake"
source: ext3 is ext2 extend { where: org = 'acme' }

#(materialize) storage="lake"
source: with_given is events extend {
  where: org in $GROUPS
  where: ts >= now - 60 days and ts < now - ${upper} days
  measure: n is count()
}

#(materialize) storage="lake"
source: chained is events extend { where: ts < now - ${upper} days } extend {
  measure: n is count()
}

// A parent built with a lower bound, chained under a child that adds an
// upper bound its own build leaves out.
#(materialize) storage="lake"
source: low_parent is events extend { where: ts >= now - 60 days; measure: n is count() }
#(materialize) storage="lake"
source: upper_child is low_parent extend { where: ts < now - ${upper} days }

#(materialize) storage="lake"
source: zoned is events extend {
  timezone: 'America/New_York'
  where: ts ? now - 60 days to now - ${upper} days
  measure: n is count()
}
`;

/** Built and served from storage, by source; false: refused, answers live. */
const SOURCES: Record<string, boolean> = {
   joined: false,
   older: true,
   between: true,
   twice: true,
   decoy: true,
   multi_line: true,
   mirrored: true,
   ranged: true,
   fixed_conjunct: false,
   mixed_range: false,
   renamed: true,
   own_upper: true,
   own_lower: true,
   ext1: true,
   ext2: true,
   ext3: true,
   with_given: true,
   zoned: true,
   chained: true,
   low_parent: true,
   upper_child: true,
};

describe("R4: upper-bound windows in adversarial shapes answer equal to live", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   let edited: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": model(30) },
         seedSql,
         materialize: true,
         clock: B,
      });
      // The same package with every upper bound's offset edited.
      edited = await buildScenario({
         files: { "m.malloy": model(20) },
         seedSql,
         materialize: true,
         clock: B,
      });
   }, 120_000);
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("builds exactly the sources it can serve", () => {
      const built = Object.entries(SOURCES)
         .filter(([, b]) => b)
         .map(([s]) => s)
         .sort();
      expect(
         Object.keys(fixture.tableIds)
            .filter((s) => s in SOURCES)
            .sort(),
      ).toEqual(built);
   });

   for (const [source, served] of Object.entries(SOURCES)) {
      for (const at of READS) {
         it(`${source}, read at ${at}`, async () => {
            fixture.setClock(at);
            const queries = [
               `run: ${source} -> { aggregate: n }`,
               `run: ${source} -> { group_by: org; aggregate: c is count() }`,
            ];
            if (source === "joined") {
               queries.push(
                  `run: joined -> { aggregate: with_next }`,
                  `run: joined -> { group_by: has_next is nxt_ts is not null; aggregate: c is count() }`,
               );
            }
            for (const malloy of queries) {
               const run = await fixture.run({
                  name: source,
                  malloy,
                  givens: source === "with_given" ? { GROUPS: ["acme"] } : {},
                  expectLive: !served,
               });
               if (served) expect(run.servedFrom).toBe("storage");
               expect(rowDifference(run.live, run.served)).toBeUndefined();
            }
         });
      }
   }

   it("keeps the address when only an upper bound's offset is edited, and reads the new offset", async () => {
      for (const source of ["older", "between", "renamed", "multi_line"]) {
         expect(edited.tableIds[source]).toBe(fixture.tableIds[source]);
      }
      for (const at of READS.slice(0, 3)) {
         edited.setClock(at);
         fixture.setClock(at);
         for (const source of ["older", "between"]) {
            const malloy = `run: ${source} -> { aggregate: n }`;
            const before = await fixture.run({ name: source, malloy });
            const after = await edited.run({ name: source, malloy });
            expect(after.servedFrom).toBe("storage");
            expect(rowDifference(after.live, after.served)).toBeUndefined();
            // Twenty days' more rows are old enough under the edited offset.
            expect(Number(after.served[0].n)).toBeGreaterThan(
               Number(before.served[0].n),
            );
         }
      }
   });
});
