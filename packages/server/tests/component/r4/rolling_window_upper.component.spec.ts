// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for upper-bound and two-sided rolling windows on the served copy
 * (materialization v2; plan, Rolling windows, question 2 decided 2026-11-12,
 * option A): with no refresh interval the widest set any read needs is every
 * row that exists at build time, so a `where:` keeping rows older than a
 * moving cut-off is left out of the build and re-applied at read, own or
 * inherited. A two-sided window builds its lower bound as written and leaves
 * its upper bound out. The table is built at a pinned clock *b* and read at
 * *b*, *b* + interval / 2, just before *b* + interval, and well past any
 * interval with no rebuild, each read equal to live over the same data.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
} from "./harness";

const B = "2026-11-10 00:00:00"; // build start
// A one-day interval's three points, then 20 and 90 days on with no rebuild.
const READS = [
   "2026-11-10 00:00:00",
   "2026-11-10 12:00:00",
   "2026-11-10 23:59:59",
   "2026-11-30 00:00:00",
   "2027-02-08 00:00:00",
];

const seedSql = [
   // One event an hour, from b - 120 days to after b + 1 day, two orgs.
   `CREATE TABLE events AS
      SELECT ts, row_number() OVER (ORDER BY ts) AS amount,
             CASE WHEN hour(ts) % 2 = 0 THEN 'acme' ELSE 'globex' END AS org
      FROM range(TIMESTAMP '${B}' - INTERVAL 120 DAY, TIMESTAMP '${B}' + INTERVAL 2 DAY, INTERVAL 1 HOUR) t(ts)`,
];

const model = `##! experimental.givens
given: GROUPS :: string[]

source: events is duckdb.table('events')

#(materialize) storage="lake"
source: older is events extend {
  where: ts < now - 30 days
  measure: n is count(), total is amount.sum()
}

source: older_base is events extend {
  where: now - 30 days > ts
}

#(materialize) storage="lake"
source: older_inherited is older_base extend {
  measure: n is count(), total is amount.sum()
}

#(materialize) storage="lake"
source: between is events extend {
  where: ts >= now - 60 days and ts < now - 30 days
  measure: n is count(), total is amount.sum()
}

source: between_base is events extend {
  where: ts >= now - 60 days
  where: ts < now - 30 days
}

#(materialize) storage="lake"
source: between_inherited is between_base extend {
  measure: n is count(), total is amount.sum()
}

#(access_filter) org in $GROUPS
#(materialize) storage="lake"
source: older_gated is events extend {
  where: ts < now - 30 days
  measure: n is count(), total is amount.sum()
}
`;

describe("R4: an upper-bound or two-sided window served at a pinned clock answers equal to live", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: {
            PERSIST_STORAGE_MODE: "on",
            PUBLISHER_MATERIALIZE: "true",
            PUBLISHER_MATERIALIZE_GATED_SOURCES: "true",
         },
      }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": model },
         seedSql,
         materialize: true,
         clock: B,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("stores every row there is for an upper bound, own or inherited", async () => {
      for (const name of ["older", "older_inherited", "older_gated"]) {
         const [stored] = await fixture.storedSQL(
            (t) => `SELECT count(*) AS n FROM ${t(name)}`,
         );
         // b - 120 days through b + 2 days, one row an hour: not trimmed to
         // b - 30 days, which a read at a later clock would come up short of.
         expect(Number(stored.n)).toBe(122 * 24);
      }
   });

   it("stores a two-sided window from its lower bound at b", async () => {
      for (const name of ["between", "between_inherited"]) {
         const [stored] = await fixture.storedSQL(
            (t) => `SELECT count(*) AS n, min(ts) AS first FROM ${t(name)}`,
         );
         expect(Number(stored.n)).toBe(62 * 24);
         expect(new Date(String(stored.first)).toISOString()).toBe(
            "2026-09-11T00:00:00.000Z",
         );
      }
   });

   const queries: [string, string][] = [];
   for (const source of [
      "older",
      "older_inherited",
      "between",
      "between_inherited",
   ]) {
      queries.push(
         [`${source}: a measure`, `run: ${source} -> { aggregate: n, total }`],
         [
            `${source}: grouped by a stored dimension`,
            `run: ${source} -> { group_by: org; aggregate: n, total }`,
         ],
      );
   }
   for (const at of READS) {
      for (const [name, malloy] of queries) {
         it(`${name}, read at ${at}`, async () => {
            fixture.setClock(at);
            const run = await fixture.run({ name, malloy });
            expect(run.servedVia).toBe("served_copy");
            expect(run.servedFrom).toBe("storage");
            // 90 days on, a two-sided window is past every row there is,
            // and a grouped read is rightly empty.
            if (!(name.startsWith("between") && at === READS[4])) {
               expect(run.live.length).toBeGreaterThan(0);
            }
            expect(rowDifference(run.live, run.served)).toBeUndefined();
         });
      }
   }

   it("widens as the clock moves: a later read of the upper bound counts more rows", async () => {
      fixture.setClock(READS[0]);
      const atB = await fixture.run({
         name: "at b",
         malloy: "run: older -> { aggregate: n }",
      });
      fixture.setClock(READS[1]);
      const later = await fixture.run({
         name: "mid",
         malloy: "run: older -> { aggregate: n }",
      });
      expect(Number(later.served[0].n)).toBe(Number(atB.served[0].n) + 12);
   });

   for (const at of [READS[1], READS[4]]) {
      for (const GROUPS of [["acme"], ["acme", "globex"], []]) {
         it(`proves the gate over an upper bound for ${JSON.stringify(GROUPS)}, read at ${at}`, async () => {
            fixture.setClock(at);
            const run = await fixture.run({
               name: "gated",
               malloy: "run: older_gated -> { group_by: org; aggregate: n }",
               givens: { GROUPS },
            });
            expect(run.servedVia).toBe("served_copy");
            expect(run.servedFrom).toBe("storage");
            expect(rowDifference(run.live, run.served)).toBeUndefined();
            for (const row of run.served) {
               expect(GROUPS).toContain(row.org as string);
            }
         });
      }
   }

   it("answers a query with a time zone live", async () => {
      fixture.setClock(READS[1]);
      const run = await fixture.run({
         name: "time zone",
         malloy:
            "run: older -> { timezone: 'America/Los_Angeles' aggregate: n }",
      });
      expect(run.servedFrom).not.toBe("storage");
      expect(rowDifference(run.live, run.served)).toBeUndefined();
   });
});
