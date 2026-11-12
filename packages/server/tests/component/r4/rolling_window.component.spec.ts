// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for rolling windows on the served copy (materialization v2, phase 3
 * PR 5; plan, Rolling windows, and phase 0 Evidence 3): a `#(materialize)`
 * source whose `where:` keeps "the last 30 days" is built at a pinned clock
 * *b*, and read through the served copy at *b*, at *b* + interval / 2 and just
 * before *b* + interval, each read equal to live at the same pinned clock. The
 * build's own `now` is *b*, the widest set a lower bound needs, and the served
 * copy re-applies the term at each read, inherited terms included.
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
// A one-day refresh interval: reads at b, mid-interval, and just before b + 1 day.
const READS = [
   "2026-11-10 00:00:00",
   "2026-11-10 12:00:00",
   "2026-11-10 23:59:59",
];

const seedSql = [
   // One event an hour, from well before b - 30 days to after b + 1 day.
   `CREATE TABLE events AS
      SELECT ts, row_number() OVER (ORDER BY ts) AS amount,
             CASE WHEN hour(ts) < 12 THEN 'am' ELSE 'pm' END AS half
      FROM range(TIMESTAMP '${B}' - INTERVAL 36 DAY, TIMESTAMP '${B}' + INTERVAL 2 DAY, INTERVAL 1 HOUR) t(ts)`,
];

const model = `source: events is duckdb.table('events')

#(materialize) storage="lake"
source: recent is events extend {
  where: ts >= now - 30 days
  dimension: age_hours is hours(ts to now)
  measure: n is count(), total is amount.sum()
}

source: last30 is events extend {
  where: now - 30 days <= ts
}

#(materialize) storage="lake"
source: last30_m is last30 extend {
  measure: n is count(), total is amount.sum()
}

#(materialize) storage="lake"
source: older_via is events extend {
  dimension: age_hours is hours(ts to now)
  where: age_hours > 720
  measure: n is count()
}

source: recent_via is events extend {
  dimension: is_recent is ts >= now - 30 days
  where: is_recent
}

#(materialize) storage="lake"
source: recent_via_m is recent_via extend {
  measure: n is count()
}

#(materialize) storage="lake"
source: older is events extend {
  where: ts < now - 30 days
  measure: n is count()
}
`;

describe("R4: a rolling window served at a pinned clock answers equal to live", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
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

   it("stores the widest set: every row from b - 30 days", async () => {
      const [stored] = await fixture.storedSQL(
         (t) => `SELECT count(*) AS n, min(ts) AS first FROM ${t("recent")}`,
      );
      // b - 30 days through b + 2 days, one row an hour.
      expect(Number(stored.n)).toBe(32 * 24);
      expect(new Date(String(stored.first)).toISOString()).toBe(
         "2026-10-11T00:00:00.000Z",
      );
   });

   const queries = [
      ["a measure over the window", "run: recent -> { aggregate: n, total }"],
      [
         "grouped by a stored dimension",
         "run: recent -> { group_by: half; aggregate: n, total }",
      ],
      [
         "a dimension that reads now, computed at read",
         "run: recent -> { where: age_hours > 700; aggregate: n }",
      ],
      [
         "a window inherited from the base",
         "run: last30_m -> { aggregate: n, total }",
      ],
   ] as const;
   for (const at of READS) {
      for (const [name, malloy] of queries) {
         it(`${name}, read at ${at}`, async () => {
            fixture.setClock(at);
            const run = await fixture.run({ name, malloy });
            expect(run.servedVia).toBe("served_copy");
            expect(run.servedFrom).toBe("storage");
            expect(run.live.length).toBeGreaterThan(0);
            expect(rowDifference(run.live, run.served)).toBeUndefined();
         });
      }
   }

   it("narrows mid-interval: a later read counts fewer rows than one at b", async () => {
      fixture.setClock(READS[0]);
      const atB = await fixture.run({
         name: "at b",
         malloy: "run: recent -> { aggregate: n }",
      });
      fixture.setClock(READS[1]);
      const later = await fixture.run({
         name: "mid",
         malloy: "run: recent -> { aggregate: n }",
      });
      expect(Number(later.served[0].n)).toBe(Number(atB.served[0].n) - 12);
   });

   for (const at of READS) {
      it(`answers a query with a time zone live, read at ${at}`, async () => {
         // Malloy takes the offset in the query zone's wall time, so across
         // the daylight-saving change inside this window a read at b needs
         // an hour older than the build stored: the served copy declines.
         fixture.setClock(at);
         const run = await fixture.run({
            name: "time zone",
            malloy:
               "run: recent -> { timezone: 'America/Los_Angeles' aggregate: n, total }",
         });
         expect(run.servedFrom).not.toBe("storage");
         expect(rowDifference(run.live, run.served)).toBeUndefined();
      });
   }

   it("reports the time-zone decline in the path report", async () => {
      fixture.setClock(READS[1]);
      fixture.setBindings(fixture.bindings);
      const run = await fixture.runAsIs(
         "run: recent -> { timezone: 'America/Los_Angeles' aggregate: n }",
      );
      expect(
         (run.path?.considered ?? []).map((c) => [c.kind, c.outcome, c.reason]),
      ).toEqual([["materialized", "skipped", "TIME_ZONE"]]);
      expect(run.path?.servedBy.kind).toBe("live");
   });

   for (const [name, malloy] of [
      [
         "an upper bound read through a dimension",
         "run: older_via -> { aggregate: n }",
      ],
      [
         "a window inherited through a dimension",
         "run: recent_via_m -> { aggregate: n }",
      ],
   ] as const) {
      it(`does not store ${name}, which answers live`, async () => {
         fixture.setClock(READS[1]);
         const run = await fixture.run({ name, malloy });
         expect(run.servedFrom).not.toBe("storage");
         expect(rowDifference(run.live, run.served)).toBeUndefined();
      });
   }

   it("does not store an upper-bound window, which answers live", async () => {
      fixture.setClock(READS[1]);
      const run = await fixture.run({
         name: "older",
         malloy: "run: older -> { aggregate: n }",
      });
      expect(run.servedVia).toBeUndefined();
      expect(run.servedFrom).not.toBe("storage");
      expect(rowDifference(run.live, run.served)).toBeUndefined();
   });
});
