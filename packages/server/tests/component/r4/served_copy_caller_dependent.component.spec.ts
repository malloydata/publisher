// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for P9: a `#(materialize)` dimension whose value depends on the caller is
 * left out of the build and computed at read by the served copy, so no table
 * holds one caller's value and every caller gets live's answer. Three
 * dependences, each checked served against live:
 *
 *  - a given: two callers with different givens, each answered from the
 *    served copy, each equal to live, and the two live answers differ (a
 *    served answer that stored one caller's value could not match both);
 *  - the current time: read at two pinned clocks, each equal to live;
 *  - the query time zone: a `timezone:` query over a table that stores only
 *    zone-free computed dimensions is served, equal to live and different
 *    from the same query without the zone; over a table that stores a
 *    zone-dependent one (declared on a base, so stored as built) it is
 *    answered live.
 *
 * The harness builds each generated source with its SQL as compiled and does
 * not run the eligibility gates, so a dimension that were stored would be
 * stored with the given's default and served wrong: the guard under test is
 * the generator's, not the gate's.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
   type R4Run,
} from "./harness";

const B = "2026-01-10 12:00:00";

const seedSql = [
   `CREATE TABLE ev AS SELECT * FROM (VALUES
      (1, 'acme', 10.0, TIMESTAMP '2026-01-01 03:00:00'),
      (2, 'acme', 20.0, TIMESTAMP '2026-01-02 23:30:00'),
      (3, 'globex', 5.0, TIMESTAMP '2026-01-05 07:15:00'),
      (4, 'globex', 7.0, TIMESTAMP '2026-01-09 01:00:00'),
      (5, 'initech', 3.0, TIMESTAMP '2026-01-09 22:45:00')
    ) AS t(id, org, amt, ts)`,
];

const model = `##! experimental { givens access_modifiers }
given: ORG :: string is 'acme'

source: ev is duckdb.table('ev')

source: base is ev extend {
  dimension: base_hour is hour(ts)
}

#(materialize) storage="lake"
source: orders is base extend {
  dimension: mine is org = $ORG
  dimension: label is pick 'mine' when mine else 'theirs'
  internal dimension: imine is org = $ORG
  dimension: hr is hour(ts)
  dimension: on_day is ts.day
  dimension: age_days is days(ts to now)
  dimension: big is amt > 6
  measure: total is amt.sum()
  measure: my_total is amt.sum() { where: imine }
  view: by_label is { group_by: label; aggregate: total }
}

#(materialize) storage="lake"
source: local is ev extend {
  dimension: hr is hour(ts)
  dimension: on_day is ts.day
  dimension: up is upper(org)
  dimension: mine is org = $ORG
  measure: total is amt.sum()
}

#(materialize) storage="lake"
source: zonal is ev extend {
  dimension: hr is hour(ts)
  measure: total is amt.sum()
}

#(materialize) storage="lake"
source: zonal_ext is zonal extend {
  dimension: hr_next is hr + 1
  dimension: up is upper(org)
}

#(materialize) storage="lake"
source: since is ev extend {
  where: ts > @2026-01-05 12:00
  measure: total is amt.sum()
}

#(materialize) storage="lake"
source: headed is (ev extend { dimension: mine is org = $ORG }) extend {
  measure: total is amt.sum()
}
`;

const CALLERS = [{ ORG: "acme" }, { ORG: "globex" }];

describe("R4: a caller-dependent dimension is computed at read, never stored (P9)", () => {
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

   /** Run `malloy` as each caller: served from the copy, equal to live. */
   async function asEachCaller(
      name: string,
      malloy: string,
      served = true,
   ): Promise<R4Run[]> {
      const runs: R4Run[] = [];
      for (const givens of CALLERS) {
         const run = await fixture.run({
            name: `${name} as ${givens.ORG}`,
            malloy,
            givens,
         });
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         expect(run.servedVia).toBe(served ? "served_copy" : undefined);
         runs.push(run);
      }
      // Different callers see different answers live, so a served answer
      // computed from one caller's stored value could not match both.
      expect(runs[0].live).not.toEqual(runs[1].live);
      return runs;
   }

   it("stores none of the caller-dependent dimensions", async () => {
      const [row] = await fixture.storedSQL(
         (t) => `SELECT * FROM ${t("orders")} LIMIT 1`,
      );
      const stored = Object.keys(row).sort();
      expect(stored).toEqual(
         ["amt", "base_hour", "big", "id", "org", "ts"].sort(),
      );
      const [localRow] = await fixture.storedSQL(
         (t) => `SELECT * FROM ${t("local")} LIMIT 1`,
      );
      expect(Object.keys(localRow).sort()).toEqual(
         ["amt", "id", "org", "ts", "up"].sort(),
      );
      // The table holds every caller's rows.
      const orgs = await fixture.storedSQL(
         (t) => `SELECT DISTINCT org FROM ${t("orders")} ORDER BY org`,
      );
      expect(orgs.map((r) => r.org)).toEqual(["acme", "globex", "initech"]);
      expect(
         (await fixture.servedCopyProblems()).filter(
            (p) => p.sourceName === "orders" || p.sourceName === "local",
         ),
      ).toEqual([]);
   });

   describe("a given", () => {
      for (const [name, malloy] of [
         [
            "grouped by a dimension that reads it",
            "run: orders -> { group_by: mine; aggregate: total }",
         ],
         [
            "grouped by one that reads it through another",
            "run: orders -> { group_by: label; aggregate: total }",
         ],
         ["the author's view over it", "run: orders -> by_label"],
         [
            "a measure filtered by an internal one",
            "run: orders -> { aggregate: my_total }",
         ],
         [
            "a query filter on it",
            "run: orders -> { where: mine; aggregate: total, n is count() }",
         ],
         [
            "beside stored dimensions",
            "run: orders -> { group_by: big, mine; aggregate: total; order_by: big, mine }",
         ],
      ] as const) {
         it(`answers each caller equal to live: ${name}`, async () => {
            await asEachCaller(name, malloy);
         });
      }

      it("leaves a source whose dimension the re-root would drop (a parenthesised head) to live, for each caller", async () => {
         await asEachCaller(
            "headed",
            "run: headed -> { group_by: mine; aggregate: total }",
            false,
         );
      });
   });

   describe("the current time", () => {
      for (const at of [B, "2026-01-12 06:00:00"]) {
         it(`computes a dimension that reads now at read, read at ${at}`, async () => {
            fixture.setClock(at);
            const run = await fixture.run({
               name: `age at ${at}`,
               malloy:
                  "run: orders -> { group_by: age_days; aggregate: total; order_by: age_days }",
               givens: CALLERS[0],
            });
            expect(run.servedVia).toBe("served_copy");
            expect(run.live.length).toBeGreaterThan(0);
            expect(rowDifference(run.live, run.served)).toBeUndefined();
         });
      }
      it("answers differently at the two clocks, so a stored age could not match both", async () => {
         const ages = [];
         for (const at of [B, "2026-01-12 06:00:00"]) {
            fixture.setClock(at);
            const run = await fixture.run({
               name: `max age at ${at}`,
               malloy: "run: orders -> { aggregate: m is max(age_days) }",
               givens: CALLERS[0],
            });
            expect(run.servedVia).toBe("served_copy");
            expect(rowDifference(run.live, run.served)).toBeUndefined();
            ages.push(run.served[0].m);
         }
         expect(ages[0]).not.toEqual(ages[1]);
         fixture.setClock(B);
      });
   });

   describe("an extension table over a parent that left a dimension out", () => {
      it("builds over the parent's table, computing the parent's left-out dimension in its build", () => {
         // `headed`'s build selects the same columns as `zonal`'s, so Malloy
         // builds the two as one target, which the extension reads.
         expect(fixture.buildReads.zonal_ext).toContain("zonal");
      });

      it("stores the inherited zone-dependent dimension as built, and leaves its own out", async () => {
         const [row] = await fixture.storedSQL(
            (t) => `SELECT * FROM ${t("zonal_ext")} LIMIT 1`,
         );
         expect(Object.keys(row)).toContain("hr");
         expect(Object.keys(row)).not.toContain("hr_next");
         const [parent] = await fixture.storedSQL(
            (t) => `SELECT * FROM ${t("zonal")} LIMIT 1`,
         );
         expect(Object.keys(parent)).not.toContain("hr");
      });

      it("answers equal to live without a zone, from its table", async () => {
         const run = await fixture.run({
            name: "zonal_ext",
            malloy:
               "run: zonal_ext -> { group_by: hr, hr_next, up; aggregate: total; order_by: hr, up }",
            givens: CALLERS[0],
         });
         expect(run.servedVia).toBe("served_copy");
         expect(rowDifference(run.live, run.served)).toBeUndefined();
      });

      it("answers a timezone: query live, since its table stores the hour as built", async () => {
         const run = await fixture.run({
            name: "zonal_ext zoned",
            malloy:
               "run: zonal_ext -> { timezone: 'America/Los_Angeles'; group_by: hr, hr_next; aggregate: total; order_by: hr }",
            givens: CALLERS[0],
         });
         expect(run.servedVia).toBeUndefined();
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         // The parent stores no zone-dependent dimension: served in the zone.
         const parent = await fixture.run({
            name: "zonal zoned",
            malloy:
               "run: zonal -> { timezone: 'America/Los_Angeles'; group_by: hr; aggregate: total; order_by: hr }",
            givens: CALLERS[0],
         });
         expect(parent.servedVia).toBe("served_copy");
         expect(rowDifference(parent.live, parent.served)).toBeUndefined();
      });
   });

   describe("the query time zone", () => {
      const zoned =
         "run: local -> { timezone: 'America/Los_Angeles'; group_by: hr, on_day; aggregate: total; order_by: on_day, hr }";
      const unzoned =
         "run: local -> { group_by: hr, on_day; aggregate: total; order_by: on_day, hr }";

      it("serves a timezone: query over zone-free stored dimensions, computing the zoned ones at read", async () => {
         const run = await fixture.run({
            name: "zoned",
            malloy: zoned,
            givens: CALLERS[0],
         });
         expect(run.servedVia).toBe("served_copy");
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         // The zone changes the answer, so a stored hour or day (computed in
         // UTC at build) could not have matched.
         const utc = await fixture.run({
            name: "unzoned",
            malloy: unzoned,
            givens: CALLERS[0],
         });
         expect(utc.servedVia).toBe("served_copy");
         expect(rowDifference(utc.live, utc.served)).toBeUndefined();
         expect(run.live).not.toEqual(utc.live);
      });

      it("serves the zone and a given together, per caller", async () => {
         await asEachCaller(
            "zone and given",
            "run: local -> { timezone: 'Asia/Kolkata'; group_by: mine, on_day; aggregate: total; order_by: on_day, mine }",
         );
      });

      it("answers live over a table whose rows a timestamp literal filtered in the build's zone", async () => {
         // `since` stores no computed dimension, but its `where:` cut-off
         // moves with the zone: at +05:30 it admits a row the build (in UTC)
         // left out of the table, so serving from the table would miss it.
         const zonedSince =
            "run: since -> { timezone: 'Asia/Kolkata'; group_by: org; aggregate: total; order_by: org }";
         const run = await fixture.run({
            name: "since zoned",
            malloy: zonedSince,
            givens: CALLERS[0],
         });
         expect(run.servedVia).toBeUndefined();
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         const utc = await fixture.run({
            name: "since",
            malloy:
               "run: since -> { group_by: org; aggregate: total; order_by: org }",
            givens: CALLERS[0],
         });
         expect(utc.servedVia).toBe("served_copy");
         expect(rowDifference(utc.live, utc.served)).toBeUndefined();
         expect(run.live).not.toEqual(utc.live);
      });

      it("answers live over a table that stores a zone-dependent dimension declared on a base", async () => {
         fixture.setBindings(fixture.bindings);
         const run = await fixture.run({
            name: "orders zoned",
            malloy:
               "run: orders -> { timezone: 'America/Los_Angeles'; group_by: base_hour; aggregate: total; order_by: base_hour }",
            givens: CALLERS[0],
         });
         expect(run.servedVia).toBeUndefined();
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         fixture.setBindings(fixture.bindings);
         const asIs = await fixture.runAsIs(
            "run: orders -> { timezone: 'America/Los_Angeles'; group_by: hr; aggregate: total }",
            CALLERS[0],
         );
         expect(
            (asIs.path?.considered ?? []).map((c) => [c.outcome, c.reason]),
         ).toEqual([["skipped", "TIME_ZONE"]]);
      });
   });
});
