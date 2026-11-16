// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for the zone a v1 `#@ persist` table is built and read in, the cases
 * served_copy_zone and served_copy_inherited_zone cover for `#(materialize)`:
 *
 *  - a table built in New York (a query stage that sets the zone, or a base
 *    that sets it), read through a join from a source that sets no zone;
 *  - a persisted source that inherits its base's zone, extend-shaped (over a
 *    SQL source: a refinement of a bare table is not planned) and
 *    query-shaped, read directly.
 *
 * Every case answers equal to live, served or declined, in a `storage=`
 * destination (the colocated variant is the conformance shape "persist over
 * a zoned base", which binds v1 colocated tables as a package does). A refinement of a base that sets only the zone is served, its
 * zone kept; one over a base that also declares zoned dimensions is not
 * re-rooted (`fields_not_carried`), so it answers live.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { strictRowDifference } from "../../../src/test_helpers/conformance/answer_difference";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
} from "./harness";

const seedSql = [
   `CREATE TABLE ev AS SELECT * FROM (VALUES
      (1, 10.5, TIMESTAMP '2024-03-15 01:45:10'),
      (2, 20.0, TIMESTAMP '2024-03-15 03:05:00'),
      (3, 5.5, TIMESTAMP '2024-07-20 22:10:00'),
      (4, 1.0, TIMESTAMP '2024-11-03 05:30:00'),
      (5, 2.0, TIMESTAMP '2024-11-03 06:30:00'),
      (6, 3.0, TIMESTAMP '2023-12-31 23:59:00')
    ) AS t(id, amt, ts)`,
];

/** The model, every table `storage=`. */
function modelFor(tier: "storage"): string {
   const at = (n: string) => `name="${n}" ${tier}="lake"`;
   return `##! experimental { persistence }
source: ev_ny is duckdb.sql("SELECT * FROM ev") extend {
  primary_key: id
  timezone: 'America/New_York'
  dimension: ts_hr is hour(ts), ts_day is ts.day
}

#@ persist ${at("pq_t")}
source: pq is duckdb.table('ev') -> {
  timezone: 'America/New_York'
  group_by: id, ts, h is hour(ts), d is ts.day, hr is ts.hour
} extend { primary_key: id }

#@ persist ${at("pqi_t")}
source: pqi is ev_ny -> { group_by: id, ts, h is hour(ts), d is ts.day }

#@ persist ${at("inh_t")}
source: inh is ev_ny extend { measure: total is amt.sum() }

source: ev_tz is duckdb.sql("SELECT * FROM ev") extend {
  primary_key: id
  timezone: 'America/New_York'
}

#@ persist ${at("inh2_t")}
source: inh2 is ev_tz extend { measure: total is amt.sum() }

source: jb is duckdb.table('ev') extend {
  primary_key: id
  join_one: pq on id = pq.id
  join_one: inh on id = inh.id
  join_one: inh2 on id = inh2.id
}
`;
}

/**
 * How a case must be answered: from the table (`served`), or live, either
 * because no stored leg is considered (`unrouted`: the query is rooted in a
 * warehouse source that reaches the stored table through a join on another
 * connection), or declined with the path report's reason (`TIME_ZONE`: a
 * value stored in New York read in another zone; `WARNED`: the served copy
 * does not carry the source, here `fields_not_carried`).
 */
type Outcome = "served" | "unrouted" | "TIME_ZONE" | "WARNED";

const CASES: [string, string, Outcome][] = [
   [
      "through a join: a stored hour",
      "run: jb -> { group_by: pq.h; aggregate: n is count() }",
      "unrouted",
   ],
   [
      "through a join: a stored day",
      "run: jb -> { group_by: pq.d; aggregate: n is count() }",
      "unrouted",
   ],
   [
      "through a join: a stored hour truncation",
      "run: jb -> { group_by: pq.hr; aggregate: n is count() }",
      "unrouted",
   ],
   [
      "through a join: an inherited-zone dimension",
      "run: jb -> { group_by: inh.ts_hr; aggregate: n is count() }",
      "unrouted",
   ],
   [
      "through a join: a truncation of an inherited-zone column",
      "run: jb -> { group_by: d is inh.ts.day; aggregate: n is count() }",
      "unrouted",
   ],
   [
      "the zone-setting query, read directly: its stored day",
      "run: pq -> { group_by: d; aggregate: n is count() }",
      "TIME_ZONE",
   ],
   [
      "through a join: a truncation of a zone-only base's column",
      "run: jb -> { group_by: d is inh2.ts.day; aggregate: n is count() }",
      "unrouted",
   ],
   [
      "inherited, over a base with zoned dimensions: hour(ts)",
      "run: inh -> { group_by: h is hour(ts); aggregate: n is count() }",
      "WARNED",
   ],
   [
      "inherited, over a base with zoned dimensions: the base's dimension",
      "run: inh -> { group_by: ts_day; aggregate: total }",
      "WARNED",
   ],
   [
      "inherited, over a zone-only base: hour(ts)",
      "run: inh2 -> { group_by: h is hour(ts); aggregate: n is count() }",
      "served",
   ],
   [
      "inherited, over a zone-only base: a day truncation",
      "run: inh2 -> { group_by: d is ts.day; aggregate: total }",
      "served",
   ],
   [
      "inherited, over a zone-only base: a time-literal filter",
      "run: inh2 -> { where: ts ? @2024-03-14; aggregate: total }",
      "served",
   ],
   [
      "inherited, query-shaped: its stored day",
      "run: pqi -> { group_by: d; aggregate: n is count() }",
      "TIME_ZONE",
   ],
   [
      "inherited, query-shaped: hour(ts) over the stored instant",
      "run: pqi -> { group_by: h2 is hour(ts); aggregate: n is count() }",
      "served",
   ],
];

for (const tier of ["storage"] as const) {
   describe(`R4: a v1 #@ persist table's zone, read through a join or inherited [${tier}]`, () => {
      let restore: () => void;
      let fixture: R4Fixture;
      beforeAll(async () => {
         restore = installSettings({
            env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
         }).restore;
         fixture = await buildScenario({
            files: { "m.malloy": modelFor(tier) },
            seedSql,
         });
      });
      afterAll(async () => {
         restore();
         await disposeScenarios();
      });
      it("leaves the source over a base with zoned dimensions out of the copy (fields_not_carried)", async () => {
         expect(await fixture.servedCopyProblems()).toContainEqual({
            sourceName: "inh",
            reason: "fields_not_carried",
         });
      });
      for (const [name, malloy, outcome] of CASES) {
         it(`${name}: equal to live, ${outcome === "served" ? "served" : `live (${outcome})`}`, async () => {
            const run = await fixture.run({ name, malloy });
            expect(rowDifference(run.live, run.served)).toBeUndefined();
            expect(strictRowDifference(run.live, run.served)).toBeUndefined();
            fixture.setBindings(fixture.bindings);
            const { path } = await fixture.runAsIs(malloy);
            const considered = (path?.considered ?? []).map((c) => c.reason);
            if (outcome === "served") {
               expect(run.servedVia).toBe("served_copy");
               return;
            }
            expect(run.servedBy?.kind).toBe("live");
            expect(considered).toEqual(outcome === "unrouted" ? [] : [outcome]);
         });
      }
   });
}
