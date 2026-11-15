// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for the zone a re-rooted source computes in. The served copy replaces a
 * source's base with its stored table, which dropped a zone the source
 * inherits from that base (`inh is ev_ny extend { … }`, `ev_ny` in another
 * file or the same one), so the copy computed every query on it in the
 * default zone: an `hour(ts)`, a truncation and a time-literal filter came back
 * in UTC where live answers in New York. The re-root now writes the author's
 * zone again, and a backstop declines any query whose source computes in
 * another zone in the copy than in the author's model.
 *
 * Every case, colocated and `storage=`, answers equal to live, served or
 * declined; the inherited-zone ones are served. The other cases press the
 * per-column zone rule: joins, extensions that set or alias a zone, nests,
 * later stages and wildcards.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
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
const baseModel = `source: ev_base is duckdb.table('ev') extend {
  primary_key: id
  dimension: ts_hr is hour(ts), ts_day is ts.day
}
source: ev_ny is duckdb.table('ev') extend {
  primary_key: id
  timezone: 'America/New_York'
  dimension: ts_hr is hour(ts), ts_day is ts.day
}
`;
/** The model, colocated (`name=`) or `storage=` in one destination. */
function modelFor(tier: string) {
   const at = (n: string) =>
      tier === "colocated" ? `name="${n}"` : `storage="lake"`;
   return `import "base.malloy"
#(materialize) ${at("mzny_t")}
source: mzny is ev_base extend {
  timezone: 'America/New_York'
  measure: total is amt.sum()
}
#(materialize) ${at("inh_t")}
source: inh is ev_ny extend { measure: total is amt.sum() }
source: jbx is duckdb.table('ev') extend {
  primary_key: id
  join_one: mzny on id = mzny.id
  dimension: jh is mzny.ts_hr
  dimension: jd is mzny.ts_day
  dimension: jmorn is mzny.ts_hr < 12
  measure: jm is count() { where: mzny.ts_hr < 12 }
  measure: jmax is mzny.ts_hr.max()
}
source: exta is mzny extend { dimension: alias_hr is ts_hr, alias_day is ts_day, morn is ts_hr < 12 }
source: extu is mzny extend { timezone: 'UTC' }

source: b_ny is duckdb.table('ev') extend {
  primary_key: id
  timezone: 'America/New_York'
  dimension: b_day is ts.day
}
#(materialize) ${at("inh2_t")}
source: inh2 is b_ny extend { measure: total is amt.sum() }
`;
}
/** Each case: its name, its query, and whether it must be served. */
const CASES: [string, string, boolean][] = [
   ["jbx jh", "run: jbx -> { group_by: jh; aggregate: n is count() }", false],
   ["jbx jd", "run: jbx -> { group_by: jd; aggregate: n is count() }", false],
   [
      "jbx jmorn",
      "run: jbx -> { group_by: jmorn; aggregate: n is count() }",
      false,
   ],
   ["jbx jm", "run: jbx -> { aggregate: jm }", false],
   ["jbx jmax", "run: jbx -> { aggregate: jmax }", false],
   [
      "jbx where",
      "run: jbx -> { where: mzny.ts_hr < 12; aggregate: n is count() }",
      false,
   ],
   [
      "jbx path",
      "run: jbx -> { group_by: mzny.ts_hr; aggregate: n is count() }",
      false,
   ],
   [
      "jbx inline extend",
      "run: jbx extend { dimension: q is mzny.ts_hr } -> { group_by: q; aggregate: n is count() }",
      false,
   ],
   [
      "jbx nest path",
      "run: jbx -> { nest: x is { group_by: mzny.ts_hr; aggregate: n is count() } }",
      false,
   ],
   [
      "exta alias UTC",
      "run: exta -> { timezone: 'UTC'; group_by: alias_hr; aggregate: n is count() }",
      false,
   ],
   [
      "exta alias_day UTC",
      "run: exta -> { timezone: 'UTC'; group_by: alias_day; aggregate: n is count() }",
      false,
   ],
   [
      "exta morn UTC",
      "run: exta -> { timezone: 'UTC'; group_by: morn; aggregate: n is count() }",
      false,
   ],
   [
      "exta measure filter UTC",
      "run: exta -> { timezone: 'UTC'; aggregate: n is count() { where: ts_hr < 12 } }",
      false,
   ],
   [
      "exta where UTC",
      "run: exta -> { timezone: 'UTC'; where: ts_hr < 12; aggregate: n is count() }",
      false,
   ],
   [
      "extu ts_hr",
      "run: extu -> { group_by: ts_hr; aggregate: n is count() }",
      false,
   ],
   [
      "mzny ts_hr UTC",
      "run: mzny -> { timezone: 'UTC'; group_by: ts_hr; aggregate: n is count() }",
      false,
   ],
   [
      "mzny ts_hr noz",
      "run: mzny -> { group_by: ts_hr; aggregate: n is count() }",
      false,
   ],
   [
      "mzny hour(ts) noz",
      "run: mzny -> { group_by: h is hour(ts); aggregate: n is count() }",
      false,
   ],
   [
      "mzny filter noz",
      "run: mzny -> { where: ts ? @2024-03-14; aggregate: n is count() }",
      false,
   ],
   [
      "mzny day.month UTC",
      "run: mzny -> { timezone: 'UTC'; group_by: m is ts_day.month; aggregate: n is count() }",
      false,
   ],
   [
      "mzny day.month noz",
      "run: mzny -> { group_by: m is ts_day.month; aggregate: n is count() }",
      false,
   ],
   [
      "mzny 2stage s2 UTC",
      "run: mzny -> { group_by: ts_day; aggregate: n is count() } -> { timezone: 'UTC'; group_by: m is ts_day.month; aggregate: t is n.sum() }",
      false,
   ],
   [
      "mzny nest UTC",
      "run: mzny -> { group_by: ts_day; nest: x is { timezone: 'UTC'; group_by: ts_hr; aggregate: n is count() } }",
      false,
   ],
   [
      "mzny nest UTC no zoned",
      "run: mzny -> { group_by: ts_day; nest: x is { timezone: 'UTC'; group_by: h is hour(ts); aggregate: n is count() } }",
      false,
   ],
   ["mzny select *", "run: mzny -> { select: *; order_by: id }", false],
   [
      "mzny select * UTC",
      "run: mzny -> { timezone: 'UTC'; select: *; order_by: id }",
      false,
   ],
   [
      "mzny now rel",
      "run: mzny -> { where: ts > now - 5000 days; group_by: ts_day; aggregate: n is count() }",
      false,
   ],
   [
      "inh hour(ts)",
      "run: inh -> { group_by: h is hour(ts); aggregate: n is count() }",
      true,
   ],
   [
      "inh ts_day",
      "run: inh -> { group_by: ts_day; aggregate: n is count() }",
      true,
   ],
   [
      "inh ts_hr",
      "run: inh -> { group_by: ts_hr; aggregate: n is count() }",
      true,
   ],
   [
      "inh filter",
      "run: inh -> { where: ts ? @2024-03-14; aggregate: n is count() }",
      true,
   ],
   [
      "inh2 hour(ts)",
      "run: inh2 -> { group_by: h is hour(ts); aggregate: n is count() }",
      true,
   ],
   [
      "inh2 filter",
      "run: inh2 -> { where: ts ? @2024-03-14; aggregate: n is count() }",
      true,
   ],
   [
      "inh2 b_day",
      "run: inh2 -> { group_by: b_day; aggregate: n is count() }",
      true,
   ],
];

for (const tier of ["colocated", "storage"]) {
   describe(`R4: a re-rooted source computes in the author's zone [${tier}]`, () => {
      let restore: () => void;
      let fixture: R4Fixture;
      beforeAll(async () => {
         restore = installSettings({
            env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
         }).restore;
         fixture = await buildScenario({
            files: { "m.malloy": modelFor(tier), "base.malloy": baseModel },
            seedSql,
            materialize: true,
         });
      });
      afterAll(async () => {
         restore();
         await disposeScenarios();
      });
      for (const [name, malloy, served] of CASES) {
         it(`${name}: equal to live${served ? ", served" : ""}`, async () => {
            const run = await fixture.run({ name, malloy });
            expect(rowDifference(run.live, run.served)).toBeUndefined();
            if (served) expect(run.servedVia).toBe("served_copy");
         });
      }
   });
}
