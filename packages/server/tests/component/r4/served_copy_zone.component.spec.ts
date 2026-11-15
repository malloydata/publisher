// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for the zone a served-copy table is read in against the zone it was
 * built in. A table holds the zone-dependent values (a computed dimension, a
 * truncation) its build evaluated in its stored source's own zone, so a query
 * that reads it in another zone is answered live, and one that reads it in
 * the same zone stays served:
 *
 * - through a join, a source's own `timezone:` is not applied (Malloy reads a
 *   joined source in the query's zone), so a table built in New York and read
 *   through a join with no zone set is read in UTC: declined;
 * - an explicit `'UTC'` over a table built in the default zone is the same
 *   zone: served; `'Etc/UTC'` is not treated as one (a dialect may not know
 *   the alias): declined;
 * - a query's own zone equal to the zone the table was built in: served.
 *
 * Every answer, served or declined, equals live. Each runs on colocated
 * tables, where the join reads them on its own connection, and on `storage=`
 * tables in one destination with the joining source materialized too, the
 * shape that served the wrong rows.
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

// In a file of its own, so each table stores the dimensions as its build
// computed them, in its source's own zone (the served copy keeps only the
// blocks of bases declared in the source's own file).
const baseModel = `source: ev_base is duckdb.table('ev') extend {
  primary_key: id
  dimension: ts_hr is hour(ts), ts_day is ts.day
}
`;

/**
 * The model for one tier: colocated tables (`name=`), or `storage=` tables in
 * one destination, where `jb` is materialized too, so its join to `mzny` is
 * read from storage.
 */
function modelFor(tier: "colocated" | "storage"): string {
   const at = (name: string) =>
      tier === "colocated" ? `name="${name}"` : `storage="lake"`;
   return `import "base.malloy"

#(materialize) ${at("mzny_t")}
source: mzny is ev_base extend {
  timezone: 'America/New_York'
  measure: total is amt.sum()
}

#(materialize) ${at("mz_t")}
source: mz is ev_base extend {
  measure: total is amt.sum()
}

${tier === "storage" ? `#(materialize) storage="lake"\n` : ""}source: jb is duckdb.table('ev') extend {
  primary_key: id
  join_one: mzny on id = mzny.id
  measure: c is count()
}
`;
}

for (const tier of ["colocated", "storage"] as const) {
   describe(`R4: a served-copy table is read in the zone it was built in, or answered live [${tier}]`, () => {
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

      /** The served copy's reason for skipping `malloy`, when it did. */
      async function skipReason(malloy: string): Promise<string | undefined> {
         const run = await fixture.runAsIs(malloy);
         return (run.path?.considered ?? []).find(
            (c) => c.kind === "materialized" && c.outcome === "skipped",
         )?.reason;
      }

      // A colocated joined table is on no chain of the queried source, so the
      // path report does not place it and its decline shows as a live answer;
      // a `storage=` one is read through `jb`'s own table, which it places.
      const declined = [
         [
            "an hour built in New York, read through a join with no zone set",
            "run: jb -> { group_by: mzny.ts_hr; aggregate: n is count(); order_by: ts_hr }",
            tier === "storage",
         ],
         [
            "a day built in New York, read through a join with no zone set",
            "run: jb -> { group_by: mzny.ts_day; aggregate: n is count(); order_by: ts_day }",
            tier === "storage",
         ],
         [
            "'Etc/UTC' over a table built in the default zone",
            "run: mz -> { timezone: 'Etc/UTC'; group_by: ts_day; aggregate: n is count(); order_by: ts_day }",
            true,
         ],
      ] as const;
      for (const [name, malloy, placed] of declined) {
         it(`declines ${name} (TIME_ZONE), equal to live`, async () => {
            const run = await fixture.run({ name, malloy });
            expect(run.servedVia).toBeUndefined();
            expect(rowDifference(run.live, run.served)).toBeUndefined();
            if (placed) expect(await skipReason(malloy)).toBe("TIME_ZONE");
         });
      }

      const served = [
         [
            "'UTC' over a table built in the default zone",
            "run: mz -> { timezone: 'UTC'; group_by: ts_day; aggregate: n is count(); order_by: ts_day }",
         ],
         [
            "the query's own zone, the one the table was built in",
            "run: mzny -> { timezone: 'America/New_York'; group_by: ts_hr; aggregate: n is count(); order_by: ts_hr }",
         ],
         [
            "the table's source with its own zone, no zone in the query",
            "run: mzny -> { group_by: ts_hr; aggregate: n is count(); order_by: ts_hr }",
         ],
         [
            "a default-zone table with no zone anywhere",
            "run: mz -> { group_by: ts_day; aggregate: n is count(); order_by: ts_day }",
         ],
         ...(tier === "storage"
            ? ([
                 [
                    "a materialized joining source that reads no zoned value",
                    "run: jb -> { aggregate: c }",
                 ],
              ] as const)
            : []),
      ] as const;
      for (const [name, malloy] of served) {
         it(`serves ${name}, equal to live`, async () => {
            const run = await fixture.run({ name, malloy });
            expect(run.servedVia).toBe("served_copy");
            expect(rowDifference(run.live, run.served)).toBeUndefined();
         });
      }
   });
}
