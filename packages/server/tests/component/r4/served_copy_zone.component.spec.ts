// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for the zone a served-copy table is read in against the zone it was
 * built in. A table holds its zone-dependent values (a computed dimension, a
 * truncation) as its build evaluated them in its stored source's zone, its own
 * or one it inherits. A query reads them in the zone its first stage computes
 * in: its own `timezone:`, else its source's (own or inherited), else the
 * default. Through a join the joined source's own zone is not applied, so a
 * joined table is read in the query's zone too. A query that reads one of
 * those values in another zone is answered live; any other query stays served:
 *
 * - a table built in New York, read through a join with no zone set (or with a
 *   Los Angeles root, or two joins deep): declined; read through a join under
 *   the query's own New York zone: served;
 * - a query that reads none of the zone-dependent values (a measure of `amt`)
 *   stays served whatever zone it reads in;
 * - a root built in New York joining a table built in UTC: its own hour is
 *   served, the joined table's hour declined;
 * - an extension that inherits its zone from its base is built in that zone;
 * - an explicit `'UTC'` over a table built in the default zone is the same
 *   zone: served; `'Etc/UTC'` is not treated as one (a dialect may not know
 *   the alias): declined.
 *
 * Every answer, served or declined, equals live, and every decline is the
 * served copy's time-zone decline. Each runs on database tables, where the
 * join reads them on its own connection, and the core cases on `storage=`
 * tables in one destination with the joining source materialized too.
 */

import { afterAll, beforeAll, describe, expect, it, spyOn } from "bun:test";
import { logger } from "../../../src/logger";
import { useMaterializationOn } from "../../../src/test_helpers/settings_env";
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
// computed them, in its source's zone (the served copy keeps only the blocks
// of bases declared in the source's own file).
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

type Tier = "database" | "storage";

/**
 * The model for one tier: database tables (`name=`), or `storage=` tables in
 * one destination, where `jb` is materialized too, so its join to `mzny` is
 * read from storage. The database model adds the join shapes.
 */
function modelFor(tier: Tier): string {
   const at = (name: string) =>
      tier === "database" ? `name="${name}"` : `storage="lake"`;
   const core = `import "base.malloy"

#(materialize) ${at("mzny_t")}
source: mzny is ev_base extend {
  timezone: 'America/New_York'
  measure: total is amt.sum()
}

#(materialize) ${at("mz_t")}
source: mz is ev_base extend {
  measure: total is amt.sum()
}
`;
   if (tier === "storage") {
      return `${core}
#(materialize) storage="lake"
source: jb is duckdb.table('ev') extend {
  primary_key: id
  join_one: mzny on id = mzny.id
  measure: c is count()
}
`;
   }
   return `${core}
#(materialize) name="inh_t"
source: inh is ev_ny extend {
  measure: total is amt.sum()
}

source: jb is duckdb.table('ev') extend {
  primary_key: id
  join_one: mzny on id = mzny.id
  join_one: mz on id = mz.id
  join_one: inh on id = inh.id
  measure: c is count()
}

source: jla is duckdb.table('ev') extend {
  primary_key: id
  timezone: 'America/Los_Angeles'
  join_one: mzny on id = mzny.id
}

source: jj is duckdb.table('ev') extend {
  primary_key: id
  join_one: jb on id = jb.id
}

source: mzj is mzny extend {
  join_one: mz on id = mz.id
}
`;
}

interface Case {
   name: string;
   malloy: string;
   /** Answered live with the served copy's time-zone decline (else served). */
   declines?: true;
   /** The tiers it runs in (default both). */
   tiers?: Tier[];
}

const DATABASE: Tier[] = ["database"];

const CASES: Case[] = [
   // A table built in New York, read through a join.
   {
      name: "a New York hour through a join, no zone set",
      malloy:
         "run: jb -> { group_by: mzny.ts_hr; aggregate: n is count(); order_by: ts_hr }",
      declines: true,
   },
   {
      name: "a New York day through a join, no zone set",
      malloy:
         "run: jb -> { group_by: mzny.ts_day; aggregate: n is count(); order_by: ts_day }",
      declines: true,
   },
   {
      name: "a New York hour through a join, under the query's own New York zone",
      malloy:
         "run: jb -> { timezone: 'America/New_York'; group_by: mzny.ts_hr; aggregate: n is count(); order_by: ts_hr }",
      tiers: DATABASE,
   },
   {
      name: "a New York day through a join, under the query's own New York zone",
      malloy:
         "run: jb -> { timezone: 'America/New_York'; group_by: mzny.ts_day; aggregate: n is count(); order_by: ts_day }",
      tiers: DATABASE,
   },
   {
      name: "a zone-free measure through a join",
      malloy: "run: jb -> { aggregate: t is mzny.total }",
      tiers: DATABASE,
   },
   {
      name: "a materialized joining source that reads no zoned value",
      malloy: "run: jb -> { aggregate: c }",
      tiers: ["storage"],
   },
   {
      name: "a New York hour through a join under a Los Angeles root",
      malloy:
         "run: jla -> { group_by: mzny.ts_hr; aggregate: n is count(); order_by: ts_hr }",
      declines: true,
      tiers: DATABASE,
   },
   {
      name: "a New York hour two joins deep",
      malloy:
         "run: jj -> { group_by: jb.mzny.ts_hr; aggregate: n is count(); order_by: ts_hr }",
      declines: true,
      tiers: DATABASE,
   },
   {
      name: "a New York hour through a join from an inline extension",
      malloy:
         "run: jb extend { dimension: z is 1 } -> { group_by: mzny.ts_hr; aggregate: n is count(); order_by: ts_hr }",
      declines: true,
      tiers: DATABASE,
   },
   {
      name: "a UTC hour through a join, under the query's New York zone",
      malloy:
         "run: jb -> { timezone: 'America/New_York'; group_by: mz.ts_hr; aggregate: n is count(); order_by: ts_hr }",
      declines: true,
      tiers: DATABASE,
   },
   // Two stored tables in different zones.
   {
      name: "a New York root's own hour, joining a UTC table",
      malloy:
         "run: mzj -> { group_by: ts_hr; aggregate: n is count(); order_by: ts_hr }",
      tiers: DATABASE,
   },
   {
      name: "a New York root's own hour, with a zone-free measure of the joined UTC table",
      malloy:
         "run: mzj -> { group_by: ts_hr; aggregate: t is mz.total; order_by: ts_hr }",
      tiers: DATABASE,
   },
   {
      name: "a UTC table's hour, joined from a New York root",
      malloy:
         "run: mzj -> { group_by: ts_hr2 is mz.ts_hr; aggregate: n is count(); order_by: ts_hr2 }",
      declines: true,
      tiers: DATABASE,
   },
   // An extension that inherits its zone from its base.
   {
      name: "an inherited New York hour, read directly",
      malloy:
         "run: inh -> { group_by: ts_hr; aggregate: n is count(); order_by: ts_hr }",
      tiers: DATABASE,
   },
   {
      name: "an inherited New York hour through a join, no zone set",
      malloy:
         "run: jb -> { group_by: inh.ts_hr; aggregate: n is count(); order_by: ts_hr }",
      declines: true,
      tiers: DATABASE,
   },
   // Direct reads.
   {
      name: "a New York day, read directly",
      malloy:
         "run: mzny -> { group_by: ts_day; aggregate: n is count(); order_by: ts_day }",
   },
   {
      name: "the query's own zone, the one the table was built in",
      malloy:
         "run: mzny -> { timezone: 'America/New_York'; group_by: ts_hr; aggregate: n is count(); order_by: ts_hr }",
   },
   {
      name: "the table's source with its own zone, no zone in the query",
      malloy:
         "run: mzny -> { group_by: ts_hr; aggregate: n is count(); order_by: ts_hr }",
   },
   {
      name: "'UTC' over a table built in the default zone",
      malloy:
         "run: mz -> { timezone: 'UTC'; group_by: ts_day; aggregate: n is count(); order_by: ts_day }",
   },
   {
      name: "'Etc/UTC' over a table built in the default zone",
      malloy:
         "run: mz -> { timezone: 'Etc/UTC'; group_by: ts_day; aggregate: n is count(); order_by: ts_day }",
      declines: true,
   },
   {
      name: "a default-zone table with no zone anywhere",
      malloy:
         "run: mz -> { group_by: ts_day; aggregate: n is count(); order_by: ts_day }",
   },
];

/** The served copy's time-zone declines, whose messages each name a zone. */
const ZONE_DECLINE = /another (time )?zone than/;

for (const tier of ["database", "storage"] as const) {
   describe(`R4: a served-copy table is read in the zone it was built in, or answered live [${tier}]`, () => {
      let fixture: R4Fixture;
      useMaterializationOn();
      beforeAll(async () => {
         fixture = await buildScenario({
            files: { "m.malloy": modelFor(tier), "base.malloy": baseModel },
            seedSql,
            materialize: true,
         });
      });
      afterAll(async () => {
         await disposeScenarios();
      });

      /**
       * The served copy's decline of `malloy`, from its debug log: the path
       * report does not place a joined table, which is on no chain of the
       * queried source, so the log is the one place every decline shows.
       */
      async function servedCopyDecline(
         malloy: string,
      ): Promise<string | undefined> {
         // The skip line is written only at debug.
         const enabled = spyOn(logger, "isDebugEnabled").mockReturnValue(true);
         const debug = spyOn(logger, "debug");
         try {
            await fixture.runAsIs(malloy);
            return debug.mock.calls
               .filter(
                  (c) =>
                     String(c[0]) === "served copy cannot answer this query",
               )
               .map((c) =>
                  String(
                     ((c as unknown[])[1] as { error?: string } | undefined)
                        ?.error,
                  ),
               )[0];
         } finally {
            debug.mockRestore();
            enabled.mockRestore();
         }
      }

      for (const c of CASES.filter((c) => !c.tiers || c.tiers.includes(tier))) {
         it(`${c.declines ? "declines" : "serves"} ${c.name}, equal to live`, async () => {
            const run = await fixture.run({ name: c.name, malloy: c.malloy });
            expect(rowDifference(run.live, run.served)).toBeUndefined();
            if (c.declines) {
               expect(run.servedVia).toBeUndefined();
               expect(await servedCopyDecline(c.malloy)).toMatch(ZONE_DECLINE);
            } else {
               expect(run.servedVia).toBe("served_copy");
            }
         });
      }
   });
}
