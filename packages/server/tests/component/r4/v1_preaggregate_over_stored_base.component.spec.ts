// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for a v1 `#@ preaggregate` over a `#@ persist storage=` base, with
 * PUBLISHER_MATERIALIZE at its default (off), where `#@ preaggregate` is the
 * spelling that builds. The line is read as a `#(rollup)` declaration
 * and built over the author's rows by the rollup leg, so it builds and serves
 * as that declaration would: a covered query is answered from the rollup,
 * equal to live, and an uncovered one as it was before, equal to live.
 *
 * Both base shapes. A query-shaped base builds its own stored table, which
 * still answers what no rollup covers. A table extended with measures is not
 * a build root, so its `#@ persist` plans nothing (`persist-not-materializable`)
 * and what no rollup covers is answered live; its rollups build and serve
 * all the same. Both placements: `storage=` and database; a database grain
 * over the query-shaped (stored) base is refused at plan
 * (`upstream_table_unusable`), and what it would cover is answered from the
 * base's stored table.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   DESTINATION,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
   type R4Run,
} from "./harness";

const seedSql = [
   `CREATE TABLE orders_raw AS SELECT * FROM (VALUES
      (1, 10.0, 'A', 'east', TIMESTAMP '2026-01-01 08:00:00'),
      (2, 20.0, 'A', 'west', TIMESTAMP '2026-01-02 09:00:00'),
      (3, 30.0, 'B', 'east', TIMESTAMP '2026-02-01 10:00:00'),
      (4,  5.0, 'B', 'east', TIMESTAMP '2026-02-03 11:00:00'),
      (5, 12.5, 'C', 'west', TIMESTAMP '2026-03-04 23:30:00')
    ) AS t(id, amount, category, region, order_time)`,
];

function expectServedFromRollup(run: R4Run): void {
   expect(run.servedBy?.kind).toBe("rollup");
   expect(rowDifference(run.live, run.served)).toBeUndefined();
}

/** Not a rollup: answered as before, equal to live. */
function expectNotFromRollup(run: R4Run): void {
   expect(run.servedBy?.kind).not.toBe("rollup");
   expect(rowDifference(run.live, run.served)).toBeUndefined();
}

const ROLLUP_BY_CATEGORY = /^daily__rollup__category__[0-9a-f]{8}$/;
const ROLLUP_BY_REGION = /^daily__rollup__region__[0-9a-f]{8}$/;

const SHAPES: Record<string, { text: string; baseStored: boolean }> = {
   "a query-shaped base": {
      baseStored: true,
      text: `##! experimental.persistence
#@ persist storage="${DESTINATION}"
source: daily is duckdb.table('orders_raw') -> {
  group_by: category, region, d is order_time.day
  aggregate: amount_sum is amount.sum(), n is count()
} extend {
  #@ preaggregate grain="category" storage="${DESTINATION}"
  #@ preaggregate grain="region"
  measure: total is amount_sum.sum()
  #@ preaggregate grain="category" storage="${DESTINATION}"
  measure: order_count is n.sum()
}
`,
   },
   "a table extended with measures": {
      baseStored: false,
      text: `##! experimental.persistence
#@ persist storage="${DESTINATION}"
source: daily is duckdb.table('orders_raw') extend {
  dimension: d is order_time.day
  #@ preaggregate grain="category" storage="${DESTINATION}"
  #@ preaggregate grain="region"
  measure: total is amount.sum()
  #@ preaggregate grain="category" storage="${DESTINATION}"
  measure: order_count is count()
}
`,
   },
};

for (const [shape, { text, baseStored }] of Object.entries(SHAPES)) {
   describe(`R4: a v1 #@ preaggregate over a storage= base, on ${shape}`, () => {
      let restore: () => void;
      let fixture: R4Fixture;
      beforeAll(async () => {
         restore = installSettings({
            env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "false" },
         }).restore;
         fixture = await buildScenario({
            files: { "m.malloy": text },
            seedSql,
            preaggregate: true,
         });
      });
      afterAll(async () => {
         restore();
         await disposeScenarios();
      });

      it(
         baseStored
            ? "builds the storage= grain into the destination, apart from the base's table, and refuses the database one"
            : "builds the storage= grain into the destination and the database one beside the base",
         () => {
            const stored = Object.keys(fixture.physicalTables);
            const storedRollups = stored.filter((n) =>
               ROLLUP_BY_CATEGORY.test(n),
            );
            expect(storedRollups).toHaveLength(1);
            // Over a stored base the database grain is refused at plan
            // (`upstream_table_unusable`): its base's table lives in a storage
            // destination a build in the warehouse cannot read.
            expect(Object.keys(fixture.rollupTables)).toEqual(
               baseStored ? [] : [expect.stringMatching(ROLLUP_BY_REGION)],
            );
            if (baseStored) {
               expect(stored.sort()).toEqual(
                  ["daily", ...storedRollups].sort(),
               );
               expect(fixture.physicalTables.daily).not.toBe(
                  fixture.physicalTables[storedRollups[0] as string],
               );
            } else {
               expect(stored).toEqual(storedRollups);
            }
         },
      );

      it("serves a query the storage= rollup covers from it, equal to live", async () => {
         expectServedFromRollup(
            await fixture.run({
               name: "storage rollup",
               malloy:
                  "run: daily -> { group_by: category; aggregate: total, order_count }",
            }),
         );
      });

      it(
         baseStored
            ? "answers a query only the refused database rollup would cover from the base's stored table, equal to live"
            : "serves a query the database rollup covers from it, equal to live",
         async () => {
            const run = await fixture.run({
               name: "database rollup",
               malloy: "run: daily -> { group_by: region; aggregate: total }",
            });
            if (baseStored) {
               expectNotFromRollup(run);
               expect(run.servedFrom).toBe("storage");
            } else {
               expectServedFromRollup(run);
            }
         },
      );

      it(
         baseStored
            ? "answers a query no rollup covers from the base's stored table"
            : "answers a query no rollup covers live, as the base has no table",
         async () => {
            const run = await fixture.run({
               name: "uncovered",
               malloy: "run: daily -> { group_by: d; aggregate: total }",
            });
            expectNotFromRollup(run);
            expect(run.servedFrom).toBe(baseStored ? "storage" : null);
         },
      );

      if (baseStored) {
         it("still serves the base's own measures from its stored table", async () => {
            const run = await fixture.run({
               name: "the base",
               malloy:
                  "run: daily -> { group_by: region, category; aggregate: s is amount_sum.sum() }",
            });
            expectNotFromRollup(run);
            expect(run.servedFrom).toBe("storage");
         });
      }
   });
}
