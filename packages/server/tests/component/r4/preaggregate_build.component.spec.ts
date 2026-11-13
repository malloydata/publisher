// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for the `#(preaggregate)` build side (materialization v2, phase 4): each
 * rollup the companion builds holds the partials its measures merge from, so
 * re-aggregating the stored table, in plain SQL and independently of Malloy,
 * equals the live query. Rollups are not served yet (that is the router leg);
 * this checks only what is built, and what is not.
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
   `CREATE TABLE orders_raw AS SELECT * FROM (VALUES
      (1, 10.0, 'A', 'east', 'AA', TIMESTAMP '2026-01-01 08:00:00'),
      (2, 20.0, 'A', 'west', 'BB', TIMESTAMP '2026-01-02 09:00:00'),
      (3, 30.0, 'B', 'east', 'AA', TIMESTAMP '2026-02-01 10:00:00'),
      (4,  5.0, 'B', 'east', 'CC', TIMESTAMP '2026-02-03 11:00:00'),
      (5, 12.5, 'C', 'west', 'AA', TIMESTAMP '2026-03-04 23:30:00')
    ) AS t(id, amount, category, region, carrier, order_time)`,
   `CREATE TABLE carriers_raw AS SELECT * FROM (VALUES
      ('AA', 5), ('BB', 7), ('CC', 11)
    ) AS t(code, fleet)`,
   `CREATE TABLE lines_raw AS SELECT * FROM (VALUES
      (1, 'p', 2.0), (1, 'q', 3.0), (2, 'p', 4.0), (4, 'q', 1.0), (5, 'p', 6.0)
    ) AS t(order_id, kind, qty)`,
];

const MODEL = `##! experimental.givens
given: REGION :: string is 'east'

source: carriers is duckdb.table('carriers_raw') extend { primary_key: code }
source: lines is duckdb.table('lines_raw')

source: orders is duckdb.table('orders_raw') extend {
  primary_key: id
  join_one: carriers with carrier
  join_many: lines on lines.order_id = id
  dimension:
    order_day is order_time.day
    line_kind is lines.kind
  #(preaggregate) grain=["category", "order_day"]
  measure: total is amount.sum()
  #(preaggregate) grain=["category"]
  measure: per_order is coalesce(amount.sum() / count(), 0)
  #(preaggregate) grain=["category"]
  measure: east_total is amount.sum() { where: region = 'east' }
  #(preaggregate) grain=["carrier"]
  measure: fleet is carriers.fleet.sum()
  #(preaggregate) grain=["carrier"]
  measure: top_fleet is carriers.fleet.max()
  #(preaggregate) grain=["category", "line_kind"]
  measure: line_total is amount.sum()
}

source: recent is duckdb.table('orders_raw') extend {
  where: order_time > now - 40 days
  #(preaggregate) grain=["category"]
  measure: recent_total is amount.sum()
}

#(access_filter) region = $REGION
source: gated is duckdb.table('orders_raw') extend {
  #(preaggregate) grain=["category", "region"]
  measure: gated_total is amount.sum()
}
`;

describe("R4: #(preaggregate) rollups build the partials their measures merge from", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   /** The colocated table of the rollup of `base` at `grain`. */
   const table = (base: string, grain: string[]) => {
      const prefix = `${base}__rollup__${grain.join("_")}__`;
      const names = Object.keys(fixture.rollupTables).filter((n) =>
         n.startsWith(prefix),
      );
      expect(names.length).toBe(1);
      return fixture.rollupTables[names[0] as string] as string;
   };

   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": MODEL },
         seedSql,
         preaggregate: true,
         clock: "2026-02-05 00:00:00",
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   const cases: [string, string, () => string][] = [
      [
         "a sum stored by day, re-summed to its category",
         "run: orders -> { group_by: category; aggregate: total }",
         () =>
            `SELECT category, CAST(SUM(total__partial) AS DOUBLE) AS total FROM ${table("orders", ["category", "order_day"])} GROUP BY 1`,
      ],
      [
         "a ratio of totals, from two partials",
         "run: orders -> { group_by: category; aggregate: per_order }",
         () =>
            `SELECT category, COALESCE(SUM(per_order__partial0) / SUM(per_order__partial1), 0) AS per_order FROM ${table("orders", ["category"])} GROUP BY 1`,
      ],
      [
         "a filtered sum",
         "run: orders -> { group_by: category; aggregate: east_total }",
         () =>
            `SELECT category, CAST(SUM(east_total__partial) AS DOUBLE) AS east_total FROM ${table("orders", ["category"])} GROUP BY 1`,
      ],
      [
         "a join-level sum keyed in the grain",
         "run: orders -> { group_by: carrier; aggregate: fleet }",
         () =>
            `SELECT carrier, SUM(fleet__partial) AS fleet FROM ${table("orders", ["carrier"])} GROUP BY 1`,
      ],
      [
         "a join-level max keyed in the grain (row 23)",
         "run: orders -> { group_by: carrier; aggregate: top_fleet }",
         () =>
            `SELECT carrier, MAX(top_fleet__partial) AS top_fleet FROM ${table("orders", ["carrier"])} GROUP BY 1`,
      ],
      [
         "a sum at a fan-out grain, grouped by it",
         "run: orders -> { group_by: category, line_kind; aggregate: line_total }",
         () =>
            `SELECT category, line_kind, CAST(SUM(line_total__partial) AS DOUBLE) AS line_total FROM ${table("orders", ["category", "line_kind"])} GROUP BY 1, 2`,
      ],
   ];
   for (const [name, malloy, truth] of cases) {
      it(`stores ${name}`, async () => {
         const run = await fixture.run({ name, malloy, truthSql: truth() });
         expect(rowDifference(run.live, run.truth ?? [])).toBeUndefined();
      });
   }

   it("builds nothing over a gated source, whose rollups no query reads yet (row 21)", () => {
      // The grain keeps the gate's field, so A4 alone would build it; while a
      // gated entry point skips every rollup, the table would cost a build
      // for nothing.
      expect(
         Object.keys(fixture.rollupTables).filter((n) =>
            n.startsWith("gated__rollup__"),
         ),
      ).toEqual([]);
   });

   it("builds nothing for a source whose rows read the current time (S25b)", () => {
      expect(
         Object.keys(fixture.rollupTables).filter((n) =>
            n.startsWith("recent__rollup__"),
         ),
      ).toEqual([]);
   });
});
