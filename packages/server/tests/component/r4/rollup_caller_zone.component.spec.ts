// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for a caller's `timezone:` over a rollup built in the model's zone
 * (coverage row `b-tz-caller`). The rollup follows the served copy's rule:
 * what the build evaluated must not depend on the zone, and what the query
 * evaluates itself it evaluates in its own zone over the stored values, as
 * live does.
 *
 * - **Served:** a stored `date` column, grouped or filtered; a stored
 *   `timestamp` the query truncates or compares itself; the same read
 *   through a join to the rollup's source.
 * - **Live:** a stored truncation (a computed dimension over a timestamp,
 *   or a query base's truncated column), a base `where:` reading a time,
 *   a measure that reads a time (an hour extracted in the build's zone), and either read through a join.
 *
 * Each runs in four zones (one with a 45-minute offset), on a database and
 * a `storage=` rollup, for `#(preaggregate)` and `#@ preaggregate`, and
 * every answer equals live.
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

const ZONES = [
   "UTC",
   "America/Los_Angeles",
   "Asia/Kolkata",
   "Pacific/Chatham",
] as const;

// Times near midnight UTC, so most of them fall on another day in each zone.
const seedSql = [
   `CREATE TABLE orders_tz AS SELECT * FROM (VALUES
      (1, 10.0, 'A', TIMESTAMP '2026-01-01 00:30:00', DATE '2026-01-02'),
      (2, 20.0, 'A', TIMESTAMP '2026-01-01 23:50:00', DATE '2026-01-03'),
      (3, 30.0, 'B', TIMESTAMP '2026-01-02 11:40:00', DATE '2026-01-02'),
      (4,  5.0, 'B', TIMESTAMP '2026-01-31 22:00:00', DATE '2026-02-01'),
      (5, 12.5, 'C', TIMESTAMP '2026-02-01 03:15:00', DATE '2026-02-02'),
      (6,  7.5, 'C', TIMESTAMP '2026-02-01 13:00:00', DATE '2026-02-03')
    ) AS t(id, amount, category, order_time, ship_date)`,
   `CREATE TABLE cats_tz AS SELECT DISTINCT category FROM orders_tz`,
];

const ROUTES = {
   "#(preaggregate)": (grain: string[], p: string) =>
      `#(preaggregate) grain=[${grain.map((g) => `"${g}"`).join(", ")}]${p}`,
   "#@ preaggregate": (grain: string[], p: string) =>
      `#@ preaggregate grain="${grain.join(",")}"${p}`,
};

function model(route: keyof typeof ROUTES, p: string): string {
   const r = (grain: string[]) => ROUTES[route](grain, p);
   return `##! experimental { persistence }
source: timed is duckdb.table('orders_tz') extend {
  dimension: order_day is order_time.day
  ${r(["category", "order_time", "ship_date"])}
  measure: total is amount.sum()
  ${r(["category", "order_day"])}
  measure: day_total is amount.sum()
  ${r(["category"])}
  measure: hour_total is sum(hour(order_time))
}
// A base whose where: reads a time: the build filtered its rows in the
// model's zone.
source: recent is duckdb.table('orders_tz') extend {
  where: order_time > @2026-01-01 12:00
  ${r(["category", "ship_date"])}
  measure: recent_total is amount.sum()
}
// A base that is a query: its day is a truncation stored as a column, with
// its timeframe.
source: daily is duckdb.table('orders_tz') -> {
  group_by: d is order_time.day, category
  aggregate: amt is amount.sum()
} extend {
  ${r(["category", "d"])}
  measure: daily_total is amt.sum()
}
source: lines is duckdb.table('orders_tz') extend {
  dimension: line_day is order_time.day
  ${r(["category", "ship_date"])}
  measure: line_total is amount.sum()
  ${r(["category", "line_day"])}
  measure: line_day_total is amount.sum()
  ${r(["category"])}
  measure: line_hours is sum(hour(order_time))
}
${
   // A storage= rollup is joined in its destination, so only from a source
   // stored there too.
   p
      ? `#@ persist storage="${DESTINATION}"
source: cats is duckdb.table('cats_tz') -> { select: * } extend {`
      : `source: cats is duckdb.table('cats_tz') extend {`
}
  primary_key: category
  join_many: lines on lines.category = category
}
`;
}

/** The query, run in zone `z`. */
type Case = { name: string; malloy: (z: string) => string };

const SERVED: Case[] = [
   {
      name: "groups by a stored date",
      malloy: (z) =>
         `run: timed -> { timezone: '${z}'; group_by: ship_date; aggregate: total; order_by: ship_date }`,
   },
   {
      name: "filters on a stored date",
      malloy: (z) =>
         `run: timed -> { timezone: '${z}'; where: ship_date = @2026-01-02; group_by: category; aggregate: total; order_by: category }`,
   },
   {
      name: "truncates a stored timestamp itself",
      malloy: (z) =>
         `run: timed -> { timezone: '${z}'; group_by: d is order_time.day; aggregate: total; order_by: d }`,
   },
   {
      name: "compares a stored timestamp with a literal itself",
      malloy: (z) =>
         `run: timed -> { timezone: '${z}'; where: order_time < @2026-01-02 06:00; group_by: category; aggregate: total; order_by: category }`,
   },
   {
      name: "a nest's own zone over a stored date",
      malloy: (z) =>
         `run: timed -> { group_by: category; nest: n is { timezone: '${z}'; group_by: ship_date; aggregate: total; order_by: ship_date }; order_by: category }`,
   },
];

const SERVED_THROUGH_JOIN: Case[] = [
   {
      name: "a joined rollup's measure",
      malloy: (z) =>
         `run: cats -> { timezone: '${z}'; group_by: category; aggregate: lines.line_total; order_by: category }`,
   },
   {
      name: "a joined rollup's measure filtered on its stored date",
      malloy: (z) =>
         `run: cats -> { timezone: '${z}'; where: lines.ship_date = @2026-01-02; group_by: category; aggregate: lines.line_total; order_by: category }`,
   },
];

const LIVE: Case[] = [
   {
      name: "groups by a stored truncation",
      malloy: (z) =>
         `run: timed -> { timezone: '${z}'; group_by: order_day; aggregate: day_total; order_by: order_day }`,
   },
   {
      name: "filters on a stored truncation",
      malloy: (z) =>
         `run: timed -> { timezone: '${z}'; where: order_day = @2026-01-01; group_by: category; aggregate: day_total; order_by: category }`,
   },
   {
      name: "a measure that reads a time",
      malloy: (z) =>
         `run: timed -> { timezone: '${z}'; group_by: category; aggregate: hour_total; order_by: category }`,
   },
   {
      name: "groups by a day a query base truncated",
      malloy: (z) =>
         `run: daily -> { timezone: '${z}'; group_by: d; aggregate: daily_total; order_by: d }`,
   },
   {
      name: "a base whose where: reads a time",
      malloy: (z) =>
         `run: recent -> { timezone: '${z}'; group_by: ship_date; aggregate: recent_total; order_by: ship_date }`,
   },
   {
      name: "a stored truncation through a join",
      malloy: (z) =>
         `run: cats -> { timezone: '${z}'; group_by: lines.line_day; aggregate: lines.line_day_total; order_by: line_day }`,
   },
   {
      name: "a measure that reads a time, through a join",
      malloy: (z) =>
         `run: cats -> { timezone: '${z}'; group_by: category; aggregate: lines.line_hours; order_by: category }`,
   },
];

function expectRollup(run: R4Run): void {
   expect(rowDifference(run.live, run.served)).toBeUndefined();
   expect(run.servedBy?.kind).toBe("rollup");
}

function expectLive(run: R4Run): void {
   expect(rowDifference(run.live, run.served)).toBeUndefined();
   expect(run.servedBy?.kind).not.toBe("rollup");
}

for (const route of Object.keys(ROUTES) as (keyof typeof ROUTES)[]) {
   for (const [placement, p] of [
      ["database", ""],
      ["storage=", ` storage="${DESTINATION}"`],
   ] as const) {
      describe(`R4: a caller's time zone over a ${route} rollup [${placement}]`, () => {
         let restore: () => void;
         let fixture: R4Fixture;
         beforeAll(async () => {
            restore = installSettings({
               env: {
                  PUBLISHER_MATERIALIZE: "true",
                  PERSIST_STORAGE_MODE: "on",
               },
            }).restore;
            fixture = await buildScenario({
               files: { "m.malloy": model(route, p) },
               seedSql,
               preaggregate: true,
            });
         });
         afterAll(async () => {
            restore();
            await disposeScenarios();
         });

         it("plans every rollup, so each case reads the one it names", () => {
            expect(fixture.rollupRefusals).toEqual([]);
         });
         for (const zone of ZONES) {
            for (const c of SERVED) {
               it(`${c.name}, in ${zone}: served from the rollup, equal to live`, async () => {
                  expectRollup(
                     await fixture.run({
                        name: c.name,
                        malloy: c.malloy(zone),
                     }),
                  );
               });
            }
            for (const c of SERVED_THROUGH_JOIN) {
               it(`${c.name}, in ${zone}: served from the rollup through the join, equal to live`, async () => {
                  expectRollup(
                     await fixture.run({
                        name: c.name,
                        malloy: c.malloy(zone),
                     }),
                  );
               });
            }
            for (const c of LIVE) {
               it(`${c.name}, in ${zone}: answered live, equal to live`, async () => {
                  expectLive(
                     await fixture.run({
                        name: c.name,
                        malloy: c.malloy(zone),
                        expectLive: true,
                     }),
                  );
                  // Declined for the zone, not for anything else.
                  fixture.setBindings(fixture.bindings);
                  const { path } = await fixture.runAsIs(c.malloy(zone));
                  expect(
                     (path?.considered ?? []).map((x) => [x.kind, x.reason]),
                  ).toContainEqual(["rollup", "TIME_ZONE"]);
               });
            }
         }

         it("a declined read differs from the stored one in another zone, which is why it stays live", async () => {
            const read = async (z: string) => {
               fixture.setBindings([]);
               try {
                  return (
                     await fixture.runAsIs(
                        `run: timed -> { timezone: '${z}'; group_by: order_day; aggregate: day_total; order_by: order_day }`,
                     )
                  ).rows;
               } finally {
                  fixture.setBindings(fixture.bindings);
               }
            };
            const utc = await read("UTC");
            for (const zone of ZONES.slice(1)) {
               expect(rowDifference(utc, await read(zone))).toBeDefined();
            }
         });
      });
   }
}
