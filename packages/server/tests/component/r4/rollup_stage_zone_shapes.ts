// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The shapes of rollup_stage_zone (DuckDB) and rollup_stage_zone_postgres:
 * a rollup over a base whose query stage computes its truncations in Los
 * Angeles, its seed, and the reads.
 */

// Times either side of the spring-forward (2024-03-10) and fall-back
// (2024-11-03) transitions in Los Angeles, and near midnight UTC.
export const STAGE_ZONE_SEED = [
   `CREATE TABLE ev AS SELECT * FROM (VALUES
      (1, 10.0, 'A', TIMESTAMP '2024-01-06 09:30:00'),
      (2, 20.0, 'A', TIMESTAMP '2024-01-07 03:15:00'),
      (3, 30.0, 'B', TIMESTAMP '2024-03-10 09:30:00'),
      (4,  5.0, 'B', TIMESTAMP '2024-03-10 10:30:00'),
      (5, 12.5, 'C', TIMESTAMP '2024-11-03 08:30:00'),
      (6,  7.5, 'C', TIMESTAMP '2024-11-03 09:30:00'),
      (7,  1.0, 'A', TIMESTAMP '2024-11-04 07:59:00')
    ) AS t(id, amount, category, ts)`,
];

/** How a rollup is declared: `#(rollup)`, or the older `#@ preaggregate`. */
export const ROUTES = {
   "#(rollup)": (grain: string, p: string) =>
      `#(rollup) grain=["${grain}"]${p}`,
   "#@ preaggregate": (grain: string, p: string) =>
      `#@ preaggregate grain="${grain}"${p}`,
};

/** The model: `table` reads the seeded table, `p` the rollup's placement. */
export function stageZoneModel(
   table: string,
   persist: string,
   p: string,
   route: keyof typeof ROUTES = "#(rollup)",
) {
   const r = (grain: string) => ROUTES[route](grain, p);
   return `##! experimental { persistence }
source: raw is ${table}
${persist}
source: daily is raw -> {
  timezone: 'America/Los_Angeles'
  group_by: d is ts.day, w is ts.week, h is ts.hour
  aggregate: amt is amount.sum()
} extend {
  ${r("d")}
  measure: day_total is amt.sum()
  ${r("w")}
  measure: week_total is amt.sum()
  ${r("h")}
  measure: hour_total is amt.sum()
}
// The stage inherits the zone of the base it runs on.
source: raw_la is ${table} extend { timezone: 'America/Los_Angeles' }
${persist}
source: inherited is raw_la -> {
  group_by: d is ts.day
  aggregate: amt is amount.sum()
} extend {
  ${r("d")}
  measure: inh_total is amt.sum()
}
source: rolled is daily extend {
  ${r("d")}
  measure: rolled_day is amt.sum()
  ${r("w")}
  measure: rolled_week is amt.sum()
  ${r("h")}
  measure: rolled_hour is amt.sum()
}
`;
}

/** Each read: the zone clause it sets, if any. */
export const READ_ZONES: [string, string][] = [
   ["no zone", ""],
   ["in Los Angeles", "timezone: 'America/Los_Angeles'; "],
   ["in New York", "timezone: 'America/New_York'; "],
];

/** Each query reads a truncation the base's query computed in Los Angeles. */
export const STAGE_ZONE_QUERIES: [string, (z: string) => string][] = [
   [
      "daily by d",
      (z) =>
         `run: daily -> { ${z}group_by: d; aggregate: day_total; order_by: d }`,
   ],
   [
      "daily by w",
      (z) =>
         `run: daily -> { ${z}group_by: w; aggregate: week_total; order_by: w }`,
   ],
   [
      "daily by h",
      (z) =>
         `run: daily -> { ${z}group_by: h; aggregate: hour_total; order_by: h }`,
   ],
   [
      "rolled by d",
      (z) =>
         `run: rolled -> { ${z}group_by: d; aggregate: rolled_day; order_by: d }`,
   ],
   [
      "rolled by w",
      (z) =>
         `run: rolled -> { ${z}group_by: w; aggregate: rolled_week; order_by: w }`,
   ],
   [
      "rolled by h",
      (z) =>
         `run: rolled -> { ${z}group_by: h; aggregate: rolled_hour; order_by: h }`,
   ],
   [
      "inherited by d",
      (z) =>
         `run: inherited -> { ${z}group_by: d; aggregate: inh_total; order_by: d }`,
   ],
   [
      "daily filtered on d",
      (z) =>
         `run: daily -> { ${z}where: d = @2024-03-10; aggregate: day_total }`,
   ],
];

/** Queries that read none of the zoned truncations. */
export const UNZONED_READS: [string, (z: string) => string][] = [
   ["daily total", (z) => `run: daily -> { ${z}aggregate: day_total }`],
   ["rolled total", (z) => `run: rolled -> { ${z}aggregate: rolled_day }`],
];
