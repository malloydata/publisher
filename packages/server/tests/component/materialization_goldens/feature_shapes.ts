// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The goldens' feature packages: one per materialization feature the
 * conformance fixture does not reach, each over the fixture's synthetic data
 * (`data/*.csv`, and `raw.malloy`'s `orders_raw`, `customers_raw` and
 * `lines_raw`). Annotations are written as they are, not rendered per
 * variant. The models follow the R4 specs that test each feature; these
 * freeze the plan, the generated text and the path reports, not the rows,
 * which the R4 specs assert.
 *
 * `storage="lake"` names the destination the package harness wires.
 */

import { RAW_MODEL } from "../r4/conformance/data";
import type { GoldenQuery } from "./package_case";

export interface FeatureShape {
   /** Short and unique: names the golden files. */
   name: string;
   /** Model files; the first is where queries run unless they say otherwise. */
   files: Record<string, string>;
   queries: GoldenQuery[];
   /** Settings beyond the harness's; see `GoldenCase.env`. */
   env?: Record<string, string>;
}

/** Gated sources may be materialized and read from rollups. */
const GATED_ON = { PUBLISHER_MATERIALIZE_GATED_SOURCES: "true" };

const raw = { "raw.malloy": RAW_MODEL };

/** Gates over rollups and materialized sources, with the gated switch off and on. */
const GATED: Pick<FeatureShape, "files" | "queries"> = {
   files: {
      "m.malloy": `##! experimental { givens access_modifiers }
given: REGION :: string
given: ROLE :: string

import "raw.malloy"

#(access_filter) region = $REGION
source: by_column is orders_raw extend {
  #(preaggregate) grain=["status", "region"]
  measure: total is amount.sum()
}

#(authorize) 'admin' = $ROLE
source: by_lock is orders_raw extend {
  #(preaggregate) grain=["status"]
  measure: total is amount.sum()
}

#(access_filter) region = $REGION
#(materialize) storage="lake"
source: gated_mat is orders_raw extend {
  measure:
 n is count()
 total is amount.sum()
}

#(authorize) 'admin' = $ROLE
#(materialize) storage="lake"
source: locked_mat is orders_raw extend {
  measure: n is count()
}
`,
      ...raw,
   },
   queries: [
      {
         name: "a row filter over a rollup",
         malloy: "run: by_column -> { group_by: status; aggregate: total }",
         givens: { REGION: "north", ROLE: "viewer" },
      },
      {
         name: "a lock admitting over a rollup",
         malloy: "run: by_lock -> { group_by: status; aggregate: total }",
         givens: { REGION: "north", ROLE: "admin" },
      },
      {
         name: "a lock refusing",
         malloy: "run: by_lock -> { group_by: status; aggregate: total }",
         givens: { REGION: "north", ROLE: "viewer" },
      },
      {
         name: "a row filter over a materialized source",
         malloy: "run: gated_mat -> { group_by: status; aggregate: n, total }",
         givens: { REGION: "south", ROLE: "viewer" },
      },
      {
         name: "a lock over a materialized source",
         malloy: "run: locked_mat -> { group_by: region; aggregate: n }",
         givens: { REGION: "south", ROLE: "admin" },
      },
   ],
};

export const FEATURE_SHAPES: FeatureShape[] = [
   {
      name: "v1 preaggregate",
      files: {
         "m.malloy": `##! experimental { persistence composite_sources }
import "raw.malloy"

source: orders is orders_raw extend {
  dimension: m is created_at.month
  #@ preaggregate grain="region,m"
  measure: total is amount.sum()
  measure: n is count()
}
`,
         ...raw,
      },
      queries: [
         {
            name: "a covered grain",
            malloy: "run: orders -> { group_by: region; aggregate: total }",
         },
         {
            name: "the timeframe grain",
            malloy: "run: orders -> { group_by: m; aggregate: total }",
         },
         {
            name: "an uncovered dimension",
            malloy: "run: orders -> { group_by: status; aggregate: total }",
         },
         {
            name: "a measure the rollup does not hold",
            malloy: "run: orders -> { group_by: region; aggregate: n }",
         },
      ],
   },
   {
      name: "persisted views",
      files: {
         "m.malloy": `import "raw.malloy"

source: orders is orders_raw extend {
  measure:
    n is count()
    total is amount.sum()
  #@ persist storage="lake"
  view: by_region is { group_by: region; aggregate: n, total }
  #@ persist name="orders_by_status"
  view: by_status is { group_by: status; aggregate: n }
  #@ persist storage="lake"
  view: two_stage is { group_by: region; aggregate: n } -> { select: region, n; where: n > 1 }
  #@ persist storage="lake"
  view: recent is { where: created_at > now - 30 days; group_by: region; aggregate: n }
  view: plain is { group_by: status; aggregate: total }
}

source: north_orders is orders extend {
  where: region = 'north'
}
`,
         ...raw,
      },
      queries: [
         { name: "a storage= view", malloy: "run: orders -> by_region" },
         { name: "a colocated view", malloy: "run: orders -> by_status" },
         { name: "a two-stage view", malloy: "run: orders -> two_stage" },
         { name: "a rolling-window view", malloy: "run: orders -> recent" },
         { name: "an unpersisted view", malloy: "run: orders -> plain" },
         {
            name: "a persisted view refined",
            malloy: "run: orders -> by_region + { where: region = 'north' }",
         },
         {
            name: "a persisted view through an extension",
            malloy: "run: north_orders -> by_region",
         },
      ],
   },
   {
      name: "materialize with an inline join_one",
      files: {
         "m.malloy": `import "raw.malloy"

source: customers is customers_raw extend {
  dimension: loud_name is upper(name)
  measure: customer_count is count()
}

#(materialize) storage="lake" inline=["customers"]
source: orders_wide is orders_raw extend {
  join_one: customers with customer_id
  dimension: customer_region is customers.home_region
  measure:
    order_count is count()
    total is amount.sum()
    customers_ordering is count(customers.id)
  view: by_segment is { group_by: customers.segment; aggregate: order_count, total }
}

#(materialize) name="orders_wide_local" inline=["customers"]
source: orders_wide_local is orders_raw extend {
  join_one: customers with customer_id
  dimension:
    customer_region is customers.home_region
    customer_segment is customers.segment
  measure:
    order_count is count()
    total is amount.sum()
}
`,
         ...raw,
      },
      queries: [
         {
            name: "a joined dimension, storage=",
            malloy:
               "run: orders_wide -> { group_by: customers.segment; aggregate: order_count, total }",
         },
         {
            name: "a dimension reading the join",
            malloy:
               "run: orders_wide -> { group_by: customer_region; aggregate: customers_ordering }",
         },
         { name: "a declared view", malloy: "run: orders_wide -> by_segment" },
         {
            name: "the joined source's own measure",
            malloy:
               "run: orders_wide -> { group_by: region; aggregate: customers.customer_count }",
         },
         {
            name: "a joined dimension, colocated",
            malloy:
               "run: orders_wide_local -> { group_by: customers.loud_name; aggregate: total }",
         },
      ],
   },
   {
      name: "pair tables",
      files: {
         "m.malloy": `import "raw.malloy"

source: lines is lines_raw extend {
  measure: qty_total is qty.sum()
}

// One join_many: a one-hop pair table.
#(materialize) storage="lake" inline=["lines"]
source: orders_pair is orders_raw extend {
  join_many: lines on lines.order_id = id
  measure:
    order_n is count()
    amount_sum is amount.sum()
    line_n is lines.count()
    qty_sum is lines.qty.sum()
}

source: orders_with_lines is orders_raw extend {
  join_many: lines on lines.order_id = id
}

// A path of two join_manys: one table, a row flag per level.
#(materialize) storage="lake" inline=["orders", "orders.lines"]
source: customers_path is customers_raw extend {
  join_many: orders is orders_with_lines on orders.customer_id = id
  measure:
    customer_n is count()
    order_n is orders.count()
    qty_sum is orders.lines.qty.sum()
}

// A path and another join_many: two blocks of one table.
#(materialize) storage="lake" inline=["orders.lines", "big_orders"]
source: customers_blocks is customers_raw extend {
  join_many: orders is orders_with_lines on orders.customer_id = id
  join_many: big_orders is orders_raw on big_orders.customer_id = id
  measure:
    customer_n is count()
    qty_sum is orders.lines.qty.sum()
    big_n is big_orders.count()
}
`,
         ...raw,
      },
      queries: [
         {
            name: "one hop: child measures by a parent dimension",
            malloy:
               "run: orders_pair -> { group_by: region; aggregate: order_n, amount_sum, line_n, qty_sum }",
         },
         {
            name: "one hop: grouped by the child",
            malloy:
               "run: orders_pair -> { group_by: lines.sku; aggregate: qty_sum }",
         },
         {
            name: "one hop: parent only",
            malloy: "run: orders_pair -> { aggregate: order_n }",
         },
         {
            name: "a path: the leaf by the root",
            malloy:
               "run: customers_path -> { group_by: segment; aggregate: customer_n, qty_sum }",
         },
         {
            name: "a path: the middle level beside the leaf",
            malloy:
               "run: customers_path -> { group_by: segment; aggregate: order_n, qty_sum }",
         },
         {
            name: "blocks: the first block",
            malloy:
               "run: customers_blocks -> { group_by: segment; aggregate: qty_sum }",
         },
         {
            name: "blocks: the second block",
            malloy:
               "run: customers_blocks -> { group_by: segment; aggregate: big_n }",
         },
         {
            name: "blocks: both blocks",
            malloy:
               "run: customers_blocks -> { group_by: segment; aggregate: qty_sum, big_n }",
         },
      ],
   },
   {
      name: "hidden fields",
      env: GATED_ON,
      files: {
         "m.malloy": `##! experimental { givens access_modifiers }
given: GROUPS :: string[]

source: base is duckdb.table('data/orders.csv') include { private: id; internal: customer_id; public: * } extend {
  internal dimension: region_code is upper(region)
}

#(access_filter) region in $GROUPS
#(materialize) storage="lake"
source: orders is base extend {
  private dimension: secret is amount * 2
  dimension: north is region_code = 'NORTH'
  measure:
    total is amount.sum()
    n is count()
    secret_total is secret.sum()
}

#(materialize) storage="lake"
source: wide is duckdb.table('data/orders.csv') include { private: amount; public: * } extend {
  measure: amt is amount.sum()
}
`,
      },
      queries: [
         {
            name: "a private dimension's measure under a row filter",
            malloy:
               "run: orders -> { group_by: region; aggregate: total, n, secret_total }",
            givens: { GROUPS: ["north", "south"] },
         },
         {
            name: "an internal dimension",
            malloy: "run: orders -> { group_by: north; aggregate: n }",
            givens: { GROUPS: ["north", "east"] },
         },
         {
            name: "an internal raw column",
            malloy: "run: orders -> { select: customer_id; order_by: 1 }",
            givens: { GROUPS: ["west"] },
         },
         {
            name: "a measure over a private column",
            malloy: "run: wide -> { group_by: status; aggregate: amt }",
         },
      ],
   },
   {
      name: "given terms",
      files: {
         "m.malloy": `##! experimental { givens access_modifiers }
given:
  REGION :: string is 'north'
  MIN_AMT :: number is 0

import "raw.malloy"

// Its own given-reading where:.
#(materialize) storage="lake"
source: own is orders_raw extend {
  where: region = $REGION
  measure:
    n is count()
    total is amount.sum()
}

source: base is orders_raw extend {
  where: region = $REGION
  dimension: at_least is amount >= $MIN_AMT
}

// A where: it inherits from its base.
#(materialize) storage="lake"
source: inherited is base extend {
  measure:
    n is count()
    total is amount.sum()
}

// A where: on a base's given-reading dimension.
#(materialize) storage="lake"
source: by_base_dimension is base extend {
  where: at_least
  measure: n is count()
}

// Both: an inherited term and one of its own.
#(materialize) storage="lake"
source: both is base extend {
  where: amount >= $MIN_AMT
  measure: n is count()
}
`,
         ...raw,
      },
      queries: [
         {
            name: "own term, default given",
            malloy: "run: own -> { group_by: status; aggregate: n, total }",
         },
         {
            name: "own term, a caller's given",
            malloy: "run: own -> { group_by: status; aggregate: n, total }",
            givens: { REGION: "south" },
         },
         {
            name: "inherited term",
            malloy: "run: inherited -> { group_by: status; aggregate: n }",
            givens: { REGION: "east" },
         },
         {
            name: "a base's dimension",
            malloy:
               "run: by_base_dimension -> { group_by: status; aggregate: n }",
            givens: { REGION: "west", MIN_AMT: 50 },
         },
         {
            name: "inherited and own",
            malloy: "run: both -> { group_by: status; aggregate: n }",
            givens: { REGION: "north", MIN_AMT: 100 },
         },
      ],
   },
   {
      name: "rolling windows",
      files: {
         "m.malloy": `import "raw.malloy"

// Lower bound: rows newer than a moving instant.
#(materialize) storage="lake"
source: recent is orders_raw extend {
  where: created_at >= now - 30 days
  measure: n is count()
}

// Upper bound: rows older than a moving instant.
#(materialize) storage="lake"
source: older is orders_raw extend {
  where: created_at < now - 30 days
  measure: n is count()
}

// Two-sided.
#(materialize) storage="lake"
source: between is orders_raw extend {
  where: created_at >= now - 60 days and created_at < now - 30 days
  measure: n is count()
}

source: older_base is orders_raw extend {
  where: now - 30 days > created_at
}

// Inherited from a base.
#(materialize) storage="lake"
source: older_inherited is older_base extend {
  measure: n is count()
}
`,
         ...raw,
      },
      queries: [
         {
            name: "lower",
            malloy: "run: recent -> { group_by: region; aggregate: n }",
         },
         {
            name: "upper",
            malloy: "run: older -> { group_by: region; aggregate: n }",
         },
         {
            name: "two-sided",
            malloy: "run: between -> { group_by: region; aggregate: n }",
         },
         {
            name: "inherited upper",
            malloy:
               "run: older_inherited -> { group_by: region; aggregate: n }",
         },
         {
            name: "upper, narrowed by the query",
            malloy:
               "run: older -> { where: created_at < now - 60 days; group_by: region; aggregate: n }",
         },
      ],
   },
   {
      name: "extension tables and name=",
      files: {
         "m.malloy": `##! experimental.givens
given: REGION :: string is 'east'

import "raw.malloy"

source: orders is orders_raw extend {
  measure:
    n is count()
    total is amount.sum()
}

#(materialize) storage="lake" name="stored_t"
source: stored is orders extend {
  dimension: is_big is amount > 100
}

#(materialize) storage="lake" name="east_t"
source: east is stored extend {
  where: region = 'east'
}

#(materialize) storage="lake" name="east_big_t"
source: east_big is east extend {
  where: is_big
}

#(materialize) storage="lake" name="by_given_t"
source: by_given is stored extend {
  where: region = $REGION
}

#(materialize) storage="lake" name="renamed_t"
source: renamed is stored extend {
  rename: order_id is id
  measure: id_total is order_id.sum()
}

#(materialize) name="colocated_ext_t"
source: colocated_ext is orders extend {
  where: status = 'shipped'
}
`,
         ...raw,
      },
      queries: [
         {
            name: "the parent",
            malloy: "run: stored -> { group_by: is_big; aggregate: n, total }",
         },
         {
            name: "an extension with a fixed where:",
            malloy: "run: east -> { group_by: status; aggregate: n }",
         },
         {
            name: "two deep",
            malloy: "run: east_big -> { aggregate: n, total }",
         },
         {
            name: "a given-reading extension",
            malloy: "run: by_given -> { group_by: status; aggregate: n }",
            givens: { REGION: "west" },
         },
         {
            name: "a rename",
            malloy: "run: renamed -> { group_by: region; aggregate: id_total }",
         },
         {
            name: "a colocated extension",
            malloy: "run: colocated_ext -> { group_by: region; aggregate: n }",
         },
      ],
   },
   {
      name: "preaggregate rollups and a join to a rollup",
      files: {
         "m.malloy": `import "raw.malloy"

source: lines is lines_raw extend {
  #(preaggregate) grain=["order_id"]
  measure: qty_total is qty.sum()
  #(preaggregate) grain=["order_id", "sku"]
  measure: line_n is count()
}

source: orders is orders_raw extend {
  join_many: lines on lines.order_id = id
  dimension: m is created_at.month
  #(preaggregate) grain=["region"]
  measure: total is amount.sum()
  #(preaggregate) grain=["region", "m"] storage="lake"
  measure: n is count()
  view: by_region is { group_by: region; aggregate: total }
}
`,
         ...raw,
      },
      queries: [
         {
            name: "a colocated rollup",
            malloy: "run: orders -> { group_by: region; aggregate: total }",
         },
         {
            name: "a storage= rollup",
            malloy: "run: orders -> { group_by: m; aggregate: n }",
         },
         { name: "a declared view", malloy: "run: orders -> by_region" },
         {
            name: "a join to a rollup",
            malloy:
               "run: orders -> { group_by: region; aggregate: lines.qty_total }",
         },
         {
            name: "a join to a rollup, filtered by the parent",
            malloy:
               "run: orders -> { where: id = 4; aggregate: lines.qty_total, lines.line_n }",
         },
         {
            name: "not covered",
            malloy: "run: orders -> { group_by: status; aggregate: total }",
         },
      ],
   },
   {
      name: "time zones",
      files: {
         "m.malloy": `import "raw.malloy"

source: orders is orders_raw extend {
  dimension: m is created_at.month
  #(preaggregate) grain=["region", "m"]
  measure: total is amount.sum()
}

source: zoned is orders_raw extend {
  timezone: 'America/Los_Angeles'
  dimension: m is created_at.month
  #(preaggregate) grain=["region", "m"]
  measure: total is amount.sum()
}

source: rezoned is orders extend { timezone: 'America/New_York' }

#(materialize) storage="lake"
source: mat is orders_raw extend {
  dimension: created_day is created_at.day
  measure: n is count()
}

#(materialize) storage="lake" inline=["customers"]
source: wide is orders_raw extend {
  join_one: customers is customers_raw with customer_id
  measure: n is count()
}
`,
         ...raw,
      },
      queries: [
         {
            name: "a rollup, no zone",
            malloy: "run: orders -> { group_by: m; aggregate: total }",
         },
         {
            name: "a rollup under a query time zone",
            malloy:
               "run: orders -> { timezone: 'America/Los_Angeles'; group_by: m; aggregate: total }",
         },
         {
            name: "a rollup under a nest's time zone",
            malloy:
               "run: orders -> { group_by: region; nest: n is { timezone: 'America/Los_Angeles'; group_by: m; aggregate: total } }",
         },
         {
            name: "a zoned source's rollup",
            malloy: "run: zoned -> { group_by: m; aggregate: total }",
         },
         {
            name: "an extension that sets its own zone",
            malloy: "run: rezoned -> { group_by: m; aggregate: total }",
         },
         {
            name: "a materialized source's stored day, no zone",
            malloy: "run: mat -> { group_by: created_day; aggregate: n }",
         },
         {
            name: "a materialized source's stored day under a query time zone",
            malloy:
               "run: mat -> { timezone: 'America/Los_Angeles'; group_by: created_day; aggregate: n }",
         },
         {
            name: "an inlined join under a query time zone",
            malloy:
               "run: wide -> { timezone: 'America/Los_Angeles'; group_by: customers.segment; aggregate: n }",
         },
      ],
   },
   { name: "gated sources", ...GATED },
   { name: "gated sources, gated switch on", env: GATED_ON, ...GATED },
];
