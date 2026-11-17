// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Conformance shapes for cases the fixture did not reach: rollups read
 * through named views and extensions, rollups over typed and zoned bases,
 * persisted sources over a zoned base, a rolling window under `#@ persist`, joins declared on or read by a
 * persisted source, persisted views over persisted and materialized sources,
 * and `#(materialize)` over colocated and `storage=` sources joined in one
 * model. Each is served like every other shape (conformance.component.spec.ts):
 * every answer equal to live, at the tier it names.
 */

import type { Shape } from "./shapes";

const HEADER = "##! experimental { persistence }";

export const COVERAGE_SHAPES: Shape[] = [
   {
      name: "rollup read through named views and extensions",
      about: "A rollup (v1 #@ preaggregate; v2 #(preaggregate)) answering a named view, a view refined from it, and an extension of its base that adds a dimension over the grain. Join-to-rollup is r4/rollup_coverage, whose build checks the parent key the leg needs",
      files: {
         "m.malloy": `${HEADER}
import "raw.malloy"

source: orders is orders_raw extend {
  @PREAGG(region)
  measure: total is amount.sum()
  @PREAGG(region)
  measure: n is count()
  view: by_region is { group_by: region; aggregate: total, n }
  view: by_region_sorted is by_region + { order_by: region }
}

source: orders_ext is orders extend {
  dimension: zone is upper(region)
}
`,
      },
      queries: [
         {
            name: "a named view",
            malloy: "run: orders -> by_region",
            expect: "rollup",
         },
         {
            name: "a view refined from another",
            malloy: "run: orders -> by_region_sorted",
            expect: "rollup",
         },
         {
            name: "an extension, by a grain dimension",
            malloy: "run: orders_ext -> { group_by: region; aggregate: total }",
            expect: "rollup",
         },
         {
            name: "an extension, by its own dimension over the grain",
            malloy:
               "run: orders_ext -> { group_by: zone; aggregate: total, n }",
            expect: "rollup",
         },
      ],
   },
   {
      name: "rollup over a base with typed columns",
      about: "A colocated rollup over a base whose grain is a DECIMAL or a TIMESTAMPTZ, whose measures sum a BIGINT past 2^53 and a DECIMAL, and which carries STRUCT and MAP columns outside the grain. In a storage= destination the BIGINT sum comes back rounded and the TIMESTAMPTZ grain as a timestamp: known gaps, in r4/rollup_coverage. An array column is the next shape: v1 refuses it as a fan-out join",
      variants: ["database"],
      files: {
         "m.malloy": `${HEADER}

source: typed is duckdb.sql("""
  SELECT * FROM (VALUES
    (CAST('9007199254740993' AS BIGINT), CAST(1.25 AS DECIMAL(10,2)),
     TIMESTAMPTZ '2024-01-01 10:00:00+00', 'a', {'k': 1}, MAP {'x': 1}),
    (CAST('9007199254740991' AS BIGINT), CAST(1.25 AS DECIMAL(10,2)),
     TIMESTAMPTZ '2024-01-01 10:00:00+00', 'a', {'k': 2}, MAP {'y': 2}),
    (CAST('-9007199254740993' AS BIGINT), CAST(2.50 AS DECIMAL(10,2)),
     TIMESTAMPTZ '2024-06-30 23:30:00+00', 'b', {'k': 3}, MAP {'z': 3})
  ) AS t(big, dec, tstz, g, st, mp)
""") extend {
  @PREAGG(g)
  measure: big_sum is big.sum()
  @PREAGG(g)
  measure: dec_sum is dec.sum()
  @PREAGG(dec)
  measure: n_by_dec is count()
  @PREAGG(tstz)
  measure: n_by_tstz is count()
}
`,
      },
      queries: [
         {
            name: "a BIGINT sum past 2^53",
            malloy:
               "run: typed -> { group_by: g; aggregate: big_sum; order_by: g }",
            expect: "rollup",
            gaps: {
               "database@parquet": {
                  kind: "wrong",
                  reason:
                     "F-13: DuckLake writes the sum's HUGEINT column to Parquet as a DOUBLE while its catalog says HUGEINT, so the rollup answers -9007199254740992 for -9007199254740993; fixed by the DECIMAL(38,0) build cast and the 128-bit read guard in #1846",
                  issue: "#1846",
               },
            },
         },
         {
            name: "a DECIMAL sum",
            malloy:
               "run: typed -> { group_by: g; aggregate: dec_sum; order_by: g }",
            expect: "rollup",
         },
         {
            name: "a DECIMAL grain",
            malloy:
               "run: typed -> { group_by: dec; aggregate: n_by_dec; order_by: dec }",
            expect: "rollup",
         },
         {
            name: "a TIMESTAMPTZ grain",
            malloy:
               "run: typed -> { group_by: tstz; aggregate: n_by_tstz; order_by: tstz }",
            expect: "rollup",
         },
      ],
   },
   {
      name: "rollup over a base with an array column",
      about: "A #(preaggregate) rollup over a base carrying an array column outside its grain (v1 #@ preaggregate refuses that base as a fan-out join and fails the load: r4/coverage_refusals)",
      variants: ["materialize", "materialize_database"],
      files: {
         "m.malloy": `${HEADER}

source: listed is duckdb.sql("""
  SELECT * FROM (VALUES ('a', 1.5, [1, 2]), ('a', 2.0, [3]), ('b', 4.0, []))
    AS t(g, amount, lst)
""") extend {
  @PREAGG(g)
  measure: total is amount.sum()
}
`,
      },
      queries: [
         {
            name: "a sum by the grain",
            malloy:
               "run: listed -> { group_by: g; aggregate: total; order_by: g }",
            expect: "rollup",
         },
      ],
   },
   {
      name: "rollup over a zoned base",
      about: "A rollup declared on a source that sets its own timezone:, as v1 #@ preaggregate and as v2 #(preaggregate): both are refused (preaggregate-not-built, reason time_zone), so every query answers live",
      files: {
         "m.malloy": `${HEADER}
import "raw.malloy"

source: zoned is orders_raw extend {
  timezone: 'America/New_York'
  dimension: created_day is created_at.day
  @PREAGG(created_day)
  measure: total is amount.sum()
  @PREAGG(region)
  measure: n is count()
}
`,
      },
      queries: [
         {
            name: "by a zoned truncation in the grain",
            malloy:
               "run: zoned -> { group_by: created_day; aggregate: total; order_by: created_day }",
            expect: "live",
         },
         {
            name: "by a grain that reads no zone",
            malloy: "run: zoned -> { group_by: region; aggregate: n }",
            expect: "live",
         },
      ],
   },
   {
      name: "rollup over non-ASCII names",
      about: "A rollup on a source, grain dimension and measures named with non-ASCII and CJK identifiers",
      files: {
         "m.malloy": `${HEADER}
import "raw.malloy"

source: bestellungen is orders_raw extend {
  dimension: 地域 is region
  @PREAGG(地域)
  measure: summe is amount.sum()
  @PREAGG(地域)
  measure: 件数 is count()
}
`,
      },
      queries: [
         {
            name: "by the non-ASCII grain",
            malloy:
               "run: bestellungen -> { group_by: 地域; aggregate: summe, 件数 }",
            expect: "rollup",
         },
      ],
   },
   {
      name: "array queries under persist",
      about: "##! experimental.array_queries: a persisted query that stores an array query's result, and a persisted source that stores the array a caller's array query reads",
      variants: ["database", "storage", "materialize", "materialize_database"],
      files: {
         "m.malloy": `##! experimental { persistence array_queries }

source: carts is duckdb.sql("""
  SELECT * FROM (VALUES
    (1, 'a', [{'sku': 'x', 'price': 1.5}, {'sku': 'y', 'price': 2.0}]),
    (2, 'b', [{'sku': 'x', 'price': 4.0}]),
    (3, 'a', [])
  ) AS t(id, g, items)
""")

@PERSIST(cart_totals)
source: cart_totals is carts -> {
  select: id, g, t is items -> { aggregate: s is price.sum() }
} extend {
  measure: total is t.sum()
}

@PERSIST(cart_rows)
source: cart_rows is carts -> { select: id, g, items }
`,
      },
      queries: [
         {
            name: "a stored array-query result",
            malloy:
               "run: cart_totals -> { group_by: g; aggregate: total; order_by: g }",
         },
         {
            name: "a caller's array query over a stored array",
            malloy:
               "run: cart_rows -> { select: id, s is items -> { aggregate: t is price.sum() }; order_by: id }",
         },
      ],
   },
   {
      name: "a classified persisted source",
      about: "#(classification) on a persisted query's output fields, under a #(carries) ceiling on the source",
      files: {
         "m.malloy": `${HEADER}
import "raw.malloy"

#(carries) ["pii", "none"]
@PERSIST(classified)
source: classified is orders_raw -> {
  group_by:
    #(classification) "pii"
    customer_id
    #(classification) "none"
    region
  aggregate:
    #(classification) "none"
    total is amount.sum()
}
`,
      },
      queries: [
         {
            name: "a none-classed column",
            malloy:
               "run: classified -> { group_by: region; aggregate: t is total.sum() }",
         },
         {
            name: "a pii-classed column",
            malloy:
               "run: classified -> { group_by: customer_id; aggregate: t is total.sum(); order_by: customer_id }",
         },
      ],
   },
   {
      name: "persist with a rolling window",
      about: "#@ persist on a query whose body filters on now (refused, current_time: built, it would bake the window in as of the build, so it answers live), and on one whose extend block does (built, the window applied at read). The window is wide, so the goldens hold still; r4/persist_rolling_window_coverage cuts the rows at a pinned clock",
      variants: ["storage", "database"],
      files: {
         "m.malloy": `${HEADER}
import "raw.malloy"

@PERSIST(recent_body)
source: recent_body is orders_raw -> {
  where: created_at >= now - 3650 days
  select: id, region, amount, created_at
} extend {
  measure: total is amount.sum()
}

@PERSIST(recent_read)
source: recent_read is orders_raw -> {
  select: id, region, amount, created_at
} extend {
  where: created_at >= now - 3650 days
  measure: total is amount.sum()
}
`,
      },
      queries: [
         {
            name: "a window in the persisted query",
            malloy:
               "run: recent_body -> { group_by: region; aggregate: total }",
            expect: "live",
         },
         {
            name: "a window in the extend block",
            malloy:
               "run: recent_read -> { group_by: region; aggregate: total }",
         },
      ],
   },
   {
      name: "persist over a zoned base",
      about: "A persisted refinement of a base that sets only timezone: (it inherits the zone), a persisted query stage that sets the zone itself, and a warehouse source reading both through joins",
      files: {
         "m.malloy": `${HEADER}
import "raw.malloy"

source: orders_ny is orders_raw extend {
  timezone: 'America/New_York'
}

@PERSIST(inherits_zone)
source: inherits_zone is orders_ny extend {
  measure: total is amount.sum()
}

@PERSIST(sets_zone)
source: sets_zone is orders_raw -> {
  timezone: 'America/New_York'
  group_by: id, created_at, h is hour(created_at), d is created_at.day
} extend { primary_key: id }

source: reader is orders_raw extend {
  join_one: z is inherits_zone on z.id = id
  join_one: s is sets_zone on s.id = id
}
`,
      },
      queries: [
         {
            name: "the inherited zone: hour(ts)",
            malloy:
               "run: inherits_zone -> { group_by: h is hour(created_at); aggregate: total }",
         },
         {
            name: "the inherited zone: a day truncation",
            malloy:
               "run: inherits_zone -> { group_by: d is created_at.day; aggregate: total; order_by: d }",
         },
         {
            name: "the zone-setting stage: its stored hour",
            malloy:
               "run: sets_zone -> { group_by: h; aggregate: n is count() }",
         },
         {
            name: "through a join: the zone-setting stage's stored day",
            malloy: "run: reader -> { group_by: s.d; aggregate: n is count() }",
            // A day stored in New York, read through a join in the default zone.
            expect: "live",
         },
         {
            name: "through a join: a truncation of the inherited-zone column",
            malloy:
               "run: reader -> { group_by: zd is z.created_at.day; aggregate: n is count() }",
            // Colocated, the join is on one connection and the truncation is
            // computed in the reader's zone over the stored instant; stored,
            // the joined table is on another connection.
            expect: { storage: "live", materialize: "live" },
         },
      ],
   },
   {
      name: "joins under persist",
      about: "A persisted query reading through a join_many and through a join_cross, and a persisted source that declares a join_many to a warehouse source",
      files: {
         "m.malloy": `${HEADER}
import "raw.malloy"

@PERSIST(line_totals)
source: line_totals is orders_raw extend {
  join_many: l is lines_raw on l.order_id = id
} -> {
  group_by: region
  aggregate: lines is l.count(), avg_qty is l.qty.avg(), n is count()
}

@PERSIST(segment_totals)
source: segment_totals is orders_raw extend {
  join_cross: c is customers_raw on c.id = customer_id
} -> {
  group_by: segment is c.segment
  aggregate: t is amount.sum()
}

@PERSIST(with_lines)
source: with_lines is orders_raw -> {
  select: id, region, amount
} extend {
  join_many: l is lines_raw on l.order_id = id
  measure: total is amount.sum()
}
`,
      },
      queries: [
         {
            name: "a persisted query through a join_many",
            malloy:
               "run: line_totals -> { group_by: region; aggregate: c is lines.sum(), a is avg_qty.max(), nn is n.sum() }",
         },
         {
            name: "a persisted query through a join_cross",
            malloy:
               "run: segment_totals -> { group_by: segment; aggregate: tt is t.sum() }",
         },
         {
            name: "the declared join_many, unread",
            malloy: "run: with_lines -> { group_by: region; aggregate: total }",
         },
         {
            name: "the declared join_many, read",
            malloy:
               "run: with_lines -> { group_by: region; aggregate: total, lines is l.count() }",
            // The warehouse source it joins is not in the destination.
            expect: { storage: "live", materialize: "live" },
         },
      ],
   },
   {
      name: "persisted view over a persisted source",
      about: "#@ persist on a view of a source that is itself persisted (v1 #@ persist; v2 #(materialize))",
      files: {
         "m.malloy": `${HEADER}
import "raw.malloy"

@PERSIST(pv_base)
source: pv_base is orders_raw -> {
  select: id, region, status, amount
} extend {
  measure: total is amount.sum()
  @PVIEW(pv_base_by_region)
  view: by_region is { group_by: region; aggregate: total }
}
`,
      },
      queries: [
         {
            name: "the view called",
            malloy: "run: pv_base -> by_region",
            expect: "view",
         },
         {
            name: "the persisted source itself",
            malloy: "run: pv_base -> { group_by: status; aggregate: total }",
         },
      ],
   },
   {
      name: "materialize: colocated and storage= in one model",
      about: "A colocated #(materialize) dimension source and a storage= #(materialize) fact source in one model, each alone and joined (fixed annotations, not rendered per variant)",
      variants: ["materialize"],
      files: {
         "m.malloy": `${HEADER}
import "raw.malloy"

#(materialize) name="mz_dim_colocated"
source: mz_dim is customers_raw extend {
  measure: customers is count()
}

#(materialize) storage="lake"
source: mz_fact is orders_raw extend {
  measure: total is amount.sum()
}
`,
         "joined.malloy": `${HEADER}
import "m.malloy"

source: mz_joined is mz_fact extend {
  join_one: c is mz_dim on c.id = customer_id
}
`,
      },
      queries: [
         {
            name: "the colocated source alone",
            malloy:
               "run: mz_dim -> { group_by: segment; aggregate: customers }",
            expect: "database",
         },
         {
            name: "the storage= source alone",
            malloy: "run: mz_fact -> { group_by: region; aggregate: total }",
         },
         {
            name: "the storage= source joined to the colocated one",
            model: "joined.malloy",
            malloy:
               "run: mz_joined -> { group_by: c.segment; aggregate: total }",
            expect: "live",
         },
      ],
   },
];
