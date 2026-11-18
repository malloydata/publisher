// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Rollup conformance shapes on a Postgres origin
 * (r4/rollup_postgres_parity.component.spec.ts). Each is a model file over
 * the seed below, its rollups marked `@PREAGG(grain, …)` and rendered by
 * {@link renderFiles} for the two tiers a `#(rollup)` has: database
 * (`materialize_database`, built in Postgres) and `storage=`
 * (`materialize`, built into a DuckLake destination). Every query's served
 * answer is compared with live's; `expect` says whether a rollup answers it
 * or it is declined and answered live. One shape is no rollup but a
 * persisted copy (`@PERSIST()`, `expect: "served"`), for the integers past
 * 2^53 a stored read must give as live does on both paths.
 */

import type { Shape } from "./shapes";

/**
 * A shape whose declined queries name the reason the router reports for the
 * rollup that would otherwise answer them.
 */
export type PostgresRollupShape = Shape & {
   queries: (Shape["queries"][number] & { reason?: string })[];
};

/** The Postgres seed every shape reads, in the warehouse's `public` schema. */
export const PG_SEED = [
   // u: unconstrained numeric; wide: wider than DECIMAL(38); big: past 2^53.
   `CREATE TABLE orders (
      id bigint PRIMARY KEY, customer_id bigint, region text, status text,
      amount double precision, price numeric(10,2), u numeric,
      wide numeric(50,10), big bigint, created_at timestamp,
      created_tz timestamptz, order_date date)`,
   // A NULL region, a NULL customer and one with no row in customers, NULL
   // measures, two instants across a year boundary, one on each daylight
   // saving change in New York.
   `INSERT INTO orders VALUES
      (1, 1, 'north', 'placed', 10.5, 1.10, 0.1, 0.1, 9007199254740993,
         '2024-12-31 23:30:00', '2024-12-31 23:30:00+00', '2024-12-31'),
      (2, 2, 'north', 'shipped', 20.25, 2.20, 0.2, 0.2, 9007199254740991,
         '2025-01-01 00:15:00', '2025-01-01 04:15:00+00', '2025-01-01'),
      (3, 1, 'south', 'shipped', NULL, 3.33, 1.005,
         12345678901234567890.0123456789, -9007199254740993,
         '2025-01-15 12:00:00', '2025-01-15 02:00:00+00', '2025-01-15'),
      (4, 3, 'south', 'returned', 7.75, NULL, NULL, NULL, NULL,
         '2025-02-01 00:00:00', '2025-02-01 03:00:00+00', '2025-02-01'),
      (5, NULL, NULL, 'placed', 3.0, 0.01, 0.3, 0.3, 1,
         '2025-02-14 18:45:00', '2025-02-14 23:45:00+00', '2025-02-14'),
      (6, 9, 'east', 'shipped', 100.0, 99.99, 2.5, 1.0000000001, 2,
         '2025-03-09 06:30:00', '2025-03-09 06:30:00+00', '2025-03-09'),
      (7, 2, 'east', 'shipped', 250.5, 0.05, 0.25, 2.5, 3,
         '2025-03-09 07:30:00', '2025-03-09 07:30:00+00', '2025-03-09'),
      (8, 3, 'north', 'placed', 0.1, 0.10, 0.1, 0.1, 9007199254740993,
         '2025-06-30 23:59:59', '2025-06-30 23:59:59+00', '2025-06-30'),
      (9, 4, 'west', 'shipped', 0.2, 0.20, 0.2, 0.2, 4,
         '2025-11-02 05:30:00', '2025-11-02 05:30:00+00', '2025-11-02'),
      (10, 1, 'west', 'returned', NULL, 10.00, 3.000, 7, 5,
         '2025-11-02 06:30:00', '2025-11-02 06:30:00+00', '2025-11-02'),
      (11, 2, NULL, 'shipped', 42.0, 4.20, 0.000001, 0.0000000001, 6,
         '2025-12-31 23:00:00', '2026-01-01 04:00:00+00', '2025-12-31'),
      (12, 3, 'south', 'placed', 5.5, 5.50, 0.7, 0.7, 7,
         '2026-01-01 00:00:00', '2026-01-01 05:00:00+00', '2026-01-01')`,
   // The persisted-copy shape's own table, so its copy re-roots no other
   // shape's source: the same big integers as orders.big.
   `CREATE TABLE big_copy AS SELECT id, region, big FROM orders`,
   // 200 values near 9e18, whose sums pass 1e21, where live's answer turns
   // from a string to an exponential JSON number.
   `CREATE TABLE huge AS SELECT g AS id,
      (ARRAY['north', 'south', 'east'])[1 + g % 3] AS region,
      9000000000000000000 + g AS big
      FROM generate_series(1, 200) AS g`,
   // Customer 5 has no orders; order 6's customer 9 has no row.
   `CREATE TABLE customers (id bigint PRIMARY KEY, segment text, home_region text)`,
   `INSERT INTO customers VALUES
      (1, 'smb', 'north'), (2, 'enterprise', 'east'), (3, 'smb', NULL),
      (4, 'consumer', 'west'), (5, 'consumer', 'south')`,
   // Orders 4, 9 and 12 have no lines; line 99 has no order.
   `CREATE TABLE lines (line_id bigint PRIMARY KEY, order_id bigint, sku text, qty bigint)`,
   `INSERT INTO lines VALUES
      (1, 1, 'a', 2), (2, 1, 'b', 1), (3, 2, 'a', 5), (4, 3, 'c', NULL),
      (5, 5, 'a', 1), (6, 6, 'b', 3), (7, 6, 'b', 4), (8, 7, 'c', 2),
      (9, 8, 'a', 1), (10, 10, 'c', 6), (11, 11, 'a', 2), (12, 99, 'z', 50)`,
];

/** The raw sources each shape starts from, on the `pg` connection. */
export const PG_RAW_MODEL = `source: orders_raw is pg.table('public.orders') extend { primary_key: id }
source: customers_raw is pg.table('public.customers') extend { primary_key: id }
source: lines_raw is pg.table('public.lines') extend { primary_key: line_id }
`;

const HEADER = `##! experimental { persistence }
import "raw.malloy"`;

/**
 * The querying source of a join-to-rollup: in the `storage=` tier it is
 * stored in the rollup's destination (`#(materialize) storage=`), the only
 * place a `storage=` rollup is read through a join.
 */
const joinParent = (
   storage: boolean,
) => `${storage ? '#(materialize) storage="lake"\n' : ""}source: j_parent is orders_raw extend {
  join_many: j_lines on j_lines.order_id = id
  join_one: j_cust with customer_id
  measure: order_n is count()
}`;

const joinShape = (storage: boolean): PostgresRollupShape => ({
   name: "joins",
   about: "A grain dimension read through a join_one, and a rollup read through a join_many and a join_one from another source (join-to-rollup), with the parent's own count beside it",
   variants: [storage ? "materialize" : "materialize_database"],
   files: {
      "joins.malloy": `${HEADER}

source: j_orders is orders_raw extend {
  join_one: c is customers_raw with customer_id
  dimension: segment is c.segment
  @PREAGG(segment)
  measure: seg_total is amount.sum()
  @PREAGG(segment)
  measure: seg_n is count()
}

source: j_lines is lines_raw extend {
  @PREAGG(order_id)
  measure: qty_total is qty.sum()
  @PREAGG(order_id)
  measure: line_n is count()
  @PREAGG(order_id)
  measure: qty_hi is qty.max()
}

source: j_cust is customers_raw extend {
  @PREAGG(id)
  measure: cust_n is count()
}

${joinParent(storage)}
`,
   },
   queries: [
      {
         name: "by a grain dimension read through a join_one",
         model: "joins.malloy",
         malloy:
            "run: j_orders -> { group_by: segment; aggregate: seg_total, seg_n; order_by: segment }",
         expect: "rollup",
      },
      {
         name: "by the joined field itself, not the grain dimension",
         model: "joins.malloy",
         malloy:
            "run: j_orders -> { group_by: c.segment; aggregate: seg_total; order_by: segment }",
         expect: "live",
         reason: "GRAIN_TOO_COARSE",
      },
      {
         name: "through a join_many, by a parent dimension",
         model: "joins.malloy",
         malloy:
            "run: j_parent -> { group_by: region; aggregate: j_lines.qty_total, j_lines.line_n, j_lines.qty_hi; order_by: region }",
         expect: "rollup",
      },
      {
         name: "through a join_many, beside the parent's own count",
         model: "joins.malloy",
         malloy:
            "run: j_parent -> { group_by: status; aggregate: order_n, j_lines.qty_total; order_by: status }",
         expect: "rollup",
      },
      {
         name: "through a join_one keyed on the grain, beside the parent's own count",
         model: "joins.malloy",
         malloy:
            "run: j_parent -> { group_by: region; aggregate: order_n, j_cust.cust_n; order_by: region }",
         expect: "rollup",
      },
      {
         name: "an aggregate the query writes through the join",
         model: "joins.malloy",
         malloy:
            "run: j_parent -> { group_by: region; aggregate: q is j_lines.qty.sum(); order_by: region }",
         expect: "live",
         reason: "MISSING_FIELD",
      },
   ],
});

export const ROLLUP_POSTGRES_SHAPES: PostgresRollupShape[] = [
   {
      name: "mergeable measures",
      about: "sum, count, min and max (of a number, a date and a timestamp), avg as a ratio of partials, a filtered count and arithmetic over partials; an avg or a distinct count the query writes is declined",
      files: {
         "measures.malloy": `${HEADER}

source: m_orders is orders_raw extend {
  @PREAGG(region)
  measure: total is amount.sum()
  @PREAGG(region)
  measure: n is count()
  @PREAGG(region)
  measure: lo is amount.min()
  @PREAGG(region)
  measure: hi is amount.max()
  @PREAGG(region)
  measure: first_day is order_date.min()
  @PREAGG(region)
  measure: last_seen is created_at.max()
  @PREAGG(region)
  measure: avg_amount is amount.sum() / count() { where: amount != null }
  @PREAGG(region)
  measure: shipped_n is count() { where: status = 'shipped' }
  @PREAGG(region)
  measure: spread is amount.max() - amount.min()
}
`,
      },
      queries: [
         {
            name: "every kind, by the grain",
            model: "measures.malloy",
            malloy:
               "run: m_orders -> { group_by: region; aggregate: total, n, lo, hi, avg_amount, shipped_n, spread; order_by: region }",
            expect: "rollup",
         },
         {
            name: "min and max of a date and a timestamp",
            model: "measures.malloy",
            malloy:
               "run: m_orders -> { group_by: region; aggregate: first_day, last_seen; order_by: region }",
            expect: "rollup",
         },
         {
            name: "merged across the grain (no group_by)",
            model: "measures.malloy",
            malloy:
               "run: m_orders -> { aggregate: total, n, lo, hi, avg_amount, shipped_n, spread }",
            expect: "rollup",
         },
         {
            name: "a having on a stored measure",
            model: "measures.malloy",
            malloy:
               "run: m_orders -> { group_by: region; aggregate: total; having: n > 2; order_by: region }",
            expect: "rollup",
         },
         {
            name: "an avg the query writes",
            model: "measures.malloy",
            malloy:
               "run: m_orders -> { group_by: region; aggregate: a is amount.avg(); order_by: region }",
            expect: "live",
            reason: "MISSING_FIELD",
         },
         {
            name: "a distinct count the query writes",
            model: "measures.malloy",
            malloy:
               "run: m_orders -> { group_by: region; aggregate: c is count(customer_id); order_by: region }",
            expect: "live",
            reason: "MISSING_FIELD",
         },
      ],
   },
   {
      name: "numeric and bigint precision",
      about: "Partials and grains over numeric(10,2), unconstrained numeric, numeric(50,10) and bigint past 2^53",
      files: {
         "precision.malloy": `${HEADER}

source: p_orders is orders_raw extend {
  @PREAGG(region)
  measure: price_sum is price.sum()
  @PREAGG(region)
  measure: u_sum is u.sum()
  @PREAGG(region)
  measure: wide_sum is wide.sum()
  @PREAGG(region)
  measure: big_sum is big.sum()
  @PREAGG(region)
  measure: u_lo is u.min()
  @PREAGG(region)
  measure: wide_hi is wide.max()
  @PREAGG(region)
  measure: big_hi is big.max()
  @PREAGG(region)
  measure: mixed is sum(u + price)
  @PREAGG(region)
  measure: u_avg is u.sum() / count() { where: u != null }
  @PREAGG(price)
  measure: n_by_price is count()
  @PREAGG(u)
  measure: n_by_u is count()
  @PREAGG(wide)
  measure: n_by_wide is count()
  @PREAGG(big)
  measure: n_by_big is count()
}
`,
      },
      queries: [
         {
            name: "a numeric(10,2) sum",
            model: "precision.malloy",
            malloy:
               "run: p_orders -> { group_by: region; aggregate: price_sum; order_by: region }",
            expect: "rollup",
         },
         {
            name: "an unconstrained numeric sum, min and ratio",
            model: "precision.malloy",
            malloy:
               "run: p_orders -> { group_by: region; aggregate: u_sum, u_lo, u_avg; order_by: region }",
            expect: "rollup",
         },
         {
            name: "a numeric(50,10) sum and max",
            model: "precision.malloy",
            malloy:
               "run: p_orders -> { group_by: region; aggregate: wide_sum, wide_hi; order_by: region }",
            expect: "rollup",
         },
         {
            name: "a bigint sum and max past 2^53",
            model: "precision.malloy",
            malloy:
               "run: p_orders -> { group_by: region; aggregate: big_sum, big_hi; order_by: region }",
            expect: "rollup",
         },
         {
            name: "a bigint sum and max past 2^53, re-aggregated across the grain",
            model: "precision.malloy",
            malloy: "run: p_orders -> { aggregate: big_sum, big_hi }",
            expect: "rollup",
         },
         {
            name: "a bigint grain filtered by an exact literal past 2^53",
            model: "precision.malloy",
            malloy:
               "run: p_orders -> { where: big = 9007199254740993; aggregate: n_by_big }",
            expect: "rollup",
         },
         {
            name: "a sum over a numeric expression",
            model: "precision.malloy",
            malloy:
               "run: p_orders -> { group_by: region; aggregate: mixed; order_by: region }",
            expect: "rollup",
         },
         {
            name: "merged across the grain",
            model: "precision.malloy",
            malloy:
               "run: p_orders -> { aggregate: price_sum, u_sum, wide_sum, mixed, u_avg }",
            expect: "rollup",
         },
         {
            name: "a numeric(10,2) grain",
            model: "precision.malloy",
            malloy:
               "run: p_orders -> { group_by: price; aggregate: n_by_price; order_by: price }",
            expect: "rollup",
         },
         {
            name: "an unconstrained numeric grain",
            model: "precision.malloy",
            malloy:
               "run: p_orders -> { group_by: u; aggregate: n_by_u; order_by: u }",
            expect: "rollup",
         },
         {
            name: "a numeric(50,10) grain",
            model: "precision.malloy",
            malloy:
               "run: p_orders -> { group_by: wide; aggregate: n_by_wide; order_by: wide }",
            expect: "rollup",
         },
         {
            name: "a bigint grain past 2^53",
            model: "precision.malloy",
            malloy:
               "run: p_orders -> { group_by: big; aggregate: n_by_big; order_by: big }",
            expect: "rollup",
         },
      ],
   },
   {
      name: "time grains",
      about: "Grains of a timestamp, its day and month truncations, a date and a timestamptz; a truncation the query takes of a grain; a caller's timezone: one that reads no time, truncates a stored timestamptz itself or filters a stored timestamp (served, in its own zone), and one that reads a stored day truncation (declined)",
      files: {
         "time.malloy": `${HEADER}

source: t_orders is orders_raw extend {
  dimension: created_day is created_at.day
  dimension: created_month is created_at.month
  dimension: tz_day is created_tz.day
  @PREAGG(created_at)
  measure: n_at is count()
  @PREAGG(created_day)
  measure: total_by_day is amount.sum()
  @PREAGG(created_month)
  measure: total_by_month is amount.sum()
  @PREAGG(order_date)
  measure: n_by_date is count()
  @PREAGG(tz_day)
  measure: n_by_tz_day is count()
  @PREAGG(created_tz)
  measure: n_tz is count()
  @PREAGG(region)
  measure: n_region is count()
}
`,
      },
      queries: [
         {
            name: "by a day grain",
            model: "time.malloy",
            malloy:
               "run: t_orders -> { group_by: created_day; aggregate: total_by_day; order_by: created_day }",
            expect: "rollup",
         },
         {
            name: "by a month grain",
            model: "time.malloy",
            malloy:
               "run: t_orders -> { group_by: created_month; aggregate: total_by_month; order_by: created_month }",
            expect: "rollup",
         },
         {
            name: "a month the query truncates from a timestamp grain",
            model: "time.malloy",
            malloy:
               "run: t_orders -> { group_by: m is created_at.month; aggregate: n_at; order_by: m }",
            expect: "rollup",
         },
         {
            name: "a year the query truncates from a date grain",
            model: "time.malloy",
            malloy:
               "run: t_orders -> { group_by: y is order_date.year; aggregate: n_by_date; order_by: y }",
            expect: "rollup",
         },
         {
            name: "by a day of a timestamptz",
            model: "time.malloy",
            malloy:
               "run: t_orders -> { group_by: tz_day; aggregate: n_by_tz_day; order_by: tz_day }",
            expect: "rollup",
         },
         {
            name: "a day the query truncates from a timestamptz grain",
            model: "time.malloy",
            malloy:
               "run: t_orders -> { group_by: d is created_tz.day; aggregate: n_tz; order_by: d }",
            expect: "rollup",
         },
         {
            name: "filtered on a timestamp grain",
            model: "time.malloy",
            malloy:
               "run: t_orders -> { where: created_at ? @2025; aggregate: n_at }",
            expect: "rollup",
         },
         {
            name: "filtered on a date grain",
            model: "time.malloy",
            malloy:
               "run: t_orders -> { where: order_date ? @2025-03; aggregate: n_by_date }",
            expect: "rollup",
         },
         {
            name: "a caller's timezone, reading no time",
            model: "time.malloy",
            malloy:
               "run: t_orders -> { timezone: 'America/New_York'; group_by: region; aggregate: n_region; order_by: region }",
            expect: "rollup",
         },
         {
            name: "a caller's timezone, by a day grain",
            model: "time.malloy",
            malloy:
               "run: t_orders -> { timezone: 'America/New_York'; group_by: created_day; aggregate: total_by_day; order_by: created_day }",
            expect: "live",
            reason: "TIME_ZONE",
         },
         {
            name: "a caller's timezone, truncating a timestamptz grain",
            model: "time.malloy",
            malloy:
               "run: t_orders -> { timezone: 'America/New_York'; group_by: d is created_tz.day; aggregate: n_tz; order_by: d }",
            expect: "rollup",
         },
         {
            name: "a caller's timezone, filtering a timestamp grain",
            model: "time.malloy",
            malloy:
               "run: t_orders -> { timezone: 'America/New_York'; where: created_at ? @2025; aggregate: n_at }",
            expect: "rollup",
         },
      ],
   },
   joinShape(true),
   joinShape(false),
   {
      name: "filters",
      about: "A two-dimension grain filtered by one dimension, by NULL, by an alternation and a negation; a filtered measure; a rollup over a source with its own where:; a filter on a field outside the grain and an extension that adds a where: are declined",
      files: {
         "filters.malloy": `${HEADER}

source: f_orders is orders_raw extend {
  @PREAGG(region, status)
  measure: total is amount.sum()
  @PREAGG(region, status)
  measure: n is count()
  @PREAGG(region, status)
  measure: shipped_total is amount.sum() { where: status = 'shipped' }
}

source: f_shipped is orders_raw extend {
  where: status = 'shipped'
  @PREAGG(region)
  measure: total is amount.sum()
}

source: f_ext is f_orders extend { where: status != 'returned' }
`,
      },
      queries: [
         {
            name: "filtered by a grain dimension",
            model: "filters.malloy",
            malloy:
               "run: f_orders -> { where: status = 'shipped'; group_by: region; aggregate: total, n; order_by: region }",
            expect: "rollup",
         },
         {
            name: "filtered to the NULL group",
            model: "filters.malloy",
            malloy:
               "run: f_orders -> { where: region = null; group_by: status; aggregate: total, n; order_by: status }",
            expect: "rollup",
         },
         {
            name: "filtered by an alternation",
            model: "filters.malloy",
            malloy:
               "run: f_orders -> { where: status ? 'shipped' | 'placed'; group_by: region; aggregate: n; order_by: region }",
            expect: "rollup",
         },
         {
            name: "filtered by a negation (NULL excluded)",
            model: "filters.malloy",
            malloy:
               "run: f_orders -> { where: region != 'north'; group_by: region; aggregate: total; order_by: region }",
            expect: "rollup",
         },
         {
            name: "a filtered measure",
            model: "filters.malloy",
            malloy:
               "run: f_orders -> { group_by: region; aggregate: shipped_total; order_by: region }",
            expect: "rollup",
         },
         {
            name: "a stored measure filtered in the query",
            model: "filters.malloy",
            malloy:
               "run: f_orders -> { group_by: region; aggregate: placed is total { where: status = 'placed' }; order_by: region }",
            // Declined on DuckDB too: the router reads a filter the query
            // puts on a stored measure as an aggregate of the filter's field.
            expect: "live",
            reason: "MISSING_FIELD",
         },
         {
            name: "a rollup over a source's own where:",
            model: "filters.malloy",
            malloy:
               "run: f_shipped -> { group_by: region; aggregate: total; order_by: region }",
            expect: "rollup",
         },
         {
            name: "filtered by a field outside the grain",
            model: "filters.malloy",
            malloy:
               "run: f_orders -> { where: amount > 10; group_by: region; aggregate: total; order_by: region }",
            expect: "live",
            reason: "GRAIN_TOO_COARSE",
         },
         {
            name: "an extension that adds a where:",
            model: "filters.malloy",
            malloy:
               "run: f_ext -> { group_by: region; aggregate: total; order_by: region }",
            expect: "live",
            reason: "GRAIN_TOO_COARSE",
         },
      ],
   },
   {
      name: "a persisted copy's big integers",
      about: "Not a rollup: a #(materialize) copy of the source, served from its table, whose bigint past 2^53 must answer as live's does, rounded and with live's type",
      files: {
         "copy.malloy": `${HEADER}

@PERSIST()
source: c_orders is pg.table('public.big_copy') extend {
  primary_key: id
  measure: big_sum is big.sum()
  measure: big_hi is big.max()
}
`,
      },
      queries: [
         {
            name: "a bigint past 2^53, selected",
            model: "copy.malloy",
            malloy: "run: c_orders -> { select: id, big; order_by: id }",
            expect: "served",
         },
         {
            name: "a bigint past 2^53, grouped by",
            model: "copy.malloy",
            malloy:
               "run: c_orders -> { group_by: big; aggregate: n is count(); order_by: big }",
            expect: "served",
         },
         {
            name: "a bigint sum and max past 2^53",
            model: "copy.malloy",
            malloy:
               "run: c_orders -> { group_by: region; aggregate: big_sum, big_hi; order_by: region }",
            expect: "served",
         },
         {
            name: "a filter on an exact literal past 2^53",
            model: "copy.malloy",
            malloy:
               "run: c_orders -> { where: big = 9007199254740993; select: id, big; order_by: id }",
            expect: "served",
         },
         {
            name: "big + 1",
            model: "copy.malloy",
            malloy:
               "run: c_orders -> { select: id, b1 is big + 1; order_by: id }",
            expect: "served",
         },
         {
            name: "a nested query over a bigint past 2^53",
            model: "copy.malloy",
            malloy:
               "run: c_orders -> { group_by: region; aggregate: big_hi; nest: rows is { select: id, big; order_by: id }; order_by: region }",
            expect: "served",
         },
      ],
   },
   {
      name: "sums past 1e21",
      about: "Sums of 200 values near 9e18, past 1e21, where live answers a JSON number in exponent form: a persisted copy's total, per-region, sum() + 1 and a pipeline over the sum, and a rollup's per-region and re-aggregated total",
      files: {
         "huge.malloy": `${HEADER}

@PERSIST()
source: h_copy is pg.table('public.huge') extend {
  primary_key: id
  measure: total is big.sum()
}

source: h_roll is pg.table('public.huge') extend {
  primary_key: id
  @PREAGG(region)
  measure: h_total is big.sum()
}
`,
      },
      queries: [
         {
            name: "a copy's total sum",
            model: "huge.malloy",
            malloy: "run: h_copy -> { aggregate: total }",
            expect: "served",
         },
         {
            name: "a copy's sum by region",
            model: "huge.malloy",
            malloy:
               "run: h_copy -> { group_by: region; aggregate: total; order_by: region }",
            expect: "served",
         },
         {
            name: "a copy's sum() + 1",
            model: "huge.malloy",
            malloy:
               "run: h_copy -> { group_by: region; aggregate: t1 is big.sum() + 1; order_by: region }",
            expect: "served",
         },
         {
            name: "a pipeline over a copy's sum",
            model: "huge.malloy",
            malloy:
               "run: h_copy -> { group_by: region; aggregate: total } -> { aggregate: grand is total.sum(), top is total.max() }",
            expect: "served",
         },
         {
            name: "a rollup's sum by region",
            model: "huge.malloy",
            malloy:
               "run: h_roll -> { group_by: region; aggregate: h_total; order_by: region }",
            expect: "rollup",
         },
         {
            name: "a rollup's sum re-aggregated across its grain",
            model: "huge.malloy",
            malloy: "run: h_roll -> { aggregate: h_total }",
            expect: "rollup",
         },
      ],
   },
];
