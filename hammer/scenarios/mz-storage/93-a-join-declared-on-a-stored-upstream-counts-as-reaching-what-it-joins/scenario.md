---
id: a-join-declared-on-a-stored-upstream-counts-as-reaching-what-it-joins
tags: orchestration, chained, build-control, serve-correctness
package: sjs
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A join declared on a stored upstream counts as reaching what it joins

`daily` is a colocated persist source that declares `join_one: c is
daily_counts`, and `daily_counts` is stored in `lake`. `rollup` is a `storage=`
source over `daily` reading `c.order_count`. Nothing between `rollup` and
`daily` is an intermediate the build could carry, and `daily`'s table is a
warehouse table — so whether this chain is "chained" at all is decided by the
compiler's reach, not by what the downstream names: rendered with every stored
entry available, `rollup`'s SQL substitutes `daily`'s table and **inlines**
`daily_counts`, because the warehouse cannot read a lake table.

Three things must hold. The build must treat `rollup` as reading a stored
upstream (a `storage=` table was inlined), so under `strictUpstreams` it is
refused rather than quietly recomputing `daily_counts` — the table the
orchestrator pinned — and non-strict it is recomputed and the entry says so.
And the rows must mean what the entry says: after the warehouse changes, a
non-strict rebuild of `rollup` alone reads `daily`'s stale stored table and the
*recomputed* `daily_counts`, so one measure is stale and the other is fresh in
the same row. That split is exactly the skew `recomputed` warns of.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.sjs_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |
| 4            | 2026-01-02      | 25         |

## Model sjs.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.sjs_orders')

#@ persist name="sjs_daily_counts" storage=lake
source: daily_counts is orders -> {
  group_by: order_date
  aggregate: order_count is count()
}

#@ persist name="sjs_daily"
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
} extend {
  join_one: c is daily_counts on order_date = c.order_date
}

#@ persist name="sjs_rollup" storage=lake
source: rollup is daily -> {
  aggregate:
    grand_total is total_amount.sum()
    orders is c.order_count.sum()
}
```

## Publish

`daily_counts` lands in the lake, `daily` in the warehouse. `rollup` cannot be
built over `daily`'s table in the lake (it is not there), so it is recomputed:
`daily` substituted from its warehouse table, `daily_counts` inlined.

expect binding: daily_counts -> lake
expect binding: rollup -> lake
expect upstreams: rollup -> recomputed

## Query rollup

```malloy
run: rollup -> { select: grand_total, orders }
```

Expect:

| grand_total:num | orders:num |
| --------------- | ---------- |
| 375             | 4          |

## Build refused (orchestrated, strict, pkg=sjs)

Strict: `rollup` alone, reusing both stored upstreams. `daily_counts` is a
pinned stored table the build SQL would inline, and the build cannot stack on
`daily`, which is not in the lake — so strict refuses rather than recomputing.

- rollup -> sjs_rollup__g2 @ lake
  reference: daily
  reference: daily_counts

cites: materialized outside destination 'lake'

## Mutate orders_pg.sjs_orders

A new order on a date `daily` already holds, so both measures have a chance to
see it: the join from `daily`'s stored rows only reaches dates that table has.

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 99           | 2026-01-02      | 1000       |

## Build (orchestrated, pkg=sjs)

Non-strict: `rollup` alone, over the same two references. The build cannot
stack on `daily` (not in the lake), so it falls back to a recompute whose SQL
reads `daily`'s stored table as built — stale — and recomputes `daily_counts`
from the warehouse — fresh — and the entry says so.

- rollup -> sjs_rollup__g2 @ lake (recomputed)
  reference: daily
  reference: daily_counts

cites: materialized outside destination 'lake'

## Bind sjs

## Query rollup (again)

One row, two freshnesses: `grand_total` from the stale table (a fresh `daily`
would say 1375), `orders` counting the new order. That is what a `recomputed`
entry warns a reader to expect.

Expect:

| grand_total:num | orders:num |
| --------------- | ---------- |
| 375             | 5          |
