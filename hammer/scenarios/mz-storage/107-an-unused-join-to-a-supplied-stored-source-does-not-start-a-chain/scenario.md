---
id: an-unused-join-to-a-supplied-stored-source-does-not-start-a-chain
tags: orchestration, chained, build-control
package: ujs
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# An unused join to a supplied stored source does not start a chain

`rollup` is a `storage=` source over the colocated `daily`, and it declares
`join_one: c is counts` to a source stored in `lake` — a join it never
reads. `counts` is supplied by reference. The compiler prunes the unused
join, so `rollup`'s SQL reads `daily`'s warehouse table and nothing in the
lake: a passthrough build, as before the join was declared.

Whether a build stacks on a parent is decided by what its SQL reads, not by
what the model declares. Were the declared join enough to start a chained
attempt, that attempt would find `daily` outside the destination and strict
would refuse a build the passthrough completes.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.ujs_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |

## Model ujs.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.ujs_orders')

#@ persist name="ujs_counts" storage=lake
source: counts is orders -> {
  group_by: order_date
  aggregate: order_count is count()
}

#@ persist name="ujs_daily"
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}

#@ persist name="ujs_rollup" storage=lake
source: rollup is daily extend {
  join_one: c is counts on order_date = c.order_date
} -> {
  aggregate: grand_total is total_amount.sum()
}
```

## Publish

expect binding: counts -> lake
expect binding: rollup -> lake

## Query rollup

```malloy
run: rollup -> { select: grand_total }
```

Expect:

| grand_total:num |
| --------------- |
| 350             |

## Mutate orders_pg.ujs_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 99           | 2026-01-03      | 1000       |

## Build (orchestrated, strict, pkg=ujs)

Strict: `rollup` alone, `daily` and `counts` by reference. The SQL reads
`daily`'s table and never `counts`; the build is the passthrough, from the
stale table.

- rollup -> ujs_rollup__g2 @ lake (reused)
  reference: daily
  reference: counts

## Bind ujs

## Query rollup (again)

Expect:

| grand_total:num |
| --------------- |
| 350             |
