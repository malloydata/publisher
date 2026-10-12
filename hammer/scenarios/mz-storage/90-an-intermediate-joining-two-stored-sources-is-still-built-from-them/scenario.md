---
id: an-intermediate-joining-two-stored-sources-is-still-built-from-them
tags: serve-correctness, chained, orchestration
package: ctp
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# An intermediate that joins other sources is carried when every source it joins is stored

What decides whether an intermediate can be carried is not whether it joins,
but what it reaches. `an-intermediate-reaching-the-warehouse-…` joins a table
nothing materializes and is recomputed. Here `joined` joins `daily` to
`daily_counts`, and both are stored in the destination — so the join runs over
two stored tables, and `rollup` is built from them, under strict, reading
neither from the warehouse.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.ctp_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |
| 4            | 2026-01-02      | 25         |

## Model ctp.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.ctp_orders')

#@ persist name="ctp_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}

#@ persist name="ctp_daily_counts" storage=lake
source: daily_counts is orders -> {
  group_by: order_date
  aggregate: order_count is count()
}

source: joined is daily extend {
  join_one: c is daily_counts on order_date = c.order_date
}

#@ persist name="ctp_rollup" storage=lake
source: rollup is joined -> {
  aggregate:
    grand_total is total_amount.sum()
    orders is c.order_count.sum()
}
```

## Publish

expect binding: daily -> lake
expect binding: daily_counts -> lake
expect binding: rollup -> lake
expect upstreams: rollup -> reused

## Query rollup

```malloy
run: rollup -> { select: grand_total, orders }
```

Expect:

| grand_total:num | orders:num |
| --------------- | ---------- |
| 375             | 4          |

## Mutate orders_pg.ctp_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 99           | 2026-01-03      | 1000       |

## Build (orchestrated, strict, pkg=ctp)

Rebuild `rollup` alone, reusing both stored parents.

- rollup -> ctp_rollup__g2 @ lake (reused)
  reference: daily
  reference: daily_counts

## Bind ctp

## Query rollup (again)

Stale on both measures ⇒ both parents were read from their stored tables.

Expect:

| grand_total:num | orders:num |
| --------------- | ---------- |
| 375             | 4          |
