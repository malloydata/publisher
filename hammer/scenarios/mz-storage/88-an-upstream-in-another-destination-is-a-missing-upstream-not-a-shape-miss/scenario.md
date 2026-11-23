---
id: an-upstream-in-another-destination-is-a-missing-upstream-not-a-shape-miss
tags: orchestration, chained, build-control
package: cxd
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A stored upstream in another destination is a missing upstream, and the downstream is refused at plan

A chained build reads its upstreams from the destination it writes to. `daily`
is stored in `lake`; `rollup` is declared into `far`, a second destination, and
reads `daily` through `daily_wide`. From `far`, `daily`'s table does not exist.

The table exists, but not where `rollup`'s build could read it, and no build
recomputes a stored upstream from raw. So `rollup` is refused when the package
is planned, never built, and answers live, equal to the warehouse.

## Connection far (type=ducklake)

A second, isolated destination for `rollup`.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.cxd_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-02      | 200        |

## Model cxd.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.cxd_orders')

#@ persist name="cxd_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}

source: daily_wide is daily -> { select: * } extend {
  dimension: big_day is total_amount > 100
}

#@ persist name="cxd_rollup" storage=far
source: rollup is daily_wide -> {
  aggregate: grand_total is total_amount.sum()
}
```

## Publish

`daily` materializes in `lake`; `rollup` cannot read it from `far`, so it is
not built.

expect binding: daily -> lake

## Build refusals

The plan refuses `rollup`, its upstream's table being in another destination.

Expect:

| source | tier    | reason                  |
| ------ | ------- | ----------------------- |
| rollup | storage | upstream_table_unusable |

## Warns

cites: persist-not-built

## Query rollup

```malloy
run: rollup -> { select: grand_total }
```

Expect:

| grand_total:num |
| --------------- |
| 300             |

## Mutate orders_pg.cxd_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 99           | 2026-01-03      | 1000       |

## Query rollup (again)

`rollup` answers through `daily`, which still serves its snapshot: the 1000
is not in it.

Expect:

| grand_total:num |
| --------------- |
| 300             |
