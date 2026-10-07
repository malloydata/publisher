---
id: a-parents-view-over-an-unbindable-join-keeps-its-dimensions
tags: orchestration, chained, build-control, serve-correctness
package: pvd
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A parent's view over an unbindable join keeps its dimensions

`daily` is stored, with a `where:`, a join to the warehouse table `regions`,
a view that reads that join, and a dimension `big is total > 150` that reads
nothing but the table. `rollup` groups by `big`.

The view cannot compile over the rebound parent — its join is to a table the
destination cannot bind — so the first tier fails. The next tiers drop the
view, then the join, and keep the dimensions and measures: `rollup` reads
`big`, finds it declared, and stacks on `daily`'s table. A ladder that fell
straight from everything to the `where:` alone would drop `big` with the view
and refuse `rollup` as a shape it could not carry. Filtered on every tier:
the 50 is never counted.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.pvd_orders

| order_id:int | order_date:date | region_id:int | amount:num |
| ------------ | --------------- | ------------- | ---------- |
| 1            | 2026-01-01      | 10            | 100        |
| 2            | 2026-01-01      | 20            | 50         |
| 3            | 2026-01-02      | 10            | 200        |

## Data orders_pg.pvd_regions

| id:int | name:text |
| ------ | --------- |
| 10     | east      |
| 20     | west      |

## Model pvd.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.pvd_orders')
source: regions is orders_pg.table('public.pvd_regions')

#@ persist name="pvd_daily" storage=lake
source: daily is orders -> {
  group_by: order_date, region_id
  aggregate: total is amount.sum()
} extend {
  where: total > 60
  join_one: r is regions on region_id = r.id
  dimension: big is total > 150
  view: by_region is { group_by: r.name; aggregate: total.sum() }
}

#@ persist name="pvd_rollup" storage=lake
source: rollup is daily -> {
  group_by: big
  aggregate: grand is total.sum()
}
```

## Publish

expect binding: daily -> lake
expect binding: rollup -> lake
expect upstreams: rollup -> reused

## Query rollup

```malloy
run: rollup -> { select: big, grand; order_by: big asc }
```

Expect:

| big:bool | grand:num |
| -------- | --------- |
| false    | 100       |
| true     | 200       |

## Mutate orders_pg.pvd_orders

| order_id:int | order_date:date | region_id:int | amount:num |
| ------------ | --------------- | ------------- | ---------- |
| 99           | 2026-01-03      | 10            | 1000       |

## Build (orchestrated, strict, pkg=pvd)

- rollup -> pvd_rollup__g2 @ lake (reused)
  reference: daily

## Bind pvd

## Query rollup (again)

Expect:

| big:bool | grand:num |
| -------- | --------- |
| false    | 100       |
| true     | 200       |
