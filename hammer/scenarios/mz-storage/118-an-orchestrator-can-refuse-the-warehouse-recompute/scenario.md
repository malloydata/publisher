---
id: an-orchestrator-can-refuse-the-warehouse-recompute
tags: serve-correctness, chained, orchestration
package: owr
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# The warehouse recompute is refused at plan: no host build can choose it

The companion of `86-an-intermediate-reaching-the-warehouse-is-recomputed-and-says-so`,
over the same shape. `daily_regional` joins the stored `daily` to `regions`, a
warehouse table nothing materializes, so the only build of `rollup` would
recompute `daily` from the warehouse.

An orchestrator could once choose that recompute with `warehouseReach:
recompute` or refuse it with `refuse` (ms2data/service#6756). No build
recomputes a stored upstream from raw any more, so the option is gone (a
request that sends it is refused), and the plan refuses `rollup` outright, before and after `daily` is
bound: the plan a host reads lists it as refused, never as a source to
instruct, so no host build of it can be dispatched, let alone succeed.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.owr_orders

| order_id:int | order_date:date | region_id:int | amount:num |
| ------------ | --------------- | ------------- | ---------- |
| 1            | 2026-01-01      | 10            | 100        |
| 2            | 2026-01-01      | 20            | 200        |
| 3            | 2026-01-02      | 10            | 400        |

## Data orders_pg.owr_regions

| region_id:int | region:text |
| ------------- | ----------- |
| 10            | east        |
| 20            | west        |

## Model owr.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.owr_orders')
source: regions is orders_pg.table('public.owr_regions')

#@ persist name="owr_daily" storage=lake
source: daily is orders -> {
  group_by: order_date, region_id
  aggregate: total_amount is amount.sum()
}

source: daily_regional is daily extend {
  join_one: r is regions on region_id = r.region_id
}

#@ persist name="owr_rollup" storage=lake
source: rollup is daily_regional -> {
  group_by: region is r.region
  aggregate: grand_total is total_amount.sum()
}
```

## Publish

`daily` materializes; `rollup` is not built, as in scenario 86.

expect binding: daily -> lake

## Build refusals

Expect:

| source | tier    | reason                  |
| ------ | ------- | ----------------------- |
| rollup | storage | upstream_table_unusable |

## Warns

cites: it reads the source warehouse (regions) through 'daily_regional'

## Build (orchestrated, pkg=owr)

A host build of what the plan lists: `daily` builds, twice.

- daily -> owr_daily__g2 @ lake

## Build (orchestrated, pkg=owr)

- daily -> owr_daily__g3 @ lake

## Build refusals

Still refused after the host's builds.

Expect:

| source | tier    | reason                  |
| ------ | ------- | ----------------------- |
| rollup | storage | upstream_table_unusable |
