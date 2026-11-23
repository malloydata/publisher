---
id: an-orchestrator-can-refuse-the-warehouse-recompute
tags: serve-correctness, chained, orchestration
package: owr
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# `warehouseReach` no longer chooses: the warehouse recompute is refused whatever it says

The companion of `86-an-intermediate-reaching-the-warehouse-is-recomputed-and-says-so`,
over the same shape. `daily_regional` joins the stored `daily` to `regions`, a
warehouse table nothing materializes, so the only build of `rollup` would
recompute `daily` from the warehouse.

An orchestrator could once choose that recompute with `warehouseReach:
recompute` or refuse it with `refuse` (ms2data/service#6756). No build
recomputes a stored upstream from raw any more, so the knob is accepted and
ignored: the build of `rollup` fails, naming the intermediate, under either
value, and reads nothing from the warehouse.

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

## Build refused (orchestrated, strict, reach=refuse, pkg=owr)

Rebuild `rollup` alone, reusing the stored `daily`, with the recompute refused.
The run fails, naming the intermediate.

- rollup -> owr_rollup__g2 @ lake
  reference: daily

cites: daily_regional

## Build refused (orchestrated, strict, reach=recompute, pkg=owr)

The same build with the recompute asked for: refused the same way, since the
value is ignored.

- rollup -> owr_rollup__g3 @ lake
  reference: daily

cites: daily_regional
