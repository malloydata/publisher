---
id: an-orchestrator-can-refuse-the-warehouse-recompute
tags: serve-correctness, chained, orchestration
package: owr
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# An orchestrator that says `warehouseReach: refuse` fails the warehouse recompute instead of building it

The companion of `86-an-intermediate-reaching-the-warehouse-is-recomputed-and-says-so`,
over the same shape. `daily_regional` joins the stored `daily` to `regions`, a
warehouse table nothing materializes, so the only build of `rollup` recomputes
`daily` from the warehouse. Under `strictUpstreams` that recompute is permitted
by default (`warehouseReach: recompute`), and scenario 86 pins it.

An orchestrator that must never have the warehouse do work it did not schedule
sends `warehouseReach: refuse` (ms2data/service#6756). The strict build then
fails `rollup` with the reason the entry would have carried, names the knob, and
reads nothing from the warehouse. Sent explicitly as `recompute`, the same build
succeeds and recomputes, so the value is read, not merely present.

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

Both materialize, as in scenario 86.

expect binding: daily -> lake
expect binding: rollup -> lake

## Build refused (orchestrated, strict, reach=refuse, pkg=owr)

Rebuild `rollup` alone under strict, reusing the stored `daily`, with the
recompute refused. The run fails, naming the knob and the intermediate.

- rollup -> owr_rollup__g2 @ lake
  reference: daily

cites: warehouseReach: refuse

## Build (orchestrated, strict, reach=recompute, pkg=owr)

The same build with the default sent explicitly: it recomputes and says so.

- rollup -> owr_rollup__g3 @ lake (recomputed)
  reference: daily

cites: daily_regional
