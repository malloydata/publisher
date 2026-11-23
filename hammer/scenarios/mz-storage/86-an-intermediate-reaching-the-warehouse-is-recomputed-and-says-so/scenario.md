---
id: an-intermediate-reaching-the-warehouse-is-recomputed-and-says-so
tags: serve-correctness, chained, orchestration
package: cir
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A downstream whose intermediate reaches the warehouse is not built, and answers live

`daily_regional` joins the stored `daily` to `regions`, a warehouse table nothing
materializes. The destination holds no such table, so no build of `rollup` over
`daily`'s stored rows exists: the only build there would be recomputes `daily`
from the warehouse with the join beside it.

No build recomputes a stored upstream from raw, so `rollup` is **not built**: a
run that is asked for it fails it, with a reason naming the intermediate the
destination could not stand in for, and `daily` still builds. `rollup` then
**answers live**, so it always agrees with the warehouse, where a recomputed
table would have mixed `daily`'s snapshot with fresh rows.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.cir_orders

| order_id:int | order_date:date | region_id:int | amount:num |
| ------------ | --------------- | ------------- | ---------- |
| 1            | 2026-01-01      | 10            | 100        |
| 2            | 2026-01-01      | 20            | 200        |
| 3            | 2026-01-02      | 10            | 400        |

## Data orders_pg.cir_regions

| region_id:int | region:text |
| ------------- | ----------- |
| 10            | east        |
| 20            | west        |

## Model cir.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.cir_orders')
source: regions is orders_pg.table('public.cir_regions')

#@ persist name="cir_daily" storage=lake
source: daily is orders -> {
  group_by: order_date, region_id
  aggregate: total_amount is amount.sum()
}

source: daily_regional is daily extend {
  join_one: r is regions on region_id = r.region_id
}

#@ persist name="cir_rollup" storage=lake
source: rollup is daily_regional -> {
  group_by: region is r.region
  aggregate: grand_total is total_amount.sum()
}
```

## Publish

`daily` materializes; `rollup` cannot be built over `daily`'s stored table, so
it is not built.

expect binding: daily -> lake

## Query rollup

Answered live.

```malloy
run: rollup -> { select: region, grand_total; order_by: region asc }
```

Expect:

| region | grand_total:num |
| ------ | --------------- |
| east   | 500             |
| west   | 200             |

## Mutate orders_pg.cir_orders

| order_id:int | order_date:date | region_id:int | amount:num |
| ------------ | --------------- | ------------- | ---------- |
| 99           | 2026-01-03      | 10            | 1000       |

## Build refused (orchestrated, pkg=cir)

Rebuild `rollup` alone, reusing the `daily` already built. The run fails
`rollup` and names the intermediate the destination could not stand in for.

- rollup -> cir_rollup__g2 @ lake
  reference: daily

cites: daily_regional

## Bind cir

## Query rollup (again)

Still live, so it sees the 1000.

Expect:

| region | grand_total:num |
| ------ | --------------- |
| east   | 1500            |
| west   | 200             |

## Query daily

`daily` still serves the snapshot it was built from.

```malloy
run: daily -> { aggregate: total is total_amount.sum() }
```

Expect:

| total:num |
| --------- |
| 700       |
