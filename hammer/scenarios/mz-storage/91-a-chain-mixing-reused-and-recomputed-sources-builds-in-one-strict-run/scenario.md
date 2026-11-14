---
id: a-chain-mixing-reused-and-recomputed-sources-builds-in-one-strict-run
tags: serve-correctness, chained, orchestration
package: cmx
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A package whose chain mixes reusable and warehouse-reaching intermediates builds in one strict run, each source reporting its own path

The shape a real model takes: raw tables prepared by several non-persisted
sources, three stored levels, and the intermediates between them differing in
what they reach. `daily_regional` joins a warehouse table, so `regional` is
recomputed. `regional_wide` only reshapes `regional`, so `summary` is built from
`regional`'s stored table. One orchestrated strict build of all three must
succeed, and each entry must report its own path — not the run as a whole, since
the two paths have different freshness semantics and a consumer reads them per
source.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.cmx_orders

| order_id:int | order_date:date | region_id:int | amount:num |
| ------------ | --------------- | ------------- | ---------- |
| 1            | 2026-01-01      | 10            | 100        |
| 2            | 2026-01-01      | 20            | 200        |
| 3            | 2026-01-02      | 10            | 400        |

## Data orders_pg.cmx_regions

| region_id:int | region:text |
| ------------- | ----------- |
| 10            | east        |
| 20            | west        |

## Model cmx.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.cmx_orders')
source: regions is orders_pg.table('public.cmx_regions')

#@ persist name="cmx_daily" storage=lake
source: daily is orders -> {
  group_by: order_date, region_id
  aggregate: total_amount is amount.sum()
}

source: daily_regional is daily extend {
  join_one: r is regions on region_id = r.region_id
}

#@ persist name="cmx_regional" storage=lake
source: regional is daily_regional -> {
  group_by: region is r.region
  aggregate: total is total_amount.sum()
}

source: regional_wide is regional -> { select: * } extend {
  dimension: big is total > 300
}

#@ persist name="cmx_summary" storage=lake
source: summary is regional_wide -> {
  aggregate: grand_total is total.sum()
}
```

## Build (orchestrated, strict, pkg=cmx)

All three in one strict instruction list, upstream first. `regional` is
recomputed (its intermediate reaches `regions`); `summary` is built from
`regional`'s stored table (its intermediate reaches nothing else). The run
succeeds.

- daily -> cmx_daily__g1 @ lake
- regional -> cmx_regional__g1 @ lake (recomputed)
- summary -> cmx_summary__g1 @ lake (reused)

cites: daily_regional

## Bind cmx

## Query summary

```malloy
run: summary -> { select: grand_total }
```

Expect:

| grand_total:num |
| --------------- |
| 700             |

## Query regional

```malloy
run: regional -> { select: region, total; order_by: region asc }
```

Expect:

| region | total:num |
| ------ | --------- |
| east   | 500       |
| west   | 200       |
