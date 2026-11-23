---
id: a-chain-mixing-reused-and-recomputed-sources-builds-in-one-strict-run
tags: serve-correctness, chained, orchestration
package: cmx
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A chain mixing reusable and warehouse-reaching intermediates builds what it can in one run, and the rest answers live

The shape a real model takes: raw tables prepared by several non-persisted
sources, three stored levels, and the intermediates between them differing in
what they reach. `daily_regional` joins a warehouse table, so `regional` has no
build over `daily`'s stored table, and nothing recomputes from raw: the plan
refuses it. `summary` reads only `regional`, which has no table, so the plan
refuses it too. `daily` builds, and the two not built answer live, equal to
the warehouse.

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

## Build refusals

`regional` reads `regions` through `daily_regional`: the plan refuses it.
`summary` reads `regional`'s table, which the plan will never build, so the
plan refuses it too, naming `regional`, rather than leaving a run to fail it.

Expect:

| source   | tier    | reason                  |
| -------- | ------- | ----------------------- |
| regional | storage | upstream_table_unusable |
| summary  | storage | upstream_table_unusable |

## Warns

cites: its upstream 'regional' declares a persisted table, but it is refused

## Build (orchestrated, strict, pkg=cmx)

What the plan lists: `daily` builds.

- daily -> cmx_daily__g1 @ lake

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
