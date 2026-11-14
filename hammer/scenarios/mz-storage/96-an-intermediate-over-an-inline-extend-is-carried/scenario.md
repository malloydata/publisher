---
id: an-intermediate-over-an-inline-extend-is-carried
tags: orchestration, chained, build-control, serve-correctness
package: iie
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# An intermediate over an inline extend is carried

`hits is (daily extend { join_one: r is regions_kept … }) -> { … }` is the
idiom for "join, then aggregate" in one declaration: the parenthesized source
is never named, and the compiler embeds it in `hits`'s definition rather than
giving it an identity. Both `daily` and `regions_kept` are stored, so `hits`
reads only stored tables and `rollup`, stored over `hits`, is a chained build.
The build must see through the parentheses — the base the inline source
extends, and the join it declares — or `hits` is not carried, `rollup`'s model
does not compile, and strict refuses a source whose every input is a stored
table.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.iie_orders

| order_id:int | region_id:int | amount:num |
| ------------ | ------------- | ---------- |
| 1            | 10            | 100        |
| 2            | 20            | 50         |
| 3            | 10            | 200        |

## Data orders_pg.iie_regions

| region_id:int | region:text |
| ------------- | ----------- |
| 10            | east        |
| 20            | west        |

## Model iie.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.iie_orders')
source: regions is orders_pg.table('public.iie_regions')

#@ persist name="iie_daily" storage=lake
source: daily is orders -> {
  group_by: region_id
  aggregate: total is amount.sum()
}

#@ persist name="iie_regions" storage=lake
source: regions_kept is regions -> { select: * }

source: hits is (daily extend {
  join_one: r is regions_kept on region_id = r.region_id
}) -> {
  group_by: region is r.region
  aggregate: total is total.sum()
}

#@ persist name="iie_rollup" storage=lake
source: rollup is hits -> {
  group_by: region
  aggregate: grand_total is total.sum()
}
```

## Publish

expect binding: daily -> lake
expect binding: regions_kept -> lake
expect binding: rollup -> lake
expect upstreams: rollup -> reused

## Query rollup

```malloy
run: rollup -> { select: region, grand_total; order_by: region asc }
```

Expect:

| region | grand_total:num |
| ------ | --------------- |
| east   | 300             |
| west   | 50              |

## Mutate orders_pg.iie_orders

| order_id:int | region_id:int | amount:num |
| ------------ | ------------- | ---------- |
| 99           | 10            | 1000       |

## Build (orchestrated, strict, pkg=iie)

Strict: `rollup` alone over the two stored tables, through the inline extend.

- rollup -> iie_rollup__g2 @ lake (reused)
  reference: daily
  reference: regions_kept

## Bind iie

## Query rollup (again)

East still 300: read from `daily`'s table.

Expect:

| region | grand_total:num |
| ------ | --------------- |
| east   | 300             |
| west   | 50              |
