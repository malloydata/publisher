---
id: a-chained-build-reads-its-own-intermediate-not-a-same-named-stored-source
tags: serve-correctness, chained, security
package: chsh
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A chained build reads its own intermediate, never a same-named source another model stored

The build-side twin of
`a-same-named-source-in-another-model-is-not-served-from-the-table`. In
`report.malloy`, `rollup` is built from `daily` through `mid`, a non-persisted
intermediate. In `other.malloy`, an unrelated `mid` (a max, with the same
columns) is stored in the same destination. A chained build rebinds its
upstreams by name, and the intermediate lift skips any name already bound, so
before malloydata/publisher#1001 `rollup` was built from the other model's
`mid` and the wrong rows were written into its table. Upstream bindings now
carry the sourceIDs they materialize, and only the downstream's own sources are
rebound.

## Publisher

- PERSIST_STORAGE_MODE: on

## Model chsh/report.malloy

The model queries read.

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.chsh_orders')

#@ persist name="chsh_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}

source: mid is daily -> {
  group_by: order_date
  aggregate: v is total_amount.sum()
}

#@ persist name="chsh_rollup" storage=lake
source: rollup is mid -> {
  aggregate: grand_total is v.sum()
}
```

## Model chsh/other.malloy

An unrelated `mid` with the same columns, stored in the same destination.

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.chsh_orders')

#@ persist name="chsh_other_mid" storage=lake
source: mid is orders -> {
  group_by: order_date
  aggregate: v is amount.max()
}
```

## Data orders_pg.chsh_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |
| 4            | 2026-01-02      | 25         |

## Publish (sources=daily+mid)

The two stored parents first, so the other model's `mid` is in the destination
when `rollup` builds.

expect binding: daily -> lake
expect binding: mid -> lake

## Publish

`rollup` builds from `daily`'s stored table, through this model's own `mid`.

expect binding: rollup -> lake

## Query rollup

The sum of the daily sums (375), not the sum of the other model's daily maxima
(300).

```malloy
run: rollup -> { select: grand_total }
```

Expect:

| grand_total:num |
| --------------- |
| 375             |
