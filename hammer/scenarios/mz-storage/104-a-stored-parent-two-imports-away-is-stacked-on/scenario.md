---
id: a-stored-parent-two-imports-away-is-stacked-on
tags: orchestration, chained, serve-correctness, imports
package: tim
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A stored parent two imports away is stacked on

Three files: `a.malloy` declares the stored `daily`; `b.malloy` imports it
and declares the stored `weekly` over it; `c.malloy`, the model queries run
against, imports `b.malloy` and declares the stored `monthly` over `weekly`.
An import re-exports nothing, so `c.malloy`'s namespace holds `monthly` and
`weekly` and not `daily`; `b.malloy`'s holds `weekly` and `daily` and not
`orders`. Each stored source is planned through the last model that compiles
it — `weekly` through `c.malloy`, where its parent is two imports away.

Both chained builds must stack on their parents: `monthly` on `weekly`'s
table, and `weekly` on `daily`'s, which its planning model never names.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.tim_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |

## Model tim/c.malloy

```malloy
##! experimental.persistence

import "b.malloy"

#@ persist name="tim_monthly" storage=lake
source: monthly is weekly -> {
  aggregate: grand_total is weekly_total.sum()
}
```

## Model tim/b.malloy

```malloy
##! experimental.persistence

import "a.malloy"

#@ persist name="tim_weekly" storage=lake
source: weekly is daily -> {
  group_by: order_date
  aggregate: weekly_total is total_amount.sum()
}
```

## Model tim/a.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.tim_orders')

#@ persist name="tim_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}
```

## Publish

expect binding: daily -> lake
expect binding: weekly -> lake
expect binding: monthly -> lake
expect upstreams: weekly -> reused
expect upstreams: monthly -> reused

## Query monthly

```malloy
run: monthly -> { select: grand_total }
```

Expect:

| grand_total:num |
| --------------- |
| 350             |

## Mutate orders_pg.tim_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 99           | 2026-01-03      | 1000       |

## Build (orchestrated, strict, pkg=tim)

Strict: `weekly` and `monthly`, `daily` by reference. Both stack.

- weekly -> tim_weekly__g2 @ lake (reused)
- monthly -> tim_monthly__g2 @ lake (reused)
  reference: daily

## Bind tim

## Query monthly (again)

Still 350.

Expect:

| grand_total:num |
| --------------- |
| 350             |
