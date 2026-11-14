---
id: a-dependency-only-the-compiler-reaches-is-still-strict
tags: orchestration, chained, build-control
package: dcr
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A dependency only the compiler reaches is still strict

`daily` is a colocated persist source whose own `extend {}` joins the persisted
`counts`; `rollup` is a `storage=` source over `daily` that reads
`c.order_count` through that join. The walk over the model stops at `daily`,
so it never names `counts`. The compiler does: `rollup`'s SQL inlines
`counts` whenever no manifest holds it.

Under `strictUpstreams`, with `daily` supplied by reference and `counts`
supplied by nothing, that inlining is a recompute of a pinned table — exactly
what strict exists to refuse. The refusal must come from what the compiler
actually reaches, not from what the walk names. Non-strict, the build
recomputes and the entry says which upstream and why.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.dcr_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |

## Model dcr.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.dcr_orders')

#@ persist name="dcr_counts"
source: counts is orders -> {
  group_by: order_date
  aggregate: order_count is count()
}

#@ persist name="dcr_daily"
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
} extend {
  join_one: c is counts on order_date = c.order_date
}

#@ persist name="dcr_rollup" storage=lake
source: rollup is daily -> {
  aggregate:
    grand_total is total_amount.sum()
    orders is c.order_count.sum()
}
```

## Publish

expect binding: rollup -> lake
expect upstreams: rollup -> reused

## Query rollup

```malloy
run: rollup -> { select: grand_total, orders }
```

Expect:

| grand_total:num | orders:num |
| --------------- | ---------- |
| 350             | 3          |

## Build refused (orchestrated, strict, pkg=dcr)

`daily` by reference, `counts` by nothing: the build would inline `counts`.

- rollup -> dcr_rollup__g2 @ lake
  reference: daily

cites: counts

## Build (orchestrated, pkg=dcr)

Non-strict: the same instruction recomputes `counts` and says so.

- rollup -> dcr_rollup__g2 @ lake (recomputed)
  reference: daily

cites: counts
