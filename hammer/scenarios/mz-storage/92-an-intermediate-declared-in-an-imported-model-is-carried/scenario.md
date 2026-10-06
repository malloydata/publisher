---
id: an-intermediate-declared-in-an-imported-model-is-carried
tags: serve-correctness, chained
package: cim
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# An intermediate declared in an imported model is carried, under that file's flags

`cross-model-dag` proves a persist source resolves across an `import`. Here the
chain itself crosses one: `base.malloy` declares the raw source, the stored
`daily`, and `daily_public` over it — an `include {}` under that file's
`access_modifiers` flag — and `agg.malloy` imports it and persists `rollup` over
`daily_public`. The build must find `daily_public` in the imported model, lift
its text from that file, and compile it under that file's flags, not the
importing file's.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.cim_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |
| 4            | 2026-01-02      | 25         |

## Model cim/agg.malloy

The entry model, declared first so queries run against it.

```malloy
##! experimental.persistence
import "base.malloy"

#@ persist name="cim_rollup" storage=lake
source: rollup is daily_public -> {
  aggregate: grand_total is total_amount.sum()
}
```

## Model cim/base.malloy

```malloy
##! experimental { persistence, access_modifiers }

source: orders is orders_pg.table('public.cim_orders')

#@ persist name="cim_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
  aggregate: order_count is count()
}

source: daily_public is daily -> { select: * } include {
  public:
    order_date
    total_amount
}
```

## Publish

expect binding: daily -> lake
expect binding: rollup -> lake
expect upstreams: rollup -> reused

## Query rollup

```malloy
run: rollup -> { select: grand_total }
```

Expect:

| grand_total:num |
| --------------- |
| 375             |

## Mutate orders_pg.cim_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 99           | 2026-01-03      | 1000       |

## Build (orchestrated, strict, pkg=cim)

- rollup -> cim_rollup__g2 @ lake (reused)
  reference: daily

## Bind cim

## Query rollup (again)

Stale ⇒ read from `daily`'s stored table through the imported intermediate.

Expect:

| grand_total:num |
| --------------- |
| 375             |
