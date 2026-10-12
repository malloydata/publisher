---
id: chained-through-an-intermediate
tags: serve-correctness, chained, orchestration
package: cti
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A stored source reads its stored upstream through the sources between them

`chained-persist` proves a downstream that NAMES its stored upstream is built by
reading the upstream's table. Models rarely name it directly: the public
sources are `select: *` wrappers, an `extend` adds a dimension, a query reshapes
the rows, and the `#@ persist` sits one or more sources above the one that is
stored. Here `rollup` reads `daily` only through `daily_wide`, a non-persisted
`extend` over it.

The rule is the same one: the build must read `daily`'s stored table, carrying
`daily_wide` into the model it compiles rather than treating it as an undefined
name. Both paths produce the same 375, so the number is not the proof. The
proof is the strict orchestrated rebuild after the warehouse changes: `reused`
on the entry, and a rollup that still reads 375 because it was computed from
`daily`'s snapshot, not from the warehouse.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.cti_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |
| 4            | 2026-01-02      | 25         |

## Model cti.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.cti_orders')

#@ persist name="cti_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}

source: daily_wide is daily -> { select: * } extend {
  dimension: big_day is total_amount > 100
}

#@ persist name="cti_rollup" storage=lake
source: rollup is daily_wide -> {
  aggregate: grand_total is total_amount.sum()
}
```

## Publish

Both materialize, and `rollup` is built from `daily`'s stored table — through
`daily_wide`, which the build carries.

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

## Mutate orders_pg.cti_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 99           | 2026-01-03      | 1000       |

## Build (orchestrated, strict, pkg=cti)

Rebuild `rollup` alone, reusing the `daily` already built, under strict. The
entry must say the upstream was reused: a recompute would have read the
warehouse, where a 1000 now sits.

- rollup -> cti_rollup__g2 @ lake (reused)
  reference: daily

## Bind cti

## Query rollup (again)

Still 375: computed from `daily`'s stored rows, which predate the mutation.

Expect:

| grand_total:num |
| --------------- |
| 375             |
