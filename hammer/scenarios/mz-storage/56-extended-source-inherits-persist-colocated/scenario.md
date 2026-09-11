---
id: extended-source-inherits-persist-colocated
tags: serve-correctness, safety
package: esc
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# An extension shares the base's colocated table, not a second one — COLOCATED

The colocated (in-warehouse, no `storage=`) twin of `extended-source-inherits-persist`.

A source that EXTENDS a persisted source inherits its `#@ persist name=`, and that
inheritance is by design in Malloy (`#@ -persist` opts out) — without it the extension
could never be served from a table at all. An `extend` adds query-time fields and changes
no materialization SQL, so `daily` and `daily_with_avg` compile identically, share one
content address, and must share ONE physical table in the source warehouse.

Both sources therefore belong in the plan; what must never appear is a second table. The
`entity` column is the assertion that carries that.

Worth noting against the external twin: the colocated tier substitutes through the
same-connection manifest, which is keyed by content ADDRESS, so every source sharing an
address resolves automatically. The storage tier binds by source NAME, which is why it
needed an explicit fix to route an extension's read — see
`extend-routes-to-the-base-table`. Colocated was never exposed to that failure.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.esc_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |

## Model esc.malloy

A plain colocated `#@ persist` (no `storage=`).

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.esc_orders')

#@ persist name="esc_daily"
source: daily is orders -> {
  group_by: order_date
  aggregate:
    total_amount is amount.sum()
    num_orders is count()
}

source: daily_with_avg is daily extend {
  dimension: avg_order_value is total_amount / num_orders
}
```

## Publish

Materialize the package colocated (into the source warehouse).

## Query daily

```malloy
run: daily -> { select: order_date, total_amount; order_by: order_date asc }
```

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 200          |

## Query daily_with_avg

```malloy
run: daily_with_avg -> { select: order_date, total_amount, avg_order_value; order_by: order_date asc }
```

Expect:

| order_date | total_amount | avg_order_value |
| ---------- | ------------ | --------------- |
| 2026-01-01 | 150          | 75              |
| 2026-01-02 | 200          | 200             |

## Build targets

Both sources are in the plan, both carrying the inherited `name=`. One content address
(label `A`) across both, so one colocated table.

Expect:

| source         | writes    | entity |
| -------------- | --------- | ------ |
| daily          | esc_daily | A      |
| daily_with_avg | esc_daily | A      |
