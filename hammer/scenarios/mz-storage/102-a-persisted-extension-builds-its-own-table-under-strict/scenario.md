---
id: a-persisted-extension-builds-its-own-table-under-strict
tags: orchestration, chained, build-control
package: pex
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A persisted extension builds its own table under strict

`daily_wide is daily extend { … }` inherits `#@ persist` and shares `daily`'s
table: one address, two names. The build is instructed by address, and when
several names share it the plan may hand the build to either — here the
extension, declared last. Building the extension runs the defining query; it
does not read a `daily` table, because the table it would read is the one it
is building. A first strict build with nothing by reference must therefore
succeed, not refuse itself as "missing `daily`".

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.pex_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |

## Model pex.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.pex_orders')

#@ persist name="pex_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate:
    total_amount is amount.sum()
    num_orders is count()
}

source: daily_wide is daily extend {
  dimension: avg_order is total_amount / num_orders
}
```

## Build (orchestrated, strict, pkg=pex)

The shared address, strict, nothing by reference.

- daily -> pex_daily__g1 @ lake

## Bind pex

## Query daily_wide

```malloy
run: daily_wide -> { select: order_date, total_amount, avg_order; order_by: order_date asc }
```

servedFrom: storage

Expect:

| order_date | total_amount:num | avg_order:num |
| ---------- | ---------------- | ------------- |
| 2026-01-01 | 150              | 75            |
| 2026-01-02 | 200              | 200           |
