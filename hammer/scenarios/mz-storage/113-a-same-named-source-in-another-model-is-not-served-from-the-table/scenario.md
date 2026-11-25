---
id: a-same-named-source-in-another-model-is-not-served-from-the-table
tags: serve-correctness, security
package: shadow
---

<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A same-named source in another model is never served from the persisted table

Source names are not unique in a package. Here `persisted.malloy` materializes
`daily` (a sum) into the lake, and `report.malloy` declares an unrelated `daily`
(a max) with the same column names. Serve bindings are pushed to every model by
name, and schema narrowing only intersects columns, so before
malloydata/publisher#1001 the report's `daily` was answered from the persisted
sum: wrong rows, not a fallback. Each binding now carries the sourceIDs it
materializes, and a model keeps it only for its own source of that name.

## Publisher

- PERSIST_STORAGE_MODE: on

## Model shadow/report.malloy

The model queries read. Its `daily` is NOT persisted, and shares the persisted
one's columns on purpose: overlap is what made this wrong data.

```malloy
source: orders is orders_pg.table('public.shadow_orders')

source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.max()
}
```

## Model shadow/persisted.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.shadow_orders')

#@ persist name="shadow_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}
```

## Data orders_pg.shadow_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |
| 4            | 2026-01-02      | 25         |

## Publish

The persisted `daily` builds and binds.

expect binding: daily -> lake

## Query report daily

The report's own definition (the max), not the persisted sum of 150 and 225.

```malloy
run: daily -> { select: order_date, total_amount; order_by: order_date asc }
```

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 100          |
| 2026-01-02 | 200          |

## Mutate orders_pg.shadow_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 99           | 2026-01-01      | 1000       |

## Query report daily (again)

Answered live: it sees the new row, which no stored table holds.

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 1000         |
| 2026-01-02 | 200          |
