---
id: cross-model-dag
tags: serve-correctness, chained
package: xm
---

# Cross-model DAG: persist source builds from an imported model

The source dependency spans two model files in the same package. The entry model
(`agg.malloy`) imports a base model (`base.malloy`) and declares the persist
source on top of the imported `orders`. The build must resolve the import,
materialize `daily` into the lake, and serve it routed — proving the storage
tier follows the source DAG across an `import` boundary, not just within one file.

## Publisher

- PERSIST_STORAGE_MODE: on

## Model xm/agg.malloy

The entry model. It imports the base model and persists an aggregate over it.

```malloy
##! experimental.persistence

import "base.malloy"

#@ persist name="xm_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}
```

## Model xm/base.malloy

The imported base — just the raw source binding.

```malloy
source: orders is orders_pg.table('public.xm_orders')
```

## Data orders_pg.xm_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |
| 4            | 2026-01-02      | 25         |

## Publish

The build resolves the import and materializes `daily` from `orders`.

expect binding: daily -> lake

## Query daily

```malloy
run: daily -> { select: order_date, total_amount; order_by: order_date asc }
```

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 225          |

## Mutate orders_pg.xm_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 99           | 2026-01-01      | 1000       |

## SQL raw source really changed

```sql
SELECT order_date, sum(amount) AS total FROM xm_orders GROUP BY order_date ORDER BY order_date;
```

Expect:

| order_date | total:num |
| ---------- | --------- |
| 2026-01-01 | 1150      |
| 2026-01-02 | 225       |

## Query daily (again)

Stale ⇒ served from the DuckLake snapshot, across the import boundary.

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 225          |
