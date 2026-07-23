---
id: flat-source
tags: serve-correctness
package: d0
---

# Flat persist source: Postgres → DuckLake

A single aggregate is persisted into DuckLake and served routed at `mode=on`.
This is the baseline: build → serve → correct values → prove it was served from
the materialized snapshot (not recomputed live).

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.orders

| order_id:int | order_date:date | region:text | amount:num |
| ------------ | --------------- | ----------- | ---------- |
| 1            | 2026-01-01      | US          | 100        |
| 2            | 2026-01-01      | US          | 50         |
| 3            | 2026-01-02      | EU          | 200        |
| 4            | 2026-01-02      | US          | 25         |

## Model d0.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.orders')

#@ persist name="d0_daily" storage=lake
source: daily_orders is orders -> {
  group_by: order_date
  aggregate:
    order_count is count()
    total_amount is amount.sum()
}
```

## Publish

Build the package — the auto-run materializes `daily_orders` into `lake`.

expect binding: daily_orders -> lake

## Query daily rollup

```malloy
run: daily_orders -> { select: order_date, total_amount; order_by: order_date asc }
```

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 225          |

## Mutate orders_pg.orders

Append a large order for 2026-01-01. If the query recomputed live, that date's
total would jump to 1150.

| order_id:int | order_date:date | region:text | amount:num |
| ------------ | --------------- | ----------- | ---------- |
| 99           | 2026-01-01      | US          | 1000       |

## SQL raw source really changed

Prove the mutation actually landed in the warehouse (not an assumption): the raw
source table now totals 1150 for 2026-01-01.

```sql
SELECT order_date, sum(amount) AS total FROM orders GROUP BY order_date ORDER BY order_date;
```

Expect:

| order_date | total |
| ---------- | ----- |
| 2026-01-01 | 1150  |
| 2026-01-02 | 225   |

## Query daily rollup (again)

Re-run the query. Even though the source now totals 1150, a **stale** 150 proves
the query was served from the DuckLake snapshot, not recomputed live.

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 225          |
