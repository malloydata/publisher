---
id: extend-routes-to-the-base-table
tags: serve-correctness, storage
package: erb
---

# An extension of a persisted source must route its READ to the base's table

The design intent, now that inheritance is understood to be deliberate: an
extension of a persisted source adds query-time fields and therefore must NOT get
a materialized table of its own — but it MUST be able to serve from the base's.

`extended-source-inherits-persist` asserts both sources answer correctly, but
without mutating the warehouse, so it cannot tell a routed read from a live
recompute — stored and live values are identical. Its note records that the
routing was measured and found to land on only ONE of the two names, chosen by
build-iteration order, leaving the other serving live. This scenario mutates, so
the distinction is visible.

`daily` declares the persist; `daily_with_avg` inherits it and adds a query-time
average. One table is built (they share a content address). Both should read it.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.erb_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |

## Model erb.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.erb_orders')

#@ persist name="erb_daily" storage=lake
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

expect binding: daily -> lake

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

The extension answers, computing its average at query time.

```malloy
run: daily_with_avg -> { select: order_date, total_amount, avg_order_value; order_by: order_date asc }
```

Expect:

| order_date | total_amount | avg_order_value |
| ---------- | ------------ | --------------- |
| 2026-01-01 | 150          | 75              |
| 2026-01-02 | 200          | 200             |

## Mutate orders_pg.erb_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 99           | 2026-01-01      | 1000       |

## SQL raw source really changed

```sql
SELECT order_date, sum(amount) AS total FROM erb_orders GROUP BY order_date ORDER BY order_date;
```

Expect:

| order_date | total:num |
| ---------- | --------- |
| 2026-01-01 | 1150      |
| 2026-01-02 | 200       |

## Query daily (again)

STALE — the declaring source keeps its routing to the lake table.

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 200          |

## Query daily_with_avg (again)

STALE — the extension reads the base's materialized table and computes its average
over the stored columns. A live recompute would read 1150 / 1 order.

Expect:

| order_date | total_amount | avg_order_value |
| ---------- | ------------ | --------------- |
| 2026-01-01 | 150          | 75              |
| 2026-01-02 | 200          | 200             |
