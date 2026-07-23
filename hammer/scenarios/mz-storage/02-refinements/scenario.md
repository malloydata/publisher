---
id: refinements
tags: serve-correctness
package: d1
---

# Refinements re-emitted over storage

A persist source extended with a dimension, a measure, and a view. The serve-shape
transform must re-emit all three over the materialized table, so querying the view
and the measure both route to storage.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.d1_orders

| order_id:int | order_date:date | region:text | amount:num |
| ------------ | --------------- | ----------- | ---------- |
| 1            | 2026-01-01      | US          | 100        |
| 2            | 2026-01-01      | US          | 50         |
| 3            | 2026-01-02      | EU          | 200        |
| 4            | 2026-01-02      | US          | 25         |

## Model d1.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.d1_orders')

#@ persist name="d1_daily" storage=lake
source: daily_orders is orders -> {
  group_by: order_date
  aggregate:
    order_count is count()
    total_amount is amount.sum()
} extend {
  dimension: is_big is total_amount > 175
  measure: max_total is total_amount.max()
  view: by_size is {
    group_by: is_big
    aggregate: n is count()
    order_by: is_big asc
  }
}
```

## Publish

expect binding: daily_orders -> lake

## Query base rollup

```malloy
run: daily_orders -> { select: order_date, total_amount; order_by: order_date asc }
```

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 225          |

## Query re-emitted measure

`max_total` is a measure declared on the persisted source; it must serve over storage.

```malloy
run: daily_orders -> { aggregate: mt is max_total }
```

Expect:

| mt:num |
| ------ |
| 225    |

## Query re-emitted view

`by_size` is a view on the persisted source; querying it must route to storage.

```malloy
run: daily_orders -> by_size
```

Expect:

| is_big:bool | n:int |
| ----------- | ----- |
| false       | 1     |
| true        | 1     |

## Mutate orders_pg.d1_orders

| order_id:int | order_date:date | region:text | amount:num |
| ------------ | --------------- | ----------- | ---------- |
| 99           | 2026-01-01      | US          | 1000       |

## Query base rollup (again)

Stale ⇒ served from the snapshot.

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 225          |
