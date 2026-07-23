---
id: empty-result-set
tags: serve-correctness
package: es
---

# A zero-row persist source serves an empty table, not an error

A `#@ persist` rollup whose filter matches nothing materializes to an EMPTY
DuckLake table. Serving it must return zero rows cleanly — not error, not fall
back to live. The routing proof is the mirror of the usual one: after publishing
we add a row the filter WOULD include, and the served query stays empty, because
it reads the empty snapshot rather than recomputing live.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.es_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-02      | 25         |

## Model es.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.es_orders')

#@ persist name="es_big" storage=lake
source: big_orders is orders -> {
  where: amount > 1000000
  group_by: order_date
  aggregate: total_amount is amount.sum()
}
```

## Publish

expect binding: big_orders -> lake

## Query rollup

The materialized table is empty; the served query returns zero rows.

```malloy
run: big_orders -> { select: order_date, total_amount; order_by: order_date asc }
```

Expect:

| order_date | total_amount |
| ---------- | ------------ |

## Mutate orders_pg.es_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 99           | 2026-01-01      | 2000000    |

## Query rollup (again)

Still empty ⇒ served from the (empty) snapshot. A live recompute would now
surface the 2,000,000 row.

Expect:

| order_date | total_amount |
| ---------- | ------------ |
