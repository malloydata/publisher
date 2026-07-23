---
id: schema-qualified-name
tags: serve-correctness, naming
package: sq
---

# Schema-qualified persist name (name="analytics.daily_orders")

The persist `name=` may carry a schema (`analytics.daily_orders`). By design the
storage tier does **not** auto-create the target schema — creating it is the
operator/orchestrator's responsibility (the same decision under which inlining was
disabled). So the story is: the build fails while the schema is missing; the
operator provisions the schema on the destination; the build then succeeds and
serves.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.sq_orders

| order_id:int | order_date:date | region:text | amount:num |
| ------------ | --------------- | ----------- | ---------- |
| 1            | 2026-01-01      | US          | 100        |
| 2            | 2026-01-01      | US          | 50         |
| 3            | 2026-01-02      | EU          | 200        |
| 4            | 2026-01-02      | US          | 25         |

## Model sq.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.sq_orders')

#@ persist name="analytics.daily_orders" storage=lake
source: daily_orders is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}
```

## Build refused

The schema `analytics` does not exist in the catalog yet, so the build fails —
the tier does not create it.

cites: not found in ducklakecatalog

## Operator lake

The operator provisions the schema on the destination — through its OWN
read-write DuckLake client, external to the publisher (the publisher exposes no
read-write DDL path on a storage destination).

```sql
CREATE SCHEMA IF NOT EXISTS analytics;
```

## Publish

Now the schema exists, the build succeeds and materializes into
`lake.analytics.daily_orders`.

expect binding: daily_orders -> lake

## Query rollup

```malloy
run: daily_orders -> { select: order_date, total_amount; order_by: order_date asc }
```

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 225          |

## Mutate orders_pg.sq_orders

| order_id:int | order_date:date | region:text | amount:num |
| ------------ | --------------- | ----------- | ---------- |
| 99           | 2026-01-01      | US          | 1000       |

## Query rollup (again)

Stale ⇒ served from the schema-qualified `analytics.daily_orders` snapshot.

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 225          |
