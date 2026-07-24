---
id: package-unload-while-serving
tags: lifecycle
package: puw
---

# Deleting a package while it serves from storage stops serving

A package materialized and serving from DuckLake is deleted (`DELETE
/packages/:pkg`). It must unload: the package no longer resolves and queries
against it are refused. (The DuckLake table is orchestrator-owned; a package
unload stops *serving* — it is not a garbage-collect of the stored data.)

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.puw_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-02      | 25         |

## Model puw.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.puw_orders')

#@ persist name="puw_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}
```

## Publish

expect binding: daily -> lake

## Query rollup

Serving from storage.

```malloy
run: daily -> { select: order_date, total_amount; order_by: order_date asc }
```

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 100          |
| 2026-01-02 | 25           |

## Delete puw

The package is unloaded and removed from the serving set.

## Query rollup (again, refused)

The package no longer resolves, so the query is refused.
