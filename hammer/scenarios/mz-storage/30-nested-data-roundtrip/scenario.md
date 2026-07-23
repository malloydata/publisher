---
id: nested-data-roundtrip
tags: serve-correctness, data-fidelity
package: nd
---

# Nested (repeated-record) output round-trips through DuckLake

A persist source whose output carries a nested field (`nest:` → a repeated
record / LIST-of-STRUCT). The build must materialize the nested column into
DuckLake and the serve transform must reconstruct it, so both the flat columns
AND the nested column are queryable over storage. We verify the nested data by
un-nesting it back out at serve time and checking the per-region totals.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.nd_orders

| order_id:int | order_date:date | region:text | amount:num |
| ------------ | --------------- | ----------- | ---------- |
| 1            | 2026-01-01      | US          | 100        |
| 2            | 2026-01-01      | EU          | 50         |
| 3            | 2026-01-02      | US          | 200        |
| 4            | 2026-01-02      | EU          | 25         |

## Model nd.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.nd_orders')

#@ persist name="nd_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
  nest: by_region is {
    group_by: region
    aggregate: region_total is amount.sum()
    order_by: region asc
  }
}
```

## Publish

expect binding: daily -> lake

## Query flat

The flat (non-nested) columns serve from storage.

```malloy
run: daily -> { select: order_date, total_amount; order_by: order_date asc }
```

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 225          |

## Query unnested

Un-nest the stored `by_region` repeated record and re-aggregate — proves the
nested column round-tripped through DuckLake with its data intact.

```malloy
run: daily -> {
  group_by: region is by_region.region
  aggregate: region_total is by_region.region_total.sum()
  order_by: region asc
}
```

Expect:

| region | region_total |
| ------ | ------------ |
| EU     | 75           |
| US     | 300          |

## Mutate orders_pg.nd_orders

| order_id:int | order_date:date | region:text | amount:num |
| ------------ | --------------- | ----------- | ---------- |
| 99           | 2026-01-01      | US          | 1000       |

## Query flat (again)

Stale ⇒ served from the snapshot (the nested materialization is a real snapshot,
not a live recompute).

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 225          |
