---
id: nested-data-roundtrip
tags: serve-correctness, data-fidelity
package: nd
---

# Nested (repeated-record) output builds, and its traversal falls back to live

A persist source whose output carries a nested field (`nest:` → a repeated
record / LIST-of-STRUCT). The build materializes the nested column into DuckLake,
but the serve transform declares any array/struct column as `json` — an opaque
carry, not a reconstructed repeated record — so a query that TRAVERSES the nested
field (`by_region.region`) does not resolve against the serve shape and falls back
to live.

That is safe (the answer is right, computed in the warehouse) and it is what this
scenario pins: the source's flat columns serve from storage while any nested
traversal recomputes. Worth knowing because a source with a nested field looks
fully materialized and is, for its scalar columns — but every query touching the
nested field pays the live cost.

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

Un-nest `by_region` and re-aggregate. The values are correct — but this step
alone cannot say which path served them, since stored and live are identical
before the mutation below.

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

## Query unnested (again)

FRESH (US = 1300, not the stored 300) ⇒ the nested traversal fell back to live.
Contrast `## Query flat (again)` directly above, which is STALE — the same source
serves its scalar columns from the snapshot in the same breath. This is the pair
that shows the `json` carry is opaque: the column is in the table, but record-field
traversal does not resolve through the serve shape.

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
| US     | 1300         |
