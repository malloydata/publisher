---
id: visibility-leak-guard
tags: security, visibility
package: vis
---

# Visibility: an `except:`-hidden column must not be reachable over storage

A persisted source hides a column with `except:`. That column must not be
reachable when the source is served from storage — the physical table is
projected to the source's public surface at build time, so the hidden column is
never materialized (a DuckLake virtual source exposes every physical column of
the stored table regardless of the declared shape, so narrowing at serve alone is
not enough). Public fields route; the hidden column is refused.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.vis_orders

| order_id:int | order_date:date | region:text | amount:num |
| ------------ | --------------- | ----------- | ---------- |
| 1            | 2026-01-01      | US          | 100        |
| 2            | 2026-01-01      | US          | 50         |
| 3            | 2026-01-02      | EU          | 200        |
| 4            | 2026-01-02      | US          | 25         |

## Model vis.malloy

```malloy
##! experimental.persistence

#@ persist name="vis_daily" storage=lake
source: daily is orders_pg.sql("SELECT order_date, region, amount FROM vis_orders") extend {
  except: region
}
```

## Publish

expect binding: daily -> lake

## Query public fields

```malloy
run: daily -> { group_by: order_date; aggregate: total_amount is amount.sum(); order_by: order_date asc }
```

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 225          |

## Query hidden region (refused)

The `except:`-hidden column is not materialized and not served.

```malloy
run: daily -> { group_by: region }
```

## Mutate orders_pg.vis_orders

| order_id:int | order_date:date | region:text | amount:num |
| ------------ | --------------- | ----------- | ---------- |
| 99           | 2026-01-01      | US          | 1000       |

## Query public fields (again)

Stale ⇒ public fields served from the storage snapshot (narrowing to the public
surface didn't break routing).

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 225          |
