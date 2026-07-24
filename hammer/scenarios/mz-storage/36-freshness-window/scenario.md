---
id: freshness-window
tags: serve-correctness, freshness
package: fr
---

# Freshness gate on the storage tier — enforced, uniform with in-warehouse

`storage=` is a placement choice, orthogonal to freshness: the DuckLake serve path
honors `{window, fallback}` the SAME way the colocated serve does (see
`freshness-window-in-warehouse`). When a bound entry is stale past its window, the
per-query gate drops it from the serve shape and falls back per `fallback` — the
storage analogue of the colocated path dropping it from the build manifest. This scenario drives
both outcomes on the storage tier: `stale_ok` keeps serving the (stale) lake table;
`live` recomputes from the source warehouse. Flip this source to `storage=source`
and the behavior is identical.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.fr_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |

## Model fr.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.fr_orders')

#@ persist name="fr_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}
```

## Publish

expect binding: daily -> lake

## Query base

```malloy
run: daily -> { select: order_date, total_amount; order_by: order_date asc }
```

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 200          |

## Mutate orders_pg.fr_orders

Change the source so a live recompute (`1150`) differs from the lake snapshot.

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 99           | 2026-01-01      | 1000       |

## Bind fr (asof=2000-01-01T00:00:00Z, fresh=1, fallback=stale_ok)

Bind the entry stale-past-window (`dataAsOf` = year 2000, 1s window) with
`stale_ok` — keep serving the stale lake table.

## Query base (again)

Still `150` ⇒ `stale_ok` serves the stale lake table (does not recompute).

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 200          |

## Bind fr (asof=2000-01-01T00:00:00Z, fresh=1, fallback=live)

Same stale entry, now `fallback=live` — the gate drops it from the serve shape.

## Query base (again)

Live ⇒ `1150`. The storage serve honored the window and recomputed from the source
warehouse — the same outcome the in-warehouse path gives (`freshness-window-in-warehouse`).

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 1150         |
| 2026-01-02 | 200          |
