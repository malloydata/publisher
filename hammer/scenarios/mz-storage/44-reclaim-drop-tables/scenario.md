---
id: reclaim-drop-tables
tags: lifecycle, build-control
package: gc
---

# Reclaim a materialization (dropTables) frees the table and reverts to live

Deleting a materialization with `?dropTables=true` runs the destination-aware
read-write drop of the physical table in the storage destination — the tier's GC
primitive. After a reclaim, the source has no materialized table to serve from, so
once the package re-establishes its serve bindings it reverts to serving live from
the source warehouse (never a dangling route to a table that no longer exists).

The proof: build + route, mutate so a live recompute differs from the snapshot,
confirm the snapshot is being served (stale), then reclaim the table and restart.
A fresh (post-mutation) result on the re-query proves the table was dropped and
serving fell back to live.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.gc_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |

## Model gc.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.gc_orders')

#@ persist name="gc_daily" storage=lake
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

## Mutate orders_pg.gc_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 99           | 2026-01-01      | 1000       |

## Query base (again)

Still `150` ⇒ served from the materialized snapshot (routed).

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 200          |

## Reclaim gc

Drop the materialized table via `?dropTables=true`.

## Restart

Re-establish serving from the store. The reclaimed materialization is gone, so
`daily` has no table to bind — it reverts to serving live.

## Query base (again)

Live `1150` ⇒ the table was reclaimed and serving fell back to the warehouse
(no dangling route to the dropped table).

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 1150         |
| 2026-01-02 | 200          |
