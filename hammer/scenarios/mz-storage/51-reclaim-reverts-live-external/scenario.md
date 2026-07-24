---
id: reclaim-reverts-live-external
tags: lifecycle, build-control
package: rrx
---

# Reclaim reverts to live IN PLACE (external) — no restart

`reclaim-drop-tables` (44) proves reclaim reverts to live across a RESTART — but a
restart re-derives bindings via the load-time rebind, which masks the *in-place*
post-delete rebind that `deleteMaterialization` runs synchronously. This pins that
inline path for the **external** (`storage=`) tier: after a reclaim, serving must
revert to live immediately, WITHOUT a restart — the post-delete rebind re-derives
from the latest remaining materialization (here none ⇒ cleared).

Proof: build + route, mutate so live differs from the snapshot, confirm stale
(routed), reclaim, then re-query with NO restart. Fresh ⇒ the inline post-delete
rebind cleared the binding and serving fell back to live.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.rrx_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |

## Model rrx.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.rrx_orders')

#@ persist name="rrx_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}
```

## Publish

expect binding: daily -> lake

## Mutate orders_pg.rrx_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 99           | 2026-01-01      | 1000       |

## Query base

Still `150` ⇒ served from the materialized snapshot (routed).

```malloy
run: daily -> { select: order_date, total_amount; order_by: order_date asc }
```

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 200          |

## Reclaim rrx

Drop the materialized table via `?dropTables=true`. No restart follows.

## Query base (again)

Live `1150` ⇒ the inline post-delete rebind cleared the storage binding (no
remaining materialization) and serving fell back to the warehouse — in place, with
no restart.

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 1150         |
| 2026-01-02 | 200          |
