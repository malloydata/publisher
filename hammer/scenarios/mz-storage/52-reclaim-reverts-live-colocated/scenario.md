---
id: reclaim-reverts-live-colocated
tags: lifecycle, build-control
package: rrc
---

# Reclaim reverts to live IN PLACE (colocated) — no restart

The colocated analogue of `reclaim-reverts-live-external` (51): after reclaiming a
**colocated** (`#@ persist`, no `storage=`) materialization, serving must revert to
live immediately, WITHOUT a restart. The inline post-delete rebind must re-derive
BOTH tiers — so a dropped colocated table is not left routed.

This exercises the same symmetry as `colocated-restart-survival` (50), on the
delete path instead of the load path: the post-delete rebind must cover colocated,
not just `storage=`, or a reclaimed colocated source keeps routing to a table that
was just dropped (a query error), instead of falling back to live.

Runs with the tier `off` (colocated is the v0 path, not gated by the storage kill
switch), proving the colocated post-delete rebind is not behind the storage gate.

Proof: build + route, mutate so live differs from the snapshot, confirm stale
(routed), reclaim, then re-query with NO restart. Fresh ⇒ the inline post-delete
rebind cleared the colocated binding and serving fell back to live.

## Publisher

- PERSIST_STORAGE_MODE: off

## Data orders_pg.rrc_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |

## Model rrc.malloy

A plain colocated `#@ persist` — no `storage=`.

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.rrc_orders')

#@ persist name="rrc_daily"
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}
```

## Publish

`daily` builds colocated into its own (source) warehouse.

## Mutate orders_pg.rrc_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 99           | 2026-01-01      | 1000       |

## Query base

Still `150` ⇒ served from the colocated snapshot (routed).

```malloy
run: daily -> { select: order_date, total_amount; order_by: order_date asc }
```

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 200          |

## Reclaim rrc

Drop the materialized table via `?dropTables=true`. No restart follows.

## Query base (again)

Live `1150` ⇒ the inline post-delete rebind cleared the colocated binding (no
remaining materialization) and serving fell back to the warehouse — in place, with
no restart, and with the storage tier off.

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 1150         |
| 2026-01-02 | 200          |
