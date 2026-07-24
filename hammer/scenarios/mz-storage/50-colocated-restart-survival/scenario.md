---
id: colocated-restart-survival
tags: lifecycle, durability
package: cs
---

# Restart survival: a COLOCATED materialization is re-established from the store

The storage-tier analogue is `restart-survival` (12); this pins the same property
for the **colocated** (v0, in-warehouse) tier. A plain `#@ persist` source (no
`storage=`) materializes into its own warehouse and serves from that snapshot.
After a restart that PRESERVES the store (no re-init, no rebuild), serving must be
re-established on load from the persisted manifest — the query keeps routing to the
materialized snapshot instead of silently reverting to live-recompute.

Two things this scenario deliberately fixes-and-pins together:
- **Both tiers now survive restart, symmetrically.** Before, serve routing was
  in-memory (set only by a build's auto-load), so a restart silently reverted a
  materialized source to serving live. `rebindServeBindingsFromLocalStore` now
  re-derives it on load for colocated too, not just `storage=`.
- **Colocated is independent of `PERSIST_STORAGE_MODE`.** Colocated is the v0 path
  and is not gated by the storage kill switch, so this runs with the tier **off**
  (the default). Restart-survival for colocated holds even when the external tier
  is off — proving the colocated rebind is not behind the storage gate.

Proof (as in `off-builds-colocated`): build, mutate, re-query. A frozen snapshot
serves the pre-mutation value (**stale**); a live serve would show the mutation
(**fresh**). Stale after the restart ⇒ routing was restored from the store.

## Publisher

- PERSIST_STORAGE_MODE: off

## Data orders_pg.cs_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |

## Model cs.malloy

A plain colocated `#@ persist` — no `storage=`.

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.cs_orders')

#@ persist name="cs_daily"
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}
```

## Publish

`daily` builds colocated into its own (source) warehouse. No `storage=`, so no
storage binding is produced.

## Query base

```malloy
run: daily -> { select: order_date, total_amount; order_by: order_date asc }
```

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 200          |

## Mutate orders_pg.cs_orders

Change the source so a live recompute would read `1150` for 2026-01-01; the
colocated snapshot stays `150`.

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 99           | 2026-01-01      | 1000       |

## Query base (again)

Still `150` ⇒ served from the colocated snapshot (not recomputed live). Confirms
colocated serving is active BEFORE the restart.

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 200          |

## Restart

Reboot preserving the store — no rebuild, tier still `off`.

## Query base (again)

Still `150` ⇒ colocated serving survived the restart, re-established from the
persisted manifest on load (not lost, and not silently recomputed live). Same
durability the `storage=` tier gets in `restart-survival` (12) — now symmetric,
and holding even with the storage tier off.

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 200          |
