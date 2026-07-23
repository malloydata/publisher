---
id: restart-survival
tags: lifecycle, durability
package: rs
---

# Restart survival: serving is re-established from the store after a restart

A built source serves from storage. After a server restart that PRESERVES the
materialization store (no re-init, no rebuild), serving must be re-established on
load (`rebindStorageServeBindings` from the persisted store) — the query still
routes to the materialized table.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.rs_orders

| order_id:int | order_date:date | region:text | amount:num |
| ------------ | --------------- | ----------- | ---------- |
| 1            | 2026-01-01      | US          | 100        |
| 2            | 2026-01-01      | US          | 50         |
| 3            | 2026-01-02      | EU          | 200        |
| 4            | 2026-01-02      | US          | 25         |

## Model rs.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.rs_orders')

#@ persist name="rs_daily" storage=lake
source: daily_orders is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}
```

## Publish

expect binding: daily_orders -> lake

## Mutate orders_pg.rs_orders

Change the source so a live serve would read 1150 for 2026-01-01; the snapshot
stays 150.

| order_id:int | order_date:date | region:text | amount:num |
| ------------ | --------------- | ----------- | ---------- |
| 99           | 2026-01-01      | US          | 1000       |

## Query rollup

```malloy
run: daily_orders -> { select: order_date, total_amount; order_by: order_date asc }
```

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 225          |

## Restart

Reboot preserving the store — no rebuild.

## Query rollup (again)

Still stale 150 ⇒ serving survived the restart and was re-established from the
store (not lost, and not silently recomputed live).

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 225          |
