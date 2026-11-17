---
id: delete-run-then-drop-table
tags: lifecycle, build-control
package: gc
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Delete a run (record-only), drop its table yourself, and serving stays live

Deleting a materialization is record-only: the table the run built stays in the
storage destination, and table cleanup is the caller's job. Once the record is
gone the source has no run to serve from, so after a restart re-establishes the
serve bindings it serves live from the source warehouse; the caller then drops
the table through the destination, and serving is unaffected (never a dangling
route to a table that no longer exists).

The proof: build + route, mutate so a live recompute differs from the snapshot,
confirm the snapshot is being served (stale), delete the run, check the table is
still there, drop it through the destination, and restart. A fresh
(post-mutation) result on the re-query proves serving fell back to live.

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

## Forget gc

Delete the latest run's record. Record-only: no table is dropped.

## Destination tables

The delete left the table in place.

Expect:

| name     |
| -------- |
| gc_daily |

## Destination drop gc_daily

The caller drops the table itself, through the destination.

## Destination tables

Expect:

| name |
| ---- |

## Restart

Re-establish serving from the store. The deleted run is gone, so `daily` has no
table to bind — it serves live.

## Query base (again)

Live `1150` ⇒ serving fell back to the warehouse (no dangling route to the
dropped table).

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 1150         |
| 2026-01-02 | 200          |
