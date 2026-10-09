---
id: an-orchestrator-drops-a-superseded-generation-and-reclaims-its-storage
tags: orchestration, operator, gc
package: dgc
---

<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# An orchestrator drops a superseded generation and reclaims its storage

An orchestrator that assigns its own generational table names owns their
garbage collection. Once serving has moved to `dgc_daily__g002`, generation 1
is garbage — but a storage destination is not reachable through the connection
routes, so the storage-destination routes are how the orchestrator lists the
destination, drops the old generation, and reclaims the bytes behind it.

Proves the three steps are distinct, and each does only its own part:

- dropping retires the table in the catalog, and the files stay in storage;
- a file cleanup inside the recovery window deletes nothing;
- outside it, the first cleanup expires the snapshots and only schedules the
  files, and the next one deletes them — the file age counts from when the
  snapshot expiry scheduled them;
- the serving generation is untouched throughout.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.dgc_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-02      | 200        |

## Model dgc.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.dgc_orders')

#@ persist name="dgc_daily" storage=lake
source: daily_orders is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}
```

## Build (orchestrated, pkg=dgc)

- daily_orders -> dgc_daily__g001 @ lake

## Mutate orders_pg.dgc_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 3            | 2026-01-01      | 1000       |

## Build (orchestrated, pkg=dgc)

- daily_orders -> dgc_daily__g002 @ lake

## Bind dgc

Serving moves to generation 2; generation 1 is now superseded.

## Destination tables

Expect:

| name              |
| ----------------- |
| dgc_daily__g001 |
| dgc_daily__g002 |

## Destination drop dgc_daily__g001

## Destination tables

Expect:

| name              |
| ----------------- |
| dgc_daily__g002 |

## Destination files

Dropped in the catalog, but its files are still in storage.

Expect:

| table             |
| ----------------- |
| dgc_daily__g001 |
| dgc_daily__g002 |

## Destination cleanup (snapshots=7d, files=0s)

Everything is inside a 7-day recovery window, so nothing is reclaimed.

deleted: 0

## Destination cleanup (snapshots=0s, files=0s)

Expires the snapshots that still reference generation 1 and schedules its
files — scheduled at this moment, so not yet older than the cutoff.

deleted: 0

## Destination cleanup (snapshots=0s, files=0s)

deleted: 1

## Destination files

Expect:

| table             |
| ----------------- |
| dgc_daily__g002 |

## Query served generation

```malloy
run: daily_orders -> { select: order_date, total_amount; order_by: order_date asc }
```

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 1100         |
| 2026-01-02 | 200          |
