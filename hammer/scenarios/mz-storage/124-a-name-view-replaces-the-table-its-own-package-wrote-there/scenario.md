---
id: a-name-view-replaces-the-table-its-own-package-wrote-there
tags: materialize, orchestration, operator
package: nvt
---

<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A `name=` view replaces the table its own package wrote there

A self-instructed build of a `#(materialize)` source names its table `name=`
itself. When the package later moves to orchestrated builds, the host names
each table by generation and Publisher puts a view at `name=` reading it. The
table already at `name=` was written by this package's own committed run, as
Publisher's records show, so the view replaces it rather than refusing the
build: the table is renamed aside and the view made in one DuckLake
transaction, and the aside copy is dropped once the run commits. Back on self-instructed builds,
the table replaces the package's own view.

Proves, against a real DuckLake destination:

- the orchestrated build succeeds, and `nvt_daily` is now a view reading the
  generation the host named, so a reader outside Malloy sees the new rows;
- the destination's table listing no longer holds the old table, nor its
  aside copy, once the run has committed and dropped that copy (just after
  the commit);
- a forced self-instructed build puts a table back at `nvt_daily`.

## Publisher

- PERSIST_STORAGE_MODE: on
- PUBLISHER_MATERIALIZE: true

## Data orders_pg.nvt_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-02      | 200        |

## Model nvt.malloy

```malloy
source: orders is orders_pg.table('public.nvt_orders')

#(materialize) name="nvt_daily" storage="lake"
source: daily_orders is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}
```

## Publish

Self-instructed: the table is called `nvt_daily` itself.

expect binding: daily_orders -> lake

## Destination tables

Expect:

| name      |
| --------- |
| nvt_daily |

## Mutate orders_pg.nvt_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 3            | 2026-01-01      | 1000       |

## Build (orchestrated, pkg=nvt)

The host names the generation. The table at `nvt_daily` is this package's
own, so the view replaces it instead of refusing the build.

- daily_orders -> nvt_daily__g001 @ lake

## Destination tables (within=10s)

The old table is gone, and so is the copy kept aside until the commit; a view
is never listed as a table. The run drops the aside copy just after its commit,
and the commit is what reports the run finished, so a listing taken the moment
the run reads finished can still hold the copy for a few milliseconds. The
listing is re-read until it settles, within a bound: a copy that is never
dropped still fails here.

Expect:

| name              |
| ----------------- |
| nvt_daily__g001 |

## Operator lake

A reader outside Malloy reads the new generation through the stable name.

```sql
SELECT order_date::VARCHAR AS order_date, total_amount::INTEGER AS total_amount
FROM lake.nvt_daily ORDER BY order_date;
```

Expect:

| order_date | total_amount:int |
| ---------- | ---------------- |
| 2026-01-01 | 1100             |
| 2026-01-02 | 200              |

## Publish (forceRefresh)

Self-instructed again: the table replaces the package's own view.

expect binding: daily_orders -> lake

## Destination tables

Expect:

| name              |
| ----------------- |
| nvt_daily         |
| nvt_daily__g001 |
