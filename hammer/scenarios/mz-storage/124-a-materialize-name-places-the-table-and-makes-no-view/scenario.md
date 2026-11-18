---
id: a-materialize-name-places-the-table-and-makes-no-view
tags: materialize, orchestration, operator
package: nvt
---

<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# `#(materialize) name=` places the table, and no view is made

A self-instructed build of a `#(materialize)` source writes its table at
`name=` itself, and a forced self-instructed build rebuilds it there in place.
When the package later builds by generation (a host's instructions), the host
names the table, and Publisher makes no view at `name=`: the table already
there stays as the package's own, and the generation is a table of its own
beside it.

Replaces the scenario that proved a `name=` view replaced the table its own
package wrote there: Publisher no longer makes `name=` views.

The forced rebuild comes before the host's build, not after it: once a host
has built a version, that version serves the host's manifest or answers live,
never a local run's tables, so a self-instructed run of it is refused (409
`MATERIALIZATION_CONFLICT`).

Proves, against a real DuckLake destination:

- the self-instructed build writes `nvt_daily`, a table;
- a forced self-instructed build writes `nvt_daily` again, with the new rows;
- the orchestrated build writes the generation the host named, and leaves
  `nvt_daily` a table, with no view anywhere in the destination.

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

## Publish (forceRefresh)

Self-instructed again, with the new order: the table at `name=` is rebuilt in
place.

expect binding: daily_orders -> lake

## Operator lake

```sql
SELECT order_date::VARCHAR AS order_date, total_amount::INTEGER AS total_amount
FROM lake.nvt_daily ORDER BY order_date;
```

Expect:

| order_date | total_amount:int |
| ---------- | ---------------- |
| 2026-01-01 | 1100             |
| 2026-01-02 | 200              |

## Build (orchestrated, pkg=nvt)

The host names the generation; no view is made at `name=`.

- daily_orders -> nvt_daily__g001 @ lake

## Destination tables

Both are tables: the generation, and the package's own table at `name=`.

Expect:

| name            |
| --------------- |
| nvt_daily       |
| nvt_daily__g001 |

## Operator lake

No view anywhere in the destination, and `nvt_daily` still holds the rows the
forced self-instructed build wrote.

```sql
SELECT 'view:' || view_name AS item FROM duckdb_views() WHERE database_name = 'lake' AND NOT internal
UNION ALL
SELECT 'rows:' || CAST(sum(total_amount)::INTEGER AS VARCHAR) AS item FROM lake.nvt_daily
ORDER BY item;
```

Expect:

| item      |
| --------- |
| rows:1300 |
