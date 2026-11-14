---
id: a-stored-parent-reached-through-a-selective-import-is-stacked-on
tags: orchestration, chained, serve-correctness, imports
package: sim
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A stored parent reached through a selective import is stacked on

`orders.malloy` declares two stored sources, `daily` and `weekly is daily ->
{ … }`. `reports.malloy`, the model queries run against, does
`import { weekly } from "orders.malloy"` — so its namespace holds `weekly`
and not `daily`, which `weekly` is built from. The package plans `weekly`
through the last model that compiles it, which is `reports.malloy`.

A build of `weekly` must still read `daily`'s table: the model the build is
handed knows `daily` as a hidden dependency, not as a name, and a chain that
stacks on its parent when both are declared in one file must stack on it
when one is reached through an import. Treating the unnamed parent as a
read of the warehouse would recompute `daily` under strict and report it as
a shape the destination cannot express — for a source whose every input is
a stored table.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.sim_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |

## Model sim/reports.malloy

```malloy
##! experimental.persistence

import { weekly } from "orders.malloy"
```

## Model sim/orders.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.sim_orders')

#@ persist name="sim_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}

#@ persist name="sim_weekly" storage=lake
source: weekly is daily -> {
  aggregate: grand_total is total_amount.sum()
}
```

## Publish

expect binding: daily -> lake
expect binding: weekly -> lake
expect upstreams: weekly -> reused

## Query weekly

```malloy
run: weekly -> { select: grand_total }
```

Expect:

| grand_total:num |
| --------------- |
| 350             |

## Mutate orders_pg.sim_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 99           | 2026-01-03      | 1000       |

## Build (orchestrated, strict, pkg=sim)

Strict: `weekly` alone, `daily` by reference. Stacks on the table.

- weekly -> sim_weekly__g2 @ lake (reused)
  reference: daily

## Bind sim

## Query weekly (again)

Still 350: `daily`'s stored rows, not the warehouse's.

Expect:

| grand_total:num |
| --------------- |
| 350             |
