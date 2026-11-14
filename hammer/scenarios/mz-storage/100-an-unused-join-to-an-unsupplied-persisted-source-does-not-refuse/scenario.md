---
id: an-unused-join-to-an-unsupplied-persisted-source-does-not-refuse
tags: orchestration, chained, build-control
package: ujp
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# An unused join to an unsupplied persisted source does not refuse

`rollup` is a `storage=` source over the colocated `daily`, and declares
`join_one: c is counts` that it never reads; `counts` is a persist source
nothing supplies to the build. The compiler inlines only the joins a query
uses, so `rollup`'s SQL reads `daily`'s table and never touches `counts`:
nothing is recomputed. Strict must look at what the SQL reads, not at what
the model declares — a declared-but-unused join to an unsupplied table is not
a recompute, and refusing it would turn a valid build away.

(With `daily` itself in storage the build would have to stack on it, and a
model that declares a join to a source the destination cannot bind does not
compile — that shape is refused under strict, as it always was.)

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.ujp_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |

## Model ujp.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.ujp_orders')

#@ persist name="ujp_counts"
source: counts is orders -> {
  group_by: order_date
  aggregate: order_count is count()
}

#@ persist name="ujp_daily"
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}

#@ persist name="ujp_rollup" storage=lake
source: rollup is daily extend {
  join_one: c is counts on order_date = c.order_date
} -> {
  aggregate: grand_total is total_amount.sum()
}
```

## Publish

expect binding: rollup -> lake

## Query rollup

```malloy
run: rollup -> { select: grand_total }
```

Expect:

| grand_total:num |
| --------------- |
| 350             |

## Mutate orders_pg.ujp_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 99           | 2026-01-03      | 1000       |

## Build (orchestrated, strict, pkg=ujp)

Strict: `rollup` over `daily` by reference, `counts` by nothing. The join is
never read, so nothing is recomputed, and the build succeeds from `daily`'s
table — stale, which is the proof it was read rather than recomputed.

- rollup -> ujp_rollup__g2 @ lake (reused)
  reference: daily

## Bind ujp

## Query rollup (again)

Expect:

| grand_total:num |
| --------------- |
| 350             |
