---
id: persist-without-flag-served-live
tags: eligibility, build-control, needs-attention
package: pwf
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Without `##! experimental.persistence`, a `#@ persist` source is still planned, built and served

Malloy plans persisted sources only in a file that carries `##!
experimental.persistence`; its `getBuildTargets()` throws on any other. Before the
v1 cutover, `compilePackageBuildPlan` skipped such a file, so a `#@ persist` in a
model that forgot the header was silently inert and served live (this scenario's
id still names that).

Under the v1 cutover (the only path since 6g), the planner
compiles a build copy of the file with the flag as its first line (D3), so the
author's file needs no flag: the source is planned under its own address, built
into the lake and served from its table by the served copy.

The proof: build, mutate the warehouse, re-query. A **stale** re-query (the
pre-mutation value) proves the stored table answered.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.pwf_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |

## Model pwf.malloy

Note the **missing** `##! experimental.persistence` header.

```malloy
source: orders is orders_pg.table('public.pwf_orders')

#@ persist name="pwf_daily" storage=lake
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

## Mutate orders_pg.pwf_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 99           | 2026-01-01      | 1000       |

## Query base (again)

Stale `150` ⇒ the stored table answered: the flagless file was planned from its
build copy and its source built.

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 200          |
