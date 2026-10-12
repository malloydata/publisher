---
id: an-upstream-in-another-destination-is-a-missing-upstream-not-a-shape-miss
tags: orchestration, chained, build-control
package: cxd
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A stored upstream in another destination is a missing upstream, which strict refuses and non-strict recomputes

A chained build reads its upstreams from the destination it writes to. `daily`
is stored in `lake`; `rollup` is declared into `far`, a second destination, and
reads `daily` through `daily_wide`. From `far`, `daily`'s table does not exist.

Two failures look alike here and must be told apart. A downstream that cannot be
EXPRESSED over its parents (`an-intermediate-reaching-the-warehouse-…`) has no
build over them, so strict permits the recompute. A parent that IS materialized
but somewhere this build cannot read is not that: the table exists, the
orchestrator pinned it, and recomputing it would rebuild it unasked. That is the
miss strict exists to refuse, so under strict the run fails, naming the upstream
and the destination it is absent from — not the compiler's "undefined object",
which reads the same for both. Non-strict, the recompute is allowed and the
entry says so.

## Connection far (type=ducklake)

A second, isolated destination for `rollup`.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.cxd_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-02      | 200        |

## Model cxd.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.cxd_orders')

#@ persist name="cxd_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}

source: daily_wide is daily -> { select: * } extend {
  dimension: big_day is total_amount > 100
}

#@ persist name="cxd_rollup" storage=far
source: rollup is daily_wide -> {
  aggregate: grand_total is total_amount.sum()
}
```

## Publish

Non-strict: `rollup` cannot read `daily` from `far`, so it is recomputed from
the warehouse, and its entry says so.

expect binding: daily -> lake
expect binding: rollup -> far
expect upstreams: rollup -> recomputed

## Query rollup

```malloy
run: rollup -> { select: grand_total }
```

Expect:

| grand_total:num |
| --------------- |
| 300             |

## Build refused (orchestrated, strict, pkg=cxd)

Strict: the same `rollup`, reusing the `daily` already built in `lake`. The
upstream is materialized and referenced, but not where this build can read it,
so strict refuses — and the reason says where it is missing from.

- rollup -> cxd_rollup__g2 @ far
  reference: daily

cites: materialized in destination 'lake', not 'far'

## Mutate orders_pg.cxd_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 99           | 2026-01-03      | 1000       |

## Publish (forceRefresh, sources=rollup)

Rebuild `rollup` alone, non-strict. It cannot read `daily` from `far`, so it
recomputes from the warehouse — and the rows say so. (Binding only `rollup`'s
new manifest leaves `daily` serving live from here on, which is why the strict
refusal above runs first: its `reference: daily` is enriched from the bound
manifest, and after this step that manifest no longer holds `daily`.)

expect upstreams: rollup -> recomputed

## Query rollup (again)

The recompute saw the 1000.

Expect:

| grand_total:num |
| --------------- |
| 1300            |
