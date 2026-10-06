---
id: strict-still-refuses-an-upstream-never-built-through-an-intermediate
tags: orchestration, chained, build-control
package: snb
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# strictUpstreams: an upstream never built is still refused when it is reached through an intermediate

`strict-upstreams-refused` pins the direct case: `rollup` names `daily`, `daily`
was never built nor referenced, strict refuses. This is the same rule with a
non-persisted source between them. Carrying intermediates into the build widened
what a strict build can express; it must not have widened what strict permits.
A persisted upstream the build cannot see is a dispatch miss whichever path
reaches it, and recomputing it would rebuild a table the orchestrator meant to
pin — exactly what the recompute `an-intermediate-reaching-the-warehouse-…`
allows must not extend to.

So: same model as `chained-through-an-intermediate`, `rollup` built alone under
strict with `daily` neither built nor referenced, and the run refuses, naming
the missing upstream.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.snb_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-02      | 200        |

## Model snb.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.snb_orders')

#@ persist name="snb_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}

source: daily_wide is daily -> { select: * } extend {
  dimension: big_day is total_amount > 100
}

#@ persist name="snb_rollup" storage=lake
source: rollup is daily_wide -> {
  aggregate: grand_total is total_amount.sum()
}
```

## Build refused (orchestrated, strict, pkg=snb)

`daily` is a persisted upstream of `rollup`, reached through `daily_wide`, and
nothing in this build provides it. Strict refuses rather than recomputing it,
and the refusal names the upstream and says what would have supplied it. The
cite is the refusal's own wording, not the compiler's manifest miss, so this
pins that the refusal is decided from what the source reaches.

- rollup -> snb_rollup__g1 @ lake

cites: 'daily' is not in this build's manifest (neither built in this run nor supplied by reference)
