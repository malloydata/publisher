---
id: cross-worker-reference-isolation
tags: orchestration, cluster
package: xw
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A host-supplied reference at the wrong tier is refused, never silently recomputed

Cross-worker reuse DOES work when the orchestrator refreshes the manifest first
(see `cross-worker-refreshed-manifest`). This scenario pins the complementary
NEGATIVE case: two workers that share one config but keep separate
materialization stores, where the second is handed a bare thin reference to a
table the first built. That is a case the control plane must prevent, so the
build refuses loudly rather than reading or rebuilding across that boundary.

Two publishers share one config (same warehouse, DuckLake, packages) but have
SEPARATE materialization stores — two orchestrator workers. `p1` builds the
upstream; `p2` (which never built it, and was NOT sent a refreshed manifest) is
asked to build the downstream with only a thin reference. The reference names
the stored table but carries no `storageDestinationName`, so it reads as a
warehouse table, while `daily` is declared `storage=lake`: it is supplied at
the wrong tier. An instructed build refuses that, strict or not (decided
2026-11-12), naming the upstream, where the entry is and where the upstream is
declared. A self-instructed build would recompute it from raw with a warning
instead.

A reference CAN carry `storageDestinationName` (since #1619), but even labelled,
a thin reference has no column list, so `p2` still could not stack on the
stored table; it could only recompute it. The takeaway: to build a downstream
on a worker that didn't build the upstream, refresh the manifest on that worker
first (the realistic flow). Wiring in `hooks.ts`.

## Data orders_pg.xw_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |

## Model xw.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.xw_orders')

#@ persist name="xw_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}

#@ persist name="xw_rollup" storage=lake
source: rollup is daily -> {
  aggregate: grand_total is total_amount.sum()
}
```

## Publisher p1

p1 builds the upstream — its store now holds the record.

## Publish xw (sources=daily)

expect binding: daily -> lake

## Publisher p2

p2 has a SEPARATE store and never built the upstream.

## Build refused (orchestrated, pkg=xw)

Ask p2 to build the downstream referencing `daily` from p1. The thin reference
is unlabeled, so it is at the wrong tier for a `storage=` upstream, and the
instructed build refuses it rather than recomputing `daily` from raw.

- rollup -> xw_rollup__g1 @ lake
  reference: daily (from=p1)

cites: the host supplied an upstream entry

## Build refused (orchestrated, strict, pkg=xw)

The same build under a strict manifest refuses the same way. Strictness does
not change what an instructed build does with an entry the host supplied at the
wrong tier.

- rollup -> xw_rollup__g2 @ lake
  reference: daily (from=p1)

cites: the host supplied an upstream entry
