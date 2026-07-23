---
id: cross-worker-reference-isolation
tags: orchestration, cluster
package: xw
---

# The negative case: a thin reference with NO resolution source fails loudly

Cross-worker reuse DOES work when the orchestrator refreshes the manifest first
(see `cross-worker-refreshed-manifest`). This scenario pins the complementary
NEGATIVE case: when a worker has neither the record nor a refreshed manifest, a
bare thin reference is not enough, and the build must fail loudly — never silently
recompute wrong or serve a table it can't reach.

Two publishers share one config (same warehouse, DuckLake, packages) but have
SEPARATE materialization stores — two orchestrator workers. `p1` builds the
upstream; `p2` (which never built it, and was NOT sent a refreshed manifest) is
asked to build the downstream with only a thin reference. `p2` can't resolve the
upstream from its own store, has no bound manifest to resolve from, and the wire
`ManifestReference` can't carry the storage fields — so the build fails.

The takeaway: to build a downstream on a worker that didn't build the upstream,
refresh the manifest on that worker first (the realistic flow). Wiring in
`hooks.ts`.

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

Ask p2 to build the downstream referencing `daily` from p1. p2 can't resolve the
storage upstream (no local record, no refreshed manifest) and the thin reference
can't carry the storage fields — so the build fails loudly rather than serving
wrong data.

- rollup -> xw_rollup__g1 @ lake
  reference: daily (from=p1)

cites: does not exist
