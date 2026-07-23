---
id: reference-manifest-reuse
tags: orchestration, chained
package: rmr
---

# Reference-manifest reuse: a downstream reads a prior-run upstream table

The orchestrator can build a downstream persist source WITHOUT rebuilding its
upstream, by supplying the upstream's already-built table as a `referenceManifest`
entry. The downstream build resolves the upstream reference to the existing table
instead of recomputing the chain from the warehouse.

The proof is a mutation wedged between the two builds: we build the upstream, then
mutate the source, then build the downstream referencing the upstream. If the
downstream truly reused the upstream's table it sees the PRE-mutation snapshot
(`grand_total = 375`); a recompute from the warehouse would see the mutation
(`1375`). The `reference: daily` clause names the upstream — the harness resolves
its id + physical table from the latest manifest (the publisher resolves the
storage fields via resolve-local).

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.rmr_orders

| order_id:int | order_date:date | region:text | amount:num |
| ------------ | --------------- | ----------- | ---------- |
| 1            | 2026-01-01      | US          | 100        |
| 2            | 2026-01-01      | US          | 50         |
| 3            | 2026-01-02      | EU          | 200        |
| 4            | 2026-01-02      | US          | 25         |

## Model rmr.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.rmr_orders')

#@ persist name="rmr_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}

#@ persist name="rmr_rollup" storage=lake
source: rollup is daily -> {
  aggregate: grand_total is total_amount.sum()
}
```

## Publish rmr (sources=daily)

Build ONLY the upstream `daily` (auto-run). This is the "prior run".

expect binding: daily -> lake

## Mutate orders_pg.rmr_orders

Change the source AFTER `daily` built — a reuse reads the old table; a recompute
would pick this up.

| order_id:int | order_date:date | region:text | amount:num |
| ------------ | --------------- | ----------- | ---------- |
| 99           | 2026-01-01      | US          | 1000       |

## Build (orchestrated, pkg=rmr)

Build ONLY the downstream, referencing the already-built `daily` by name.

- rollup -> rmr_rollup__g1 @ lake
  reference: daily

## Bind rmr

Distribute the downstream build's manifest so the serve routes to it.

## Query rollup

The downstream reused `daily`'s pre-mutation table (`375`) — not a warehouse
recompute (`1375`).

```malloy
run: rollup -> { select: grand_total }
```

Expect:

| grand_total:num |
| --------------- |
| 375             |
