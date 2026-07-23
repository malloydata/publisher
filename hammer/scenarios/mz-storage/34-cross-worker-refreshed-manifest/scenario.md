---
id: cross-worker-refreshed-manifest
tags: orchestration, cluster, needs-attention
package: xwr
---

# Cross-worker build via a refreshed manifest (the realistic orchestrator flow)

The flow we actually expect to work (James):

- 2 publishers p1, p2; 1 package, 2 `storage=` sources: `daily`, `monthly`
  (`monthly` reads `daily`).
- `daily` is built on p1. The orchestrator refreshes the manifest on BOTH p1 and
  p2 (a `manifestLocation` bind — the same distribution channel scenarios 21/24
  use).
- `monthly` is built on p2. It SHOULD resolve+build from `daily`'s materialized
  lake table, because p2 holds `daily`'s entry via the refreshed manifest — even
  though p2 never built `daily` itself.

The reuse probe: mutate the source after `daily` builds; if `monthly` reused
`daily`'s table it sees the pre-mutation snapshot (`grand_total = 350`); a
recompute from the warehouse sees the mutation (`1350`). Wiring in `hooks.ts`.

## Data orders_pg.xwr_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |

## Model xwr.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.xwr_orders')

#@ persist name="xwr_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}

#@ persist name="xwr_monthly" storage=lake
source: monthly is daily -> {
  aggregate: grand_total is total_amount.sum()
}
```

## Publisher p1

## Publish xwr (sources=daily)

p1 builds the upstream.

expect binding: daily -> lake

## Publisher p2

Start p2 (a separate store; it never built `daily`).

## Bind xwr (pub=p1, from=p1)

The orchestrator refreshes `daily`'s manifest on p1 …

## Bind xwr (pub=p2, from=p1)

… and on p2 — the key step: p2 now holds `daily`'s entry via the refreshed
manifest, though it never built it.

## Mutate orders_pg.xwr_orders

Change the source AFTER `daily` built — a reuse reads the old table; a recompute
would pick this up.

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 99           | 2026-01-01      | 1000       |

## Build (orchestrated, pkg=xwr)

p2 builds the downstream with NO reference couriered — it must resolve `daily`
from the refreshed manifest it holds.

- monthly -> xwr_monthly__g1 @ lake

## Bind xwr (pub=p2)

Bind p2's monthly build so the serve routes to it.

## Query monthly (pub=p2)

`monthly` reused `daily`'s pre-mutation table (`350`), not a warehouse recompute
(`1350`).

```malloy
run: monthly -> { select: grand_total }
```

Expect:

| grand_total:num |
| --------------- |
| 350             |

## Note (since=2026-07-23)

> GREEN: the build now resolves the upstream from the bound (orchestrator-distributed)
> manifest. Architectural follow-up (James): the build's upstream resolution now
> draws on THREE sources — the couriered `referenceManifest`, the bound manifest,
> and the local materialization store. That smells like incremental domain drift.
> Evaluate collapsing to a single source of truth — **the bound manifest is
> authoritative; the local store is ephemeral** — if that holds up in the standalone
> (no-orchestrator) case too. If it collapses, resolve-local-from-store and the
> `referenceManifest` courier may both become redundant.
