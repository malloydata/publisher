---
id: orchestrator-manifest-bind
tags: orchestration
package: orch
---

# Orchestrator manifest bind controls the serve source

In production the orchestrator — not the publisher — is the authoritative
producer of what gets served: it stamps a build manifest and PATCHes the
package's `manifestLocation`, and the publisher fetches + binds it. This scenario
drives that path directly (the harness plays the orchestrator): it binds a full manifest, a
present-but-empty manifest, and clears the binding, and shows each controls
whether the query is served from storage or live.

The signal is staleness: after publishing we mutate the source. A query served
from the materialized snapshot stays stale (`150`); a query served live reflects
the mutation (`1150`).

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.orch_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |
| 4            | 2026-01-02      | 25         |

## Model orch.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.orch_orders')

#@ persist name="orch_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}
```

## Publish

The publisher's own post-build load binds the freshly built table (local-store
rebind).

expect binding: daily -> lake

## Query rollup

```malloy
run: daily -> { select: order_date, total_amount; order_by: order_date asc }
```

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 225          |

## Mutate orders_pg.orch_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 99           | 2026-01-01      | 1000       |

## Query rollup (again)

Stale ⇒ served from the materialized snapshot (the local-store binding).

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 225          |

## Bind orch (empty)

The orchestrator binds a present-but-EMPTY manifest — it vouches for nothing —
so the storage binding is dropped and serving reverts to live.

## Query rollup (again)

Live ⇒ reflects the mutation (`1150`). Proves the orchestrator's (empty) manifest
overrode the publisher's local-store binding.

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 1150         |
| 2026-01-02 | 225          |

## Bind orch

The orchestrator binds the FULL manifest (the last build's entries) via
`manifestLocation`.

## Query rollup (again)

Stale again ⇒ the orchestrator-bound manifest routes back to the materialized snapshot.

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 225          |

## Bind orch (clear)

Clearing `manifestLocation` (null) is an explicit "stop serving materialized" —
the publisher drops BOTH the in-warehouse table substitution and the storage
serve bindings and reverts to live (it does NOT fall back to the local-store
rebind; an operator
that clears the manifest wants live). Distinct code path from
the empty-manifest bind above, same outcome.

## Query rollup (again)

Live again ⇒ reflects the mutation (`1150`).

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 1150         |
| 2026-01-02 | 225          |
