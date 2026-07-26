---
id: multi-publisher-shared-table
tags: orchestration, cluster
package: cl
---

# Cluster: build on one publisher, bind the manifest to another

A real deployment runs a cluster of stateless publishers sharing one DuckLake
tier. The orchestrator materializes a source ONCE (on any worker), then hands the
resulting manifest to the others; every worker then serves the same physical
table without rebuilding it. This runs two publishers — `p1` and `p2` — against
the same config (so they share the source warehouse and the lake), and queries
each directly with `(pub=…)` so the difference shows side by side.

The signal is staleness: after `p1` builds, we mutate the source. A worker serving
from the shared snapshot is stale (`150`); a worker with no binding falls back to
live (`1150`).

## Publisher p1

- PERSIST_STORAGE_MODE: on

## Data orders_pg.cl_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |
| 4            | 2026-01-02      | 25         |

## Model cl.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.cl_orders')

#@ persist name="cl_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}
```

## Publish (pub=p1)

`p1` materializes `cl_daily` into the shared lake and binds it locally.

expect binding: daily -> lake

## Publisher p2

A second worker on the same config. It has the package but has NOT built or been
given a manifest. (Starting it also makes it active, but the queries below name
their target explicitly.)

- PERSIST_STORAGE_MODE: on

## Mutate orders_pg.cl_orders

Change the shared source AFTER p1 built.

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 99           | 2026-01-01      | 1000       |

## Query rollup (pub=p1)

`p1` serves from its lake snapshot — stale (`150`).

```malloy
run: daily -> { select: order_date, total_amount; order_by: order_date asc }
```

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 225          |

## Query rollup (again, pub=p2)

Same query, on `p2` — it has no materialization and no binding, so it falls back
to live: the mutation IS visible (`1150`).

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 1150         |
| 2026-01-02 | 225          |

## Bind cl (pub=p2, from=p1)

The orchestrator hands p1's build manifest to p2 (it points at the shared
`lake.cl_daily` — same content address, same table).

## Query rollup (again, pub=p1)

`p1`: unchanged, still its snapshot (`150`).

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 225          |

## Query rollup (again, pub=p2)

`p2` now serves the SAME lake snapshot p1 built — stale (`150`), without ever
running a build itself.

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 225          |
