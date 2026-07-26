---
id: within-package-collision
tags: eligibility, safety
package: col
---

# Within-package persist-target collision surfaces as a warning

Two DISTINCT persist sources (different `sourceEntityId`) that resolve to the
same physical target — the same `#@ persist name=` in the same destination — would
clobber each other: the second build's replace overwrites the first, two serve
bindings point at one table, and a GC drop of one takes out the other. The
publisher detects this at load and surfaces it as an operator **warning** on the
package status (it does NOT fail the package — warn-only, so a pre-existing latent
collision never darks an already-published package).

Here `alpha` and `beta` both declare `name="col_dup" storage=lake`. The package
loads and still serves (the warning is non-fatal), and the collision is visible on
the status `warnings` array.

The escalation — rejecting a *publish* of a colliding package once
`PERSIST_COLLISION_ENFORCE` is set — is a deployment-level switch fixed at process
start; it is covered by the `persistence_policy` unit tests (the harness starts
publishers at a `PERSIST_STORAGE_MODE` only, so it can't yet flip that flag
per-publisher).

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.col_orders

| order_id:int | order_date:date | region:text | amount:num |
| ------------ | --------------- | ----------- | ---------- |
| 1            | 2026-01-01      | US          | 100        |
| 2            | 2026-01-01      | US          | 50         |
| 3            | 2026-01-02      | EU          | 200        |

## Model col.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.col_orders')

#@ persist name="col_dup" storage=lake
source: alpha is orders -> {
  group_by: order_date
  aggregate: n is count()
}

#@ persist name="col_dup" storage=lake
source: beta is orders -> {
  group_by: region
  aggregate: total is amount.sum()
}
```

## Warns col

The collision is surfaced on the package status warnings (not just the log).

cites: resolve to the same materialized table

## Query alpha

The package still serves despite the collision — the warning is non-fatal.

```malloy
run: alpha -> { select: order_date, n; order_by: order_date asc }
```

Expect:

| order_date | n |
| ---------- | - |
| 2026-01-01 | 2 |
| 2026-01-02 | 1 |
