---
id: within-package-collision
tags: eligibility, safety
package: col
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

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

Neither source is served from the shared table, which holds whichever build ran
last: queries on both are answered live.

Rejecting a *publish* of a colliding package (the default; `PERSIST_COLLISION_ENFORCE=false`
opts out) is covered by `collision-enforce-refuses-publish`.

## Publisher

The harness publishes the package, and a colliding publish is refused by
default, so this publisher opts out to reach the load-time warning.

- PERSIST_STORAGE_MODE: on
- PERSIST_COLLISION_ENFORCE: false

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

The package still answers despite the collision — the warning is non-fatal —
from the warehouse, not from the table the two sources share.

```malloy
run: alpha -> { select: order_date, n; order_by: order_date asc }
```

Expect:

| order_date | n |
| ---------- | - |
| 2026-01-01 | 2 |
| 2026-01-02 | 1 |
