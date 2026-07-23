---
id: collision-enforce-refuses-publish
tags: eligibility, safety, config
package: coe
---

# PERSIST_COLLISION_ENFORCE rejects a colliding publish

A within-package persist-target collision is warn-only by default (see
`within-package-collision`), so a package published before the check existed
never breaks on a routine re-publish. A deployment that has audited its packages
can opt into strictness with `PERSIST_COLLISION_ENFORCE=true`, which turns the
collision into a hard **publish rejection** — but only on the author-in-the-loop
publish path (POST /packages), never at startup/reload (still fail-safe).

The discriminating pair: the SAME colliding package published against two
publishers. The one booted with `PERSIST_COLLISION_ENFORCE=true` refuses the
publish; the one without accepts it (warn-only). The flag is fixed at process
start, so each is a separate publisher (`## Publisher` carries the env var).

`alpha` and `beta` both declare `name="coe_dup" storage=lake` — the collision.

## Publisher strict

Boot with the enforce flag set.

- PERSIST_STORAGE_MODE: on
- PERSIST_COLLISION_ENFORCE: true

## Data orders_pg.coe_orders

| order_id:int | order_date:date | region:text | amount:num |
| ------------ | --------------- | ----------- | ---------- |
| 1            | 2026-01-01      | US          | 100        |
| 2            | 2026-01-02      | EU          | 200        |

## Model coe.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.coe_orders')

#@ persist name="coe_dup" storage=lake
source: alpha is orders -> {
  group_by: order_date
  aggregate: n is count()
}

#@ persist name="coe_dup" storage=lake
source: beta is orders -> {
  group_by: region
  aggregate: total is amount.sum()
}
```

## Republish refused coe

Under `PERSIST_COLLISION_ENFORCE`, publishing the colliding package is rejected.

cites: resolve to the same materialized table

## Publisher lax

A second publisher WITHOUT the flag (the default posture).

- PERSIST_STORAGE_MODE: on

## Republish coe

Warn-only: the same colliding package publishes successfully (the collision is
surfaced as a warning, not a rejection).
