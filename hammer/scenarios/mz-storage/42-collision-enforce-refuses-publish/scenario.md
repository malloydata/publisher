---
id: collision-enforce-refuses-publish
tags: eligibility, safety, config
package: coe
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A colliding publish is refused by default; PERSIST_COLLISION_ENFORCE=false opts out

A within-package persist-target collision is a hard **publish rejection** by
default (`PERSIST_COLLISION_ENFORCE` defaults to `true`) — but only on the
author-in-the-loop publish path (POST /packages), never at startup/reload, which
stays fail-safe and only warns (see `within-package-collision`). A deployment
whose existing packages carry a latent collision it cannot fix yet opts out with
`PERSIST_COLLISION_ENFORCE=false`, which turns the rejection back into a warning.

The discriminating pair: the SAME colliding package published against two
publishers. The one booted with the default refuses the publish; the one booted
with `PERSIST_COLLISION_ENFORCE=false` accepts it (warn-only). The flag is fixed
at process start, so each is a separate publisher (`## Publisher` carries the
env var).

`alpha` and `beta` both declare `name="coe_dup" storage=lake` — the collision.

Package versioning is off on both publishers: `## Republish` re-publishes in place (`POST /packages` with no location), which a versioned package refuses with `PACKAGE_IS_VERSIONED`. This scenario is about the collision gate, not versions.

## Publisher strict

Boot with the default: collisions are enforced.

- PERSIST_STORAGE_MODE: on
- PUBLISHER_PACKAGE_VERSIONING: off

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

By default, publishing the colliding package is rejected.

cites: resolve to the same materialized table

## Publisher lax

A second publisher that opts out.

- PERSIST_STORAGE_MODE: on
- PUBLISHER_PACKAGE_VERSIONING: off
- PERSIST_COLLISION_ENFORCE: false

## Republish coe

Warn-only: the same colliding package publishes successfully (the collision is
surfaced as a warning, not a rejection).
