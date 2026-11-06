---
id: derived-setting-names
tags: config, settings
package: sdn
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A publisher configured by the derived names behaves as by today's

Step 2 of the configuration design gives every setting a name derived from its
key: `persist.collisionEnforcementEnabled` is
`PUBLISHER_PERSIST_COLLISION_ENFORCEMENT_ENABLED`, where today it is
`PERSIST_COLLISION_ENFORCE`. The harness already starts every publisher by the
derived names (`PUBLISHER_PERSIST_STORAGE_MODE`, `--root`, `--mcp-port`), so
every storage scenario serves through them; this one sets a switch only by its
new name, and checks it does what the old one did (`collision-enforce-refuses-publish`).

`alpha` and `beta` both declare `name="sdn_dup" storage=lake`, the collision.

## Publisher strict

- PERSIST_STORAGE_MODE: on
- PUBLISHER_PERSIST_COLLISION_ENFORCEMENT_ENABLED: true

## Data orders_pg.sdn_orders

| order_id:int | order_date:date | region:text | amount:num |
| ------------ | --------------- | ----------- | ---------- |
| 1            | 2026-01-01      | US          | 100        |
| 2            | 2026-01-02      | EU          | 200        |

## Model sdn.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.sdn_orders')

#@ persist name="sdn_dup" storage=lake
source: alpha is orders -> {
  group_by: order_date
  aggregate: n is count()
}

#@ persist name="sdn_dup" storage=lake
source: beta is orders -> {
  group_by: region
  aggregate: total is amount.sum()
}
```

## Republish refused sdn

The new name turns enforcement on: the colliding publish is rejected.

cites: resolve to the same materialized table

## Publisher lax

- PERSIST_STORAGE_MODE: on
- PUBLISHER_PERSIST_COLLISION_ENFORCEMENT_ENABLED: false

## Republish sdn

And off: the same package publishes, with the collision as a warning.
