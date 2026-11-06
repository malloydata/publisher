---
id: boolean-setting-spellings
tags: config, settings
package: sbs
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A boolean setting reads every spelling, any case

The settings registry reads every boolean setting with one reader: `true/false`,
`1/0`, `yes/no` and `on/off`, in any case (docs/configuration.md). This holds it
end to end, on a setting fixed at process start: `PERSIST_COLLISION_ENFORCE`
spelled `Yes` makes a publisher refuse a colliding publish, and spelled `OFF`
leaves it warn-only (see `collision-enforce-refuses-publish`, which uses `true`).

`alpha` and `beta` both declare `name="sbs_dup" storage=lake`, the collision.

## Publisher strict

- PERSIST_STORAGE_MODE: on
- PERSIST_COLLISION_ENFORCE: Yes

## Data orders_pg.sbs_orders

| order_id:int | order_date:date | region:text | amount:num |
| ------------ | --------------- | ----------- | ---------- |
| 1            | 2026-01-01      | US          | 100        |
| 2            | 2026-01-02      | EU          | 200        |

## Model sbs.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.sbs_orders')

#@ persist name="sbs_dup" storage=lake
source: alpha is orders -> {
  group_by: order_date
  aggregate: n is count()
}

#@ persist name="sbs_dup" storage=lake
source: beta is orders -> {
  group_by: region
  aggregate: total is amount.sum()
}
```

## Republish refused sbs

`Yes` is on: publishing the colliding package is rejected.

cites: resolve to the same materialized table

## Publisher lax

- PERSIST_STORAGE_MODE: on
- PERSIST_COLLISION_ENFORCE: OFF

## Republish sbs

`OFF` is off: the same package publishes, with the collision as a warning.
