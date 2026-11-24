---
id: strict-upstreams-refused
tags: orchestration, chained, build-control
package: su
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A downstream whose upstream is not built is refused, whatever `strictUpstreams` says

A chained build reads its upstream's stored table or is not built at all: no
run recomputes an upstream from raw. `strictUpstreams` used to choose between
those two outcomes; it is now accepted and ignored, so the same inputs give the
same refusal with the flag on and off.

`rollup` depends on `daily`; we build `rollup` ALONE (no `reference:` to
`daily`, and `daily` was never materialized), so the only way to satisfy the
upstream would be a recompute from raw:

- **strict** → refused (the upstream can't be resolved).
- **non-strict** → refused the same way: the flag no longer licenses the
  recompute.

Building `daily` first and then `rollup` over it is the way to a stored
`rollup`, and that serves routed.

Contrast with `cross-worker-reference-isolation`, which refuses a build whose
*reference* can't resolve at all.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.su_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |

## Model su.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.su_orders')

#@ persist name="su_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}

#@ persist name="su_rollup" storage=lake
source: rollup is daily -> {
  aggregate: grand_total is total_amount.sum()
}
```

## Build refused (orchestrated, pkg=su)

Build `rollup` alone under `strictUpstreams`, with `daily` neither built nor
referenced. The upstream has no table to read, so the build refuses
(runtime-manifest-strict-miss) instead of silently recomputing.

- rollup -> su_rollup__g1 @ lake

cites: manifest

## Build refused (orchestrated, pkg=su)

The SAME build without strict: refused the same way, since no build
recomputes an upstream from raw.

- rollup -> su_rollup__g1 @ lake

cites: manifest

## Build (orchestrated, pkg=su)

`daily` and `rollup` together: `rollup` is built over the `daily` table this
run writes, and says so.

- daily -> su_daily__g1 @ lake
- rollup -> su_rollup__g1 @ lake (reused)

## Bind su

Distribute the build's manifest so the serve routes `rollup` to its
materialized table.

## Query rollup

The stacked build produced the right rollup, served from storage.

```malloy
run: rollup -> { select: grand_total }
```

Expect:

| grand_total:num |
| --------------- |
| 350             |
