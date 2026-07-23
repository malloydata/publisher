---
id: strict-upstreams-refused
tags: orchestration, chained, build-control
package: su
---

# strictUpstreams: refuse the recompute-from-raw fallback

A chained build whose parent isn't available to reuse has two documented
outcomes, chosen by the orchestrated build's `strictUpstreams` flag: **without**
strict, the parent is recomputed from raw (Tier 2) and the child builds; **with**
strict, that live recompute is forbidden, so the build refuses loudly rather than
silently recomputing an upstream the orchestrator meant to pin.

This scenario is the discriminating pair — the SAME build inputs, the flag flips
the outcome. `rollup` depends on `daily`; we build `rollup` ALONE (no
`reference:` to `daily`, and `daily` was never materialized), so the only way to
satisfy the upstream is a recompute from raw:

- **strict** → refused (the upstream can't be resolved and strict forbids the
  recompute fallback).
- **non-strict** → succeeds (daily is recomputed from raw, rollup materializes),
  and the result serves routed.

Contrast with `cross-worker-reference-isolation`, which refuses a build whose
*reference* can't resolve at all (fails regardless of strict); here the same
inputs succeed once strict is off.

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

## Build refused (orchestrated, strict, pkg=su)

Build `rollup` alone under `strictUpstreams`, with `daily` neither built nor
referenced. Strict forbids recomputing the upstream live, so the build refuses
(runtime-manifest-strict-miss) instead of silently recomputing.

- rollup -> su_rollup__g1 @ lake

cites: manifest

## Build (orchestrated, pkg=su)

The SAME build without strict: `daily` is recomputed from raw (Tier 2) and
`rollup` materializes into the lake at the caller-assigned name.

- rollup -> su_rollup__g1 @ lake

## Bind su

Distribute the (non-strict) build's manifest so the serve routes `rollup` to its
materialized table.

## Query rollup

The non-strict recompute produced the right rollup, served from storage.

```malloy
run: rollup -> { select: grand_total }
```

Expect:

| grand_total:num |
| --------------- |
| 350             |
