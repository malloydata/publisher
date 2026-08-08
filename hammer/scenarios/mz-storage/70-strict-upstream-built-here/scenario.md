---
id: strict-upstream-built-here
tags: orchestration, chained, build-control
package: sbh
---

# strictUpstreams: an upstream built in the same call must satisfy the gate

`strictUpstreams` documents two ways an upstream may be satisfied — it fails only
for an upstream "neither built here nor present in referenceManifest". Every
other chained scenario covers the second way: the upstream is already built and
arrives by `reference:` (`reference-manifest-reuse`), from another worker
(`cross-worker-reference-isolation`), from a refreshed manifest
(`cross-worker-refreshed-manifest`), or is absent so strict refuses
(`strict-upstreams-refused`).

This is the FIRST way, and nothing else exercises it: one orchestrated strict
build whose instruction list holds BOTH the upstream and its dependent. No
`reference:` is possible or needed — `daily` is not an already-materialized
artifact to point at, it is a target of this very build. An orchestrator that
dispatches a package's persist sources together, on the package's first build,
emits exactly this shape: there is no prior generation to reference, so if
"built here" does not count, a chained package can never make its first build.

The contrast with `strict-upstreams-refused` is the whole point — same strict
flag, same chained model, and the ONLY difference is whether `daily` is in the
instruction list. Absent, strict must refuse. Present, strict must be satisfied.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.sbh_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |

## Model sbh.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.sbh_orders')

#@ persist name="sbh_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}

#@ persist name="sbh_rollup" storage=lake
source: rollup is daily -> {
  aggregate: grand_total is total_amount.sum()
}
```

## Build (orchestrated, strict, pkg=sbh)

Both sources in one strict instruction list, upstream first. `daily` is built
here, so `rollup` must resolve it from this build rather than refusing — and
must not recompute it from raw either, which is what strict exists to forbid.

- daily -> sbh_daily__g1 @ lake
- rollup -> sbh_rollup__g1 @ lake

## Bind sbh

Distribute the build's manifest so both sources serve from their materialized
tables.

## Query daily

```malloy
run: daily -> { select: order_date, total_amount; order_by: order_date asc }
```

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 200          |

## Query rollup

The dependent's value proves it read a correct `daily` — whether from the table
just built or recomputed, the number is the same, so the number alone is not the
assertion. The assertion is that the build SUCCEEDED at the caller-assigned
names above, which a strict refusal would have prevented.

```malloy
run: rollup -> { select: grand_total }
```

Expect:

| grand_total:num |
| --------------- |
| 350             |

## Mutate orders_pg.sbh_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 99           | 2026-01-03      | 1000       |

## Query rollup (again)

Stale ⇒ served from the built table, not recomputed live against the mutated
source. This is what separates "resolved the upstream" from "quietly recomputed
it": a live recompute would pick the new row up.

Expect:

| grand_total:num |
| --------------- |
| 350             |
