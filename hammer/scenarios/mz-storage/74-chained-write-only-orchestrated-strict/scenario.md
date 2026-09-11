---
id: chained-write-only-orchestrated-strict
tags: orchestration, chained, kill-switch
package: cwos
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# strictUpstreams must not change what an orchestrated chained build can resolve

The discriminating twin of `chained-write-only-orchestrated`: identical model,
identical instruction list, identical mode. The ONLY difference is
`strictUpstreams`, and it must not change the outcome here.

That is the whole claim. `strictUpstreams` decides what happens to an upstream
the build cannot otherwise resolve — it forbids the silent recompute-from-raw
that `strict-upstreams-refused` pins. It is not supposed to decide whether an
upstream present in the instruction list can be found. `daily` is built by this
very call, so there is nothing for strict to forbid: no recompute is needed, and
the flag should be inert.

Read as a pair, the two scenarios say something neither says alone. Non-strict
passes, so orchestration, chaining, and `write-only` are each individually fine.
Strict fails on the same inputs, so the flag is the sole cause — not the mode,
not the chain, not the orchestrated path.

Kept separate from `strict-upstream-built-here` because the consequence is
different, not just the mode. That scenario says a chained package cannot make
its FIRST build. This one says it cannot be built in the state an operator
deliberately stops at while enabling the tier — writing on, routing still off.
A fix that only satisfies `on` would leave the safe rollout order broken, and
this scenario is what would still be red.

## Publisher

Build into the lake, serve live.

- PERSIST_STORAGE_MODE: write-only

## Data orders_pg.cwos_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |

## Model cwos.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.cwos_orders')

#@ persist name="cwos_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}

#@ persist name="cwos_rollup" storage=lake
source: rollup is daily -> {
  aggregate: grand_total is total_amount.sum()
}
```

## Build (orchestrated, strict, pkg=cwos)

The same two instructions as the non-strict twin, under strict. `daily` is built
here, so strict has nothing to refuse and the build must succeed at both
caller-assigned names.

- daily -> cwos_daily__g1 @ lake
- rollup -> cwos_rollup__g1 @ lake

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

```malloy
run: rollup -> { select: grand_total }
```

Expect:

| grand_total:num |
| --------------- |
| 350             |

## Mutate orders_pg.cwos_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 99           | 2026-01-03      | 1000       |

## Query rollup (again)

`write-only` serves live, so the mutation is visible — the same serve behaviour
the non-strict twin asserts. Pinned here too so a fix cannot turn the build
green by quietly routing reads that this mode says must stay live.

Expect:

| grand_total:num |
| --------------- |
| 1350            |
