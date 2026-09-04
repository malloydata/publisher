---
id: preaggregate-storage-stale-falls-back
tags: serve-correctness, preaggregation, storage, freshness
package: pf
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A stale rollup leaves the serving set, and the query is answered from the base

Freshness is orthogonal to placement: a rollup bound stale past its window with
`fallback=live` is dropped from the serve shape exactly as an authored `storage=` source
is, and the query is answered from the base instead.

Worth its own scenario because a rollup's fallback is not the same shape as an ordinary
source's. When a stored source is dropped, the query recompiles against that source's own
definition. When a rollup is dropped, the composite loses a member — and because the
lake composite carries no base member, losing its only member means the composite covers
nothing at all and the query stops compiling against the shape entirely. Two different
mechanisms reaching the same place, and only one of them is exercised by the storage
tier's own freshness scenario.

`stale_ok` is checked first, so the two outcomes are distinguished by the fallback rather
than by the window: the same stale entry serves the stored answer under one policy and
recomputes under the other.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.pf_orders

| order_id:int | category:text | amount:num |
| ------------ | ------------- | ---------- |
| 1            | books         | 100        |
| 2            | books         | 50         |
| 3            | tools         | 200        |

## Model pf.malloy

```malloy
##! experimental { persistence composite_sources }

source: orders is orders_pg.table('public.pf_orders') extend {
  measure:
    #@ preaggregate grain="category" storage=lake
    total is amount.sum()
}
```

## Publish

## Query by category

```malloy
run: orders -> { group_by: category; aggregate: total; order_by: category asc }
```

Expect:

| category | total |
| -------- | ----- |
| books    | 150   |
| tools    | 200   |

## Mutate orders_pg.pf_orders

So a recompute (`1150`) differs from what the rollup stored.

| order_id:int | category:text | amount:num |
| ------------ | ------------- | ---------- |
| 99           | books         | 1000       |

## Bind pf (asof=2000-01-01T00:00:00Z, fresh=1, fallback=stale_ok)

Stale past its window, but the policy says a stale rollup is still worth serving.

## Query by category (again)

Still `150` — `stale_ok` keeps the member in the shape and the stored answer is returned.

Expect:

| category | total |
| -------- | ----- |
| books    | 150   |
| tools    | 200   |

## Bind pf (asof=2000-01-01T00:00:00Z, fresh=1, fallback=live)

The same stale entry under the policy that refuses it.

## Query by category (again)

`1150`. The member left the serving set, the composite was left covering nothing, and
the query was answered from the base — correct, unaccelerated, and silent, which is why
the fallback is logged at info rather than debug.

The answer is a single `GROUP BY` of the base, not a rollup recomputed and then merged.
That distinction is invisible in the rows and is the reason the colocated companion is
skipped when every rollup is storage-bound: its composite would still pick the rollup
member, find no colocated table behind it, and rebuild the rollup at query time — two
stages to produce what one stage already produces.

Expect:

| category | total |
| -------- | ----- |
| books    | 1150  |
| tools    | 200   |
