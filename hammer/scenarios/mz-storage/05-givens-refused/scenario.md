---
id: givens-refused
tags: eligibility, security
package: f1
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A given the persisted query READS is refused, wherever it was declared

`scoped` carries `where: region ~ $REGION`, and `scoped_rollup` is a query over
it. The query reads that filter, so the predicate is in the build SQL with the
given's value already substituted — the artifact is one region's rows, and the
source's own `filterList` is empty, so nothing is left for the read to re-apply.

The given is not written inside the persisted query here; it is inherited from
the source the query reads. That is the point of this case. What decides the
refusal is whether the BUILD substitutes a value, not which line the author
typed the given on — and a filter one derivation up is substituted just as
surely as one written in place.

The contrast is `tenant-scoped-source-serves-per-caller`, where the given sits in
the persist source's OWN extend block: absent from the build, re-applied per
caller, and served from the tier.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.f1_orders

| order_id:int | order_date:date | region:text | amount:num |
| ------------ | --------------- | ----------- | ---------- |
| 1            | 2026-01-01      | US          | 100        |
| 2            | 2026-01-02      | EU          | 200        |

## Model f1.malloy

```malloy
##! experimental.persistence
##! experimental.givens

given: REGION :: filter<string> is f''

source: base is orders_pg.table('public.f1_orders')

source: scoped is base extend {
  where: region ~ $REGION
}

#@ persist name="f1_scoped" storage=lake
source: scoped_rollup is scoped -> {
  group_by: order_date
  aggregate: t is amount.sum()
}
```

## Build refused

The package compiles (givens are valid Malloy), but the build is refused by the
eligibility gate and ends FAILED.

cites: substituted at build time

## Build refusals

Expect:

| source        | tier    | reason                   |
| ------------- | ------- | ------------------------ |
| scoped_rollup | storage | given_in_persisted_query |
