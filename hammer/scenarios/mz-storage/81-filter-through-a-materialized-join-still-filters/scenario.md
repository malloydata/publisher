---
id: filter-through-a-materialized-join-still-filters
tags: serve-correctness, joins
package: fmj
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A filter reaching through a materialized join still serves only the rows it selects

`north_orders` filters through a join to `regions`, and `regions` IS materialized,
so the join is re-declared on the serve shape and the filter resolves at the
richest tier. `broken_orders` next to it filters through a join to a source that
is NOT materialized and cannot be served at all, which forces the shape down to
its floor and makes the publisher isolate the bindings it can still serve.

The floor carries no joins. So `north_orders` — servable at the richest tier —
fails the isolation probe and is withheld along with the genuinely unservable
one. That is a known limit (see `bindingsWhoseFiltersCompile`), it is fail-safe,
and this scenario deliberately does NOT assert which tier answers: the rule is
that the rows are the rows the filter selects, and a fix that let the join
survive the probe must keep it that way.

What it does pin is that no arrangement here serves `north_orders` unfiltered —
375 rather than 150 is the failure this guards against — and that `big_orders`,
whose filter needs nothing but the stored columns, keeps its tier regardless.

There is deliberately no post-mutation assertion on `north_orders`: a stale
answer and a fresh one are both correct, and asserting either would pin the
current tier and turn a fix into a red scenario.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.fmj_regions

| region_id:text | region_name:text |
| -------------- | ---------------- |
| r1             | North            |
| r2             | South            |

## Data orders_pg.fmj_tiers

| region_id:text | tier_name:text |
| -------------- | -------------- |
| r1             | Gold           |
| r2             | Silver         |

## Data orders_pg.fmj_orders

| order_id:int | amount:num | region_id:text |
| ------------ | ---------- | -------------- |
| 1            | 100        | r1             |
| 2            | 50         | r1             |
| 3            | 200        | r2             |
| 4            | 25         | r2             |

## Model fmj.malloy

`regions` is materialized, so a join to it is carried onto the shape. `tiers` is
not, so a filter through it can never be reproduced.

```malloy
##! experimental.persistence

#@ persist name="fmj_regions" storage=lake
source: regions is orders_pg.sql('SELECT region_id, region_name FROM public.fmj_regions')

source: tiers is orders_pg.table('public.fmj_tiers')

#@ persist name="fmj_north" storage=lake
source: north_orders is orders_pg.sql('SELECT order_id, amount, region_id FROM public.fmj_orders') extend {
  join_one: regions on region_id = regions.region_id
  where: regions.region_name = 'North'
}

#@ persist name="fmj_broken" storage=lake
source: broken_orders is orders_pg.sql('SELECT order_id, amount, region_id FROM public.fmj_orders') extend {
  join_one: tiers on region_id = tiers.region_id
  where: tiers.tier_name = 'Gold'
}

#@ persist name="fmj_big" storage=lake
source: big_orders is orders_pg.sql('SELECT order_id, amount, region_id FROM public.fmj_orders') extend {
  where: amount >= 100
}
```

## Publish

expect binding: big_orders -> lake

## Query north total

Orders 1 and 2 are in North. 375 here would mean the filter was dropped.

```malloy
run: north_orders -> { aggregate: total is amount.sum() }
```

Expect:

| total:num |
| --------- |
| 150       |

## Query broken total

The genuinely unservable one, and still correct — served live.

```malloy
run: broken_orders -> { aggregate: total is amount.sum() }
```

Expect:

| total:num |
| --------- |
| 150       |

## Query big total

```malloy
run: big_orders -> { aggregate: total is amount.sum() }
```

Expect:

| total:num |
| --------- |
| 300       |

## Mutate orders_pg.fmj_orders

| order_id:int | amount:num | region_id:text |
| ------------ | ---------- | -------------- |
| 99           | 1000       | r1             |

## Query big total (again)

Stale `300` ⇒ `big_orders` kept the tier while two siblings lost it.

```malloy
run: big_orders -> { aggregate: total is amount.sum() }
```

Expect:

| total:num |
| --------- |
| 300       |
