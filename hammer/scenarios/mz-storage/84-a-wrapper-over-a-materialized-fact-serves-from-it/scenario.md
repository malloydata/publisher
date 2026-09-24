---
id: a-wrapper-over-a-materialized-fact-serves-from-it
tags: serve-correctness, givens
package: wmf
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A public wrapper over a materialized private fact serves from the fact's table

The private-fact / public-wrapper idiom: `#@ persist` sits on `_orders_fact`, and
queries name wrappers whose query reads it — `orders is _orders_fact -> { select:
* }`. Malloy gives such a wrapper no identity of its own: it does not extend the
fact, it is not persistent, and it has no binding. So the serve shape, which
rebinds only materialized sources, used to have no source named `orders`, and
every query naming it fell back live, however fresh the fact's table was.

The shape now carries each such wrapper verbatim over the rebound fact, when
everything the wrapper reads is itself on the shape. The fact's own `where:`
still applies per caller underneath it. A wrapper that reaches past the fact —
here, a join to a warehouse table nothing materializes — is left off and serves
live, which is the only thing it can do. So does one that reads through a join
of the fact's own that the shape does not carry (`fr`, to the same warehouse
table) — and it costs only itself: the other wrappers, and the fact's view,
keep the tier.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.wmf_orders

| order_id:int | org_id:int | region_id:int | amount:num |
| ------------ | ---------- | ------------- | ---------- |
| 1            | 1          | 10            | 100        |
| 2            | 1          | 20            | 200        |
| 3            | 2          | 10            | 400        |

## Data orders_pg.wmf_regions

| region_id:int | region:text |
| ------------- | ----------- |
| 10            | east        |
| 20            | west        |

## Model wmf.malloy

```malloy
##! experimental.persistence
##! experimental.givens

given:
  ORG_ID :: number is 1

source: raw is orders_pg.sql('SELECT order_id, org_id, region_id, amount FROM public.wmf_orders')
source: regions is orders_pg.sql('SELECT region_id, region FROM public.wmf_regions')

#@ persist name="wmf_orders_fact" storage=lake
source: _orders_fact is raw -> { select: * } extend {
  where: org_id = $ORG_ID
  join_one: fr is regions on region_id = fr.region_id
  measure: total is amount.sum()
  view: by_org is { group_by: org_id; aggregate: total }
}

source: orders is _orders_fact -> { select: * } extend {
  measure: order_count is count()
}

source: totals is _orders_fact -> { group_by: org_id; aggregate: total }

source: orders_by_region is _orders_fact -> { select: * } extend {
  join_one: r is regions on region_id = r.region_id
}

source: by_fact_region is _orders_fact -> { group_by: fr.region; aggregate: total }
```

## Publish

expect binding: _orders_fact -> lake

## Query org 1 through the wrapper

```malloy
run: orders -> { aggregate: order_count, s is amount.sum() }
```

givens: ORG_ID=1
servedFrom: storage

Expect:

| order_count:int | s:num |
| --------------- | ----- |
| 2               | 300   |

## Query org 2 through the wrapper

The fact's org term applies underneath the wrapper, per caller.

```malloy
run: orders -> { aggregate: order_count, s is amount.sum() }
```

givens: ORG_ID=2
servedFrom: storage

Expect:

| order_count:int | s:num |
| --------------- | ----- |
| 1               | 400   |

## Query org 1 through the aggregating wrapper

A wrapper whose query aggregates is carried the same way: its pipeline runs over
the fact's stored rows.

```malloy
run: totals -> { select: org_id, total }
```

givens: ORG_ID=1
servedFrom: storage

Expect:

| org_id:int | total:num |
| ---------- | --------- |
| 1          | 300       |

## Query org 1 through the fact's view

The fact's view is still on the shape: the wrapper that cannot compile was
dropped alone, not by thinning every binding's views.

```malloy
run: _orders_fact -> by_org
```

givens: ORG_ID=1
servedFrom: storage

Expect:

| org_id:int | total:num |
| ---------- | --------- |
| 1          | 300       |

## Query org 1 by the fact's region

```malloy
run: by_fact_region -> { select: region, total; order_by: region }
```

givens: ORG_ID=1

Expect:

| region:text | total:num |
| ----------- | --------- |
| east        | 100       |
| west        | 200       |

## Query org 1 by region

```malloy
run: orders_by_region -> { group_by: r.region; aggregate: s is amount.sum(); order_by: region }
```

givens: ORG_ID=1

Expect:

| region:text | s:num |
| ----------- | ----- |
| east        | 100   |
| west        | 200   |

## Mutate orders_pg.wmf_orders

Order 1 doubles in the warehouse.

```sql
UPDATE wmf_orders SET amount = 200 WHERE order_id = 1;
```

## Query org 1 through the wrapper (again)

Still 300: the wrapper reads the fact's stored table. A live recompute would
answer 400.

givens: ORG_ID=1
servedFrom: storage

Expect:

| order_count:int | s:num |
| --------------- | ----- |
| 2               | 300   |

## Query org 1 by region (again)

East moves to 200: this wrapper joins `regions`, which nothing materializes, so
it is not on the shape and was answered live from the warehouse all along.

givens: ORG_ID=1

Expect:

| region:text | s:num |
| ----------- | ----- |
| east        | 200   |
| west        | 200   |

## Query org 1 by the fact's region (again)

Live too: it reads through `fr`, which the shape does not carry.

givens: ORG_ID=1

Expect:

| region:text | total:num |
| ----------- | --------- |
| east        | 200       |
| west        | 200       |
