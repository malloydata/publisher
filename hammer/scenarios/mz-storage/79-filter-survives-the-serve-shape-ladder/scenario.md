---
id: filter-survives-the-serve-shape-ladder
tags: serve-correctness, joins
package: fsl
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# The serve-shape ladder thins refinements, never a source's filter

The ladder drops the riskiest refinement category and retries (full → drop views
→ drop views+joins → base-only) so that one un-reproducible refinement cannot
cost the whole package its tier. A source's `where:` must not be thinned that
way: dropping a view costs the tier for queries that use it, while dropping a
filter answers with rows the source excludes.

`serve-shape-drops-view` pins the view rung on an unfiltered source. This is the
same arrangement with a filter added, so the ladder is forced to thin a source
that has one: the view traverses a join to a NON-materialized source and cannot
be reproduced, so the view category is dropped — and the filter has to survive
that drop.

Without the filter, this source's rows total 375. With it, 300.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.fsl_regions

| region_id:text | region_name:text |
| -------------- | ---------------- |
| r1             | North            |
| r2             | South            |

## Data orders_pg.fsl_orders

| order_id:int | amount:num | region_id:text |
| ------------ | ---------- | -------------- |
| 1            | 100        | r1             |
| 2            | 50         | r1             |
| 3            | 200        | r2             |
| 4            | 25         | r2             |

## Model fsl.malloy

Only `orders` persists. `by_region` reaches `regions`, which does not, so the
view cannot be reproduced over the stored columns and the ladder must thin it.

```malloy
##! experimental.persistence

source: regions is orders_pg.table('public.fsl_regions')

#@ persist name="fsl_orders" storage=lake
source: orders is orders_pg.sql('SELECT order_id, amount, region_id FROM public.fsl_orders') extend {
  where: amount >= 100

  join_one: regions on region_id = regions.region_id
  view: by_region is {
    group_by: regions.region_name
    aggregate: total is amount.sum()
    order_by: region_name asc
  }
}
```

## Publish

expect binding: orders -> lake

## Query fact total

The source's own fields, so this is the query the ladder's surviving tier serves.
300 rather than 375 ⇒ the filter is still on the shape after the view was
dropped.

```malloy
run: orders -> { aggregate: total is amount.sum() }
```

Expect:

| total:num |
| --------- |
| 300       |

## Query via view

Traverses the non-materialized join, so it falls back live. Correct either way
before the mutation.

```malloy
run: orders -> by_region
```

Expect:

| region_name | total:num |
| ----------- | --------- |
| North       | 100       |
| South       | 200       |

## Mutate orders_pg.fsl_orders

A qualifying order, so it moves both answers if both are live.

| order_id:int | amount:num | region_id:text |
| ------------ | ---------- | -------------- |
| 99           | 1000       | r1             |

## Query fact total (again)

Still 300 ⇒ answered from the materialized table, with the filter applied. A
1300 here would mean live; a 375 or 1375 would mean the filter was thinned away
with the view.

```malloy
run: orders -> { aggregate: total is amount.sum() }
```

Expect:

| total:num |
| --------- |
| 300       |

## Query via view (again)

Fresh 1100 for North ⇒ the view category really was dropped and this query fell
back live, which is what forces the filter to have survived on the tier above.

```malloy
run: orders -> by_region
```

Expect:

| region_name | total:num |
| ----------- | --------- |
| North       | 1100      |
| South       | 200       |
