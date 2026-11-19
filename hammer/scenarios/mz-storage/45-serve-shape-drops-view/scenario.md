---
id: serve-shape-drops-view
tags: serve-correctness, joins
package: fbv
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A view through a join the served copy leaves out answers live, base still routes

Turning storage on must never make a query wrong, and one definition the stored
table cannot answer must not cost the source its table. `join-to-non-persisted`
pins that for an inline join traversal; this pins it for a **view**: a materialized
source carries a named view (turtle) that traverses a join to a NON-materialized
source. In the served copy `orders` is re-rooted on its lake table while `regions`
stays on `orders_pg`, so Malloy refuses the join across connections. The served
copy leaves the join out, with every definition that reads it (here the view), and
keeps the rest of the source — a query over the source's own fields still routes
to the table, while a query THROUGH the view is declined (`cross_connection`) and
answered live.

Same mutation-then-rerun proof as the join scenario: after a mutation the base
aggregate stays stale (routed), the view reflects the change (served live).

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.fbv_regions

| region_id:text | region_name:text |
| -------------- | ---------------- |
| r1             | North            |
| r2             | South            |

## Data orders_pg.fbv_orders

| order_id:int | amount:num | region_id:text |
| ------------ | ---------- | -------------- |
| 1            | 100        | r1             |
| 2            | 50         | r1             |
| 3            | 200        | r2             |
| 4            | 25         | r2             |

## Model fbv.malloy

```malloy
##! experimental.persistence

source: regions is orders_pg.table('public.fbv_regions')

#@ persist name="fbv_orders" storage=lake
source: orders is orders_pg.table('public.fbv_orders') -> {
  group_by: order_id, amount, region_id
} extend {
  join_one: regions on region_id = regions.region_id
  view: by_region is {
    group_by: regions.region_name
    aggregate: total is amount.sum()
    order_by: region_name asc
  }
}
```

## Publish

Only `orders` persists; the `by_region` view traverses the non-materialized
`regions`, so it can't be reproduced over the stored columns.

expect binding: orders -> lake

## Query fact total

Source's own fields — no view, no join traversal.

```malloy
run: orders -> { aggregate: total is amount.sum() }
```

Expect:

| total:num |
| --------- |
| 375       |

## Query via view

Calls the `by_region` view (traverses the non-materialized join). Correct either
way before the mutation.

```malloy
run: orders -> by_region
```

Expect:

| region_name | total:num |
| ----------- | --------- |
| North       | 150       |
| South       | 225       |

## Mutate orders_pg.fbv_orders

| order_id:int | amount:num | region_id:text |
| ------------ | ---------- | -------------- |
| 99           | 1000       | r1             |

## Query fact total (again)

Stale `375` ⇒ the fact-only query routed to the materialized table.

Expect:

| total:num |
| --------- |
| 375       |

## Query via view (again)

Fresh `1150` for North ⇒ the view was left out of the served copy with the join it
reads, and the view query was answered live, while base serving (above) kept
working.

Expect:

| region_name | total:num |
| ----------- | --------- |
| North       | 1150      |
| South       | 225       |
