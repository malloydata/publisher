---
id: unservable-filter-withholds-only-its-own-source
tags: serve-correctness, joins
package: ufw
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A filter the shape cannot reproduce costs its own source the tier, not the model's

A source's `where:` is always carried onto the serve shape, and a source whose
`where:` cannot be reproduced there must serve live rather than serve unfiltered.
The shape is ONE generated model covering every binding in it, so the blunt
version of that rule sends the whole model live over one bad filter.

`north_orders` filters through a join to `regions`, which is not materialized —
the join is not emitted, so its filter cannot resolve and its binding cannot
compile. `big_orders`, in the same model, has a filter that reproduces fine. Only
`north_orders` loses the tier.

And `big_orders` keeps its tier at full strength, not at the floor: its own view
still serves from storage. Withholding a sibling re-enters the ladder with the
survivors rather than freezing them at the shape that failed, so a source that
did nothing wrong does not lose its joins and views to a neighbour's filter.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.ufw_regions

| region_id:text | region_name:text |
| -------------- | ---------------- |
| r1             | North            |
| r2             | South            |

## Data orders_pg.ufw_orders

| order_id:int | amount:num | region_id:text |
| ------------ | ---------- | -------------- |
| 1            | 100        | r1             |
| 2            | 50         | r1             |
| 3            | 200        | r2             |
| 4            | 25         | r2             |

## Model ufw.malloy

```malloy
##! experimental.persistence

source: regions is orders_pg.table('public.ufw_regions')

#@ persist name="ufw_big" storage=lake
source: big_orders is orders_pg.sql('SELECT order_id, amount, region_id FROM public.ufw_orders') extend {
  where: amount >= 100

  view: by_region is {
    group_by: region_id
    aggregate: total is amount.sum()
    order_by: region_id asc
  }
}

#@ persist name="ufw_north" storage=lake
source: north_orders is orders_pg.sql('SELECT order_id, amount, region_id FROM public.ufw_orders') extend {
  join_one: regions on region_id = regions.region_id
  where: regions.region_name = 'North'
}
```

## Publish

expect binding: big_orders -> lake

## Query big total

`amount >= 100` selects orders 1 and 3.

```malloy
run: big_orders -> { aggregate: total is amount.sum() }
```

Expect:

| total:num |
| --------- |
| 300       |

## Query north total

Correct, and served live — the filter reaches a join the shape cannot carry.

```malloy
run: north_orders -> { aggregate: total is amount.sum() }
```

Expect:

| total:num |
| --------- |
| 150       |

## Mutate orders_pg.ufw_orders

A North order that qualifies for both filters, so it moves whichever answer is
live.

| order_id:int | amount:num | region_id:text |
| ------------ | ---------- | -------------- |
| 99           | 1000       | r1             |

## Query big total (again)

Stale `300` ⇒ `big_orders` kept the tier. Its sibling's unservable filter did not
take it down with it — a `1300` here would mean the whole model went live.

```malloy
run: big_orders -> { aggregate: total is amount.sum() }
```

Expect:

| total:num |
| --------- |
| 300       |

## Query north total (again)

Fresh `1150` ⇒ `north_orders` really is serving live, which is what makes the
stale answer above a per-source withholding rather than nothing having failed at
all.

```malloy
run: north_orders -> { aggregate: total is amount.sum() }
```

Expect:

| total:num |
| --------- |
| 1150      |

## Query big by region (again)

Still the stale `100` / `200` ⇒ `big_orders` kept its VIEW, not just its rows.
Freezing the survivors at the floor that failed would have dropped the view from
the shape and sent this query live, where it would read `1100` for `r1`.

```malloy
run: big_orders -> by_region
```

Expect:

| region_id | total:num |
| --------- | --------- |
| r1        | 100       |
| r2        | 200       |
