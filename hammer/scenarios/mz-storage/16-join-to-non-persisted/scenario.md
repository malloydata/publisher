---
id: join-to-non-persisted
tags: serve-correctness, joins
package: jnp
---

# Join to a non-persisted source: base routes, the join falls back to live

A persisted fact (`orders`) carries a `join_one` to a dimension (`regions`) that
is NOT persisted. This exercises the serve fallback ladder on ONE source: a query
over the fact's own fields must route to the materialized table, but a query that
traverses the join to the non-materialized dimension cannot compile against the
serve shape and must fall back to live — without disabling base serving.

The proof is one mutation, two re-runs. After publishing we append a large order.
The fact-only aggregate stays stale (served from storage); the join-traversing
aggregate reflects the change (served live).

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.jnp_regions

| region_id:text | region_name:text |
| -------------- | ---------------- |
| r1             | North            |
| r2             | South            |

## Data orders_pg.jnp_orders

| order_id:int | amount:num | region_id:text |
| ------------ | ---------- | -------------- |
| 1            | 100        | r1             |
| 2            | 50         | r1             |
| 3            | 200        | r2             |
| 4            | 25         | r2             |

## Model jnp.malloy

```malloy
##! experimental.persistence

source: regions is orders_pg.table('public.jnp_regions')

#@ persist name="jnp_orders" storage=lake
source: orders is orders_pg.table('public.jnp_orders') -> {
  group_by: order_id, amount, region_id
} extend {
  join_one: regions on region_id = regions.region_id
}
```

## Publish

Only `orders` persists; `regions` is a plain (live) source and does not bind.

expect binding: orders -> lake

## Query fact total

Fact-only aggregate — no join traversal.

```malloy
run: orders -> { aggregate: total is amount.sum() }
```

Expect:

| total:num |
| --------- |
| 375       |

## Query by region

Traverses the join to the non-persisted dimension. Correct either way before the
mutation.

```malloy
run: orders -> { group_by: regions.region_name; aggregate: total is amount.sum(); order_by: region_name asc }
```

Expect:

| region_name | total:num |
| ----------- | --------- |
| North       | 150       |
| South       | 225       |

## Mutate orders_pg.jnp_orders

Append a large order to `r1`.

| order_id:int | amount:num | region_id:text |
| ------------ | ---------- | -------------- |
| 99           | 1000       | r1             |

## SQL raw fact really changed

```sql
SELECT sum(amount) AS total FROM jnp_orders;
```

Expect:

| total:num |
| --------- |
| 1375      |

## Query fact total (again)

Stale `375` ⇒ the fact-only query routed to the materialized table.

Expect:

| total:num |
| --------- |
| 375       |

## Query by region (again)

Fresh `1150` for North ⇒ the join to the non-persisted dimension forced a live
fallback, while base serving (above) kept working.

Expect:

| region_name | total:num |
| ----------- | --------- |
| North       | 1150      |
| South       | 225       |
