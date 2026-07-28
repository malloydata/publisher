---
id: join-across-two-persisted
tags: serve-correctness, joins
package: jt
---

# Join across two persisted sources: both legs routed to storage

Two independent persist sources — a `regions` dimension and an `orders` fact —
are each materialized into the lake. The fact carries a `join_one` to the
dimension. A query that groups a fact aggregate by a dimension field must
re-emit the join over BOTH materialized tables (not recompute either leg live).

The stale-proof covers both legs at once: after publishing we change the raw
fact (append a big order) AND the raw dimension (rename a region). If either leg
went live, the re-run would show it — a changed name (dimension leg live) or a
jumped total (fact leg live). A fully stale result proves both legs served from
storage.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.jt_regions

| region_id:text | region_name:text |
| -------------- | ---------------- |
| r1             | North            |
| r2             | South            |

## Data orders_pg.jt_orders

| order_id:int | amount:num | region_id:text |
| ------------ | ---------- | -------------- |
| 1            | 100        | r1             |
| 2            | 50         | r1             |
| 3            | 200        | r2             |
| 4            | 25         | r2             |

## Model jt.malloy

```malloy
##! experimental.persistence

#@ persist name="jt_regions" storage=lake
source: regions is orders_pg.table('public.jt_regions') -> {
  group_by: region_id, region_name
}

#@ persist name="jt_orders" storage=lake
source: orders is orders_pg.table('public.jt_orders') -> {
  group_by: order_id, amount, region_id
} extend {
  join_one: regions on region_id = regions.region_id
}
```

## Publish

Both persist sources materialize into the lake.

expect binding: regions -> lake
expect binding: orders -> lake

## Query join by region

```malloy
run: orders -> { group_by: regions.region_name; aggregate: total is amount.sum(); order_by: region_name asc }
```

Expect:

| region_name | total:num |
| ----------- | --------- |
| North       | 150       |
| South       | 225       |

## Mutate orders_pg.jt_orders

Append a large order to `r1`. If the fact leg recomputed live, North's total
would jump to 1150.

| order_id:int | amount:num | region_id:text |
| ------------ | ---------- | -------------- |
| 99           | 1000       | r1             |

## Mutate orders_pg.jt_regions

Rename `r1`. If the dimension leg recomputed live, North would read
`NorthChanged`.

```sql
UPDATE jt_regions SET region_name = 'NorthChanged' WHERE region_id = 'r1';
```

## SQL both raw sources really changed

```sql
SELECT o.region_id, r.region_name, sum(o.amount) AS total
FROM jt_orders o JOIN jt_regions r ON o.region_id = r.region_id
GROUP BY o.region_id, r.region_name ORDER BY o.region_id;
```

Expect:

| region_id | region_name  | total:num |
| --------- | ------------ | --------- |
| r1        | NorthChanged | 1150      |
| r2        | South        | 225       |

## Query join by region (again)

Both legs stale ⇒ still `North` / `150` (dimension leg from its snapshot, fact
leg from its snapshot).

Expect:

| region_name | total:num |
| ----------- | --------- |
| North       | 150       |
| South       | 225       |
