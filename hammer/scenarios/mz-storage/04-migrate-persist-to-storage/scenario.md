---
id: migrate-persist-to-storage
tags: migration, serve-correctness
package: emig
---

# Migrate a persist source from in-warehouse to storage= (DuckLake)

Start with a plain `#@ persist` source (no `storage=`), served the in-warehouse
way: materialized into the source warehouse and substituted by table name at
query time. Then add `storage=lake` and restart the publisher into `on`. The
switch must force a rebuild into DuckLake, bind a serve binding, and serve
equivalent values — now via the DuckLake serve path. The mode change is a
publisher restart, which also re-reads the edited model from disk.

## Data orders_pg.emig_orders

| order_id:int | order_date:date | region:text | amount:num |
| ------------ | --------------- | ----------- | ---------- |
| 1            | 2026-01-01      | US          | 100        |
| 2            | 2026-01-01      | US          | 50         |
| 3            | 2026-01-02      | EU          | 200        |
| 4            | 2026-01-02      | US          | 25         |

## Model emig.malloy

The starting model: a plain persist, no `storage=`.

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.emig_orders')

#@ persist name="emig_daily"
source: daily_orders is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}
```

## Publisher

The publisher runs with the storage tier off — a plain in-warehouse persist.

- PERSIST_STORAGE_MODE: off

## Publish

In-warehouse build — materialized into the Postgres source warehouse.

## Query rollup

```malloy
run: daily_orders -> { select: order_date, total_amount; order_by: order_date asc }
```

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 225          |

## Model emig.malloy

The switch: same source, now `storage=lake`.

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.emig_orders')

#@ persist name="emig_daily" storage=lake
source: daily_orders is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}
```

## Publisher

Restart into `on`. The restart re-reads the edited model and enables the storage
tier.

- PERSIST_STORAGE_MODE: on

## Publish

The switch forces a rebuild into DuckLake — the existing in-warehouse table can't
be reused for a storage serve, because the content address does not encode the
destination.

expect binding: daily_orders -> lake

## Query rollup (again)

The storage serve returns the same values the in-warehouse build did.

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 225          |

## Mutate orders_pg.emig_orders

| order_id:int | order_date:date | region:text | amount:num |
| ------------ | --------------- | ----------- | ---------- |
| 99           | 2026-01-01      | US          | 1000       |

## Query rollup (again)

Stale ⇒ genuinely serving from the DuckLake snapshot.

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 225          |
