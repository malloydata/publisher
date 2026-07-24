---
id: chained-persist
tags: serve-correctness, chained
package: ch
---

# Chained persist: build a downstream from an upstream's materialized table

A downstream persist source (`rollup`) depends on an upstream persist source
(`daily`), both `storage=lake`. The build must materialize `daily` first, then
build `rollup` by reading `daily`'s materialized table (not recomputing from the
warehouse). Both then serve from storage.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.ch_orders

| order_id:int | order_date:date | region:text | amount:num |
| ------------ | --------------- | ----------- | ---------- |
| 1            | 2026-01-01      | US          | 100        |
| 2            | 2026-01-01      | US          | 50         |
| 3            | 2026-01-02      | EU          | 200        |
| 4            | 2026-01-02      | US          | 25         |

## Model ch.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.ch_orders')

#@ persist name="ch_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}

#@ persist name="ch_rollup" storage=lake
source: rollup is daily -> {
  aggregate: grand_total is total_amount.sum()
}
```

## Publish

Both persist sources materialize into the lake; the downstream builds from the
upstream's materialized table.

expect binding: daily -> lake
expect binding: rollup -> lake

## Query daily

```malloy
run: daily -> { select: order_date, total_amount; order_by: order_date asc }
```

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 225          |

## Query rollup

```malloy
run: rollup -> { select: grand_total }
```

Expect:

| grand_total:num |
| --------------- |
| 375             |

## Mutate orders_pg.ch_orders

| order_id:int | order_date:date | region:text | amount:num |
| ------------ | --------------- | ----------- | ---------- |
| 99           | 2026-01-01      | US          | 1000       |

## Query daily (again)

Stale ⇒ served from the upstream snapshot.

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 225          |

## Query rollup (again)

Stale ⇒ the downstream serves from its own snapshot too.

Expect:

| grand_total:num |
| --------------- |
| 375             |
