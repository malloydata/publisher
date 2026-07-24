---
id: security-endpoint-readonly
tags: security
package: sec2
---

# Security: the connection sqlQuery endpoint is read-only for a storage destination

Besides Malloy, the publisher exposes `POST /connections/<c>/sqlQuery`. For a
storage destination that connection is attached **read-only**, so DDL through the
endpoint — qualified to actually target the lake — must be refused, and the
materialized table must survive.

(Note: an *unqualified* `CREATE SCHEMA x` via this endpoint would land in the
session's throwaway `:memory:` catalog and be harmless; these attacks qualify
with `lake.` so they truly target the destination.)

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.sec2_orders

| order_id:int | order_date:date | region:text | amount:num |
| ------------ | --------------- | ----------- | ---------- |
| 1            | 2026-01-01      | US          | 100        |
| 2            | 2026-01-01      | US          | 50         |
| 3            | 2026-01-02      | EU          | 200        |
| 4            | 2026-01-02      | US          | 25         |

## Model sec2.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.sec2_orders')

#@ persist name="sec2_daily" storage=lake
source: daily_orders is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}
```

## Publish

expect binding: daily_orders -> lake

## Query rollup

```malloy
run: daily_orders -> { select: order_date, total_amount; order_by: order_date asc }
```

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 225          |

## Connection lake (refused)

DROP the materialized table through the endpoint — must be refused (read-only).

```sql
DROP TABLE IF EXISTS lake.main.sec2_daily;
```

## Connection lake (refused)

CREATE a schema in the lake through the endpoint — must be refused (read-only).

```sql
CREATE SCHEMA lake.hacked_via_endpoint;
```

## Mutate orders_pg.sec2_orders

| order_id:int | order_date:date | region:text | amount:num |
| ------------ | --------------- | ----------- | ---------- |
| 99           | 2026-01-01      | US          | 1000       |

## Query rollup (again)

Stale 150 proves the table survived the endpoint attacks and is still routed.

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 225          |
