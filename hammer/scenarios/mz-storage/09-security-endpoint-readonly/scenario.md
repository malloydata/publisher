---
id: security-endpoint-readonly
tags: security
package: sec2
---

# Security: the connection endpoints cannot reach a storage destination

Besides Malloy, the publisher exposes `POST /connections/<c>/sqlQuery`, and it
takes a caller-supplied connection name that no layer above validates. A
destination is not in that namespace — it lives in `storageDestinations`,
which no connection endpoint reads — so DDL aimed at the lake through this route
is refused before it becomes SQL, and the materialized table must survive.

(Note: the attacks qualify with `lake.` so they would truly target the destination
if they resolved at all; an *unqualified* `CREATE SCHEMA x` here would land in the
session's throwaway `:memory:` catalog and be harmless either way. A read-only
attach still backs the serve path, but it is no longer the guard being tested —
an endpoint that cannot name the destination never gets that far.)

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

DROP the materialized table through the endpoint — must be refused.

```sql
DROP TABLE IF EXISTS lake.main.sec2_daily;
```

## Connection lake (refused)

CREATE a schema in the lake through the endpoint — must be refused.

```sql
CREATE SCHEMA lake.hacked_via_endpoint;
```

## Connection lake_probe (refused)

A user connection of their own, pointing at the same catalog, is still attached
read-only — so even where the name DOES resolve, the endpoint cannot write.

```sql
CREATE SCHEMA lake_probe.hacked_via_probe;
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
