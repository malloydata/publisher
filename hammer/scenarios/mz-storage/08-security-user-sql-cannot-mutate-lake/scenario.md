---
id: security-user-sql-cannot-mutate-lake
tags: security
package: sec1
---

# Security: user Malloy/SQL cannot mutate the DuckLake destination

A user authors Malloy (models and ad-hoc queries) and can embed raw SQL via
`connection.sql(...)`. That raw SQL must NOT be able to run DDL against the
storage destination — no `DROP`, no `CREATE SCHEMA`, no writes. The guard is the
publisher's **read-only** attach of the destination on the serve path.

Rigorous survival proof: if an attack actually dropped the materialized table,
the serve path would silently fall back to live (and still return the right
numbers). So after the attacks we mutate the source and re-query — a **stale**
value proves the storage table survived the attack *and* is still routed. A live
(fresh) value would mean the table was dropped.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.sec1_orders

| order_id:int | order_date:date | region:text | amount:num |
| ------------ | --------------- | ----------- | ---------- |
| 1            | 2026-01-01      | US          | 100        |
| 2            | 2026-01-01      | US          | 50         |
| 3            | 2026-01-02      | EU          | 200        |
| 4            | 2026-01-02      | US          | 25         |

## Model sec1.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.sec1_orders')

#@ persist name="sec1_daily" storage=lake
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

## Query attack: DROP the materialized table (refused)

A user query embedding raw SQL that tries to drop the materialized table must be
refused (read-only attach), never executed.

```malloy
run: lake.sql('DROP TABLE IF EXISTS main.sec1_daily') -> { select: x is 1 }
```

## Query attack: CREATE SCHEMA in the lake (refused)

```malloy
run: lake.sql('CREATE SCHEMA hacked_by_user') -> { select: x is 1 }
```

## Query attack: DROP the whole main schema (refused)

```malloy
run: lake.sql('DROP SCHEMA main CASCADE') -> { select: x is 1 }
```

## Query attack: multi-statement injection (refused)

A DROP smuggled after a benign SELECT (multi-statement) must not execute either.

```malloy
run: lake.sql('SELECT 1 AS x; DROP TABLE IF EXISTS main.sec1_daily') -> { select: x }
```

## Mutate orders_pg.sec1_orders

| order_id:int | order_date:date | region:text | amount:num |
| ------------ | --------------- | ----------- | ---------- |
| 99           | 2026-01-01      | US          | 1000       |

## Query rollup (again)

Stale 150 (not 1150) proves the storage table survived every attack and is still
routed — the read-only guard held.

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 225          |
