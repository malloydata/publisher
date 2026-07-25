---
id: persisted-sql-select-storage
tags: serve-correctness, sql-select
package: ss
---

# A persisted `sql_select` served from storage

Malloy persists two source types — `query_source` (`-> { … }`) and `sql_select`
(`conn.sql("…")`) — but its compile-time substitution only rewrites a
`query_source` to read its table. `getStructSourceSQL`'s `case 'sql_select'`
re-inlines the SQL with no manifest lookup, so a persisted `sql_select` queried
directly is recomputed and its table goes unread. Only a `%{ … }` interpolation of
it consults the manifest.

The storage tier does not use that substitution. It re-declares the source as
`conn.virtual('handle')::Shape` and resolves through `virtualMap` — a different
branch of the same switch, not typed to `query_source` — so it serves an
`sql_select` from its table just as it serves a `query_source`. The stale answer
below is that proof.

This is a real capability difference between the tiers, not an accident of this
scenario: the colocated control (`persisted-sql-select-colocated`) builds the table
and then recomputes anyway. Read them as a pair.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.ss_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |

## Model ss.malloy

```malloy
##! experimental.persistence

#@ persist name="ss_raw" storage=lake
source: raw is orders_pg.sql('SELECT order_date, amount FROM public.ss_orders')
```

## Publish ss

expect binding: raw -> lake

## Query rollup

```malloy
run: raw -> { aggregate: total is amount.sum() }
```

Expect:

| total |
| ----- |
| 150   |

## Mutate orders_pg.ss_orders

```sql
INSERT INTO ss_orders VALUES (3, '2026-01-01', 1000);
```

## Query rollup (again)

```malloy
run: raw -> { aggregate: total is amount.sum() }
```

Expect:

| total |
| ----- |
| 150   |
