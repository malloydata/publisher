---
id: data-type-roundtrip
tags: serve-correctness
package: dt
---

# Data-type & NULL round-trip through DuckLake

A materialized source must serve back exactly what it captured — across column
types and NULLs, not just aggregated numbers. This projects a row set with an
integer, a date, a decimal, a boolean, and text (plus an all-NULL row) from
Postgres into DuckLake, then serves it and compares every cell. A stale-proof
(append a row, re-query, still see the original count) confirms the values came
from the materialized snapshot, not a live recompute.

> Note: the harness compares dates/timestamps at day granularity, so this covers
> type FIDELITY and NULL handling; sub-day timestamp precision would need a finer
> comparator (left for a follow-up).

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.dt_rows

| id:int | d:date     | amt:num | flag:bool | label:text |
| ------ | ---------- | ------- | --------- | ---------- |
| 1      | 2026-01-01 | 100.50  | true      | alpha      |
| 2      | 2026-06-15 | 0       | false     | beta       |
| 3      |            |         |           |            |

## Model dt.malloy

```malloy
##! experimental.persistence

source: raw is orders_pg.table('public.dt_rows')

#@ persist name="dt_all" storage=lake
source: all_rows is raw -> {
  select: id, d, amt, flag, label
  order_by: id
}
```

## Publish

expect binding: all_rows -> lake

## Query all rows

Every column type and the all-NULL row round-trip identically.

```malloy
run: all_rows -> { select: id, d, amt, flag, label; order_by: id asc }
```

Expect:

| id | d          | amt:num | flag  | label |
| -- | ---------- | ------- | ----- | ----- |
| 1  | 2026-01-01 | 100.5   | true  | alpha |
| 2  | 2026-06-15 | 0       | false | beta  |
| 3  |            |         |       |       |

## Mutate orders_pg.dt_rows

Append a fourth row. If served live, the re-query would return 4 rows.

| id:int | d:date     | amt:num | flag:bool | label:text |
| ------ | ---------- | ------- | --------- | ---------- |
| 4      | 2026-12-31 | 9.99    | true      | delta      |

## Query all rows (again)

Still exactly the original three rows ⇒ served from the DuckLake snapshot, with
types and NULLs intact.

Expect:

| id | d          | amt:num | flag  | label |
| -- | ---------- | ------- | ----- | ----- |
| 1  | 2026-01-01 | 100.5   | true  | alpha |
| 2  | 2026-06-15 | 0       | false | beta  |
| 3  |            |         |       |       |
