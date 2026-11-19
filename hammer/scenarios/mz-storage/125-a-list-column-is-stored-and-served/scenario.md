---
id: a-list-column-is-stored-and-served
tags: materialize, serve-correctness, data-fidelity
package: mzl
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A `#(materialize) storage=` source over a list column stores the list and serves from it

`s_arr` is the shape of a per-station rollup with an `array_agg(...)` column,
the way a BigQuery `ARRAY` arrives: a SQL source whose `v` is a list of strings,
with a `count()` measure added. Its table holds `v` as a list, and the source is
served from it: a query that does not read the list, and one that unnests it.

A build used to project the stored table to the source's atomic fields, and
Malloy types a list column as a source of its own, so the table held only
`station_id`. The served copy then found a declared column the table lacked
and sent every query on `s_arr` live, `count()` included.

The rows added after the build prove which tier answered: both queries keep
answering the stored rows.

## Publisher

- PERSIST_STORAGE_MODE: on
- PUBLISHER_MATERIALIZE: true

## Data orders_pg.mzl_trips

| trip_id:int | station_id:int | subscriber_type:text |
| ----------- | -------------- | -------------------- |
| 1           | 10             | member               |
| 2           | 10             | casual               |
| 3           | 10             | member               |
| 4           | 20             | member               |

## Model mzl.malloy

```malloy
#(materialize) storage="lake"
source: s_arr is orders_pg.sql("""
  SELECT station_id,
         array_agg(DISTINCT subscriber_type ORDER BY subscriber_type) AS v
  FROM public.mzl_trips
  GROUP BY 1
""") extend {
  measure: n is count()
}
```

## Publish

expect binding: s_arr -> lake

## Mutate orders_pg.mzl_trips

| trip_id:int | station_id:int | subscriber_type:text |
| ----------- | -------------- | -------------------- |
| 5           | 30             | founder              |

## Query stations

A query that reads no list: the stored two stations, not the warehouse's three.

```malloy
run: s_arr -> { aggregate: n }
```

servedFrom: storage

Expect:

| n:num |
| ----- |
| 2     |

## Query subscriber types

A query that unnests the list, from the table: no `founder`.

```malloy
run: s_arr -> {
  group_by: subscriber_type is v.each
  aggregate: stations is count()
  order_by: subscriber_type asc
}
```

servedFrom: storage

Expect:

| subscriber_type | stations:num |
| --------------- | ------------ |
| casual          | 1            |
| member          | 2            |
