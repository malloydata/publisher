---
id: a-destination-moved-to-a-new-schema-serves-from-it-without-a-reload
tags: serve-correctness, lifecycle
package: d0
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A destination moved to a new schema serves from it without a reload

An orchestrator can move a storage destination to another catalog, under the
same name, with a `destinationSchema` its tables live in there: copy the tables
across, then push the destination's new config in an environment update. The
manifest does not change, since the table names and the destination name are the
same. A loaded package must then read its materialized tables from the new
catalog's schema, with no package reload and no server restart.

## Publisher

- PERSIST_STORAGE_MODE: on

## Connection lake2 (type=ducklake)

## Data orders_pg.orders

| order_id:int | order_date:date | region:text | amount:num |
| ------------ | --------------- | ----------- | ---------- |
| 1            | 2026-01-01      | US          | 100        |
| 2            | 2026-01-01      | US          | 50         |
| 3            | 2026-01-02      | EU          | 200        |
| 4            | 2026-01-02      | US          | 25         |

## Model d0.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.orders')

#@ persist name="d0_daily" storage=lake
source: daily_orders is orders -> {
  group_by: order_date
  aggregate:
    order_count is count()
    total_amount is amount.sum()
}
```

## Publish

expect binding: daily_orders -> lake

## Query daily rollup

```malloy
run: daily_orders -> { select: order_date, total_amount; order_by: order_date asc }
```

servedFrom: storage

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 225          |

## Operator lake

The table the build wrote, and its column types, which the copy in the new
catalog has to match.

```sql
SELECT table_schema, table_name, column_name, data_type
FROM information_schema.columns
WHERE table_name = 'd0_daily'
ORDER BY ordinal_position;
```

Expect:

| table_schema | table_name | column_name  | data_type     |
| ------------ | ---------- | ------------ | ------------- |
| main         | d0_daily   | order_date   | DATE          |
| main         | d0_daily   | order_count  | BIGINT        |
| main         | d0_daily   | total_amount | DOUBLE        |

## Operator lake2

The orchestrator provisions the new catalog: the schema, and the table under the
same name. Its totals differ from the old catalog's on purpose, so a query's
answer says which catalog it read.

```sql
CREATE SCHEMA IF NOT EXISTS materialized;
CREATE TABLE materialized.d0_daily (order_date DATE, order_count BIGINT, total_amount DOUBLE);
INSERT INTO materialized.d0_daily VALUES ('2026-01-01', 2, 7777), ('2026-01-02', 2, 8888);
```

## Destination repoint (dest=lake, to=lake2, schema=materialized)

Push the cutover: `lake` keeps its name and now names `lake2`'s catalog, with its
tables in `materialized`.

## Query daily rollup (again)

The loaded package now reads `lake.materialized.d0_daily` in the new catalog,
from storage, without a reload.

servedFrom: storage

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 7777         |
| 2026-01-02 | 8888         |
