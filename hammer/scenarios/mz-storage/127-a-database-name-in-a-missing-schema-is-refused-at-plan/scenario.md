---
id: a-database-name-in-a-missing-schema-is-refused-at-plan
tags: naming, refusal, operator
package: cnf
---

<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A database `name=` in a schema that does not exist is refused at plan, not failed at build

A database `#@ persist` (no `storage=`) is built in its own warehouse, at its
`name=`. No build creates the schema a `name=` names: its owner does. Before,
a `name=` naming a schema that did not exist planned cleanly, with no
refusal and no warning, and every build then failed on the warehouse's own
"schema does not exist". Now the plan reads each target schema once, and
refuses a table whose schema is missing as `container_not_found`, with a
`persist-not-built` warning naming the schema and the fix (the sibling `storage=`
tier's `destinationSchema` check, on the database tier). The source answers
live. Once the operator creates the schema and the package is planned again,
it builds there.

Proves, against a real Postgres warehouse:

- the source is refused at plan as `container_not_found`, on the database tier;
- the package warns `persist-not-built`, naming the schema;
- after the operator creates the schema and the server reloads, a build
  writes the table into it.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.cnf_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |

## Model cnf.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.cnf_orders')

#@ persist name="cnf_reports.daily_orders"
source: daily_orders is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}
```

## Build refusals

Expect:

| source       | tier     | reason              |
| ------------ | -------- | ------------------- |
| daily_orders | database | container_not_found |

## Warns

cites: cnf_reports

## Query rollup

Refused at plan, so answered live.

```malloy
run: daily_orders -> { select: order_date, total_amount; order_by: order_date asc }
```

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 200          |

## Operator orders_pg

The operator provisions the schema in the warehouse.

```sql
CREATE SCHEMA IF NOT EXISTS cnf_reports;
```

## Restart

The package is planned again, and its schema is found.

## Build refusals

Expect:

| source |
| ------ |

## Publish

## Operator orders_pg

The table is in the schema its `name=` names.

```sql
SELECT order_date::text AS order_date, total_amount::int AS total_amount
FROM cnf_reports.daily_orders ORDER BY order_date;
```

Expect:

| order_date | total_amount:int |
| ---------- | ---------------- |
| 2026-01-01 | 150              |
| 2026-01-02 | 200              |
