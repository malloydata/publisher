---
id: partition-column-must-be-in-the-stored-table
tags: eligibility, partitioning
package: pcs
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A partition column must be one the stored table has

The stored table is the source's PUBLIC projection — a hidden column is never
materialized — so a `partition=` naming anything outside that surface names a
column the layout statement would run against and not find.

Caught at publish, where the message can name the column, rather than mid-build
as a DDL error against a generated table name.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.pcs_orders

| order_id:int | org_id:int | amount:num |
| ------------ | ---------- | ---------- |
| 1            | 1          | 100        |
| 2            | 2          | 60         |

## Model pcs.malloy

```malloy
##! experimental.persistence

source: raw is orders_pg.sql('SELECT order_id, org_id, amount FROM public.pcs_orders')

#@ persist name="pcs_orders" storage=lake partition="region"
source: orders is raw -> { select: * }
```

## Build refused

cites: 'region'

## Build refusals

Expect:

| source | tier    | reason                   |
| ------ | ------- | ------------------------ |
| orders | storage | partition_column_unknown |
