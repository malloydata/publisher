---
id: partition-on-a-colocated-persist-is-refused
tags: eligibility, partitioning
package: pws
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# `partition=` on a colocated persist is refused at publish

A colocated `#@ persist` writes into the customer's own warehouse, where the
publisher never laid a table out. `partition=` is retired everywhere, so here it
is refused at publish exactly as on a `storage=` source
(`partition-on-a-storage-persist-is-refused`), not by a gate of its own.

## Publisher

- PERSIST_STORAGE_MODE: on
- PUBLISHER_PACKAGE_VERSIONING: off

## Data orders_pg.pws_orders

| order_id:int | org_id:int | amount:num |
| ------------ | ---------- | ---------- |
| 1            | 1          | 100        |
| 2            | 2          | 60         |

## Model pws.malloy

```malloy
##! experimental.persistence

source: raw is orders_pg.sql('SELECT order_id, org_id, amount FROM public.pws_orders')

#@ persist name="pws_orders" partition="org_id"
source: orders is raw -> { select: * }
```

## Warns pws

cites: partition= is not supported

## Republish refused pws

cites: partition= is not supported

## Build refusals (pkg=pws)

No eligibility gate refuses it: the retired key is refused at publish, never as
a `partition_*` plan refusal, which the plan no longer produces.

Expect:

| source |
| ------ |
