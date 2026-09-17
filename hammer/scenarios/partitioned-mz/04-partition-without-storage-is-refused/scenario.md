---
id: partition-without-storage-is-refused
tags: eligibility, partitioning
package: pws
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# `partition=` without `storage=` is refused, not ignored

A colocated `#@ persist` CTASes into the customer's own warehouse, where the
table's layout is that warehouse's DDL and nothing the publisher writes. So
`partition=` there names something the publisher cannot do.

Refused rather than dropped, because silently honouring nothing is the one
outcome that misleads: the author declared a layout, saw a successful build, and
has no way to learn the declaration did nothing.

## Publisher

- PERSIST_STORAGE_MODE: on

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

## Build refused

cites: partition
