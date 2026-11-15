---
id: partition-on-materialize-is-refused
tags: eligibility, partitioning, materialize
package: pcs
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# `partition=` on `#(materialize)` is refused at publish

`#(materialize)` once carried `partition=` onto the `#@ persist` line it
generates. It is retired there too: the key is malformed
(`materialize-retired-key`), so the source is not built and the publish is
refused, with a message that says to remove it.

## Publisher

- PERSIST_STORAGE_MODE: on
- PUBLISHER_MATERIALIZE: true
- PUBLISHER_PACKAGE_VERSIONING: off

## Data orders_pg.pcs_orders

| order_id:int | org_id:int | amount:num |
| ------------ | ---------- | ---------- |
| 1            | 1          | 100        |
| 2            | 2          | 60         |

## Model pcs.malloy

```malloy
##! experimental.persistence

#(materialize) name="pcs_orders" storage=lake partition="org_id"
source: orders is orders_pg.sql('SELECT order_id, org_id, amount FROM public.pcs_orders')
```

## Warns pcs

cites: #(materialize) no longer reads `partition=`; remove it

## Republish refused pcs

cites: #(materialize) no longer reads `partition=`; remove it

## Query still answers live

Not built, the source is answered live from the warehouse.

```malloy
run: orders -> { aggregate: total is amount.sum() }
```

Expect:

| total:num |
| --------- |
| 160       |
