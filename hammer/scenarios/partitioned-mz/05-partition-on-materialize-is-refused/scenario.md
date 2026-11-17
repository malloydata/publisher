---
id: partition-on-materialize-is-refused
tags: eligibility, partitioning, materialize
package: pcs
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# `partition=` on `#(materialize)` fails the publish and the load

`#(materialize)` once carried `partition=` onto the `#@ persist` line it
generates. It is retired there too: the annotation is invalid
(`materialize-retired-key`), so the publish and the load fail on every path,
with a message that says to remove it. An invalid annotation is never refused
alone and answered live; only a valid one Publisher cannot build is.

## Publisher

- PUBLISHER_PERSIST_STORAGE_MODE: on
- PUBLISHER_MATERIALIZE_ENABLED: true
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

## Rejected pcs

The configured package does not load, so `orders` is not served at all, live or
stored.

## Republish refused pcs

cites: #(materialize) no longer reads `partition=`; remove it

## Build refused pcs

cites: [materialize-retired-key]
