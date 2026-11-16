---
id: partition-on-a-colocated-persist-is-refused
tags: eligibility, partitioning
package: pws
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# `partition=` on a colocated persist fails the publish and the load

A colocated `#@ persist` writes into the customer's own warehouse, where the
publisher never laid a table out. `partition=` is retired everywhere, so here it
is an invalid annotation (`persist-retired-key`) exactly as on a `storage=`
source (`partition-on-a-storage-persist-is-refused`), not a gate of its own: the
publish and the load fail, with a message that says to remove it.

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

## Rejected pws

The configured package does not load, so it is not served.

## Republish refused pws

The author-in-the-loop publish is refused, naming the key and the fix.

cites: partition= is not supported

## Build refused pws

A build cannot start on a package that does not load; the failure carries the
retired-key code, never a `partition_*` plan refusal, which the plan no longer
produces.

cites: [persist-retired-key]
