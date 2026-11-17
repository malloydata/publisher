---
id: partition-on-a-storage-persist-is-refused
tags: eligibility, partitioning
package: pla
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# `partition=` on a `storage=` persist fails the publish and the load

`partition=` once laid a stored table out as one directory per value. It was a
layout and carried no isolation, and it is no longer supported: a `#@ persist`
that declares it is an invalid annotation (`persist-retired-key`), so the
publish and the load fail on every path, like any other retired key, with a
message that says to remove it. Removing it costs scan time only;
every stripped term is re-applied at read either way, so what each caller sees
is unchanged.

Refused rather than ignored, because a package that published yesterday with a
layout must not publish today with silently none.

## Publisher

- PERSIST_STORAGE_MODE: on
- PUBLISHER_PACKAGE_VERSIONING: off

## Data orders_pg.pla_orders

| order_id:int | org_id:int | amount:num |
| ------------ | ---------- | ---------- |
| 1            | 1          | 100        |
| 2            | 2          | 60         |
| 3            | 1          | 200        |
| 4            | 3          | 15         |

## Model pla.malloy

```malloy
##! experimental.persistence
##! experimental.givens

given:
  ORG_ID :: number is 1

source: raw is orders_pg.sql('SELECT order_id, org_id, amount FROM public.pla_orders')

#@ persist name="pla_orders" storage=lake partition="org_id"
source: orders is raw -> { select: * } extend {
  where: org_id = $ORG_ID
}
```

## Rejected pla

The configured package does not load, so it is not served.

## Republish refused pla

The author-in-the-loop publish is refused, naming the key and the fix.

cites: partition= is not supported

## Build refused pla

A build cannot start on a package that does not load; the failure carries the
retired-key code.

cites: [persist-retired-key]
