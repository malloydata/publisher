---
id: a-destination-that-must-be-bootstrapped-is-never-created-by-a-build
tags: orchestration, operator, security
package: nb
---

<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A destination that must be bootstrapped is never created by a build

`a-destination-nothing-has-been-built-into-lists-as-not-found` shows the default:
the first build into a DuckLake destination creates its catalog, with whatever
settings that attach carried. An orchestrator that creates catalogs itself, through
the destination's `bootstraps` route, encrypted and owned by the role it chose, sets
`createIfNotExists: false`. Then a build into a catalog that does not exist yet is a
failure, never a quiet creation of an unencrypted catalog.

Proves:

- the build of a source into such a destination fails;
- it created nothing: the destination still lists as not found afterwards.

## Connection nb_lake (type=ducklake, metadataSchema=tenant_nb, createIfNotExists=false)

A destination whose catalog lives in the `tenant_nb` schema, which nothing has
created, and which no build may create.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.nb_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-02      | 200        |

## Model nb.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.nb_orders')

#@ persist name="nb_daily" storage=nb_lake
source: daily_orders is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}
```

## Build refused nb

cites: Existing DuckLake

## Destination tables (dest=nb_lake)

The failed build created no catalog.

status: 404
