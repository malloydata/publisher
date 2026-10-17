---
id: a-destination-nothing-has-been-built-into-lists-as-not-found
tags: orchestration, operator, gc, security
package: fresh
---

<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A destination nothing has been built into lists as not found, and looking does not create it

A per-tenant DuckLake catalog lives in its own metadata schema, and that schema
does not exist until the first build creates it. An orchestrator that sweeps
every tenant's destination for tables to reclaim therefore lists many catalogs
that do not exist yet. Their listing is a 404, so the orchestrator skips them.
An empty list would invite the drop or file cleanup that follows a listing, and
both of those attach read-write, which would create the catalog.

Proves:

- the listing answers 404, and its body never echoes the catalog's connection
  string;
- looking creates nothing, so a second listing is still a 404;
- once something has been built into the destination, the listing finds it.

## Connection fresh (type=ducklake, metadataSchema=tenant_fresh)

A destination whose catalog lives in the `tenant_fresh` schema, which nothing
has created yet.

## Publisher

- PERSIST_STORAGE_MODE: on

## Destination tables (dest=fresh)

status: 404

## Destination tables (dest=fresh)

The first listing created nothing.

status: 404

## Data orders_pg.fresh_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-02      | 200        |

## Model fresh.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.fresh_orders')

#@ persist name="fresh_daily" storage=fresh
source: daily_orders is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}
```

## Build (orchestrated, pkg=fresh)

The first build creates the catalog.

- daily_orders -> fresh_daily__g001 @ fresh

## Destination tables (dest=fresh)

Expect:

| name              |
| ----------------- |
| fresh_daily__g001 |
