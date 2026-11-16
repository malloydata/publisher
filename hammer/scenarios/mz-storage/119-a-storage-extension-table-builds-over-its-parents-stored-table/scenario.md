---
id: a-storage-extension-table-builds-over-its-parents-stored-table
tags: materialize, chained, orchestration
package: mzx
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A `storage=` extension table builds over its parent's stored table, never the warehouse

Three `#(materialize)` sources in one destination, each extending the one
before: `us_orders` adds a `where:` and a dimension to `orders`, and `us_big`
adds a `where:` over that dimension. Each extension's build reads its parent's
stored table in the lake, so its entry says `reused`, and its rows are a
function of its parent's stored rows.

That is what lets an orchestrator that must never have the warehouse do work it
did not schedule (`warehouseReach: refuse`) build the extensions: the strict
build reads only the lake, so nothing is refused. And it shows where the rows
came from: a US order added to the warehouse after `orders` was built is in
neither extension, because they were computed from the table `orders` stored,
not from the warehouse.

## Publisher

- PERSIST_STORAGE_MODE: on
- PUBLISHER_MATERIALIZE: true

## Data orders_pg.mzx_orders

| order_id:int | order_date:date | region:text | amount:num |
| ------------ | --------------- | ----------- | ---------- |
| 1            | 2026-01-01      | US          | 50         |
| 2            | 2026-01-15      | EU          | 500        |
| 3            | 2026-02-02      | US          | 200        |
| 4            | 2026-02-20      | US          | 300        |

## Model mzx.malloy

```malloy
source: orders_raw is orders_pg.table('public.mzx_orders')

#(materialize) storage="lake"
source: orders is orders_raw extend {
  dimension: order_month is order_date.month
  measure: total is amount.sum()
}

#(materialize) storage="lake"
source: us_orders is orders extend {
  where: region = 'US'
  dimension: big is amount > 100
}

#(materialize) storage="lake"
source: us_big is us_orders extend {
  where: big
}
```

## Build refused (orchestrated, strict, pkg=mzx)

Before `orders` has a table, a strict build of `us_orders` has nothing of its
parent's to read, so it is refused rather than recomputed from the warehouse,
and the refusal names the parent as the author wrote it.

- us_orders -> us_orders__g1 @ lake

cites: persisted upstream 'orders'

## Hook refusalNamesTheAuthorsParent

## Publish (sources=orders)

Only the root is built here; the orchestrator builds the extensions below.

expect binding: orders -> lake

## Mutate orders_pg.mzx_orders

| order_id:int | order_date:date | region:text | amount:num |
| ------------ | --------------- | ----------- | ---------- |
| 99           | 2026-02-25      | US          | 1000       |

## Build (orchestrated, strict, reach=refuse, pkg=mzx)

Build both extensions under strict with the warehouse recompute refused,
reusing the `orders` table built before the new order: `us_orders` over it,
and `us_big` over the `us_orders` this run builds. Each reads only the lake,
so nothing is refused, and each entry says its upstream was reused.

- us_orders -> us_orders__g2 @ lake (reused)
  reference: orders
- us_big -> us_big__g2 @ lake (reused)

## Bind mzx

## Query us_big by month

Built from `orders`' stored rows, which predate order 99.

```malloy
run: us_big -> { group_by: order_month; aggregate: total; order_by: order_month }
```

servedFrom: storage

Expect:

| order_month | total:num |
| ----------- | --------- |
| 2026-02-01  | 500       |

## Query us_orders

```malloy
run: us_orders -> { aggregate: n is count(), total }
```

servedFrom: storage

Expect:

| n:num | total:num |
| ----- | --------- |
| 3     | 550       |
