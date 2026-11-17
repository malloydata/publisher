---
id: a-materialized-source-shows-only-its-authors-name
tags: materialize, build-control
package: mzn
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A `#(materialize)` source shows only its author's name

`#(materialize)` builds a source through a generated build source, `<src>__mat`,
in a companion file the package never shows. That name is internal: the build
plan, a name-scoped run, the run's record and manifest, the package's warnings
and a query's path report all name the author's source, `mzn_orders`. A run
scoped by the author's name builds it; one scoped by the generated name selects
nothing. The table is addressed exactly as the generated source's SQL says, so
presenting it under the author's name moves no key.

## Publisher

- PERSIST_STORAGE_MODE: on
- PUBLISHER_MATERIALIZE: true

## Data orders_pg.mzn_orders

| order_id:int | order_date:date | region:text | amount:num |
| ------------ | --------------- | ----------- | ---------- |
| 1            | 2026-01-01      | US          | 100        |
| 2            | 2026-01-15      | EU          | 50         |
| 3            | 2026-02-02      | US          | 200        |

## Model mzn.malloy

```malloy
source: orders_raw is orders_pg.table('public.mzn_orders')

#(materialize) name="mzn_orders_t" storage="lake"
source: mzn_orders is orders_raw extend {
  dimension: order_month is order_date.month
  measure: total is amount.sum()
}
```

## Build targets

The plan names the author's source.

Expect:

| source     | writes       |
| ---------- | ------------ |
| mzn_orders | mzn_orders_t |

## Hook scopedRunByGeneratedNameSelectsNothing

## Publish (sources=mzn_orders)

A run scoped by the author's name builds the generated source.

expect binding: mzn_orders -> lake

## Query by month

```malloy
run: mzn_orders -> { group_by: order_month; aggregate: total; order_by: order_month }
```

servedFrom: storage

Expect:

| order_month | total |
| ----------- | ----- |
| 2026-01-01  | 150   |
| 2026-02-01  | 200   |

## Hook noGeneratedNameOnAnySurface
