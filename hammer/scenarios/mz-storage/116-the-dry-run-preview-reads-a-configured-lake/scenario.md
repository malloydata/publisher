---
id: the-dry-run-preview-reads-a-configured-lake
tags: preview, serve-correctness
package: pv
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# The dry-run preview judges a `storage=` source's views against a lake from config

`POST …/packages?dryRun=true` compiles each named view of a `storage=` source against
the columns its table will hold, before anything is built. The destination here is a
DuckLake declared in config, the path the end-to-end review found broken: Malloy learns
a destination's dialect by looking it up, and looking up a DuckLake attaches its
catalog. Before the first build that lookup failed, so every view previewed `live`
("Cannot determine dialect for connection 'lake'") although each one serves. The
preview now compiles against the destination's dialect when only the catalog is
missing, and a destination that is genuinely unreachable still fails the preview.

The preview runs twice, before the first build and after it. A dry run loads a
throwaway copy of the package that nothing is bound to, so the source itself reads
`not_built` both times; the views are what the preview judges.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.pv_orders

| order_id:int | order_date:date | region:text | amount:num |
| ------------ | --------------- | ----------- | ---------- |
| 1            | 2026-01-01      | east        | 100        |
| 2            | 2026-01-01      | west        | 50         |

## Model pv.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.pv_orders')

#@ persist name="pv_daily" storage=lake
source: daily is orders -> {
  group_by: order_date, region
  aggregate: total is amount.sum()
} extend {
  view: totals is { aggregate: t is total.sum() }
  view: by_region is { group_by: region; aggregate: t is total.sum() }
}
```

## Preview pv

- daily: not_built
- daily.totals: serves
- daily.by_region: serves

## Publish pv

expect binding: daily -> lake

## Preview pv

- daily: not_built
- daily.totals: serves
- daily.by_region: serves
