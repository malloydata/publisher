---
id: an-ungrouped-aggregate-on-a-lifted-source-serves-from-storage
tags: serve-correctness, chained, lift
package: uag
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# An ungrouped aggregate on a lifted source serves from storage

`shares` is a non-persisted source over the stored `daily`: a measure that is
an ungrouped aggregate (`total.sum() / all(total.sum())`) and a view that reads
it. The serve path carries `shares` over `daily`'s table by re-declaring what
it adds. A measure left behind is a view that does not compile, and a lift
that does not compile is withheld — so `shares` must carry the measure, or
every query on it runs live against the warehouse while `daily`'s table sits
unread beside it.

The proof is a stale answer: after the warehouse changes, `shares` still
reports the shares of the stored rows.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.uag_orders

| order_id:int | region:text | amount:num |
| ------------ | ----------- | ---------- |
| 1            | east        | 100        |
| 2            | west        | 100        |
| 3            | east        | 200        |

## Model uag.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.uag_orders')

#@ persist name="uag_daily" storage=lake
source: daily is orders -> {
  group_by: region
  aggregate: total is amount.sum()
}

source: shares is daily extend {
  measure: share is total.sum() / all(total.sum())
  view: by_region is { group_by: region; aggregate: share; order_by: region asc }
}
```

## Publish

expect binding: daily -> lake

## Query shares by region

East has 300 of 400.

```malloy
run: shares -> by_region
```

servedFrom: storage

Expect:

| region | share:num |
| ------ | --------- |
| east   | 0.75      |
| west   | 0.25      |

## Mutate orders_pg.uag_orders

| order_id:int | region:text | amount:num |
| ------------ | ----------- | ---------- |
| 99           | west        | 1000       |

## Query shares by region (again)

Unchanged: the shares came from `daily`'s stored table.

```malloy
run: shares -> by_region
```

servedFrom: storage

Expect:

| region | share:num |
| ------ | --------- |
| east   | 0.75      |
| west   | 0.25      |
