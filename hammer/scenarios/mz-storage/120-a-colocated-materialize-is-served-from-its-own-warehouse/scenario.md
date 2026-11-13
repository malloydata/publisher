---
id: a-colocated-materialize-is-served-from-its-own-warehouse
tags: materialize, serve-correctness
package: cmz
---

<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A colocated `#(materialize)` is built and served from its own warehouse

A `#(materialize)` source with no `storage=` is built into the Postgres it
reads, at its `name=`, and the served copy answers it from that table on the
source's own connection, with no storage tier at all. Its stored computed
dimension reads the stored column, and its `where:` is applied again at read.

Proves, against a real Postgres warehouse and a real server:

- the build writes the table `public.cmz_big` in the source warehouse, holding
  only the rows the source's `where:` keeps, with the computed `tier` stored;
- a query is answered from that table: after the source table changes, the
  answer is the built snapshot, not a fresh recompute;
- no storage tier is involved (`PERSIST_STORAGE_MODE: off`).

## Publisher

- PERSIST_STORAGE_MODE: off
- PUBLISHER_MATERIALIZE: true

## Data orders_pg.cmz_orders

| order_id:int | region:text | amount:num |
| ------------ | ----------- | ---------- |
| 1            | east        | 100        |
| 2            | east        | 50         |
| 3            | west        | 700        |
| 4            | west        | 250        |

## Model cmz.malloy

```malloy
source: orders is orders_pg.table('public.cmz_orders')

#(materialize) name="cmz_big"
source: big_orders is orders extend {
  where: amount >= 100
  dimension: tier is pick 'large' when amount >= 500 else 'medium'
  measure: total is amount.sum()
}
```

## Publish

## Operator orders_pg

The build wrote the source's own rows, its `where:` applied, into the warehouse
it reads, with the computed dimension stored under its own name.

```sql
SELECT region, tier, amount::INTEGER AS amount FROM public.cmz_big ORDER BY order_id;
```

Expect:

| region | tier   | amount:int |
| ------ | ------ | ---------- |
| east   | medium | 100        |
| west   | large  | 700        |
| west   | medium | 250        |

## Query by tier

```malloy
run: big_orders -> { group_by: tier; aggregate: total; order_by: tier }
```

Expect:

| tier   | total:num |
| ------ | --------- |
| large  | 700       |
| medium | 350       |

## Mutate orders_pg.cmz_orders

A qualifying order that a live query would carry into the answer.

| order_id:int | region:text | amount:num |
| ------------ | ----------- | ---------- |
| 5            | east        | 900        |

## Query by tier (again)

Stale ⇒ the answer came from the built table, with `tier` read from its stored
column and the `where:` applied over it.

Expect:

| tier   | total:num |
| ------ | --------- |
| large  | 700       |
| medium | 350       |
