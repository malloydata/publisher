---
id: filtered-chain-serves-only-filtered-rows
tags: serve-correctness, chained
package: fch
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A filtered source built ON a filtered source serves only the rows both allow

The chained shape of `filtered-source-serves-only-filtered-rows`: a persisted
source carrying a `where:`, and a second persisted source extending IT with a
`where:` of its own. Both filters have to reach the answer — the child's own, and
the parent's, which the child inherits by extending it.

Chained is the arrangement that occurs in practice: a cleaned base (`not
is_deleted`) with slices persisted on top of it (`is_open`, `not is_open`). It
exercises a second path, because a chained build computes the downstream over its
rebound parents rather than over the raw source.

What the answers below rest on, stated because they cannot show it: a filter is
not part of what gets BUILT, so once their filters are set aside these two
sources are the same relation. They content-address identically and share ONE
physical table — pinned below — and it holds every row, including the ones both
filters exclude. The artifact is deliberately wider than the source, and every
reader of it owes the filter. That is why the read side is guarded as closely as
it is in `filter-survives-the-serve-shape-ladder`: the rows to be excluded are
sitting in the table.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.fch_deals

| deal_id:int | amount:num | is_deleted:bool | is_open:bool |
| ----------- | ---------- | --------------- | ------------ |
| 1           | 100        | false           | true         |
| 2           | 50         | false           | false        |
| 3           | 200        | true            | true         |
| 4           | 25         | false           | true         |

## Model fch.malloy

```malloy
##! experimental.persistence

source: all_deals is orders_pg.sql('SELECT deal_id, amount, is_deleted, is_open FROM public.fch_deals')

#@ persist name="fch_deals_tbl" storage=lake
source: deals is all_deals extend {
  where: not is_deleted
}

#@ persist name="fch_open_tbl" storage=lake
source: pipeline_now is deals extend {
  where: is_open
}
```

## Publish

expect binding: deals -> lake

## SQL what the filters select

Deal 3 is deleted and deal 2 is closed, so the open pipeline is deals 1 and 4.

```sql
SELECT
  (SELECT sum(amount) FROM fch_deals WHERE NOT is_deleted) AS deals_total,
  (SELECT sum(amount) FROM fch_deals WHERE NOT is_deleted AND is_open) AS open_total;
```

Expect:

| deals_total:num | open_total:num |
| --------------- | -------------- |
| 175             | 125            |

## Query deals total

```malloy
run: deals -> { aggregate: total is amount.sum() }
```

Expect:

| total:num |
| --------- |
| 175       |

## Query open total

The child's `where:` AND the parent's, both applied.

```malloy
run: pipeline_now -> { aggregate: total is amount.sum() }
```

Expect:

| total:num |
| --------- |
| 125       |

## Connection lake_probe

Two declared `name=` values, ONE physical table: with their filters set aside the
two sources are the same relation, so the build content-addresses them together.
A second table here would mean the filter had entered the build, which would make
the read-side filter a double application rather than the only one.

```sql
SELECT count(*) AS n FROM information_schema.tables
WHERE table_name IN ('fch_deals_tbl','fch_open_tbl')
```

Expect:

| n:num |
| ----- |
| 1     |
