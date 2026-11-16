---
id: extend-persist-materializes-nothing-new
tags: serve-correctness, build-control, needs-attention
package: xpn
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Persisting an `extend` of a persisted source materializes nothing new

Proof that there is no value in persisting a source that merely `extend`s another
persisted source. `daily_with_avg` extends `daily` to add an average that is
computed at query time — the extension's added fields (dimensions, measures, joins)
are query-time constructs, NOT materialized columns, so its persisted relation is
byte-identical to `daily`'s. The two compile to the same build, but each declares
its OWN `name=`, and a different `name=` is a different table: the publisher builds
both, each under its own name, and each serves from its own. The second `#@
persist` buys nothing but a second copy of the same rows.

(Contrast `chained-persist` (11): a `->` DERIVED source — e.g. `monthly is daily ->
{ group_by: order_date.month }` — defines a NEW relation at a different grain and
genuinely materializes a different table. THAT has value. An `extend`, which keeps
the same grain and only adds fields, does not.)

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.xpn_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |

## Model xpn.malloy

Two persist sources with DISTINCT names (so the package is valid — no within-package
collision). `daily_with_avg` only `extend`s `daily` with a query-time field.

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.xpn_orders')

#@ persist name="daily_tbl" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate:
    total_amount is amount.sum()
    num_orders is count()
}

#@ persist name="daily_with_avg_tbl" storage=lake
source: daily_with_avg is daily extend {
  dimension: avg_order_value is total_amount / num_orders
}
```

## Publish

Both declare `#@ persist` with distinct names, so two physical tables are
materialized, with the same rows.

## Query daily

```malloy
run: daily -> { select: order_date, total_amount; order_by: order_date asc }
```

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 200          |

## Query daily_with_avg

`daily_with_avg` reads its own table, which holds the same relation as `daily`'s —
the shared columns are identical, and its `avg_order_value` is computed at query
time (not a stored column).

```malloy
run: daily_with_avg -> { select: order_date, total_amount, avg_order_value; order_by: order_date asc }
```

Expect:

| order_date | total_amount | avg_order_value |
| ---------- | ------------ | --------------- |
| 2026-01-01 | 150          | 75              |
| 2026-01-02 | 200          | 200             |

## Build targets

Both sources are targets, each declaring its OWN physical name, so each has its own
content address (`entity` labels `A` and `B`): the same build, folded with each
`name=`.

Expect:

| source         | writes             | entity |
| -------------- | ------------------ | ------ |
| daily          | daily_tbl          | A      |
| daily_with_avg | daily_with_avg_tbl | B      |

## Connection lake_probe (rows=2)

Both declared names are physically materialized, whatever the build order.

```sql
SELECT table_name FROM information_schema.tables
WHERE table_name IN ('daily_tbl','daily_with_avg_tbl')
```

## Note (since=2026-11-15)

> The distinct-names question this scenario raised is decided: a `name=` is part of
> what identifies a table, so the two names now get two tables (they used to
> collapse to one, named by build order). Still open: whether a source should carry `#@ persist` on an `extend` at all — malloy
> PR 3012 stops an *inherited* persist from making the reader a build target; an
> *own* persist on an extend (this scenario) is still accepted but is redundant
> (identical relation to the base). See also the publisher's
> `detectDroppedPersistSources` hard-refuse, which will need to switch from the
> folded annotation to the own declaration when PR 3012 is adopted (otherwise it
> hard-errors on every extend-of-a-persist).
