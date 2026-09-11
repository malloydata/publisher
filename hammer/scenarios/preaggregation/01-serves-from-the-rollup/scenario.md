---
id: preaggregate-serves-from-the-rollup
tags: serve-correctness, preaggregation
package: pa
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A rollup is created where its author says, and answers the query from there

`#@ preaggregate` synthesizes a rollup as its own `#@ persist` source and rewrites the
base into `compose(rollup, base)`. Two promises follow, and they hold independently.

**A rollup is created in the namespace it was given.** `namespace=` on the annotation
names it; absent that, the rollup inherits the namespace of its base's own
`#@ persist name=`, because a rollup of X belongs beside X. Only the container is
author-controlled — the table name stays derived, so the resolver and the content
address can rely on it. Both routes are exercised here, on one source, because a grain
is a table: two grains can be created in two places, and each must take the namespace
named for it rather than whichever the model happened to mention first.

**A query at the rollup's grain is answered from the stored table.** Not recomputed:
both return the same numbers, which is the point of the feature and also what makes a
regression here invisible in the rows. The proof is staleness — once the rollup is
built, changing the underlying data and asking again must return the answer that was
stored.

Colocated on Postgres, where the catalog can be asked which schema actually received
the table. A dialect that requires qualification cannot create an unqualified rollup at
all; Postgres accepts either, which is what makes it able to tell the two apart.

## Operator orders_pg

Both schemas have to exist before anything is built into them — nothing in the tier
creates one. Run against the source warehouse directly, as an operator would.

```sql
CREATE SCHEMA IF NOT EXISTS analytics; CREATE SCHEMA IF NOT EXISTS rollups;
```

## Data orders_pg.pa_orders

| order_id:int | category:text | region:text | amount:num |
| ------------ | ------------- | ----------- | ---------- |
| 1            | books         | US          | 100        |
| 2            | books         | EU          | 50         |
| 3            | tools         | US          | 200        |
| 4            | tools         | US          | 25         |

## Model pa.malloy

```malloy
##! experimental { persistence composite_sources }

source: pa_orders is orders_pg.table('public.pa_orders')

#@ persist name="analytics.pa_orders_tbl"
source: orders is pa_orders -> {
  select: order_id, category, region, amount
} extend {
  measure:
    #@ preaggregate grain="region" namespace="rollups"
    regional is amount.sum()
  measure:
    #@ preaggregate grain="category"
    total is amount.sum()
}
```

## Publish

## Operator orders_pg

Ask the catalog which schema received each rollup, rather than trusting the name it was
given. Two grains are two tables, so each answers for itself: `region` was sent to
`rollups` by its own annotation, and `category` — declared after it, and naming no
namespace — still inherited `analytics` from the base rather than picking up its
neighbour's.

Which grain landed where, not merely which schemas were used: the set alone passes
whichever rollup went to which, so it would read as green if the two were swapped. The
rollup's name carries its grain — `<base>__preagg__<grain>__<digest>` — so the catalog
can be asked for the pairing without knowing the digest.

```sql
SELECT table_schema, split_part(table_name, '__', 3) AS grain FROM information_schema.tables WHERE table_name LIKE '%__preagg__%' ORDER BY table_schema
```

Expect:

| table_schema | grain    |
| ------------ | -------- |
| analytics    | category |
| rollups      | region   |

## Query by category

At the rollup's grain, so the composite resolves to the rollup member.

```malloy
run: orders -> { group_by: category; aggregate: total; order_by: category asc }
```

Expect:

| category | total |
| -------- | ----- |
| books    | 150   |
| tools    | 225   |

## Mutate orders_pg.pa_orders

| order_id:int | category:text | region:text | amount:num |
| ------------ | ------------- | ----------- | ---------- |
| 99           | books         | US          | 1000       |

## Query by category (again)

That row is in the source table now, so a recompute would say 1150 for `books`. Only a
read of the stored rollup can still say 150.

Expect:

| category | total |
| -------- | ----- |
| books    | 150   |
| tools    | 225   |

## Query by region

The other rollup's grain, and the other namespace. It has to be served from `rollups`
for this to be stale — the same staleness proof as `category`, on the table whose
placement came from the annotation rather than from the base.

```malloy
run: orders -> { group_by: region; aggregate: regional; order_by: region asc }
```

Expect:

| region | regional |
| ------ | -------- |
| EU     | 50       |
| US     | 325      |

## Query off-grain

A grain neither rollup covers falls through to the base member, which is itself
persisted — so this stays stale too, and it confirms the composite still answers
queries no rollup can.

```malloy
run: orders -> { group_by: category, region; aggregate: amount_total is amount.sum(); order_by: category asc, region asc }
```

Expect:

| category | region | amount_total |
| -------- | ------ | ------------ |
| books    | EU     | 50           |
| books    | US     | 100          |
| tools    | US     | 225          |

## Note

Both promises were broken at once before this scenario existed. The synthesized rollup
carried a bare `#@ persist`, so the build self-assigned an unqualified name: harmless on
DuckDB and Postgres, which put it in the default schema, and fatal on BigQuery, which
rejects an unqualified `CREATE` outright — pre-aggregation could not build there at all.
Nothing in `#@ preaggregate` or `#@ persist` offered a way to supply a dataset, which is
what `namespace=` now does.

The placement half is why this runs colocated rather than into a storage destination: a
synthesized rollup is written to the source warehouse today regardless of the base's
`storage=`, so that is where the table can be found. Letting a rollup follow its base
into a `storage=` destination is separate work.
