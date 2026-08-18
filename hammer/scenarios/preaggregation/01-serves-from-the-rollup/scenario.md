---
id: preaggregate-serves-from-the-rollup
tags: serve-correctness, preaggregation
package: pa
---

# A rollup is created where its author says, and answers the query from there

`#@ preaggregate` synthesizes a rollup as its own `#@ persist` source and rewrites the
base into `compose(rollup, base)`. Two promises follow, and they hold independently.

**A rollup is created in the namespace it was given.** `namespace=` on the annotation
names it; absent that, the rollup inherits the namespace of its base's own
`#@ persist name=`, because a rollup of X belongs beside X. Only the container is
author-controlled — the table name stays derived, so the resolver and the content
address can rely on it.

**A query at the rollup's grain is answered from the stored table.** Not recomputed:
both return the same numbers, which is the point of the feature and also what makes a
regression here invisible in the rows. The proof is staleness — once the rollup is
built, changing the underlying data and asking again must return the answer that was
stored.

Colocated on Postgres, where the catalog can be asked which schema actually received
the table. A dialect that requires qualification cannot create an unqualified rollup at
all; Postgres accepts either, which is what makes it able to tell the two apart.

## Operator orders_pg

The schema the base names has to exist before anything is built into it — nothing in
the tier creates one. Run against the source warehouse directly, as an operator would.

```sql
CREATE SCHEMA IF NOT EXISTS analytics;
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
    #@ preaggregate grain="category"
    total is amount.sum()
}
```

## Publish

## Operator orders_pg

Ask the catalog which schema received the rollup, rather than trusting the name it was
given.

```sql
SELECT table_schema FROM information_schema.tables WHERE table_name LIKE '%__preagg__%'
```

Expect:

| table_schema |
| ------------ |
| analytics    |

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

## Query off-grain

A grain the rollup does not cover falls through to the base member, which is itself
persisted — so this stays stale too, and it confirms the composite still answers
queries the rollup cannot.

```malloy
run: orders -> { group_by: region; aggregate: amount_total is amount.sum(); order_by: region asc }
```

Expect:

| region | amount_total |
| ------ | ------------ |
| EU     | 50           |
| US     | 325          |

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
