---
id: preaggregate-storage-serves-from-the-lake
tags: serve-correctness, preaggregation, storage
package: ps
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A rollup is built into the store, and answers from there

`storage=` on a `#@ preaggregate` line builds the rollup into the managed store instead
of the source warehouse, and serves it from there. Nothing about the author's query
changes — it names `orders` and knows no rollup exists.

Two things have to hold at once, and the second is what makes the first meaningful.

**A covered query is answered from the stored rollup.** Proven by staleness, because it
cannot be proven by the numbers: a rollup returns the same answer a live recompute
would, which is the whole point of the feature and also what makes a regression here
invisible in the rows. So the source is changed after the build, and a query that still
returns the old answer can only have read the stored table.

**An uncovered query still gets the right answer.** The composite over lake rollups has
no base member — the base lives on the source warehouse, and every member of a composite
must share a connection — so a query no rollup covers does not compile against it and
falls back to live. That fallback is load-bearing rather than incidental: without it an
uncovered query would either fail or, worse, be answered from rollup rows that do not
carry the columns it asked for.

Both are exercised against the same package, after the same mutation, so the two
outcomes are told apart by their answers: the covered query is stale, the uncovered one
is fresh. Either one alone would pass while the other was broken.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.ps_orders

| order_id:int | category:text | region:text | amount:num |
| ------------ | ------------- | ----------- | ---------- |
| 1            | books         | US          | 100        |
| 2            | books         | EU          | 50         |
| 3            | tools         | US          | 200        |
| 4            | tools         | US          | 25         |

## Model ps.malloy

The base is a plain table source carrying an annotated measure — the standalone form.
Pre-aggregation does not require `#@ persist`, and a preagg base usually cannot carry
one at all, because Malloy admits only query-shaped sources as build roots and a table
extended with measures is not one. So the `#@ preaggregate` line is where a destination
is written — it is never inherited from the base, which could not be served if it were.

```malloy
##! experimental { persistence composite_sources }

source: orders is orders_pg.table('public.ps_orders') extend {
  measure:
    #@ preaggregate grain="category" storage=lake
    total is amount.sum()
}
```

## Publish

## Connection lake_probe (rows=1)

The rollup's table is in the store, not in the source warehouse. Asked of the lake
directly rather than inferred from a query answering correctly, because a correct answer
proves nothing about placement — the live path returns the same rows. A count rather than
a name, since the name carries a grain digest.

```sql
SELECT table_name FROM information_schema.tables WHERE table_name LIKE '%__preagg__%'
```

## Query by category

At the rollup's grain, so the composite resolves to the rollup member and the query is
answered from the store.

```malloy
run: orders -> { group_by: category; aggregate: total; order_by: category asc }
```

Expect:

| category | total |
| -------- | ----- |
| books    | 150   |
| tools    | 225   |

## Mutate orders_pg.ps_orders

A row large enough that a recompute cannot be mistaken for the stored answer.

| order_id:int | category:text | region:text | amount:num |
| ------------ | ------------- | ----------- | ---------- |
| 99           | books         | US          | 1000       |

## Query by category (again)

That row is in the source table now, so a recompute would say `1150` for `books`. Only a
read of the stored rollup can still say `150`.

Expect:

| category | total |
| -------- | ----- |
| books    | 150   |
| tools    | 225   |

## Query off-grain

`region` is not in any grain, so no member covers this and the composite does not
compile. The query falls back to live and sees the mutation — `1150` for books/US, the
value the stale query above must NOT have returned.

```malloy
run: orders -> { group_by: category, region; aggregate: total; order_by: category asc, region asc }
```

Expect:

| category | region | total |
| -------- | ------ | ----- |
| books    | EU     | 50    |
| books    | US     | 1100  |
| tools    | US     | 225   |

## Note

Before this, `storage=` on a `#@ preaggregate` line parsed, validated, and did nothing:
the reader took only `grain` and `namespace`, no rule rejected the unknown key, and the
rollup was built colocated in the source warehouse. Nothing documented the key, so no
advertised path reached it — which is why the refusal for what is still unsupported and
the support for what now works ship together rather than as two changes.
