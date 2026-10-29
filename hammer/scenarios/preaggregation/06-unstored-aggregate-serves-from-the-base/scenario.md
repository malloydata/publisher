---
id: preaggregate-unstored-aggregate-serves-from-the-base
tags: serve-correctness, preaggregation
package: pu
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# An aggregate the rollup stores no partial for is answered from the base

A rollup holds one row per grain group, and a declared measure survives that because
its stored partial is merged back (`total is total__partial.sum()`). An aggregate
written inline in the query has no partial. Evaluated over the rollup, it counts or
sums GROUPS: `count()` returns the number of rollup rows, and `qty.sum()` with `qty`
in the grain adds one `qty` per group.

Both compile against the rollup. The composite resolver only checks that a member
DEFINES every field a query uses: `count()` uses none, and `qty` is a grain column. So
nothing fails and the answer is quietly wrong. A rollup may answer a query only when
every aggregate in it is one of its merged measures, or an expression over them.
Anything else is answered from the base.

Two things have to hold together, and the staleness proof tells them apart. After the
build the source is mutated. A covered query must still read the stored rollup, so it
stays stale. Each unstored aggregate must read the base, so it is both right before the
mutation and fresh after it. A right answer alone does not show this, because a check
that sent every query to the base would also give right answers while losing the
acceleration.

The grain holds 2 groups over 4 rows, so a routed `count()` says `2` where the base
says `4`.

## Data orders_pg.pu_orders

| order_id:int | category:text | qty:int | amount:num |
| ------------ | ------------- | ------- | ---------- |
| 1            | books         | 1       | 100        |
| 2            | books         | 1       | 50         |
| 3            | tools         | 2       | 200        |
| 4            | tools         | 2       | 25         |

## Model pu.malloy

```malloy
##! experimental { persistence composite_sources }

source: orders is orders_pg.table('public.pu_orders') extend {
  measure:
    #@ preaggregate grain="category, qty"
    total is amount.sum()
}
```

## Publish

## Query covered

At a subset of the grain, using only the stored measure, so it is answered from the rollup.

```malloy
run: orders -> { group_by: category; aggregate: total; order_by: category asc }
```

Expect:

| category | total |
| -------- | ----- |
| books    | 150   |
| tools    | 225   |

## Query row count

The issue as reported: from the rollup this was `2`.

```malloy
run: orders -> { aggregate: n is count() }
```

Expect:

| n |
| - |
| 4 |

## Query grain sum

`qty` is in the grain, so the rollup has the column. Summed there, it gives one
`qty` per group (`1 + 2 = 3`), not one per row.

```malloy
run: orders -> { aggregate: s is qty.sum() }
```

Expect:

| s |
| - |
| 6 |

## Query having

A `having:` that counts. Over the rollup every group has one row, so neither category
passed.

```malloy
run: orders -> { group_by: category; aggregate: total; having: count() > 1; order_by: category asc }
```

Expect:

| category | total |
| -------- | ----- |
| books    | 150   |
| tools    | 225   |

## Query extend

A measure defined by an inline `extend`. The pipeline references only `n`, so the
`count()` is in the source extension rather than in the query, and the check has to
follow the reference to find it.

```malloy
run: orders extend { measure: n is count() } -> { aggregate: n }
```

Expect:

| n |
| - |
| 4 |

## Mutate orders_pg.pu_orders

| order_id:int | category:text | qty:int | amount:num |
| ------------ | ------------- | ------- | ---------- |
| 99           | books         | 1       | 1000       |

## Query covered (again)

Still `150`, so the rollup is built and the covered query is answered from it.
Without this, every assertion above would also pass if the check sent all queries to
the base.

Expect:

| category | total |
| -------- | ----- |
| books    | 150   |
| tools    | 225   |

## Query row count (again)

`5`, so the count came from the base and not the rollup. The rollup gives `2` before
and after the mutation.

Expect:

| n |
| - |
| 5 |

## Query grain sum (again)

Expect:

| s |
| - |
| 7 |
