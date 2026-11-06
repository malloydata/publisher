---
id: preaggregate-storage-unstored-aggregate-serves-live
tags: serve-correctness, preaggregation, storage
package: pv
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A lake rollup does not answer an aggregate it stores no partial for

The same rule as `preaggregate-unstored-aggregate-serves-from-the-base`, on the storage
tier. That scenario covers the colocated path. This one is needed because the storage
path routes through different code, and its composite is a different shape.

A lake rollup is offered through a composite with no base member. The base is on the
source warehouse, and every member of a composite must be on the same connection. So
here the compiler cannot fall back to a base member inside the composite. When a query
compiles against the shape at all, a rollup answers it. An inline `count()` uses no
field, so it compiles against the first lake member and returns that table's row count.
The only safeguard is a check after compile that sends the query to live.

The staleness proof is the same as the colocated scenario's. After the mutation, the
covered query reads the stored rollup and stays stale, and each unstored aggregate reads
the source and sees the new row.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.pv_orders

| order_id:int | category:text | qty:int | amount:num |
| ------------ | ------------- | ------- | ---------- |
| 1            | books         | 1       | 100        |
| 2            | books         | 1       | 50         |
| 3            | tools         | 2       | 200        |
| 4            | tools         | 2       | 25         |

## Model pv.malloy

```malloy
##! experimental { persistence composite_sources }

source: orders is orders_pg.table('public.pv_orders') extend {
  measure:
    #@ preaggregate grain="category, qty" storage=lake
    total is amount.sum()
}
```

## Publish

## Query covered

```malloy
run: orders -> { group_by: category; aggregate: total; order_by: category asc }
```

servedFrom: storage

Expect:

| category | total |
| -------- | ----- |
| books    | 150   |
| tools    | 225   |

## Query row count

From the lake rollup this would be `2`, the number of grain groups.

```malloy
run: orders -> { aggregate: n is count() }
```

Expect:

| n |
| - |
| 4 |

## Query grain sum

```malloy
run: orders -> { aggregate: s is qty.sum() }
```

Expect:

| s |
| - |
| 6 |

## Mutate orders_pg.pv_orders

| order_id:int | category:text | qty:int | amount:num |
| ------------ | ------------- | ------- | ---------- |
| 99           | books         | 1       | 1000       |

## Query covered (again)

Still `150`: the stored rollup answered.

servedFrom: storage

Expect:

| category | total |
| -------- | ----- |
| books    | 150   |
| tools    | 225   |

## Query row count (again)

`5`: answered live from the source. The lake rollup can only give `2`.

Expect:

| n |
| - |
| 5 |

## Query grain sum (again)

Expect:

| s |
| - |
| 7 |
