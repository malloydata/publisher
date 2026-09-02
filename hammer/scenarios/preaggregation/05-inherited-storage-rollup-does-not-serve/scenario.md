---
id: preaggregate-inherited-storage-does-not-serve
tags: preaggregation, storage, known-limitation
package: pi
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A rollup inherited into the store is built, and cannot serve

Pins a limitation rather than a behaviour, so that the change which lifts it has
something to flip.

A rollup inherits `storage=` from its base's `#@ persist storage=`. But a base can only
carry that annotation if it is query-shaped — Malloy admits only query-shaped sources as
build roots — and a base that builds has a serve binding of its own under the author's
name. Its rollups want to be re-exposed under that same name, and one name cannot rebind
to two shapes, so the rollups are dropped from the serve shape and every query is
answered from the base's stored table.

The rollup is still built and still refreshed. It is cost with no acceleration behind it,
and it is the case the "a rollup follows its base" headline reads most naturally as.

Declaring `storage=` on the `#@ preaggregate` line instead is unaffected: a base that is
not itself persisted has no binding to collide with, which is the path
`preaggregate-storage-serves-from-the-lake` covers.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.pi_orders

| order_id:int | category:text | amount:num |
| ------------ | ------------- | ---------- |
| 1            | books         | 100        |
| 2            | books         | 50         |
| 3            | tools         | 200        |

## Model pi.malloy

```malloy
##! experimental { persistence composite_sources }

source: pi_base is orders_pg.table('public.pi_orders')

#@ persist storage=lake
source: orders is pi_base -> {
  select: order_id, category, amount
} extend {
  measure:
    #@ preaggregate grain="category"
    total is amount.sum()
}
```

## Publish

## Connection lake_probe (rows=2)

Two tables: the base's own stored copy, and the rollup. The rollup was built — that is
the cost half of the finding.

```sql
SELECT table_name FROM information_schema.tables
```

## Query by category

```malloy
run: orders -> { group_by: category; aggregate: total; order_by: category asc }
```

Expect:

| category | total |
| -------- | ----- |
| books    | 150   |
| tools    | 200   |

## Mutate orders_pg.pi_orders

| order_id:int | category:text | amount:num |
| ------------ | ------------- | ---------- |
| 99           | books         | 1000       |

## Query by category (again)

Still `150`, but from the BASE's stored copy rather than from the rollup — the base is
materialized too, so staleness alone cannot tell the two apart here. What distinguishes
them is the lake probe below.

Expect:

| category | total |
| -------- | ----- |
| books    | 150   |
| tools    | 200   |

## Note

> The rollup in this package is built and refreshed and can never be read. Lifting it
> means letting the base's own stored binding join the composite as its final member,
> which needs the base to be materialized into the SAME destination — true here — and
> must keep base-only as the serve ladder's guaranteed floor. Until then the honest
> advice is to declare `storage=` on the `#@ preaggregate` line and leave the base
> unpersisted.
