---
id: preaggregate-storage-mode-off
tags: preaggregation, storage, mode
package: pm
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# With the storage tier off, a lake rollup is not built — and not built anywhere else

`PERSIST_STORAGE_MODE=off` is the tier's kill switch. For an authored
`#@ persist storage=`, off means the source serves live from its own warehouse: the
annotation is inert and nothing is written.

A rollup needs the same guarantee for a sharper reason. A rollup names no source in any
model file — its name is generated — so falling back to a colocated build would not
merely ignore an annotation, it would create a table in the customer's own warehouse
under a name nobody wrote and nobody can recognise. "Ignore the placement and build it
somewhere else" is the wrong reading of a kill switch here.

So: nothing is built, queries are answered from the base, and the answers stay correct.
The only thing lost is the acceleration, which is what a performance tier losing its
switch should cost.

## Publisher

- PERSIST_STORAGE_MODE: off

## Data orders_pg.pm_orders

| order_id:int | category:text | amount:num |
| ------------ | ------------- | ---------- |
| 1            | books         | 100        |
| 2            | books         | 50         |
| 3            | tools         | 200        |

## Model pm.malloy

```malloy
##! experimental { persistence composite_sources }

source: orders is orders_pg.table('public.pm_orders') extend {
  measure:
    #@ preaggregate grain="category" storage=lake
    total is amount.sum()
}
```

## Publish

## Warns pm

The degraded state is visible on `/status` rather than silent, and the warning names what
the author WROTE — the base source and the grain — rather than the rollup's generated
name, which appears in no file they can open. Asserted on the base name because that is
the part a reader can act on: a warning naming only a digest is one step from no warning.

This is also the only signal in the default configuration, since `off` is what every
deployment ships with. A first author of a `storage=` rollup reads this and nothing else.

cites: Measures of `orders` pre-aggregated at grain

## Query by category

Correct, and unaccelerated.

```malloy
run: orders -> { group_by: category; aggregate: total; order_by: category asc }
```

Expect:

| category | total |
| -------- | ----- |
| books    | 150   |
| tools    | 200   |

## Mutate orders_pg.pm_orders

| order_id:int | category:text | amount:num |
| ------------ | ------------- | ---------- |
| 99           | books         | 1000       |

## Query by category (again)

The proof that nothing was stored: with a rollup serving, this would still say `150`.
Every query is answered from the base, so it says `1150`.

Expect:

| category | total |
| -------- | ----- |
| books    | 1150  |
| tools    | 200   |
