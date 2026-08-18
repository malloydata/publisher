---
id: preaggregate-serves-from-the-rollup
tags: serve-correctness, preaggregation
package: pa
---

# A pre-aggregated measure is answered from the rollup table, not recomputed

`#@ preaggregate` synthesizes a rollup as its own `#@ persist` source and rewrites the
base into `compose(rollup, base)`. This pins the half that is invisible in the rows: a
query at the rollup's grain must be answered **from the stored table**, not recomputed.
Both return identical numbers — that is the point of the feature, and also why a
regression is silent. A serve that quietly recomputed would still be correct, and would
simply cost the scan the rollup exists to avoid.

The proof is staleness: once the rollup is built, changing the underlying rows and
asking again must return the OLD answer. Only a stored table can do that.

**Not covered here: placement.** Which schema the rollup is created in is pinned by
`preaggregation_synthesis.spec.ts`, because no dialect hammer runs against requires
qualification — DuckDB and Postgres both accept an unqualified name into their default
schema, so a scenario here cannot fail when placement regresses. BigQuery is where it
bites, and covering it needs a writable dataset this harness deliberately does not
have.

## Publisher

- PERSIST_STORAGE_MODE: on

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

#@ persist name="pa_orders_tbl"
source: orders is pa_orders -> {
  select: order_id, category, region, amount
} extend {
  measure:
    #@ preaggregate grain="category"
    total is amount.sum()
}
```

## Publish

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

**The serving assertion.** That row is in the source table now, so a recompute would
say 1150 for `books`. Only a read of the stored rollup can still say 150.

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
