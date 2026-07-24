---
id: given-on-model-materialized-source-serves
tags: serve-correctness, givens
package: gx
---

# A model-level given must not break serving a given-free materialized source

**Regression guard for code-review finding #3 (fixed).**

When a model declares a `given:` and *also* has a materialized (`storage=`) source
that is itself given-free (hence eligible), a query on the materialized source that
carries the model-level given must still serve from storage. It previously did not:
the storage serve path suppressed the build manifest on the shape runnable but still
forwarded `querySurfaceGivens`; the transient serve-shape model declares no `given:`,
so its prepare threw Malloy's "unknown given" — which lands past the routing
fallback and surfaced as a spurious 400 (no fall-back to live). The serve path now
suppresses givens on the shape path too, alongside the manifest (safe: the shape is
given-free, and the authorize check already ran against the full unfiltered givens
before routing).

A hook drives the query with the given supplied (markdown can't pass givens without a
new grammar handler) and asserts it serves the materialized snapshot.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.gx_orders

| order_id:int | order_date:date | region:text | amount:num |
| ------------ | --------------- | ----------- | ---------- |
| 1            | 2026-01-01      | US          | 100        |
| 2            | 2026-01-01      | US          | 50         |
| 3            | 2026-01-02      | EU          | 200        |

## Model gx.malloy

`REGION` is a model-level given used by the live `scoped` source, so it is on the
model surface. `daily` (the materialized source) does NOT reference it, so it stays
eligible and materializes into the lake.

```malloy
##! experimental.persistence
##! experimental.givens

given: REGION :: filter<string> is f''

source: orders is orders_pg.table('public.gx_orders')

source: scoped is orders extend {
  where: region ~ $REGION
}

#@ persist name="gx_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total is amount.sum()
}
```

## Publish

expect binding: daily -> lake

## Hook queryMaterializedWithGiven

Query `daily` (routed to the lake shape) with the model-level given `REGION` supplied.
It serves from storage; before the fix it 400'd with "unknown given" (finding #3).
