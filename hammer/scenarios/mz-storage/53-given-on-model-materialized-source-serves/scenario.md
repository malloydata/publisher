---
id: given-on-model-materialized-source-serves
tags: serve-correctness, givens
package: gx
---

# A model-level given must not break serving a given-free materialized source

When a model declares a `given:` and *also* has a materialized (`storage=`) source
that is itself given-free (hence eligible), a query on the materialized source that
carries the model-level given must still serve from storage.

The trap: the transient serve-shape model declares no `given:` of its own, so any
given forwarded to it fails Malloy's "unknown given" check — and a prepare/run throw
lands past the routing fallback, surfacing as a 400 instead of retrying live. The
serve path therefore suppresses givens on the shape path alongside the build
manifest. Clients routinely pass every model-level given, so this is the common case,
not an edge one.

The query below supplies the given and asserts it still serves the materialized
snapshot.

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

## Query materialized source with the model-level given supplied

`daily` is routed to the lake shape. The given is on the model's surface but the
materialized source does not reference it, so it must be dropped before the shape
rather than reaching it as an unknown name.

```malloy
run: daily -> { select: order_date, total; order_by: order_date asc }
```

givens: REGION=US

Expect:

| order_date | total |
| ---------- | ----- |
| 2026-01-01 | 150   |
| 2026-01-02 | 200   |
