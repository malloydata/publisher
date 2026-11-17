---
id: given-on-model-materialized-source-serves
tags: serve-correctness, givens
package: gx
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A model-level given must not break serving a given-free materialized source

When a model declares a `given:` and *also* has a materialized (`storage=`) source
that is itself given-free (hence eligible), a query on the materialized source that
carries the model-level given must still serve from storage.

The trap: a model a query is served from that did not declare the author's
`given:`s would fail Malloy's "unknown given" check for any given forwarded to it —
and a prepare/run throw lands past the routing fallback, surfacing as a 400 instead
of retrying live. The served copy is the author's own model text, so it declares the
same givens, and the query's givens are passed to it unchanged. Clients routinely
pass every model-level given, so this is the common case, not an edge one.

The query below supplies the given and asserts it still serves the materialized
snapshot.

## Publisher

- PUBLISHER_PERSIST_STORAGE_MODE: on

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

`daily` is routed to its lake table. The given is on the model's surface but the
materialized source does not reference it; the served copy declares it as the
author's model does, so it is accepted rather than refused as an unknown name.

```malloy
run: daily -> { select: order_date, total; order_by: order_date asc }
```

givens: REGION=US

Expect:

| order_date | total |
| ---------- | ----- |
| 2026-01-01 | 150   |
| 2026-01-02 | 200   |
