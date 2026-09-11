---
id: runtime-fallback-reads-the-served-shape
tags: serve-correctness, orchestration, needs-attention
package: rfs
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A source's `freshnessFallback` must be decided by what is serving it

`runtime-fallback-honors-live` establishes the promise for one source: a store that
fails under a routed query degrades to live when the binding says `live`, and keeps
erroring when it says `fail`. This scenario is about **whose** declaration decides,
once a package has more than one binding.

A host stamps freshness per manifest entry, per generation, so a package with mixed
`freshnessFallback` values is the ordinary case rather than an edge one. The
declaration that governs a query must therefore be the one attached to what is
actually serving it.

**The rule: a binding that is not serving does not get a vote.** A sibling entry the
freshness gate already dropped — one past its window, which is therefore being served
live for that very reason — must not decide the policy for a source that IS serving
from its table.

Here `weekly` is stamped stale past its window with `fallback=fail`, so the gate
drops it and it serves live. `daily` is fresh with `fallback=live`. Its table is then
dropped out-of-band. `daily` says the tier is optional, and nothing serving alongside
it says otherwise, so the query is recomputed live.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.rfs_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |

## Model rfs.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.rfs_orders')

#@ persist name="rfs_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total is amount.sum()
}

#@ persist name="rfs_weekly" storage=lake
source: weekly is orders -> {
  group_by: order_date
  aggregate: orders_count is count()
}
```

## Publish

Both sources materialize, so both physical tables really exist and the manifest below
is honest about the tables — only the stamps differ.

expect binding: daily -> lake
expect binding: weekly -> lake

## Mutate orders_pg.rfs_orders

Append to the SOURCE only, so a later 1050 proves the answer was recomputed live.

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 99           | 2026-01-01      | 900        |

## Query daily

The stale 150 proves this is served from the stored table, not recomputed: the
warehouse now holds 1050 (100 + 50 + 900).

```malloy
run: daily -> { select: order_date, total }
```

Expect:

| order_date | total |
| ---------- | ----- |
| 2026-01-01 | 150   |

## Manifest rfs

The host stamps the two entries differently, as it would across generations:
`daily` fresh and optional, `weekly` long past its window and mandatory.

- daily -> rfs_daily @ lake (fallback=live)
- weekly -> rfs_weekly @ lake (fallback=fail, asof=2020-01-01T00:00:00Z, fresh=60)

## Query daily (again)

Still 150. `weekly` being stale changes nothing for `daily`, which is fresh and
serving from its own table.

Expect:

| order_date | total |
| ---------- | ----- |
| 2026-01-01 | 150   |

## Operator lake

An operator drops the table `daily` is bound to — a reclaimed generation the binding
has not caught up with.

```sql
DROP TABLE IF EXISTS lake.rfs_daily;
```

## Query daily (again)

The routed query fails against the missing table. `daily` declares the tier optional,
and the only other binding in the manifest is not serving from a table at all, so it
has no say. 1050 proves the answer came from the warehouse.

Expect:

| order_date | total |
| ---------- | ----- |
| 2026-01-01 | 1050  |

## Note (since=2026-07-27)

> Open question, deliberately not asserted here: the decision is scoped to the
> bindings that produced the SERVE SHAPE, which is per package, not per query. So
> two sources both serving from their tables, one `live` and one `fail`, are
> fail-closed together — a query touching only the `live` one still errors. That is
> the safe direction and it matches the stated intent ("one fail-closed source is not
> degraded by a permissive neighbour"), but it is coarser than per-source: narrowing
> further means knowing which sources a query actually references, which the serve
> path does not currently derive. Worth deciding whether per-query narrowing is
> wanted before any host starts mixing `fail` with `live` on sources that serve
> together.
