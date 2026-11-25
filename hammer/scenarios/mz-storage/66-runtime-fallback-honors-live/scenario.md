---
id: runtime-fallback-honors-live
tags: serve-correctness, freshness, resilience
package: rtf
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A store that fails under a bound source degrades to live, not to an error

The freshness contract's `fallback` is honoured when a query pins the tables it
may read. It was not honoured at RUN time: once a query routed
to storage, a missing or unreadable table surfaced as an error even for a binding
that had explicitly declared `fallback=live`.

That makes the store a hard dependency the moment a query routes, for sources
whose author said the opposite. The window is ordinary rather than exotic — a
rebind that has not converged yet, or a GC that reclaimed a generation a replica
is still pointing at.

This builds a source, binds it with `fallback=live`, proves it serves from the
snapshot, then drops the physical table out-of-band and queries again. The answer
must come back live and correct, not as an error. A table that is gone answers
live whatever the binding's `freshnessFallback` says, `fail` included, so
rebinding with `fail` changes nothing.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.rtf_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |

## Model rtf.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.rtf_orders')

#@ persist name="rtf_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total is amount.sum()
}
```

## Publish

expect binding: daily -> lake

## Bind rtf (fallback=live)

The host vouches for the table and declares the tier optional for this source:
if it cannot be read, serving live is an acceptable answer.

## Query daily

Served from the materialized snapshot.

```malloy
run: daily -> { select: order_date, total }
```

Expect:

| order_date | total |
| ---------- | ----- |
| 2026-01-01 | 150   |

## Mutate orders_pg.rtf_orders

Append to the SOURCE only. The snapshot still holds 150, so a later 1150 proves
the answer was recomputed live rather than read from the table.

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 99           | 2026-01-01      | 1000       |

## Query daily (again)

Still routed to the snapshot, so still the stale 150 — the store is intact.

Expect:

| order_date | total |
| ---------- | ----- |
| 2026-01-01 | 150   |

## Operator lake

An operator drops the physical table out-of-band — a reclaimed generation a
replica is still bound to. The binding is untouched, so the next query still
routes to a table that is no longer there.

```sql
DROP TABLE IF EXISTS lake.rtf_daily;
```

## Query daily (again)

The routed query fails against the missing table. Because `fallback=live` declares
the tier optional for this source, it is recomputed live instead of erroring. 1150
rather than 150 proves the answer came from the warehouse, and the caller sees an
answer at all.

Expect:

| order_date | total |
| ---------- | ----- |
| 2026-01-01 | 1150  |

## Bind rtf (fallback=fail)

The other half. The same broken store, but the host now declares the tier
mandatory for this source.

## Query daily (again)

A bind reads every bound table against the column contract its build recorded,
and a table it cannot read is not served from; its sources answer live,
whatever their `freshnessFallback`. A table that is gone is therefore answered
live under `fail` too, rather than failing: 1150 is the correct live answer.

Expect:

| order_date | total |
| ---------- | ----- |
| 2026-01-01 | 1150  |
