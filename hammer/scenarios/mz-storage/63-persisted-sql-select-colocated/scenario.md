---
id: persisted-sql-select-colocated
tags: serve-correctness, sql-select
package: sc
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A persisted `sql_select` is served from its built table — COLOCATED

Malloy persists two source types — `query_source` (`-> { … }`) and `sql_select`
(`conn.sql("…")`) — and compile-time substitution rewrites **both** to read the
table that was built for them. The colocated tier rides that substitution: it
binds a manifest and lets the compiler rewrite the FROM. So a persisted
`sql_select` queried directly is answered from its table, exactly as a persisted
`query_source` is, and the build it paid for is the thing that answers.

The sibling of `persisted-sql-select-storage`, which reaches the same answer by a
different route — the storage tier routes through `virtualMap` rather than the
compiler's manifest lookup. Two tiers, two source kinds, one rule.

Symmetry is the point, and it is asserted rather than assumed because the
substitution is per source kind: the manifest lookup lives in one `case` per type,
so a kind can be missing from it while everything else about persistence works.
The failure that shape produces is silent — the build writes a table, every query
recomputes the SQL anyway, and only a freshness probe like this one can tell.

Proven by mutating the warehouse behind the built source and re-querying. Stale ⇒
served from the stored table. Fresh ⇒ the source was recomputed and the built
table is dead weight.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.sc_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |

## Model sc.malloy

```malloy
##! experimental.persistence

#@ persist name="sc_raw"
source: raw is orders_pg.sql('SELECT order_date, amount FROM public.sc_orders')
```

## Publish sc



## Query rollup

```malloy
run: raw -> { aggregate: total is amount.sum() }
```

Expect:

| total |
| ----- |
| 150   |

## Mutate orders_pg.sc_orders

```sql
INSERT INTO sc_orders VALUES (3, '2026-01-01', 1000);
```

## Query rollup (again)

STALE (150), not the recomputed 1150 — the query read the built table and never
touched the mutated warehouse.

```malloy
run: raw -> { aggregate: total is amount.sum() }
```

Expect:

| total |
| ----- |
| 150   |
