---
id: persisted-sql-select-colocated
tags: serve-correctness, sql-select, needs-attention
package: sc
---

# A persisted `sql_select` is built but never read on the COLOCATED path

Malloy persists two source types — `query_source` (`-> { … }`) and `sql_select`
(`conn.sql("…")`) — but its compile-time substitution only rewrites a
`query_source` to read its table. `getStructSourceSQL`'s `case 'sql_select'`
re-inlines the SQL with no manifest lookup, so a persisted `sql_select` queried
directly is recomputed and its table goes unread. Only a `%{ … }` interpolation of
it consults the manifest.

The colocated tier DOES ride that substitution — it binds a manifest and lets the
compiler rewrite the FROM — so the limitation applies in full: the build writes a
table and every query recomputes the SQL anyway. Confirmed here, and pinned so a
change (in malloy or in us) flags loudly.

The control for `persisted-sql-select-storage`, which serves the same source from
its table because it routes through `virtualMap` instead.

Stale after the source is mutated ⇒ served from the stored table. Fresh ⇒ the
source was recomputed and the built table is dead weight.

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

FRESH (1150), not the stored 150 — the built table was never read. The build cost was
paid and bought nothing.

```malloy
run: raw -> { aggregate: total is amount.sum() }
```

Expect:

| total |
| ----- |
| 1150  |

## Note (since=2026-07-24)

> A persisted `sql_select` on the colocated path builds a table that no query ever
> reads. This is malloy-core behavior, not a publisher choice: `getStructSourceSQL`'s
> `case 'query_source'` does a manifest lookup gated on `persistent`, while
> `case 'sql_select'` returns `(${getCompiledSQL(...)})` with no lookup at all — and
> `getCompiledSQL` returns the source's SQL verbatim when it has no `%{ }` segments.
> A persisted `sql_select` is only read through an interpolation of it.
>
> Two things follow. Upstream: mirroring the `query_source` lookup into
> `case 'sql_select'` looks small, and the persistence docs do not state the
> limitation — worth asking whether the asymmetry is deliberate. Publisher-side: we
> should not silently charge for a build nobody reads, so a colocated persisted
> `sql_select` deserves a package warning ("built but served live; wrap it in a
> query_source, or use storage="). Note the storage tier does NOT have this problem
> (`persisted-sql-select-storage` is green), so refusing `sql_select` at eligibility
> would be wrong — it works there.
