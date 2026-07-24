---
id: extend-persist-materializes-nothing-new
tags: serve-correctness, build-control, needs-attention
package: xpn
---

# Persisting an `extend` of a persisted source materializes nothing new

Proof that there is no value in persisting a source that merely `extend`s another
persisted source. `daily_with_avg` extends `daily` to add an average that is
computed at query time — the extension's added fields (dimensions, measures, joins)
are query-time constructs, NOT materialized columns, so its persisted relation is
byte-identical to `daily`'s. The two are content-addressed identically (same
`sourceEntityId`), so even with DISTINCT persist names the publisher materializes a
SINGLE physical table and both serve from it — the second `#@ persist` is a no-op.

(Contrast `chained-persist` (11): a `->` DERIVED source — e.g. `monthly is daily ->
{ group_by: order_date.month }` — defines a NEW relation at a different grain and
genuinely materializes a different table. THAT has value. An `extend`, which keeps
the same grain and only adds fields, does not.)

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.xpn_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |

## Model xpn.malloy

Two persist sources with DISTINCT names (so the package is valid — no within-package
collision). `daily_with_avg` only `extend`s `daily` with a query-time field.

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.xpn_orders')

#@ persist name="daily_tbl" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate:
    total_amount is amount.sum()
    num_orders is count()
}

#@ persist name="daily_with_avg_tbl" storage=lake
source: daily_with_avg is daily extend {
  dimension: avg_order_value is total_amount / num_orders
}
```

## Publish

Both declare `#@ persist`, but they are content-identical, so only one physical
table is materialized (see the hook).

## Query daily

```malloy
run: daily -> { select: order_date, total_amount; order_by: order_date asc }
```

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 200          |

## Query daily_with_avg

`daily_with_avg` reads the SAME materialized relation as `daily` — the shared
columns are identical, and its `avg_order_value` is computed at query time (not a
stored column).

```malloy
run: daily_with_avg -> { select: order_date, total_amount, avg_order_value; order_by: order_date asc }
```

Expect:

| order_date | total_amount | avg_order_value |
| ---------- | ------------ | --------------- |
| 2026-01-01 | 150          | 75              |
| 2026-01-02 | 200          | 200             |

## Build targets

Both sources are targets, each declaring its OWN physical name — and they share one
content address (`entity` label `A`), which is why the build dedups them to a single
table below.

| source         | writes             | entity |
| -------------- | ------------------ | ------ |
| daily          | daily_tbl          | A      |
| daily_with_avg | daily_with_avg_tbl | A      |

## Connection lake (rows=1)

Exactly ONE of the two declared names is physically materialized — the other is
deduped away. A row count rather than a name, because which one wins depends on
build iteration order.

```sql
SELECT table_name FROM information_schema.tables
WHERE table_name IN ('daily_tbl','daily_with_avg_tbl')
```

## Note (since=2026-07-24)

> Surprising behavior worth a decision. Two persist sources declaring DISTINCT
> physical names (`daily_tbl`, `daily_with_avg_tbl`) silently collapse to a SINGLE
> physical table, and which of the two names wins is non-deterministic (build
> iteration order). The mechanism — they share a `sourceEntityId` (content address)
> and the build dedups on it — is invisible to the author, who reasonably expects
> two names to mean two tables.
>
> The deeper concern: the build's reuse/dedup keys on `sourceEntityId` ALONE. That
> is correct for the intended optimization — the SAME logical source across
> versions reusing its prior table when content is unchanged — but it also
> collapses TWO DIFFERENT sources that merely happen to share a content address,
> conflating their identity (name, freshness, access, GC lifecycle: dropping one's
> table drops the other's). Options to consider: (a) scope content-addressed reuse
> to the same declared name/destination, so a shared `sourceEntityId` across
> DIFFERENT names does not silently share a table; and/or (b) error (or warn) when
> two distinctly-named persist targets resolve to one `sourceEntityId` — an
> ambiguous "which name wins?" that is almost always a modeling mistake. This
> pins the current (silent-collapse) behavior so a change flags loudly.
>
> Related: whether a source should carry `#@ persist` on an `extend` at all — malloy
> PR 3012 stops an *inherited* persist from making the reader a build target; an
> *own* persist on an extend (this scenario) is still accepted but is redundant
> (identical relation to the base). See also the publisher's
> `detectDroppedPersistSources` hard-refuse, which will need to switch from the
> folded annotation to the own declaration when PR 3012 is adopted (otherwise it
> hard-errors on every extend-of-a-persist).
