---
id: opt-out-persist-recomputes
tags: serve-correctness, freshness, eligibility
package: nop
---

# `#@ -persist` opts a source out of reading the pre-built table

The persistence experiment documents an opt-out annotation:

> `#@ -persist` — Opt out: recomputes the query instead of using the pre-built
> table

Nothing in the publisher tests it, on either tier, and nothing in the storage
serve path could honor it: serve bindings are keyed by source NAME and re-declare
the persisted base as a virtual source, so every reader of that base picks up the
stored table whether or not it asked to opt out. Whether that is wrong depends on
what the annotation is supposed to do, and the two available accounts disagree —
the documentation says the query is recomputed, while malloy's own
`findPersistentDependencies` flattens a non-persistent source so its persistent
dependencies "bubble up," which reads as the parent's table still being
substituted underneath.

The documentation's account is the one that holds, and the annotation is doing real
work rather than being a no-op: a PLAIN extension of the same source, probed the
same way, reads the stored snapshot (measured, but not pinned here — a plain
extension also trips the duplicate-build-target bug in
`extended-source-inherits-persist`, which would make this scenario red for an
unrelated reason).

`daily` is persisted into the lake; `daily_optout` extends it under `#@ -persist`.
After the build, the live source is mutated, so a STALE answer means the reader was
served from the stored snapshot and a FRESH one means it was recomputed.

Two things are asserted separately, because the annotation could plausibly affect
one and not the other:

1. `daily_optout` is not a build target (it must not materialize a second table).
2. Whether querying `daily_optout` returns stale or fresh rows.

Related: `extended-source-inherits-persist` pins that a PLAIN extension of a
persisted source wrongly becomes a duplicate build target (malloy PR 3012). If
`#@ -persist` suppresses that, it is the documented workaround for it — which is
why assertion 1 is worth having on its own. A plain extension is deliberately
absent here so that known-red does not confound this scenario.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.nop_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |

## Model nop.malloy

`daily_optout` reads `daily` and adds a computed field — the shape the
documentation's own example uses — but opts out of the pre-built table.

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.nop_orders')

#@ persist name="nop_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total is amount.sum()
}

#@ -persist
source: daily_optout is daily extend {
  dimension: doubled is total * 2
}
```

## Publish nop

expect binding: daily -> lake

## Hook assertOptOutIsNotABuildTarget

`daily_optout` must not appear as a build target, and only `nop_daily` may be
written.

## Query base

```malloy
run: daily -> { select: order_date, total; order_by: order_date asc }
```

Expect:

| order_date | total |
| ---------- | ----- |
| 2026-01-01 | 150   |
| 2026-01-02 | 200   |

## Mutate orders_pg.nop_orders

Diverge the live source from the stored snapshot, so stale-vs-fresh distinguishes
the two serving paths.

```sql
INSERT INTO nop_orders VALUES (4, '2026-01-01', 1000);
```

## Query base (again)

The base still serves the stored snapshot — confirms storage routing is active
for this package at the moment the opt-out is probed.

Expect:

| order_date | total |
| ---------- | ----- |
| 2026-01-01 | 150   |
| 2026-01-02 | 200   |

## Hook probeOptOutFreshness

Query `daily_optout` and record whether it recomputed (1150) or read the stored
snapshot (150).
