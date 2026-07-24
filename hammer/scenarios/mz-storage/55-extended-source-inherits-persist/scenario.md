---
id: extended-source-inherits-persist
tags: serve-correctness, safety, known-red
package: esi
---

# Extending a persisted source publishes and serves — but inherits its `#@ persist`

The user flow: author a persisted source `daily`, then a source `daily_with_avg` that
extends it to add a derived field, expecting `daily_with_avg` to READ `daily`'s
materialized table (per the docs), not re-materialize. Publish, materialize, and
query both — that all works here.

What's wrong is latent: malloy propagates `#@ persist` through `extend` (bug, fix:
malloydata/malloy PR 3012), so `daily_with_avg` inherits `name="esi_daily"` and the build
plan lists it as a SECOND target writing the same table. It happens to serve
correctly today only because `daily` and `daily_with_avg` are content-addressed
identically (same `sourceEntityId`), so the publisher's auto-run dedups the
duplicate and both read the one table. But that duplicate target is a landmine: a
host that materializes per-source (the orchestrated path) issues two builds to the
same physical table — a silent overwrite, or a collision — and the publisher's own
collision guard misses it (it dedups by `sourceEntityId`, which these share). Malloy
compiles it, so nothing fails at publish.

The `## Build targets` step surfaces the root cause: the plan must have exactly ONE
target for `esi_daily`. RED today (`daily_with_avg` inherits the tag); GREEN once
malloy PR 3012 lands and `daily_with_avg` reads `daily`'s table instead of becoming
a target.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.esi_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |

## Model esi.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.esi_orders')

#@ persist name="esi_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate:
    total_amount is amount.sum()
    num_orders is count()
}

source: daily_with_avg is daily extend {
  dimension: avg_order_value is total_amount / num_orders
}
```

## Publish

Materialize the package. (`daily_with_avg` should read `daily`'s table, not be a
second target — see `## Build targets` below.)

## Query daily

`daily` serves from its materialized table.

```malloy
run: daily -> { select: order_date, total_amount; order_by: order_date asc }
```

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 200          |

## Query daily_with_avg

`daily_with_avg` serves too — reading `daily`'s table and computing its average at
query time (not a stored column).

```malloy
run: daily_with_avg -> { select: order_date, total_amount, avg_order_value; order_by: order_date asc }
```

Expect:

| order_date | total_amount | avg_order_value |
| ---------- | ------------ | --------------- |
| 2026-01-01 | 150          | 75              |
| 2026-01-02 | 200          | 200             |

## Build targets

Only the base `daily` may write `esi_daily`. RED today: the extended reader
inherits `#@ persist` and becomes a second target for the same table (malloy PR
3012). GREEN once malloy keys persist on the source's OWN annotation.

Expect:

| source | writes    |
| ------ | --------- |
| daily  | esi_daily |

## Note (since=2026-07-24)

> Two things. (1) UPSTREAM: malloy propagates `#@ persist` through `extend`, so an
> extended reader becomes a duplicate build target (fix: malloydata/malloy PR 3012 — a
> `persistDeclared` flag from the source's OWN annotation only). This is the primary
> fix; the scenario goes green when we adopt a malloy carrying it. (2) OURS: the
> orchestrated/host path materializes per-source and issues two builds to one table
> (silent overwrite / collision), and `persistenceCollisionWarnings` misses it
> because it also dedups by `sourceEntityId`. Consider hardening the guard to flag
> two DISTINCT source names resolving to the same persist target even when their
> content address matches.
>
> (3) AUTO-RUN IS NOT SAFE EITHER — worse than this note first claimed. The base and
> the reader share a `sourceEntityId`, so the build dedups to one manifest entry, but
> that entry can be attributed to the READER: a probe with a mutated source observed
> the single storage binding land on the extension (`sourceName: daily_plain`,
> `tablePath: lake.<base target>`) while the declared persist source silently served
> LIVE. Which name wins is build-iteration order (same mechanism as
> `extend-persist-materializes-nothing-new`), so the persisted source may or may not
> keep its routing from run to run. Not pinned as an assertion here precisely because
> it is non-deterministic; the reason this scenario's own `## Query daily` step still
> passes is that it does not mutate the source, so stored and live values are
> identical and a lost binding is invisible. Measured on the storage tier; the
> colocated twin is unverified for this effect. This raises the stakes on adopting
> PR 3012: the failure is not only a duplicate build, it is the declared source
> quietly losing the materialization it asked for.
