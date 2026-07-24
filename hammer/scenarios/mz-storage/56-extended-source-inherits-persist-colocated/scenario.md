---
id: extended-source-inherits-persist-colocated
tags: serve-correctness, safety, known-red
package: esc
---

# Extending a persisted source publishes and serves — but inherits its `#@ persist` (COLOCATED)

The colocated (in-warehouse, no `storage=`) twin of `extended-source-inherits-persist`.
Same malloy-core bug (fix: malloydata/malloy PR 3012): a source that EXTENDS a
persisted source inherits its `#@ persist name=`, so the extended reader becomes a
SECOND build target writing the same table — here, colocated in the source
warehouse. Per the docs, extending a persisted source should READ its table, not
re-materialize.

The user flow: author `daily` (`#@ persist`, colocated), then `daily_with_avg` extending
it to add a derived field. Publish, materialize, and query both — that works here,
because `daily` and `daily_with_avg` share a `sourceEntityId` and the publisher's
auto-run dedups the duplicate (both read the one colocated table). But the build
plan still lists `daily_with_avg` as a second target writing the same warehouse table,
which collides when a host materializes per-source, and the collision guard misses
it (it dedups by `sourceEntityId`, which these share).

The hook surfaces the root cause: exactly ONE build target for `esc_daily`. RED
today; GREEN once malloy PR 3012 lands.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.esc_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |

## Model esc.malloy

A plain colocated `#@ persist` (no `storage=`).

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.esc_orders')

#@ persist name="esc_daily"
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

Materialize the package colocated (into the source warehouse).

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

```malloy
run: daily_with_avg -> { select: order_date, total_amount, avg_order_value; order_by: order_date asc }
```

Expect:

| order_date | total_amount | avg_order_value |
| ---------- | ------------ | --------------- |
| 2026-01-01 | 150          | 75              |
| 2026-01-02 | 200          | 200             |

## Hook assertNoDuplicateInheritedTarget

The build plan must have exactly ONE build target for `esc_daily` (the base
`daily`). RED today: `daily_with_avg` inherits `#@ persist` and appears as a second
colocated target writing the same warehouse table.

## Note (since=2026-07-24)

> Colocated twin of `extended-source-inherits-persist`. Same upstream fix
> (malloydata/malloy PR 3012 — `persistDeclared` from the source's own annotation
> only) and same publisher-side blind spot (`persistenceCollisionWarnings` dedups
> by `sourceEntityId`, which the base and extended reader share). Here the duplicate
> target writes the customer's own warehouse, so a per-source host materialize would
> issue two CTAS to the same warehouse table.
