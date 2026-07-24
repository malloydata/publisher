---
id: skip-unchanged-and-force-refresh
tags: build-control
package: sk
---

# Skip-if-unchanged carries the table; forceRefresh rebuilds it

The build is content-addressed: a source's `sourceEntityId` is a hash of its SQL
and connection, NOT its data. So rebuilding a package whose model didn't change
REUSES the existing table (skip-if-unchanged) — even if the underlying source
rows changed. `forceRefresh` (`--refresh`) overrides that and rebuilds.

This is easy to see through the snapshot: mutate the source, rebuild unchanged →
the query still shows the OLD numbers (the table was carried, not recomputed);
rebuild with forceRefresh → the query shows the NEW numbers.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.sk_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |
| 4            | 2026-01-02      | 25         |

## Model sk.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.sk_orders')

#@ persist name="sk_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}
```

## Publish

Build 1 captures the initial source.

expect binding: daily -> lake

## Query rollup

```malloy
run: daily -> { select: order_date, total_amount; order_by: order_date asc }
```

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 225          |

## Mutate orders_pg.sk_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 99           | 2026-01-01      | 1000       |

## Publish

Build 2 — the model (hence `sourceEntityId`) is unchanged, so skip-if-unchanged
carries the existing table forward. The changed source rows are NOT recomputed.

## Query rollup (again)

Still the old numbers ⇒ build 2 reused the table rather than rebuilding it.

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 225          |

## Publish (forceRefresh)

Build 3 with forceRefresh — rebuild even though nothing changed.

## Query rollup (again)

Now the new numbers ⇒ forceRefresh recomputed the table against the mutated
source (2026-01-01 jumps to 1150).

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 1150         |
| 2026-01-02 | 225          |
