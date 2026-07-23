---
id: storage-source-equivalence
tags: serve-correctness, migration
package: ss
---

# `storage=source` is the in-warehouse path (equivalent to no `storage=`)

The connection name `source` is reserved: `#@ persist ... storage=source` is NOT a
storage-tier materialization — it means the unchanged in-warehouse path (path C),
byte-for-byte the same as declaring `#@ persist` with no `storage=` at all. The
source materializes into its OWN warehouse and is served by same-connection
table-name substitution; it never lands in a lake destination and produces no
cross-connection serve binding.

This pins that equivalence: `daily` declares `storage=source`, so a build
materializes it in-warehouse and the serve routes to that snapshot — proven by a
mutation wedged after the build being **invisible** (the routed snapshot, not a
live recompute). Contrast `freshness-window-in-warehouse`, which drives the same
path via a plain `#@ persist`; the behavior here is identical.

(A connection literally *named* `source` is rejected at server startup as a
reserved name — that's config-load validation, out of this per-scenario harness's
reach; it's covered by the connection-config unit tests.)

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.ss_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |

## Model ss.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.ss_orders')

#@ persist name="ss_daily" storage=source
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}
```

## Publish

`storage=source` builds path C (in-warehouse) — no lake binding is produced.

## Query daily

```malloy
run: daily -> { select: order_date, total_amount; order_by: order_date asc }
```

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 200          |

## Mutate orders_pg.ss_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 99           | 2026-01-01      | 1000       |

## Query daily (again)

Still `150` ⇒ served from the in-warehouse materialized snapshot (the mutation is
invisible), exactly as a plain `#@ persist` behaves. `storage=source` did not
route to a lake table and did not serve live.

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 200          |
