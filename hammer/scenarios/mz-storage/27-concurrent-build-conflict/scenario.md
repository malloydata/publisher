---
id: concurrent-build-conflict
tags: lifecycle, build-control
package: cbc
---

# Concurrent builds of one package: the second is rejected

At most one active materialization per package. While a build is in flight, a
second `POST /materializations` for the same package must be rejected with a
conflict (409, `MaterializationConflictError`) rather than racing it. We fire the
first build **async** (it returns at PENDING and keeps running), fire a second
before it finishes and assert the conflict, then `## Await` the first.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.cbc_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-02      | 25         |

## Model cbc.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.cbc_orders')

#@ persist name="cbc_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}
```

## Publish cbc (async, label=first)

Fire the first build and DON'T wait — it runs in the background at PENDING.

## Build refused cbc

A second build while the first is still active must be rejected with a conflict.

cites: already has an active materialization

## Await first

The first build still completes successfully.
