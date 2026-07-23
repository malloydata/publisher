---
id: rebuild-on-model-change
tags: build-control
package: rmc
---

# A model change gives the persist source a new content address

Editing a persist source's SQL changes its content address (`sourceEntityId`) —
the skip-if-unchanged key. `skip-unchanged-and-force-refresh` (22) covers the
"unchanged ⇒ reuse" half; this covers the "changed ⇒ new address" half: after
editing the rollup (adding an aggregate), the recompiled source reports a
DIFFERENT `sourceEntityId`, and the rebuilt table serves the new shape.

The flow is markdown; two tiny hooks capture and compare the internal
`sourceEntityId` (the one thing markdown can't express — there are no variables).
`## Restart (init)` re-copies the edited model on reboot.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.rmc_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |

## Model rmc.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.rmc_orders')

#@ persist name="rmc_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}
```

## Publish rmc

Build v1.

expect binding: daily -> lake

## Hook captureEid1

Stash v1's `sourceEntityId` in shared state.

## Query base

```malloy
run: daily -> { select: order_date, total_amount; order_by: order_date asc }
```

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 200          |

## Model rmc.malloy

Edit the persisted SQL — add an aggregate. This changes the content address.

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.rmc_orders')

#@ persist name="rmc_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate:
    total_amount is amount.sum()
    order_count is count()
}
```

## Restart (init)

Reboot with `--init` so the publisher re-reads the edited model from disk.

## Hook assertEidChanged

The recompiled source reports a DIFFERENT `sourceEntityId` (not skip-if-unchanged).

## Publish rmc

Rebuild into the new content address.

## Query v2 shape

The new `order_count` column serves from storage.

```malloy
run: daily -> { select: order_date, total_amount, order_count; order_by: order_date asc }
```

Expect:

| order_date | total_amount | order_count |
| ---------- | ------------ | ----------- |
| 2026-01-01 | 150          | 2           |
| 2026-01-02 | 200          | 1           |
