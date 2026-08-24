---
id: merge-key-replaces-a-restated-row
tags: build-control, incremental
package: ivm
---

# `merge_key=` replaces a row that comes back later, instead of duplicating it

A refresh without `merge_key=` replaces a RANGE of the watermark. That is correct
as long as a row's watermark value never changes — but a row that is restated with
a LATER value has already been materialized below the new range, so replacing the
range inserts the new version alongside the old one. Two rows, one thing.

`merge_key=` names what makes a row the same row. The refresh then matches on that
identity instead of on the range, so a restated row is UPDATED wherever it already
sits — even far below the range being refreshed.

This is the guarantee, on a stored source: identity wins over position.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.ivm_orders

Row-grained on purpose — a row has an identity (`order_id`) independent of the
batch it arrived in, which is exactly the situation `merge_key=` exists for.

| order_id:int | batch:int | amount:num |
| ------------ | --------- | ---------- |
| 1            | 1         | 100        |
| 2            | 1         | 50         |
| 3            | 2         | 200        |
| 4            | 2         | 25         |

## Model ivm.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.ivm_orders')

#@ persist name="ivm_rows" storage=lake refresh="incremental" watermark="batch" merge_key="order_id"
source: order_rows is orders -> {
  group_by: order_id, batch
  aggregate: amount_total is amount.sum()
}
```

## Publish

Build 1 seeds all four rows and records coverage through batch 2.

expect binding: order_rows -> lake

## Query rows

```malloy
run: order_rows -> { select: order_id, batch, amount_total; order_by: order_id asc }
```

Expect:

| order_id | batch | amount_total |
| -------- | ----- | ------------ |
| 1        | 1     | 100          |
| 2        | 1     | 50           |
| 3        | 2     | 200          |
| 4        | 2     | 25           |

## Mutate orders_pg.ivm_orders

A batch 3 arrives, which moves the frontier without adding anything the refresh
below will cover — the range is half-open, so the frontier batch waits.

| order_id:int | batch:int | amount:num |
| ------------ | --------- | ---------- |
| 5            | 3         | 500        |

## Publish

Build 2 refreshes batch 2 and advances coverage to batch 3. Order 5 is at the
frontier, so it is deliberately left for the next refresh.

## Query rows (again)

Expect:

| order_id | batch | amount_total |
| -------- | ----- | ------------ |
| 1        | 1     | 100          |
| 2        | 1     | 50           |
| 3        | 2     | 200          |
| 4        | 2     | 25           |

## Mutate orders_pg.ivm_orders

**Order 1 comes back.** It was materialized in batch 1 and is now restated into
batch 4 — the case this scenario exists for. A batch 5 arrives alongside it to
move the frontier past batch 4, so the restatement falls inside the next refresh's
range.

```sql
UPDATE ivm_orders SET batch = 4, amount = 111 WHERE order_id = 1;
INSERT INTO ivm_orders (order_id, batch, amount) VALUES (6, 5, 1);
```

## Publish

Build 3 refreshes batches 3 and 4 — the range `[3, 5)`. It contains order 5 (new)
and order 1 (restated).

## Query rows (again)

Order 1 appears **once**, at batch 4 with its new amount: matched on `order_id`
and updated where it already sat, not inserted beside its old self. Without
`merge_key=` the batch-1 version would still be here too, because a range replace
of `[3, 5)` never touches batch 1.

Order 6 is at the frontier, so it waits for the next refresh.

Expect:

| order_id | batch | amount_total |
| -------- | ----- | ------------ |
| 1        | 4     | 111          |
| 2        | 1     | 50           |
| 3        | 2     | 200          |
| 4        | 2     | 25           |
| 5        | 3     | 500          |
