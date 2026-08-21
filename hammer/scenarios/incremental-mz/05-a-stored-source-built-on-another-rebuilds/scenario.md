---
id: a-stored-source-built-on-another-rebuilds
tags: build-control, incremental
package: ivc
---

# A stored source built on another stored source is rebuilt, not advanced

Stored sources can stack: a rollup can be built by reading the stored table its
parent already produced, rather than recomputing that parent from the warehouse.
That is cheaper and it is what makes a chain of stored sources affordable.

It also means the child cannot be advanced by a bounded delta. Its parent is
itself refreshed incrementally, and a parent's refresh can RESTATE rows anywhere
its own range reaches — including below wherever the child's own coverage has got
to. A child advancing past that point would never look at the restated rows again.

So a chained stored source declaring `refresh="incremental"` is rebuilt on every
refresh. The declaration is not rejected and not ignored: it is honoured as far as
it safely can be, and the rebuild is reported for what it is. What an author gets
is a child that always agrees with its parent's current table.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.ivc_orders

| order_id:int | batch:int | amount:num |
| ------------ | --------- | ---------- |
| 1            | 1         | 100        |
| 2            | 2         | 200        |

## Model ivc.malloy

Two stored sources, one reading the other.

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.ivc_orders')

#@ persist name="ivc_batches" storage=lake refresh="incremental" watermark="batch"
source: batch_totals is orders -> {
  group_by: batch
  aggregate: total_amount is amount.sum()
}

#@ persist name="ivc_doubled" storage=lake refresh="incremental" watermark="batch"
source: doubled is batch_totals -> {
  group_by: batch
  aggregate: doubled_total is total_amount.sum() * 2
}
```

## Publish

Build 1 seeds both: the parent from the warehouse, the child from the parent's
stored table.

expect binding: batch_totals -> lake
expect binding: doubled -> lake

## Query parent

```malloy
run: batch_totals -> { select: batch, total_amount; order_by: batch asc }
```

Expect:

| batch | total_amount |
| ----- | ------------ |
| 1     | 100          |
| 2     | 200          |

## Query child

```malloy
run: doubled -> { select: batch, doubled_total; order_by: batch asc }
```

Expect:

| batch | doubled_total |
| ----- | ------------- |
| 1     | 200           |
| 2     | 400           |

## Mutate orders_pg.ivc_orders

Batch 1 is restated — the case a chained child cannot advance through, since
batch 1 sits below any coverage the child has reached. A batch 3 arrives to move
the frontier.

```sql
UPDATE ivc_orders SET amount = 1000 WHERE order_id = 1;
INSERT INTO ivc_orders (order_id, batch, amount) VALUES (3, 3, 300);
```

## Publish (reseed)

Rebuild both from scratch, which is how an author asks for a restatement below the
watermark to be picked up.

## Query parent (again)

The parent now holds the restated batch 1.

Expect:

| batch | total_amount |
| ----- | ------------ |
| 1     | 1000         |
| 2     | 200          |
| 3     | 300          |

## Query child (again)

And the child agrees with it, row for row: it was rebuilt from the parent's
current table rather than advanced from its own coverage, so a restatement in the
parent reaches it.

Expect:

| batch | doubled_total |
| ----- | ------------- |
| 1     | 2000          |
| 2     | 400           |
| 3     | 600           |
