---
id: moving-a-source-to-storage-rebuilds-it
tags: build-control, incremental, migration
package: ivx
---

# Coverage belongs to a table, so moving a source to storage rebuilds it

A refresh's coverage is a fact about the table it was measured on: "this table
holds every row up to here". Adding `storage=` to a source does not move that
table — it starts a new one, in a different store.

So the first refresh after the move is a full rebuild, and that is the correct
answer rather than a missed optimisation. Advancing the new table from the old
one's coverage would skip everything the old table already held, silently.

Nothing an author writes changes between the two builds except the `storage=`
key: the source computes the same rows and keeps the same name.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.ivx_orders

| order_id:int | batch:int | amount:num |
| ------------ | --------- | ---------- |
| 1            | 1         | 100        |
| 2            | 2         | 200        |

## Model ivx.malloy

The starting model: an incremental source with no `storage=`, so it is
materialized into the source's own warehouse.

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.ivx_orders')

#@ persist name="ivx_batches" refresh="incremental" watermark="batch"
source: batch_totals is orders -> {
  group_by: batch
  aggregate: total_amount is amount.sum()
}
```

## Publish

Build 1 seeds the warehouse table and records coverage through batch 2.

## Mutate orders_pg.ivx_orders

A batch 3 arrives.

| order_id:int | batch:int | amount:num |
| ------------ | --------- | ---------- |
| 3            | 3         | 300        |

## Publish

Build 2 advances the warehouse table by a delta. The range is half-open, so
batch 3 sits at the frontier and waits — which is what makes the next assertion
able to tell a rebuild from an advance.

## Query batch rollup

Batch 3 is absent: the warehouse table has been advanced, not rebuilt.

```malloy
run: batch_totals -> { select: batch, total_amount; order_by: batch asc }
```

Expect:

| batch | total_amount |
| ----- | ------------ |
| 1     | 100          |
| 2     | 200          |

## Model ivx.malloy

The same source, now pointed at Credible-managed storage. One key added;
everything else — including the table's name — is untouched.

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.ivx_orders')

#@ persist name="ivx_batches" storage=lake refresh="incremental" watermark="batch"
source: batch_totals is orders -> {
  group_by: batch
  aggregate: total_amount is amount.sum()
}
```

## Restart (init)

Pick up the edited model.

## Publish

Build 3 is the first build after the move. There is no table in the destination
yet, and the coverage on record describes the warehouse table the source has just
left — so this is a full rebuild into the new store.

expect binding: batch_totals -> lake

## Query batch rollup (again)

Batch 3 is present. A rebuild recomputes the whole source, so the stored table
holds everything — including the batch the warehouse table was still waiting on.
Had the move carried the old coverage across, batch 3 would be the ONLY batch
here, and batches 1 and 2 would have been skipped for good.

Expect:

| batch | total_amount |
| ----- | ------------ |
| 1     | 100          |
| 2     | 200          |
| 3     | 300          |
