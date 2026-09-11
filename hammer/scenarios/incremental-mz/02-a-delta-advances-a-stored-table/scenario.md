---
id: incremental-delta-advances-storage
tags: build-control, incremental
package: ivs
---

# An incremental refresh advances a stored table by a bounded delta

The storage counterpart of `incremental-delta-advances`. The declarations are the
same and the guarantees are the same, but almost nothing about the mechanism is:
the source warehouse computes the bounded range and DuckLake applies the DML, so
this scenario is the proof that the two halves meet.

Three things it establishes that the colocated scenario cannot:

- **The delta lands in the lake, not the warehouse.** Every assertion below reads
  through a served query, which for a `storage=` source resolves the stored table.
  A delta applied to the wrong engine would fail loudly; one applied to the right
  engine and never committed would read stale.
- **A committed delta is visible to the read-only serving attach**, with no
  re-attach and no package reload. The catalog is consulted per transaction rather
  than pinned when the attach was made — a property of the Postgres-backed catalog
  this harness and production both use.
- **A delta and a rebuild leave DIFFERENT rows**, which is what makes "a delta
  ran" observable rather than something to take on faith. The range is half-open,
  so the frontier batch is deliberately left out: batch 3 absent while batch 2 has
  been restated is only possible if a delta ran, and batch 3 present is only
  possible if a rebuild did.

The watermark is `batch`, a number, for the same reason as in the colocated
scenario: a `date` watermark takes its range end from the run's own clock, so two
refreshes within one day produce an empty range and skip — correct, but nothing a
test can watch advance.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.ivs_orders

Batches 1 and 2, as an upstream that loads in batches would deliver them.

| order_id:int | batch:int | amount:num |
| ------------ | --------- | ---------- |
| 1            | 1         | 100        |
| 2            | 1         | 50         |
| 3            | 2         | 200        |
| 4            | 2         | 25         |

## Model ivs.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.ivs_orders')

#@ persist name="ivs_batches" storage=lake refresh="incremental" watermark="batch"
source: batch_totals is orders -> {
  group_by: batch
  aggregate:
    order_count is count()
    total_amount is amount.sum()
}
```

## Publish

Build 1 SEEDS: no boundary is recorded yet, so this is the ordinary full build
into the lake, and it records `covered_through = 2` — the frontier of what it just
materialized, probed from the stored table.

expect binding: batch_totals -> lake

## Query batch rollup

Served from the stored table, which holds both batches.

```malloy
run: batch_totals -> { select: batch, order_count, total_amount; order_by: batch asc }
```

Expect:

| batch | order_count | total_amount |
| ----- | ----------- | ------------ |
| 1     | 2           | 150          |
| 2     | 2           | 225          |

## Mutate orders_pg.ivs_orders

Two changes at once, so both boundaries of the delta's range are visible: batch 2
is RESTATED (a late order lands in a batch already materialized) and batch 3
ARRIVES.

| order_id:int | batch:int | amount:num |
| ------------ | --------- | ---------- |
| 5            | 2         | 1000       |
| 6            | 3         | 500        |

## SQL raw source really changed

Prove the mutation landed in the warehouse rather than assuming it: this runs
against the source, which is where the delta's rows are computed.

```sql
SELECT batch, sum(amount) AS total FROM ivs_orders GROUP BY batch ORDER BY batch;
```

Expect:

| batch | total |
| ----- | ----- |
| 1     | 150   |
| 2     | 1225  |
| 3     | 500   |

## Publish

Build 2. The model is unchanged, so skip-if-unchanged would ordinarily carry the
table forward untouched — the incremental exemption is what gets this source to
the build at all. The range is `[2, 3)`: the warehouse computes batch 2 alone, and
DuckLake replaces that range in the stored table.

## Query batch rollup (again)

Batch 2 now totals 1225 ⇒ the delta rewrote it, in the lake, and the read-only
serving attach can see it. Batch 3 is ABSENT ⇒ this was a delta and not a rebuild.
Batch 1 is untouched, below the range.

Expect:

| batch | order_count | total_amount |
| ----- | ----------- | ------------ |
| 1     | 2           | 150          |
| 2     | 3           | 1225         |

## Publish

Build 3 changes nothing upstream, so the frontier has not moved past
`covered_through = 3`. The refresh SKIPS — no delta, no rebuild, and no warehouse
read beyond the frontier probe — and the table stands exactly as build 2 left it.

## Query batch rollup (again)

Expect:

| batch | order_count | total_amount |
| ----- | ----------- | ------------ |
| 1     | 2           | 150          |
| 2     | 3           | 1225         |

## Publish (reseed)

Build 4 with `reseed` — the escape hatch for a boundary or a table that is no
longer trusted. The stored table is replaced wholesale by a `CREATE OR REPLACE`,
which DuckLake commits atomically, and the boundary is re-derived from what that
write landed.

`reseed` and not `forceRefresh`: the latter only defeats skip-if-unchanged, which
an incremental source is exempt from anyway, so it would leave this build
advancing by delta like any other.

## Query batch rollup (again)

Batch 3 appears ⇒ the whole source was recomputed rather than advanced.

Expect:

| batch | order_count | total_amount |
| ----- | ----------- | ------------ |
| 1     | 2           | 150          |
| 2     | 3           | 1225         |
| 3     | 1           | 500          |
