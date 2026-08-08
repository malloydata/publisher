---
id: incremental-delta-advances
tags: build-control, incremental
package: iv
---

# An incremental refresh advances the table by a bounded delta

`refresh="incremental" watermark="…"` lets a refresh apply a bounded delta in
place instead of rebuilding the whole table. This is the end-to-end proof on
Postgres: seed, add upstream rows, refresh (a DELTA, not a rebuild), then ask for
a full rebuild with `reseed`.

Two things make the delta OBSERVABLE rather than something you have to take on
faith, and both are the point of the scenario:

- **A delta is exempt from skip-if-unchanged.** `sourceEntityId` is a hash of the
  SQL, which does not move when data does — so a plain refresh of an unchanged
  model would ordinarily reuse the table (see `skip-unchanged-and-force-refresh`).
  An incremental source is exempt: its `covered_through` boundary, not its content
  address, decides whether there is work.
- **A delta and a rebuild leave DIFFERENT rows.** A rebuild recomputes the whole
  source, so it materializes every batch. A delta covers `[covered_through, frontier)`
  — half-open, so the frontier batch is deliberately left out, on the grounds that
  a batch at the frontier may still be arriving. So "batch 3 is absent while batch
  2 was restated" is only possible if a delta ran, and "batch 3 is present" is only
  possible if a rebuild did.

The watermark is `batch`, a number, on purpose: a `date` watermark takes its range
end from the run's own clock, so two refreshes within the same day produce an
empty range and skip — correct, but nothing a test can watch advance.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.iv_orders

Batches 1 and 2, as an upstream that loads in batches would deliver them.

| order_id:int | batch:int | amount:num |
| ------------ | --------- | ---------- |
| 1            | 1         | 100        |
| 2            | 1         | 50         |
| 3            | 2         | 200        |
| 4            | 2         | 25         |

## Model iv.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.iv_orders')

#@ persist name="iv_batches" refresh="incremental" watermark="batch"
source: batch_totals is orders -> {
  group_by: batch
  aggregate:
    order_count is count()
    total_amount is amount.sum()
}
```

## Publish

Build 1 SEEDS: no boundary is recorded yet, so this is the ordinary full build,
and it records `covered_through = 2` (the frontier of what it just materialized).

## SQL seeded table

The materialized table holds both batches.

```sql
SELECT batch, order_count, total_amount FROM iv_batches ORDER BY batch;
```

Expect:

| batch | order_count | total_amount |
| ----- | ----------- | ------------ |
| 1     | 2           | 150          |
| 2     | 2           | 225          |

## Mutate orders_pg.iv_orders

Two changes at once, so the delta's range boundaries are both visible: batch 2 is
RESTATED (a late order lands in a batch already materialized) and batch 3 ARRIVES.

| order_id:int | batch:int | amount:num |
| ------------ | --------- | ---------- |
| 5            | 2         | 1000       |
| 6            | 3         | 500        |

## Publish

Build 2. The model is unchanged, so skip-if-unchanged would ordinarily carry the
table forward untouched — the incremental exemption is what gets this source to
the build at all. The range is `[2, 3)`: batch 2 is recomputed and replaced.

## SQL delta advanced the same table

Batch 2 now totals 1225 ⇒ the delta rewrote it. Batch 3 is ABSENT ⇒ this was a
delta and not a rebuild, since a rebuild recomputes the whole source and would
have brought batch 3 with it. Batch 1 is untouched, below the range.

```sql
SELECT batch, order_count, total_amount FROM iv_batches ORDER BY batch;
```

Expect:

| batch | order_count | total_amount |
| ----- | ----------- | ------------ |
| 1     | 2           | 150          |
| 2     | 3           | 1225         |

## Publish

Build 3 changes nothing upstream, so the frontier has not moved past
`covered_through = 3`. The refresh SKIPS — no delta, no rebuild — and the table
stands exactly as build 2 left it.

## SQL nothing to do left the table alone

```sql
SELECT batch, order_count, total_amount FROM iv_batches ORDER BY batch;
```

Expect:

| batch | order_count | total_amount |
| ----- | ----------- | ------------ |
| 1     | 2           | 150          |
| 2     | 3           | 1225         |

## Publish (reseed)

Build 4 with `reseed` — the escape hatch for a boundary or a table that is no
longer trusted. An incremental source takes the full CTAS rebuild and its ledger
row is reset, so the whole source is recomputed from scratch.

`reseed` and not `forceRefresh`: the latter only defeats skip-if-unchanged, which
an incremental source is exempt from anyway, so it would leave this build
advancing by delta like any other. Asking for a rebuild is a separate request.

## SQL a re-seed recomputes everything

Batch 3 appears ⇒ the whole source was recomputed rather than advanced.

```sql
SELECT batch, order_count, total_amount FROM iv_batches ORDER BY batch;
```

Expect:

| batch | order_count | total_amount |
| ----- | ----------- | ------------ |
| 1     | 2           | 150          |
| 2     | 3           | 1225         |
| 3     | 1           | 500          |

## Query batch rollup

The served query reads the rebuilt table, so it agrees with the SQL above.

```malloy
run: batch_totals -> { select: batch, total_amount; order_by: batch asc }
```

Expect:

| batch | total_amount |
| ----- | ------------ |
| 1     | 150          |
| 2     | 1225         |
| 3     | 500          |
