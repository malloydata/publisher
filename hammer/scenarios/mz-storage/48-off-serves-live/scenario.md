---
id: off-serves-live
tags: config, kill-switch, needs-attention
package: obp
---

# Kill switch off: a storage= source serves LIVE, never a colocated build

When `PERSIST_STORAGE_MODE=off`, a source that declares `storage=` is NOT
materialized at all — the build skips it and it serves **live** from its own
warehouse. It must NOT fall back to a colocated build: a colocated build writes a
CTAS into the source's own warehouse, which the author did not intend (they asked
for external storage, and production grants this server read-only warehouse
access), so it could fail or land in an unexpected schema. Falling back to live is
the safe default — the tier being off never mutates the customer warehouse.

(A plain `#@ persist` with NO `storage=` is unaffected: it still builds colocated,
because that is its author's intent — the v0 path, ungated by the storage kill
switch. This scenario is specifically about a `storage=`-declaring source.)

The proof is decisive by construction. Publish a `storage=lake` source while off,
then mutate and re-query:
- **served live (skipped, correct):** the query recomputes from the source
  warehouse ⇒ the mutation is visible ⇒ **fresh**.
- if it had wrongly built colocated: the frozen colocated table would serve the
  pre-mutation snapshot ⇒ **stale**.

So a **fresh** re-query proves nothing was materialized into the warehouse.

## Publisher

- PERSIST_STORAGE_MODE: off

## Data orders_pg.obp_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |

## Model obp.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.obp_orders')

#@ persist name="obp_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}
```

## Publish

`storage=` is disabled while off, and the source is NOT downgraded to a colocated
build — the build skips it. No table is written to the warehouse; no binding is
produced.

## Query base

```malloy
run: daily -> { select: order_date, total_amount; order_by: order_date asc }
```

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 200          |

## Mutate orders_pg.obp_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 99           | 2026-01-01      | 1000       |

## Query base (again)

Fresh `1150` ⇒ served LIVE from the source warehouse — nothing was materialized.
The kill switch did NOT write a colocated CTAS into the warehouse.

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 1150         |
| 2026-01-02 | 200          |

## Note (since=2026-07-24)

> Settled (2026-07-24): off ⇒ a `storage=` source serves LIVE, never a colocated
> build — the safe default (the tier being off must not mutate the customer
> warehouse). STILL OPEN: whether off should additionally REFUSE such a source at
> publish (a loud error) rather than silently serving it live with only the
> `storageWarnings()` operator warning. Serving live is the current behavior; the
> error-vs-warn choice is deferred.
