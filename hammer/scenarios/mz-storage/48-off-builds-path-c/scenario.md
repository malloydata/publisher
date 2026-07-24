---
id: off-builds-path-c
tags: config, kill-switch, needs-attention
package: obp
---

# Kill switch off: a storage= source builds path-C (in-warehouse), not the lake

`mode-matrix` proves that `off` *serves* live; this pins the build side. When
`PERSIST_STORAGE_MODE=off`, the `storage=` annotation is fully ignored — the
source builds into its OWN warehouse (path C), exactly as a plain `#@ persist`
would, and never touches the lake. Off is the kill switch: byte-identical to the
feature not existing.

The proof is decisive by construction. Build a `storage=lake` source while off,
then mutate and re-query:
- **built path-C (correct):** the in-warehouse materialized table serves the
  pre-mutation snapshot ⇒ **stale**.
- if it had wrongly built into the lake: storage serve-routing is off AND there's
  no path-C table, so it would serve **live** (mutation visible).

So a stale re-query proves the build landed in-warehouse, not the lake.

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

`storage=` is ignored while off; `daily` builds into its own warehouse (path C).
No lake binding is produced.

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

Stale `150` ⇒ served from the in-warehouse (path-C) materialized snapshot. The
kill switch treated `storage=lake` exactly as a plain `#@ persist` — it built
in-warehouse, not into the lake, and did not serve live.

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 200          |

## Note (since=2026-07-23)

> Open question — is "off (or storage= ignored) ⇒ build path-C" actually what we
> want? Path C materializes by **writing a CTAS into the source warehouse**. In
> production, users grant this server **read-only** warehouse access — the whole
> point of the storage tier is to NOT write to their warehouse — so a path-C build
> would fail (and a partial attempt could land in an unexpected schema). We may
> prefer to **error with an explicit "this server will not write to your
> warehouse" stance** rather than silently attempt a path-C materialization that
> can't succeed. This scenario pins the CURRENT behavior (a storage= source builds
> path-C when the tier is off); revisit whether it should refuse instead — likely
> as a deployment policy, since a write-capable warehouse legitimately uses path-C
> today (the v0 in-warehouse path).
