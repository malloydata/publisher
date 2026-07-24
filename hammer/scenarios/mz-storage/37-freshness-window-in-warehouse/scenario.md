---
id: freshness-window-in-warehouse
tags: serve-correctness, freshness
package: pcf
---

# Freshness window on the colocated serve path — ENFORCED

The companion to `freshness-window` (the `storage=` tier, which serves stale by
design + a demand trigger). This path is different: a plain `#@ persist name=`
source with **no `storage=`** materializes
into its own warehouse (colocated, the v0 path) and is served by substituting the
table name at compile time. That path DOES enforce freshness: a per-query resolver
(`Package.getFreshBuildManifest` → `evaluateManifestFreshness`) drops a stale entry
whose `fallback` is `live`, so the query recomputes live.

Same setup as scenario 36, only the persist has no `storage=` — and the outcome is
the opposite: the stale-past-window `fallback: live` entry serves **live** (`1150`),
not the snapshot.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.pcf_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |

## Model pcf.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.pcf_orders')

#@ persist name="pcf_daily"
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}
```

## Publish

colocated build — materialized into the Postgres source warehouse and
served by table-name substitution.

## Query base

```malloy
run: daily -> { select: order_date, total_amount; order_by: order_date asc }
```

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 200          |

## Mutate orders_pg.pcf_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 99           | 2026-01-01      | 1000       |

## Query base (again)

Un-gated (the auto-loaded manifest has no window) ⇒ still serves the snapshot.

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 200          |

## Bind pcf (asof=2000-01-01T00:00:00Z, fresh=1, fallback=live)

Stamp the colocated entry stale-past-window (`dataAsOf` = year 2000, 1s window) with
`fallback: live`.

## Query base (again)

Live ⇒ the freshness gate dropped the stale entry and the query recomputed from the
warehouse (`1150`). This is the enforced path — contrast with `freshness-window`
(storage), which stays stale.

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 1150         |
| 2026-01-02 | 200          |

> Freshness is ONE model across `storage=` placements — `storage=` is a location,
> orthogonal to freshness (decided + implemented 2026-07-23). This path and the lake
> path (`freshness-window`) now honor `{window, fallback}` via the same gate; the two
> scenarios are a matched pair proving it.
