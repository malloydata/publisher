---
id: bad-manifest-location
tags: orchestration, resilience, needs-attention
package: bml
---

# An unreachable manifestLocation never errors the package

The orchestrator binds a `manifestLocation` the publisher cannot fetch (a
`file://` URI that does not exist). Binding must never error the package — the
query keeps succeeding. This scenario pins two DISTINCT behaviors of that failed
fetch, which differ by whether the package was already serving:

1. **On a live PATCH** (already serving from a prior build), the failed rebind is
   non-destructive: the publisher keeps serving the last-good snapshot rather
   than dropping to live. A transient manifest-store outage does not yank a
   governed package off its vouched data.
2. **On a reload** (a restart re-reads the persisted bad `manifestLocation`),
   there is no prior in-memory binding to keep, so the failed fetch degrades to
   serving live.

Either way the package serves — never a 5xx from a manifest the host can't reach.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.bml_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |

## Model bml.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.bml_orders')

#@ persist name="bml_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}
```

## Publish

expect binding: daily -> lake

## Query rollup

```malloy
run: daily -> { select: order_date, total_amount; order_by: order_date asc }
```

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 200          |

## Mutate orders_pg.bml_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 99           | 2026-01-01      | 1000       |

## Query rollup (again)

Stale ⇒ served from the storage snapshot (the publisher's local-store rebind).

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 200          |

## Bind bml (bad)

The orchestrator binds an unreachable manifestLocation while the package is
serving. The fetch fails; the failed rebind is non-destructive.

## Query rollup (again)

Still stale (`150`) ⇒ the failed PATCH-time rebind kept the last-good snapshot
rather than erroring or dropping to live.

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 200          |

## Restart

Reboot with the bad `manifestLocation` still persisted. On reload there is no
prior in-memory binding to keep, so the failed fetch degrades to serving live.

## Query rollup (again)

Live ⇒ reflects the mutation (`1150`), proving a reload with an unreachable
manifest serves live instead of erroring.

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 1150         |
| 2026-01-02 | 200          |

## Note (since=2026-07-23)

> A failed manifest fetch behaves differently by path: a live PATCH keeps the
> last-good snapshot (non-destructive), while a reload/restart serves live. Both
> avoid a 5xx, but the same manifest-store outage yields different DATA depending
> on whether a restart intervenes — and `bindManifest` logs "serving live" even
> on the PATCH path where it keeps the snapshot. Is keep-snapshot the intended
> contract on both paths, or should a failed rebind consistently go live? Decide
> the policy (and fix the log to match). Until then this scenario pins the
> CURRENT behavior so a change flags loudly.
