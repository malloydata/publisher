---
id: quoted-persist-name-colocated
tags: serve-correctness, naming, known-red
package: cq
---

# An author-quoted `#@ persist name=` must round-trip — COLOCATED

**KNOWN-RED.** The colocated twin of `quoted-persist-name`, which is GREEN: the
storage tier's write sites now quote a physical name with the same rule the serve
side reads it with. The colocated path still quotes unconditionally on write while
`Package.quoteBoundTableNames` passes an already-quoted name through on read, so a
pre-quoted `name=` breaks the same way — here against the source warehouse:

```
400 relation "Quoted Tbl" does not exist
```

Not a silent fallback: the mismatch surfaces when the query runs, so every query on
the source fails. Fixing it needs one thing the storage path did not, which is why
it is deferred rather than folded in — see `## Note`.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.cq_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |

## Model cq.malloy

No `storage=`, so this materializes into the source warehouse (the v0 colocated
path) rather than the lake.

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.cq_orders')

#@ persist name="\"Quoted Tbl\""
source: daily is orders -> {
  group_by: order_date
  aggregate: total is amount.sum()
}
```

## Publish cq

## Query base

Should serve the materialized table. RED today: the query 400s because the built
table's name carries the author's quote characters while the serve substitution asks
for the unquoted-inner form.

```malloy
run: daily -> { select: order_date, total }
```

Expect:

| order_date | total |
| ---------- | ----- |
| 2026-01-01 | 150   |

## Note (since=2026-07-24)

> Same write/read quoting divergence the storage tier just fixed, on the colocated
> (v0) path, and proven to be a hard 400 rather than a degraded serve. Deferred
> because the colocated build additionally SYNTHESIZES a staging name by
> concatenation — `${physicalTableName}${stagingSuffix(...)}` — which appends the
> suffix OUTSIDE the author's closing quote (`"Quoted Tbl"_a1b2`), so neither
> quoting rule can render it correctly. Fixing the read/write mirror alone would
> leave the staging CREATE and the RENAME disagreeing. The shape of the fix is to
> derive the staging name from the identifier's INNER text (suffix inside the
> quotes) and then apply the manifest rule to all three of staging, physical, and
> the bare RENAME target — identifier surgery the codebase has so far avoided, so
> it wants a deliberate decision rather than a drive-by.
