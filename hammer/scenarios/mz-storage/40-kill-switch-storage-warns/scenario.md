---
id: kill-switch-storage-warns
tags: config, kill-switch
package: ksw
---

# Kill switch down: a storage= source is served live AND warns

`mode-matrix` proves the serve *behavior* across the kill switch; this pins the
complementary operator signal. When `PERSIST_STORAGE_MODE` is not `on`, a source
that declares `storage=` can't be routed to a materialized table — so the publisher
serves it live and surfaces a package **warning** on the status API saying so
(distinct message per rung). The warning is how an operator notices a package is
running degraded after the switch was moved down, without reading logs.

Same package, two down-rungs (each a separate publisher process, since the mode is
fixed at startup):

- `write-only` — built into the store, but the serve path is NOT routed → warns
  "served live".
- `off` — the annotation is ignored entirely → warns "ignored ... served live".

## Publisher (write-only)

Start with the kill switch at `write-only`.

- PERSIST_STORAGE_MODE: write-only

## Data orders_pg.ksw_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |

## Model ksw.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.ksw_orders')

#@ persist name="ksw_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}
```

## Warns ksw

Under `write-only` the storage= source warns that the serve path is not routed.

cites: write-only

## Query daily

It still serves — live from the warehouse (nothing routed).

```malloy
run: daily -> { select: order_date, total_amount; order_by: order_date asc }
```

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 200          |

## Publisher (off)

Restart with the kill switch fully `off`.

- PERSIST_STORAGE_MODE: off

## Warns ksw

Under `off` the storage= annotation is ignored and the source is served live —
surfaced as a distinct warning.

cites: ignored

## Query daily

Still served live, unchanged.

```malloy
run: daily -> { select: order_date, total_amount; order_by: order_date asc }
```

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 200          |
