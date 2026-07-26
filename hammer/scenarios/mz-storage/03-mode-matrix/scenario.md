---
id: mode-matrix
tags: config, kill-switch
package: b1
---

# Mode matrix (the kill switch)

One `storage=lake` package, three modes. `PERSIST_STORAGE_MODE` is a server-level
setting fixed at startup, so each mode is a **separate publisher process** — the
scenario restarts the publisher (a `## Publisher` section) to change it, rather
than toggling it per request. Each mode is decided by a mutate-then-undo probe:

- `on` — built into lake AND served routed (stale after a source mutation)
- `write-only` — built into lake but served LIVE (mutation is visible)
- `off` — `storage=` ignored; served live from the warehouse (mutation visible)

## Data orders_pg.b1_orders

| order_id:int | order_date:date | region:text | amount:num |
| ------------ | --------------- | ----------- | ---------- |
| 1            | 2026-01-01      | US          | 100        |
| 2            | 2026-01-01      | US          | 50         |
| 3            | 2026-01-02      | EU          | 200        |
| 4            | 2026-01-02      | US          | 25         |

## Model b1.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.b1_orders')

#@ persist name="b1_daily" storage=lake
source: daily_orders is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}
```

## Publisher

Start the publisher with the tier fully on (build + routed serve).

- PERSIST_STORAGE_MODE: on

## Publish

## Query rollup

```malloy
run: daily_orders -> { select: order_date, total_amount; order_by: order_date asc }
```

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 225          |

## Mutate orders_pg.b1_orders

| order_id:int | order_date:date | region:text | amount:num |
| ------------ | --------------- | ----------- | ---------- |
| 99           | 2026-01-01      | US          | 1000       |

## Query rollup (again)

`on` routes to storage — the mutation is invisible (stale).

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 225          |

## Mutate orders_pg.b1_orders

```sql
DELETE FROM b1_orders WHERE order_id = 99;
```

## Publisher

Restart the publisher in write-only mode — it materializes into the lake but
does NOT route reads to the table.

- PERSIST_STORAGE_MODE: write-only

## Publish

## Mutate orders_pg.b1_orders

| order_id:int | order_date:date | region:text | amount:num |
| ------------ | --------------- | ----------- | ---------- |
| 99           | 2026-01-01      | US          | 1000       |

## Query rollup (again)

`write-only` built into lake but serves LIVE — the mutation IS visible (1150).

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 1150         |
| 2026-01-02 | 225          |

## Mutate orders_pg.b1_orders

```sql
DELETE FROM b1_orders WHERE order_id = 99;
```

## Publisher

Restart with the kill switch off — `storage=` is ignored entirely.

- PERSIST_STORAGE_MODE: off

## Query rollup (again)

`off` ignores `storage=` and serves live from the warehouse.

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 225          |
