---
id: off-must-not-build-into-the-warehouse
tags: orchestration, kill-switch, security
package: osw
---

# `off` must refuse an instructed destination, not redirect the write into the warehouse

`kill-switch-storage-warns` and `mode-matrix` cover what `off` does to SERVING: the
`storage=` annotation is ignored and the source is served live. Neither builds
while the switch is down, and that is the gap — because for a BUILD, "ignore the
annotation" is not a safe default the way it is for a read.

Ignoring `storage=` on a read means serving the same rows from the warehouse they
already came from. Ignoring it on a write means CREATING A TABLE in the warehouse
the model said this data may not be written to. `storage=` is a statement about
where data may NOT go, so a mode that cannot honor it has to decline the write —
the one outcome it must never produce is the data landing in the source
warehouse anyway.

The orchestrated path makes this sharp. A host does not merely pass the
annotation through; it hands the publisher an explicit destination in the
instruction and a physical name to use. Writing that name somewhere other than
the instructed destination is not a degraded build, it is a build of a different
thing, silently — and the host cannot detect it from a success response, because
the response reports success.

Stepping the mode down is a rollback, so it must stay safe on a loaded package:
declining is safe, and redirecting the write is not.

## Publisher

Kill switch fully off — destinations cannot be honored.

- PERSIST_STORAGE_MODE: off

## Data orders_pg.osw_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |

## Model osw.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.osw_orders')

#@ persist name="osw_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}
```

## Build refused (orchestrated, pkg=osw)

The instruction names `lake`. The publisher cannot write there in this mode, so
the build must fail rather than substitute the connection.

- daily -> osw_daily__g1 @ lake

## SQL the instructed name is absent from the warehouse

The assertion that actually matters, and the one no build-status check can
stand in for. Whatever the build reported, the instructed physical name must
not exist in the source warehouse.

```sql
SELECT count(*)::int AS leaked
FROM information_schema.tables
WHERE table_schema = 'public' AND table_name = 'osw_daily__g1';
```

Expect:

| leaked:int |
| ---------- |
| 0          |

## Query daily

The package is unharmed by the refusal — it still serves live, which is what
`off` means for a read.

```malloy
run: daily -> { select: order_date, total_amount; order_by: order_date asc }
```

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 200          |
