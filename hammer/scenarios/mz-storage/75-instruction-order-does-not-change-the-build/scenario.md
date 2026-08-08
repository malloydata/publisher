---
id: instruction-order-does-not-change-the-build
tags: orchestration, chained, build-control
package: iob
---

# The order instructions arrive in must not change what a build produces

`strict-upstream-built-here` lists the upstream before its dependent, which is
the natural way to write it and therefore the way that hides a bug: if the build
walked the caller's list, that scenario would pass while a caller who happened to
list them the other way round got a strict miss on an upstream the same call was
about to build.

So this is the same build with the instructions REVERSED — `rollup` first,
`daily` second — and it must behave identically. Order is the caller's
presentation of a set, not a schedule.

Hosts vary in how they emit instructions — a map iteration, a database ordering,
a set — and none of them is expressing an intent about sequence. Whatever a build
does to satisfy dependencies is below the line a host can see, so the only thing
worth promising at this level is that it does not have to care.

Worth pinning now because it only recently became observable: while a chained
storage source could not be built under `strictUpstreams` at all, such a build
failed regardless of order, so nothing could tell a correct ordering apart from
one that never got far enough to matter.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.iob_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |

## Model iob.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.iob_orders')

#@ persist name="iob_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}

#@ persist name="iob_rollup" storage=lake
source: rollup is daily -> {
  aggregate: grand_total is total_amount.sum()
}
```

## Build (orchestrated, strict, pkg=iob)

The dependent is listed FIRST. `daily` must still be built before `rollup` reads
it, which is only true if the build orders by the graph.

- rollup -> iob_rollup__g1 @ lake
- daily -> iob_daily__g1 @ lake

## Bind iob

## Query daily

```malloy
run: daily -> { select: order_date, total_amount; order_by: order_date asc }
```

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 200          |

## Query rollup

```malloy
run: rollup -> { select: grand_total }
```

Expect:

| grand_total:num |
| --------------- |
| 350             |

## Mutate orders_pg.iob_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 99           | 2026-01-03      | 1000       |

## Query rollup (again)

Stale ⇒ the reversed list still produced a materialized `rollup`, not one
recomputed live.

Expect:

| grand_total:num |
| --------------- |
| 350             |
