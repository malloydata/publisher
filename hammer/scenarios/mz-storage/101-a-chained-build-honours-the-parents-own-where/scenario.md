---
id: a-chained-build-honours-the-parents-own-where
tags: orchestration, chained, serve-correctness
package: pow
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A chained build honours the parent's own `where:`

A persist source's build SQL is the persisted relation alone: an extend-block
`where:` on the source is not in its table and refines the relation when it
is read — which is why the serve shape re-emits it. `daily` is declared with
`extend { where: total_amount > 100 }`, so its stored table holds every day,
and reading `daily` means reading the filtered rows.

A chained build rebinds `daily`'s table as the parent, and must re-declare
that `where:` on the binding — otherwise `rollup` (directly over `daily`) and
`rollup_wide` (over it through an intermediate) sum unfiltered rows, and the
stored answer is wrong in a way nothing reports. The right answer is 350:
2026-01-01 (150) and 2026-01-02 (200); 2026-01-03 (50) is excluded.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.pow_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |
| 4            | 2026-01-03      | 50         |

## Model pow.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.pow_orders')

#@ persist name="pow_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
} extend {
  where: total_amount > 100
}

source: daily_wide is daily -> { select: * }

#@ persist name="pow_rollup" storage=lake
source: rollup is daily -> {
  aggregate: grand_total is total_amount.sum()
}

#@ persist name="pow_rollup_wide" storage=lake
source: rollup_wide is daily_wide -> {
  aggregate: grand_total is total_amount.sum()
}
```

## Publish

expect binding: daily -> lake
expect binding: rollup -> lake
expect binding: rollup_wide -> lake
expect upstreams: rollup -> reused
expect upstreams: rollup_wide -> reused

## Query daily

The parent, read through its own filter.

```malloy
run: daily -> { aggregate: total is total_amount.sum() }
```

Expect:

| total:num |
| --------- |
| 350       |

## Query rollup

```malloy
run: rollup -> { select: grand_total }
```

Expect:

| grand_total:num |
| --------------- |
| 350             |

## Query rollup_wide

```malloy
run: rollup_wide -> { select: grand_total }
```

Expect:

| grand_total:num |
| --------------- |
| 350             |

## Mutate orders_pg.pow_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 99           | 2026-01-03      | 1000       |

## Build (orchestrated, strict, pkg=pow)

Both rollups alone, over the stored `daily`.

- rollup -> pow_rollup__g2 @ lake (reused)
- rollup_wide -> pow_rollup_wide__g2 @ lake (reused)
  reference: daily

## Bind pow

## Query rollup (again)

Still 350: the stored `daily` rows, filtered.

Expect:

| grand_total:num |
| --------------- |
| 350             |

## Query rollup_wide (again)

Expect:

| grand_total:num |
| --------------- |
| 350             |
