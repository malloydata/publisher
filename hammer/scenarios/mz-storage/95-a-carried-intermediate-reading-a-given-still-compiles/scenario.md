---
id: a-carried-intermediate-reading-a-given-still-compiles
tags: orchestration, chained, build-control, serve-correctness, givens
package: cig
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A carried intermediate reading a given still compiles

`daily_wide` sits between the stored `daily` and the stored `rollup`, and one
of its dimensions reads a model `given:` — a per-request value the model
declares and the build never binds. `rollup` reads none of that dimension. The
chained build carries `daily_wide`'s text into the model it compiles over the
rebound parent, and that text names `$REGION`; a model that does not declare
`REGION` does not compile, and the build would report a shape it could not
carry — refused outright under `strictUpstreams` — for a source whose only
sin is standing next to a given.

So the chained build declares the author model's givens, as the serve shape
does, and `rollup` is built from `daily`'s table: `reused`, and stale after
the warehouse changes.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.cig_orders

| order_id:int | order_date:date | region:text | amount:num |
| ------------ | --------------- | ----------- | ---------- |
| 1            | 2026-01-01      | east        | 100        |
| 2            | 2026-01-01      | west        | 50         |
| 3            | 2026-01-02      | east        | 200        |

## Model cig.malloy

```malloy
##! experimental { persistence givens }

given: REGION :: string

source: orders is orders_pg.table('public.cig_orders')

#@ persist name="cig_daily" storage=lake
source: daily is orders -> {
  group_by: order_date, region
  aggregate: total is amount.sum()
}

source: daily_wide is daily -> { select: * } extend {
  dimension: is_focus is pick 'yes' when region = $REGION else 'no'
}

#@ persist name="cig_rollup" storage=lake
source: rollup is daily_wide -> {
  aggregate: grand_total is total.sum()
}
```

## Publish

expect binding: daily -> lake
expect binding: rollup -> lake
expect upstreams: rollup -> reused

## Query rollup

```malloy
run: rollup -> { select: grand_total }
```

Expect:

| grand_total:num |
| --------------- |
| 350             |

## Mutate orders_pg.cig_orders

| order_id:int | order_date:date | region:text | amount:num |
| ------------ | --------------- | ----------- | ---------- |
| 99           | 2026-01-03      | east        | 1000       |

## Build (orchestrated, strict, pkg=cig)

Strict: `rollup` alone over the stored `daily`, through the intermediate that
reads the given. Succeeds, from the table.

- rollup -> cig_rollup__g2 @ lake (reused)
  reference: daily

## Bind cig

## Query rollup (again)

Still 350: the rows came from `daily`'s stored table.

Expect:

| grand_total:num |
| --------------- |
| 350             |
