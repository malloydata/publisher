---
id: a-carried-intermediate-compiles-under-its-files-flags
tags: serve-correctness, chained
package: cfl
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# An intermediate carried into a chained build compiles under the `##!` flags of the file that declares it

A declaration is written under its file's flags. `daily_public` uses
`include {}`, which `access_modifiers` enables, and the author's file enables
it. The model a chained build compiles is one the build assembles, so the flags
it carries are a choice — and one that carried only its own two would refuse
exactly the declarations the author's file accepts, failing the build with
`Experimental flag 'access_modifiers' is not set` on a source that compiles
fine in place.

So the rule: a chained build compiles the sources it carries under the flags of
the files they come from. `rollup` reads `daily` through `daily_public`, and it
is built from `daily`'s stored table.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.cfl_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |
| 4            | 2026-01-02      | 25         |

## Model cfl.malloy

```malloy
##! experimental { persistence, access_modifiers }

source: orders is orders_pg.table('public.cfl_orders')

#@ persist name="cfl_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
  aggregate: order_count is count()
}

source: daily_public is daily -> { select: * } include {
  public:
    order_date
    total_amount
}

#@ persist name="cfl_rollup" storage=lake
source: rollup is daily_public -> {
  aggregate: grand_total is total_amount.sum()
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
| 375             |

## Mutate orders_pg.cfl_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 99           | 2026-01-03      | 1000       |

## Build (orchestrated, strict, pkg=cfl)

- rollup -> cfl_rollup__g2 @ lake (reused)
  reference: daily

## Bind cfl

## Query rollup (again)

Stale ⇒ computed from `daily`'s stored rows, through the carried `include {}`.

Expect:

| grand_total:num |
| --------------- |
| 375             |
