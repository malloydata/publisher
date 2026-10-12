---
id: a-wrapper-with-access-modifiers-serves-from-storage
tags: serve-correctness, lift
package: wam
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A wrapper with access modifiers serves from storage

`daily_public` is the public face of the stored `daily`: a `-> { select: * }`
pass-through with an `include { public: … }` block, which compiles only under
the `access_modifiers` experiment its file enables. The serve path carries
`daily_public` over `daily`'s table as the author's text, so the shape it
compiles must enable what the author's file enables — or the lift fails,
every lift in the model is withheld with it, and every query on the public
surface runs live.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.wam_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |

## Model wam.malloy

```malloy
##! experimental { persistence, access_modifiers }

source: orders is orders_pg.table('public.wam_orders')

#@ persist name="wam_daily" storage=lake
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
```

## Publish

expect binding: daily -> lake

## Query the public wrapper

```malloy
run: daily_public -> { aggregate: grand_total is total_amount.sum() }
```

servedFrom: storage

Expect:

| grand_total:num |
| --------------- |
| 350             |

## Mutate orders_pg.wam_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 99           | 2026-01-03      | 1000       |

## Query the public wrapper (again)

Still 350: read from `daily`'s stored table.

```malloy
run: daily_public -> { aggregate: grand_total is total_amount.sum() }
```

servedFrom: storage

Expect:

| grand_total:num |
| --------------- |
| 350             |
