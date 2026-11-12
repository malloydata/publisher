---
id: a-quoted-persist-name-is-refused-colocated
tags: naming, security
package: cq
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# An author-quoted `#@ persist name=` is refused when the model loads — COLOCATED

The colocated twin of `a-quoted-persist-name-is-refused`. With no `storage=`, the
source materializes into its own warehouse, and the colocated build would also derive
a staging name from the physical name, so an author-quoted name would need quote-aware
identifier handling on three names (staging, physical, the RENAME target). The name is
refused at load instead, with the same message, before any of that is reached. Decided
2026-11-11 with `a-quoted-persist-name-is-refused`; this scenario stood known-red while
the colocated write/read mismatch for a pre-quoted name was open.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.cq_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |

## Model anchor/anchor.malloy

```malloy
source: anchor is orders_pg.sql("SELECT 0 as n")
```

## Version cq@1.0.0 (refused)

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.cq_orders')

#@ persist name="\"Quoted Tbl\""
source: daily is orders -> {
  group_by: order_date
  aggregate: total is amount.sum()
}
```

cites: persist annotation name must be a plain identifier path
