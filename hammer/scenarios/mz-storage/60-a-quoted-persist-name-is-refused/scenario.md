---
id: a-quoted-persist-name-is-refused
tags: naming, security
package: qn
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# An author-quoted `#@ persist name=` is refused when the model loads

A `#@ persist name=` value is a plain identifier path: dot-separated segments of
letters, digits, `_` and `-`. Publisher quotes each segment for the target warehouse
itself, so case is preserved and a reserved word works without the author quoting
anything (`a-mixed-case-or-reserved-persist-name-needs-no-quoting`). A name carrying
its own quote characters is refused when the model loads, before anything is built:
the name is inlined into the build's `CREATE OR REPLACE TABLE` / `DROP TABLE` DDL, and
author quoting is exactly how a name could close the identifier and append statements
(#1106).

Malloy's tag parser honours the escapes, so `name="\"Quoted Tbl\""` reaches Publisher
as `"Quoted Tbl"` with literal quote characters. This is the `storage=` case;
`a-quoted-persist-name-is-refused-colocated` is the colocated one.

Decided 2026-11-11: author quoting is not supported. Parsing and escaping an author's
quoting per dialect would reopen the injection surface #1106 closed, and nothing needs
it, since Publisher already quotes. This scenario used to argue the opposite premise (a
quoted name must round-trip write-to-read) and stood known-red while that was open.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.qn_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |

## Model anchor/anchor.malloy

```malloy
source: anchor is orders_pg.sql("SELECT 0 as n")
```

## Version qn@1.0.0 (refused)

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.qn_orders')

#@ persist name="\"Quoted Tbl\"" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total is amount.sum()
}
```

cites: persist annotation name must be a plain identifier path
