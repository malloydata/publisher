---
id: a-downstream-of-a-partitioned-upstream-stacks-on-its-table
tags: orchestration, chained, build-control, serve-correctness, partition
package: ppu
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A downstream of a partitioned upstream stacks on its table

`orders_all` is stored in `lake` with a `partition="org_id"` layout; `rollup`
is a `storage=` source over it. A partition layout is part of the address the
publisher files the entry under and no part of the key the compiler looks a
table up by, so the compiler alone never substitutes a partitioned table into
a downstream's SQL. Whether `rollup` is built from `orders_all`'s table is
therefore decided by the build naming its upstream from the model — and it
must be, or every downstream of a partitioned source is recomputed from the
warehouse (refused outright under `strictUpstreams`) while its entry reads as
if nothing were amiss.

The proof is the usual one: after the warehouse changes, a strict rebuild of
`rollup` alone that reuses `orders_all` answers from the stored table — stale —
and says `reused`.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.ppu_orders

| order_id:int | org_id:int | amount:num |
| ------------ | ---------- | ---------- |
| 1            | 1          | 100        |
| 2            | 1          | 200        |
| 3            | 2          | 400        |
| 4            | 3          | 15         |

## Model ppu.malloy

```malloy
##! experimental.persistence

source: raw is orders_pg.sql('SELECT order_id, org_id, amount FROM public.ppu_orders')

#@ persist name="ppu_orders" storage=lake partition="org_id"
source: orders_all is raw -> { select: * }

#@ persist name="ppu_rollup" storage=lake
source: rollup is orders_all -> {
  group_by: org_id
  aggregate: total is amount.sum()
}
```

## Publish

Both land in the lake, and `rollup` is built from `orders_all`'s table.

expect binding: orders_all -> lake
expect binding: rollup -> lake
expect upstreams: rollup -> reused

## Operator lake

The layout is applied: one directory per org.

```sql
SELECT DISTINCT regexp_extract(data_file, 'org_id=([0-9]+)', 1) AS org_dir
FROM ducklake_list_files('lake', 'ppu_orders')
ORDER BY org_dir;
```

Expect:

| org_dir:text |
| ------------ |
| 1            |
| 2            |
| 3            |

## Query rollup

```malloy
run: rollup -> { select: org_id, total; order_by: org_id asc }
```

Expect:

| org_id:int | total:num |
| ---------- | --------- |
| 1          | 300       |
| 2          | 400       |
| 3          | 15        |

## Mutate orders_pg.ppu_orders

| order_id:int | org_id:int | amount:num |
| ------------ | ---------- | ---------- |
| 99           | 1          | 1000       |

## Build (orchestrated, strict, pkg=ppu)

Strict: `rollup` alone, reusing the partitioned `orders_all`. Succeeds, over
the table.

- rollup -> ppu_rollup__g2 @ lake (reused)
  reference: orders_all

## Bind ppu

## Query rollup (again)

Org 1 still totals 300: the rows came from `orders_all`'s stored table, not
from the warehouse the new order landed in.

Expect:

| org_id:int | total:num |
| ---------- | --------- |
| 1          | 300       |
| 2          | 400       |
| 3          | 15        |
