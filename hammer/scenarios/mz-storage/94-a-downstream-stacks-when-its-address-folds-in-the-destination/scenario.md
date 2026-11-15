---
id: a-downstream-stacks-when-its-address-folds-in-the-destination
tags: orchestration, chained, build-control, serve-correctness, addressing
package: ppu
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A downstream stacks on its upstream when the address folds in the destination

With `PUBLISHER_PERSIST_ADDRESS_INCLUDES_DESTINATION` on, a `storage=` source's
address also covers the destination it builds into, so the address the
publisher files the entry under is no longer the key the compiler looks a table
up by. `orders_all` is stored in `lake`; `rollup` is a `storage=` source over
it. The compiler alone never substitutes `orders_all`'s table into `rollup`'s
SQL under the address, so whether `rollup` is built from that table is decided
by the run carrying the entry under both keys (`manifestForCompiler`) and the
build naming its upstream from the model. It must be, or every downstream is
recomputed from the warehouse (refused outright under `strictUpstreams`) while
its entry reads as if nothing were amiss.

The proof is the usual one: after the warehouse changes, a strict rebuild of
`rollup` alone that reuses `orders_all` answers from the stored table — stale —
and says `reused`.

## Publisher

- PERSIST_STORAGE_MODE: on
- PUBLISHER_PERSIST_ADDRESS_INCLUDES_DESTINATION: true

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

#@ persist name="ppu_orders" storage=lake
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

Strict: `rollup` alone, reusing `orders_all`, whose address and compiler key
differ. Succeeds, over the table.

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
