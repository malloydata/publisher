---
id: authorize-refused
tags: eligibility, security
package: az
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Eligibility: a #(access_filter)-gated source is refused

A `#(access_filter)` expression is a per-request *who-can-query* gate, evaluated at
query time against the caller's givens. This source's gate is the bare sentinel
`true`, which the gate grammar refuses on `#(access_filter)` at load
(`sentinel_in_access_filter`: "write `#(authorize) true`"), so the package never
loads and no build runs: the build is refused, citing the gate.

(A `storage=` source whose own gate is a proven row filter or lock is no longer
refused: it is built and served with the gate re-applied at read, as a colocated
one is (G3). What stays refused on this tier is a gate the source inherits as a
row filter, one reached through a join, and one whose build would substitute a
given — see the eligibility unit tests and
`g3_gates.component.spec.ts`.)

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.az_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-02      | 200        |

## Model az.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.az_orders')

#(access_filter) true
#@ persist name="az_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}
```

## Build refused

Materializing a `#(access_filter)`-gated source into storage is refused for safety.

cites: authorize
