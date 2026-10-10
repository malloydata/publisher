---
id: given-the-build-would-bake-is-refused
tags: eligibility, security, givens
package: gbb
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A given the BUILD substitutes is refused

A given inside the persisted query is read while the relation is being built, and
the only value available then is the declaration's default. So the artifact holds
one caller's slice, and the read path — which swaps the `FROM` and nothing else —
serves it to everyone. There is no term left to re-apply, so this cannot be made
safe at read and is refused instead.

The refusal is placed where the danger is, not one step wider. The same given in
the source's extend block is left out of the build and applied per caller
(`tenant-scoped-source-serves-per-caller`), so the safe form is one move away
from the refused one and the message says which move.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.gbb_orders

| order_id:int | org_id:int | amount:num |
| ------------ | ---------- | ---------- |
| 1            | 1          | 100        |
| 2            | 2          | 60         |

## Model gbb.malloy

A default is required for this shape to compile at all — without one the build
fails with `Given 'ORG_ID' has no value and no default`. That is what makes the
default the thing that gets baked.

```malloy
##! experimental.persistence
##! experimental.givens

given:
  ORG_ID :: number is 1

source: raw is orders_pg.sql('SELECT order_id, org_id, amount FROM public.gbb_orders')

#@ persist name="gbb_orders" storage=lake
source: orders is raw -> { where: org_id = $ORG_ID; select: * }
```

## Build refused

cites: substituted at build time

## Build refusals

Expect:

| source | tier    | reason                   |
| ------ | ------- | ------------------------ |
| orders | storage | given_in_persisted_query |
