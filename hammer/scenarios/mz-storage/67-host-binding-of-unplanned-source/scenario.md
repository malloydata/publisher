---
id: host-binding-of-unplanned-source
tags: security, orchestration
package: hbd
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A binding must not be honored for a source the build plan never planned

The sibling of `host-binding-honors-row-level-access`. That one covers a source the
build plan **knows about and refuses**: it is planned, so a refusal is recorded, and
the bind path consults it. This one covers the source the plan never contains at all.

Malloy only treats **query-shaped** sources as build roots. A filtered pass-through —
`X is <table> extend { where … }` — stays type `table`, so a `#@ persist` annotation on
it never produces a plan entry. The publisher notices and says so (the operator warning
asserted below), but nothing refuses it, because there is nothing to refuse.

**The rule: eligibility must be established positively.** A source that is absent from
the plan has not been found eligible; it has not been examined. Reading a missing
refusal as consent means the one shape Malloy silently drops is admitted — and that
shape is the row-level-access shape, so the source whose refusal matters most is the
one that arrives unexamined.

A host can name it without anyone being careless: it built the table under an older
compiler, or it names the source directly from its own records rather than from a plan
the publisher computed.

This scenario builds a legitimate given-FREE source into a real table holding every
region, then binds the given-FILTERED pass-through to that same table and queries it
with `REGION=US`. A live serve returns US only.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.hbd_orders

| order_id:int | region:text | amount:num |
| ------------ | ----------- | ---------- |
| 1            | US          | 100        |
| 2            | EU          | 200        |
| 3            | US          | 50         |

## Model hbd.malloy

```malloy
##! experimental.persistence
##! experimental.givens

given: REGION :: filter<string> is f'US'

source: base is orders_pg.table('public.hbd_orders')

// Eligible and query-shaped, so it is a build root and really materializes. Its
// columns match `scoped` below, which is what makes the forged binding compile
// rather than self-correct through the fallback ladder.
#@ persist name="hbd_all" storage=lake
source: all_rows is base -> {
  select: order_id, region, amount
}

// UNPLANNED. A filtered pass-through stays type `table`, so it is never a build
// root and this annotation produces no plan entry at all — not a refusal, an
// absence. Row-level filtered by a given, which is what makes the absence matter.
#@ persist name="hbd_scoped" storage=lake
source: scoped is base extend {
  where: region ~ $REGION
}
```

## Publish (sources=all_rows)

expect binding: all_rows -> lake

## Warns hbd

The publisher DOES notice the dropped annotation and surfaces it to an operator. It
is the bind path that never consults it.

cites: not recognized as a

## Build refusals

Nothing was refused — the premise, stated positively. `scoped` is not merely absent
from `sources`; it is absent from BOTH collections, because Malloy never handed it to
the plan as a persist source, so nothing ever examined it. That is the whole
difference from `host-binding-honors-row-level-access`, where the same forged binding
names a source the plan compiled and refused.

Expect:

| source |
| ------ |

## Query the eligible source

The real table, holding every region — correct for a source with no given in its
lineage, and the rows that leak if the forged binding is honored.

```malloy
run: all_rows -> { select: order_id, region, amount; order_by: order_id asc }
```

Expect:

| order_id | region | amount |
| -------- | ------ | ------ |
| 1        | US     | 100    |
| 2        | EU     | 200    |
| 3        | US     | 50     |

## Manifest hbd

The host authors a manifest vouching for `scoped` — the source the plan never saw —
at the real `hbd_all` table, and binds it. `(unplanned)` says the premise out loud:
the plan has no entry for this source, so the step uses the source name as the
handle, exactly as a host naming the source directly would.

- scoped -> hbd_all @ lake (unplanned)

## Query the given-filtered source

`scoped` is filtered to `$REGION`, so a live serve returns US only.

If the binding is honored, the query is answered from `hbd_all` — every region — and
the given's filter is gone for every caller.

```malloy
run: scoped -> { select: order_id, region, amount; order_by: order_id asc }
```

givens: REGION=US

Expect:

| order_id | region | amount |
| -------- | ------ | ------ |
| 1        | US     | 100    |
| 3        | US     | 50     |
