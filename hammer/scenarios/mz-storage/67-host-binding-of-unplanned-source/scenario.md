---
id: host-binding-of-unplanned-source
tags: security, orchestration
package: hbd
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A source the build plan would never plan fails the load, so no binding can reach it

The sibling of `host-binding-honors-row-level-access`. That one covers a source the
build plan **knows about and refuses**: it is planned, so a refusal is recorded, and
the bind path consults it. This one covers the source the plan would never contain
at all.

Malloy only treats **query-shaped** sources as build roots. A filtered pass-through —
`X is <table> extend { where … }` — stays type `table`, so a `#@ persist` annotation on
it never produces a plan entry: not a refusal, an absence.

**The rule: eligibility must be established positively.** A source that is absent from
the plan has not been found eligible; it has not been examined. Reading a missing
refusal as consent would admit the one shape Malloy silently drops — and that shape
is the row-level-access shape, so the source whose refusal matters most is the one
that would arrive unexamined. A host could name it without anyone being careless: it
built the table under an older compiler, or it names the source directly from its own
records rather than from a plan the publisher computed, and binds the given-filtered
source to a table holding every region.

The publisher closes that before any binding exists. A `#@ persist` on a shape Malloy
does not plan is an invalid annotation (`persist-not-materializable`), so the publish
and the load fail on every path. The package is never served, so there is no loaded
source for a forged binding to name and nothing past the given is ever answered.
The bind path's own gate against a binding for a source the plan never contained is
pinned in `package.spec.ts` (`bindStorageServeBindings eligibility gate`).

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

// Eligible and query-shaped, so it would be a build root holding every region:
// the table a forged binding for `scoped` would point at.
#@ persist name="hbd_all" storage=lake
source: all_rows is base -> {
  select: order_id, region, amount
}

// A filtered pass-through stays type `table`, so it is never a build root and
// this annotation could produce no plan entry at all. Row-level filtered by a
// given, which is what makes the absence matter.
#@ persist name="hbd_scoped" storage=lake
source: scoped is base extend {
  where: region ~ $REGION
}
```

## Rejected hbd

The configured package does not load, so neither source is served and no
binding can be made for `scoped`.

## Build refused hbd

A build cannot start on a package that does not load; the failure names the
shape and the fix.

cites: is not a source Malloy can materialize

## Build refused hbd

Under the load-failure code.

cites: [persist-not-materializable]
