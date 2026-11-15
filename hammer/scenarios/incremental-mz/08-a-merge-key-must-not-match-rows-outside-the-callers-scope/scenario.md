---
id: a-merge-key-must-not-match-rows-outside-the-callers-scope
tags: build-control, incremental, security
package: xt
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A `merge_key=` must not match rows outside the caller's scope

An author chooses `merge_key=` against the source **as they wrote it** — filtered
to one caller. `order_id` unique within an org is a reasonable identity for a
per-tenant relation, and reads that way in the model.

The artifact is not that relation. A source's extend-block `where:` is not part of
what the build persists, so the stored table holds **every** tenant's rows and the
term is re-applied per caller at read. That is the design, and for reads it is
sound. It makes the author's key ambiguous over what was actually stored: one
`order_id` now occurs once per tenant.

An incremental refresh with `merge_key=` applies its delta as
`MERGE INTO <table> ON <the author's key>`. Matching on a key that is unique per
tenant, over a table that holds all of them, matches rows belonging to **other
tenants** — and updates them. This is a cross-tenant WRITE, not a read leak: one
tenant's refresh overwrites another tenant's stored rows.

The rule is that a refresh may only write rows the author's filter admits.

Colocated on purpose (`#@ persist` with no `storage=`). The colocated gate has
never refused a given reference, and incremental runs on colocated tables, so this
combination needs nothing from the storage tier to be reachable.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.xt_orders

`order_id` 1 belongs to org 1 AND to org 2 — distinct rows, distinct owners,
identical key. This is what "unique within an org" means once the org term is gone.

| order_id:int | org_id:int | batch:int | amount:num |
| ------------ | ---------- | --------- | ---------- |
| 1            | 1          | 1         | 100        |
| 1            | 2          | 1         | 900        |
| 2            | 1          | 1         | 50         |

## Model xt.malloy

```malloy
##! experimental.persistence
##! experimental.givens

given: ORG_ID :: number is 1

source: orders is orders_pg.table('public.xt_orders')

#@ persist name="xt_rows" refresh="incremental" watermark="batch" merge_key="order_id"
source: order_rows is orders -> {
  group_by: order_id, org_id, batch
  aggregate: amount_total is amount.sum()
} extend {
  where: org_id = $ORG_ID
}
```

## Publish

Build 1 seeds every tenant's rows and records coverage through batch 1.

## Query what org 2 owns

Org 2's single row, before any refresh has run. This is the value the refresh
below must not be able to touch.

```malloy
run: order_rows -> { select: order_id, amount_total }
```

givens: ORG_ID=2

Expect:

| order_id | amount_total |
| -------- | ------------ |
| 1        | 900          |

## Mutate orders_pg.xt_orders

**Org 1 restates ITS order 1**, and nothing about org 2 changes. A later batch
arrives alongside it so the restatement falls inside the next refresh's range.

```sql
UPDATE xt_orders SET batch = 2, amount = 111 WHERE order_id = 1 AND org_id = 1;
INSERT INTO xt_orders (order_id, org_id, batch, amount) VALUES (9, 1, 3, 1);
```

## Publish

Build 2 refreshes the range containing org 1's restated row, and applies it as a
MERGE on `order_id`.

## Query what org 2 owns (again)

**Unchanged: 900.** Org 2 was not restated, was not refreshed, and appears nowhere
in the delta. A different answer here means org 1's refresh matched org 2's row on
the shared `order_id` and wrote over it.

```malloy
run: order_rows -> { select: order_id, amount_total }
```

givens: ORG_ID=2

Expect:

| order_id | amount_total |
| -------- | ------------ |
| 1        | 900          |

## Query what org 1 owns

Org 1's own restatement lands normally — the rule is that the merge is scoped, not
that it stops working.

```malloy
run: order_rows -> { select: order_id, amount_total; order_by: order_id asc }
```

givens: ORG_ID=1

Expect:

| order_id | amount_total |
| -------- | ------------ |
| 1        | 111          |
| 2        | 50           |

## Note (since=2026-09-18)

> **What closes it.** The merge's MATCH is scoped by the columns the stripped
> terms constrain, so the effective identity is the author's key plus that scope
> and the author restates nothing. `merge_key=` keeps meaning what they wrote.
>
> Scoping is all-or-nothing. A stripped term that contributes no column of this
> source — one reaching through a join — refuses the source rather than scoping
> by the remaining terms, which would narrow the match without closing it.
>
> Before the fix this build FAILED rather than corrupting: Postgres raises
> `MERGE command cannot affect row a second time` on the ambiguous match. That
> guard is the target warehouse's and not ours, which is why the storage sibling
> exists — DuckDB has no such guard and corrupted both tenants silently.
