---
id: a-query-term-over-materialized-sources-still-scopes
tags: serve-correctness, givens, security
package: qts
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A caller's term on an extension of a materialized source still scopes what it reads

`visible_orders` extends `orders_all`, joins `vis_all`, and adds
`where: vis.user_id = $USER_ID` of its own. Both sources it reads are
materialized, each once for every tenant. The extension inherits `#@ persist`
and its build is `orders_all`'s relation — the join and the `where:` are not in
it — so it writes no second table and is served from `orders_all`'s, with its
own join and term re-applied per caller.

Two things make that hold. The user term is stripped from the build and
re-applied at read like any extend-block `where:`. And the join is to a source
that is itself persisted into storage and scoped by a given, so the serve shape
re-emits it only against `vis_all`'s own binding, which re-applies `vis_all`'s
org term with the caller's value. A join to a given-scoped source that is NOT
materialized that way is still refused (`dynamic_joined_where`): nothing could
bind it per caller.

The shape must also accept the same givens the author's model does. One that
declared only the terms its own sources stripped would fail to compile a query
mentioning `$USER_ID` and serve the whole thing live.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.qts_orders

| order_id:int | org_id:int | list_id:int | amount:num |
| ------------ | ---------- | ----------- | ---------- |
| 1            | 1          | 10          | 100        |
| 2            | 1          | 20          | 200        |
| 3            | 2          | 30          | 400        |

## Data orders_pg.qts_visibility

Who may see which list. User 7 sees list 10 only; user 8 sees both of org 1's.

| org_id:int | user_id:int | list_id:int |
| ---------- | ----------- | ----------- |
| 1          | 7           | 10          |
| 1          | 8           | 10          |
| 1          | 8           | 20          |
| 2          | 9           | 30          |

## Model qts.malloy

```malloy
##! experimental.persistence
##! experimental.givens

given:
  ORG_ID  :: number is 1
  USER_ID :: number is 7

source: raw_orders is orders_pg.sql('SELECT order_id, org_id, list_id, amount FROM public.qts_orders')
source: raw_vis is orders_pg.sql('SELECT org_id, user_id, list_id FROM public.qts_visibility')

#@ persist name="qts_orders_all" storage=lake partition="org_id"
source: orders_all is raw_orders -> { select: * } extend {
  where: org_id = $ORG_ID
}

#@ persist name="qts_vis_all" storage=lake partition="org_id"
source: vis_all is raw_vis -> { select: * } extend {
  where: org_id = $ORG_ID
}

source: visible_orders is orders_all extend {
  join_many: vis is vis_all on vis.list_id = list_id
  where: vis.user_id = $USER_ID
}
```

## Publish

expect binding: orders_all -> lake

## Query what user 7 may see

One list of org 1, so 100.

```malloy
run: visible_orders -> { aggregate: total is amount.sum() }
```

givens: ORG_ID=1; USER_ID=7
servedFrom: storage

Expect:

| total:num |
| --------- |
| 100       |

## Query what user 8 may see

Both of org 1's lists, so 300. The same two artifacts, a different caller.

```malloy
run: visible_orders -> { aggregate: total is amount.sum() }
```

givens: ORG_ID=1; USER_ID=8

Expect:

| total:num |
| --------- |
| 300       |

## Query a caller of another org

User 9 sees org 2's list. 400 — and never org 1's rows, which the org term
excludes before the user term is reached.

```malloy
run: visible_orders -> { aggregate: total is amount.sum() }
```

givens: ORG_ID=2; USER_ID=9

Expect:

| total:num |
| --------- |
| 400       |

## Query a user asking outside their org

User 7 belongs to org 1. Asked for org 2, the org term admits only org 2's rows
and the user term matches none of them — so the answer is a sum over nothing,
and in particular neither 100 (org 1's, which the org term excluded) nor 400
(org 2's, which the user term did).

```malloy
run: visible_orders -> { aggregate: total is amount.sum() }
```

givens: ORG_ID=2; USER_ID=7

Expect:

| total:num |
| --------- |
| 0         |

## Mutate orders_pg.qts_orders

`servedFrom` above already names the tier, and this corroborates it without
relying on that one field: change the warehouse underneath, and an answer that
moves came from the warehouse. Order 1 doubles.

```sql
UPDATE qts_orders SET amount = 200 WHERE order_id = 1;
```

## Query what user 7 may see (again)

Still 100. A live recompute would answer 200, so this is the artifact — the
frozen rows, with the caller's org and user terms applied over them.

```malloy
run: visible_orders -> { aggregate: total is amount.sum() }
```

givens: ORG_ID=1; USER_ID=7

Expect:

| total:num |
| --------- |
| 100       |
