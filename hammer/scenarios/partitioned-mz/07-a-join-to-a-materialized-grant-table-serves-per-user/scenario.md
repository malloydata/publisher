---
id: a-join-to-a-materialized-grant-table-serves-per-user
tags: serve-correctness, givens, security
package: pvj
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A join to a materialized grant table answers per user from one artifact

The visibility idiom: `opps` is scoped to the caller's org, and joins `grants`,
a table scoped to the caller's org AND user. A dimension decides what the caller
may see by null-checking the join. Both sources are materialized, each once for
every tenant.

Neither artifact is filtered by user. `opps`'s build is its relation alone — a
persisted source's joins and dimensions are not in its build — and `grants`
leaves its extend-block `where:` out like any other. At read, the serve shape
re-emits `grants` with its terms bound to the caller's values, and re-emits the
join on `opps` against that binding. So the `visible` dimension is evaluated
over exactly the grant rows the live query would join, and two users of the same
org get different answers from the same two tables.

The join is admitted because its target is itself a persisted, storage-bound
source this gate admits, joined by name. The gate still refuses the shapes the
serve side cannot reproduce: a given in the join's `on:`, a join through an
inline refinement, and a target that is not materialized into storage.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.pvj_opps

Opp 1 is unrestricted. Opps 2 and 3 are restricted in org 1; opp 4 is org 2's.

| opp_id:int | org_id:int | restricted:int | amount:num |
| ---------- | ---------- | -------------- | ---------- |
| 1          | 1          | 0              | 100        |
| 2          | 1          | 1              | 200        |
| 3          | 1          | 1              | 400        |
| 4          | 2          | 1              | 800        |

## Data orders_pg.pvj_grants

User 7 may see opp 2; user 8 may see opps 2 and 3; user 9 may see opp 4.

| org_id:int | user_id:int | opp_id:int |
| ---------- | ----------- | ---------- |
| 1          | 7           | 2          |
| 1          | 8           | 2          |
| 1          | 8           | 3          |
| 2          | 9           | 4          |

## Data orders_pg.pvj_accounts

| account_id:int | org_id:int |
| -------------- | ---------- |
| 1              | 1          |
| 2              | 1          |
| 3              | 2          |

## Model pvj.malloy

```malloy
##! experimental.persistence
##! experimental.givens

given:
  ORG_ID  :: number is 1
  USER_ID :: number is 7

source: raw_opps is orders_pg.sql('SELECT opp_id, org_id, restricted, amount FROM public.pvj_opps')
source: raw_grants is orders_pg.sql('SELECT org_id, user_id, opp_id FROM public.pvj_grants')
source: raw_accounts is orders_pg.sql('SELECT account_id, org_id FROM public.pvj_accounts')

#@ persist name="pvj_accounts" storage=lake partition="org_id"
source: accounts is raw_accounts -> { select: * } extend {
  where: org_id = $ORG_ID
  measure: account_count is count()
}

#@ persist name="pvj_grants" storage=lake partition="org_id"
source: grants is raw_grants -> { select: * } extend {
  where: org_id = $ORG_ID and user_id = $USER_ID
}

#@ persist name="pvj_opps" storage=lake partition="org_id"
source: opps is raw_opps -> { select: * } extend {
  where: org_id = $ORG_ID
  join_one: g is grants on opp_id = g.opp_id
  dimension: visible is restricted = 0 or g.opp_id is not null
  measure:
    visible_amount is amount.sum() { where: visible }
    hidden is count() { where: not visible }
    total_amount is amount.sum()
}
```

## Publish

expect binding: opps -> lake
expect binding: grants -> lake
expect binding: accounts -> lake

## Operator lake

One table per source, holding every org's and every user's rows.

```sql
SELECT (SELECT count(*) FROM lake.pvj_opps) AS opps, (SELECT count(*) FROM lake.pvj_grants) AS grants;
```

Expect:

| opps:int | grants:int |
| -------- | ---------- |
| 4        | 4          |

## SQL what each user may see

The rule, stated in the source warehouse: an opp is visible when unrestricted or
granted to the caller.

```sql
SELECT u.org_id, u.user_id, sum(o.amount) AS visible_amount
FROM (SELECT DISTINCT org_id, user_id FROM pvj_grants) u
JOIN pvj_opps o ON o.org_id = u.org_id
WHERE o.restricted = 0
   OR EXISTS (SELECT 1 FROM pvj_grants g WHERE g.org_id = u.org_id AND g.user_id = u.user_id AND g.opp_id = o.opp_id)
GROUP BY u.org_id, u.user_id ORDER BY u.org_id, u.user_id;
```

Expect:

| org_id:int | user_id:int | visible_amount:num |
| ---------- | ----------- | ------------------ |
| 1          | 7           | 300                |
| 1          | 8           | 700                |
| 2          | 9           | 800                |

## Query user 7 sees

Opp 1 and the granted opp 2. One of org 1's opps stays hidden.

```malloy
run: opps -> { aggregate: visible_amount, hidden }
```

givens: ORG_ID=1; USER_ID=7
servedFrom: storage

Expect:

| visible_amount:num | hidden:int |
| ------------------ | ---------- |
| 300                | 1          |

## Query user 8 sees

The same org, the same two artifacts, a different user: nothing hidden. 300 here
would mean the grant table's user term was not re-applied per caller.

```malloy
run: opps -> { aggregate: visible_amount, hidden }
```

givens: ORG_ID=1; USER_ID=8
servedFrom: storage

Expect:

| visible_amount:num | hidden:int |
| ------------------ | ---------- |
| 700                | 0          |

## Query user 9 sees

Another org. Never org 1's rows: `opps`'s own org term excludes them before the
join is reached.

```malloy
run: opps -> { aggregate: visible_amount, hidden }
```

givens: ORG_ID=2; USER_ID=9
servedFrom: storage

Expect:

| visible_amount:num | hidden:int |
| ------------------ | ---------- |
| 800                | 0          |

## Query a user asking outside their org

User 7 holds no grant in org 2, so org 2's one restricted opp is hidden from them.
800 would mean a grant from another caller reached this one.

```malloy
run: opps -> { aggregate: visible_amount, hidden }
```

givens: ORG_ID=2; USER_ID=7
servedFrom: storage

Expect:

| visible_amount:num | hidden:int |
| ------------------ | ---------- |
| 0                  | 1          |

## Mutate orders_pg.pvj_grants

User 7's grant on opp 2 is revoked in the warehouse.

```sql
DELETE FROM pvj_grants WHERE user_id = 7 AND opp_id = 2;
```

## Query user 7 sees (again)

Still 300: the grant table's artifact still holds the revoked grant, and a live
recompute would answer 100. This is the property an author trades for the tier —
a revocation is visible only once the grant table rebuilds, so the grant table's
freshness window is the revocation latency.

```malloy
run: opps -> { aggregate: visible_amount, hidden }
```

givens: ORG_ID=1; USER_ID=7
servedFrom: storage

Expect:

| visible_amount:num | hidden:int |
| ------------------ | ---------- |
| 300                | 1          |

## Manifest pvj

The host keeps `opps` and `accounts` bound and marks `grants` stale past its
window with a live fallback, so the grant table's binding is withheld.

- opps -> pvj_opps @ lake (fallback=live)
- grants -> pvj_grants @ lake (fallback=live, asof=2020-01-01T00:00:00Z, fresh=60)
- accounts -> pvj_accounts @ lake (fallback=live)

## Query user 7 sees, with the grant table withheld

Without `grants` on the shape, the join on `opps` cannot be re-emitted, so
`opps` is withheld with it and serves live, from the warehouse, where the
revocation above has landed: 100, and two opps hidden. 300 would mean `visible`
was evaluated over the stored grants the host withheld; any other answer would
mean it was evaluated against no grants at all.

```malloy
run: opps -> { aggregate: visible_amount, hidden }
```

givens: ORG_ID=1; USER_ID=7

Expect:

| visible_amount:num | hidden:int |
| ------------------ | ---------- |
| 100                | 2          |

## Query accounts, with the grant table withheld

`accounts` joins nothing caller-scoped, so it keeps every refinement — including
the model measure below — and serves from its own table. Withholding `opps`
rather than thinning the shape is what keeps it here: the failure costs the one
source that needs the withheld table, and no sibling.

```malloy
run: accounts -> { aggregate: account_count }
```

givens: ORG_ID=1; USER_ID=7
servedFrom: storage

Expect:

| account_count:int |
| ----------------- |
| 2                 |
