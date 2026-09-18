---
id: a-query-term-over-materialized-sources-still-scopes
tags: serve-correctness, givens, security, known-red
package: qts
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A caller's term in a non-persisted entry point still scopes what it reads

`visible_orders` is not materialized. It joins two sources that are, and adds
`where: vis.user_id = $USER_ID` of its own. That term is part of the QUERY, not
of either artifact — so it is compiled against the two stored tables and applied
to them, and each user is answered from their own rows.

This is the arrangement to reach for when a caller's scope comes from a joined
source's own filter. Stripping such a filter out of a persisted source would not
leave a column behind to re-apply it with; it would turn the join into a
fan-out across every user, and the artifact would be wrong. So that shape is
refused (`dynamic_joined_where`), and this is what replaces it: the org term
lives on the persisted sources, where it is stripped and re-applied, and the
user term lives above them, where it was never in an artifact to begin with.

Two properties are needed, and dropping either serves these answers live —
correct, and unaccelerated.

The shape must accept the same givens the author's model does. One that declared
only the terms its own sources stripped would fail to compile a query mentioning
`$USER_ID` and serve the whole thing live. That half holds.

The shape must also carry `visible_orders` itself, over the two virtual bases.
That half does not hold, for the reason in the note below.

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

#@ -persist
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

## Note (since=2026-09-18)

> **Red on the tier, not on the answers.** Every answer above is correct today —
> the org and user terms both apply, and no caller sees another's rows. What does
> not happen is routing, which `servedFrom` reports on the first query, and which
> the mutate-and-requery at the end corroborates from the other side: the
> re-query returns the NEW value, so the rows were recomputed.
>
> The machinery to close it exists. The serve shape can carry a non-persisted
> source over materialized bases, reduced to what it adds to its base and refused
> outright if any join it declares reaches a source that is not materialized.
> What it may lawfully be applied TO is the open question, and it is a question
> about `#@ -persist` rather than about givens or partitioning.
>
> A derived source over a persisted base has two states and needs a third.
> Written plainly it INHERITS `#@ persist`, becomes a build target of its own,
> and is refused — here as `dynamic_joined_where`, with a message that says to
> "enter through a non-persisted extension that declares the join instead".
> Written the way that message advises, with `#@ -persist`, it is documented as
> recomputing "instead of using the pre-built table", which `opt-out-persist-
> recomputes` pins. So the refusal directs the author onto the one annotation
> that forbids the tier they were trying to reach, and the arrangement this
> scenario describes cannot be both built and served.
>
> Worth knowing which way the opt-out currently holds: it holds BY ACCIDENT. The
> serve path has no notion of it — bindings are keyed by source name, and every
> reader of a persisted base picks up the stored table. An opted-out source is
> served live only because no derived source is ever on the shape at all. The
> moment one can be, the opt-out needs honoring deliberately, and the lift above
> excludes `#@ -persist` for exactly that reason.
>
> Closing this means settling what `#@ -persist` promises: only that the source
> is not itself built, or also that its reads bypass its base's table. Malloy's
> own `findPersistentDependencies` bubbles a persistent dependency up through a
> non-persistent source, which reads as the base's table still being substituted;
> the publisher's documentation reads the other way. Until that is settled this
> scenario stays red, and it is what notices on the day it is not.
