---
id: tenant-scoped-source-serves-per-caller
tags: serve-correctness, givens, security
package: tsp
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# One artifact for every tenant, read per caller

`orders` is scoped to the caller's org by an extend-block `where: org_id =
$ORG_ID`. Materialized into the lake it becomes ONE table holding every org's
rows — Malloy leaves an extend-block `where:` out of the build — and each caller
is answered from their own slice of it, because the serve shape re-applies the
term with the value that caller supplied.

Before this, the storage tier refused the source outright: any given reference
was a refusal, which took the tier away from every tenant-scoped model, i.e.
every multi-tenant model worth materializing.

The refusal was aimed at the right danger and drawn in the wrong place. A given
whose value the BUILD substitutes really is frozen into the artifact and served
to everyone — that shape is still refused, in
`given-in-the-persisted-query-is-refused` beside this. A given in the extend
block was never in the artifact at all.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.tsp_orders

Two orgs, deliberately different totals, and a third row so one org has more
than one — an artifact serving whole-table answers would return 360 to both.

| order_id:int | org_id:int | amount:num |
| ------------ | ---------- | ---------- |
| 1            | 1          | 100        |
| 2            | 2          | 60         |
| 3            | 1          | 200        |

## Model tsp.malloy

```malloy
##! experimental.persistence
##! experimental.givens

given:
  ORG_ID :: number is 1

source: raw is orders_pg.sql('SELECT order_id, org_id, amount FROM public.tsp_orders')

#@ persist name="tsp_orders" storage=lake
source: orders is raw -> { select: * } extend {
  where: org_id = $ORG_ID
}
```

## Publish

expect binding: orders -> lake

## Operator lake

The artifact holds EVERY org's rows — the half that makes the rest meaningful.
The per-caller answers below are filtered at read rather than built per tenant,
so there is exactly one table here and one build behind it.

```sql
SELECT count(*) AS n FROM lake.tsp_orders;
```

Expect:

| n:int |
| ----- |
| 3     |

## SQL what each org is owed

The rule, stated in the source warehouse rather than assumed from the fixture.

```sql
SELECT org_id, sum(amount) AS total FROM tsp_orders GROUP BY org_id ORDER BY org_id;
```

Expect:

| org_id:int | total:num |
| ---------- | --------- |
| 1          | 300       |
| 2          | 60        |

## Query org 1 total

```malloy
run: orders -> { aggregate: total is amount.sum() }
```

givens: ORG_ID=1
servedFrom: storage

Expect:

| total:num |
| --------- |
| 300       |

## Query org 2 total

The same artifact, a different caller, a different answer. 360 here would mean
the shape stopped carrying the term and served the whole table.

```malloy
run: orders -> { aggregate: total is amount.sum() }
```

givens: ORG_ID=2
servedFrom: storage

Expect:

| total:num |
| --------- |
| 60        |

## Query an unsupplied given falls back to its default

The shape carries the author's declared default, so a request that binds nothing
gets the answer the live path would give it rather than failing to compile and
quietly losing the tier.

```malloy
run: orders -> { aggregate: total is amount.sum() }
```

servedFrom: storage

Expect:

| total:num |
| --------- |
| 300       |

## Note (since=2026-09-16)

> The two properties here fail in opposite directions and both are asserted, on
> purpose. A shape that stopped declaring the given fails CLOSED — the re-emitted
> `where: org_id = $ORG_ID` no longer compiles, the binding is withheld, and the
> query serves live. A shape that stopped carrying the TERM fails OPEN, compiling
> perfectly and answering 360 to everyone. Only the second is a leak, and only a
> per-caller assertion can see it: a test that checked one caller, or that checked
> routing alone, passes against it.
