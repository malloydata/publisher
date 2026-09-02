---
id: filtered-source-serves-only-filtered-rows
tags: serve-correctness, sql-select, known-red
package: fsf
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A filtered source must never serve rows its filter excludes

`high_value` is `raw extend { where: amount >= 100 }`. Whichever tier answers it,
the rows it returns are the rows that pass its filter: materializing a source
changes where its rows are read FROM, never which rows they are.

A source's `where:` is a query-time refinement rather than part of its
materialized relation — Malloy's persist artifact for an `extend` is the base's
UNFILTERED relation, and the filter is applied over it when the query runs. The
colocated tier gets that division of labour for free, because it binds a manifest
and lets the compiler rewrite the FROM underneath the author's own source, filter
still attached (`filtered-source-serves-only-filtered-rows-colocated` is the
control, and it is green). The storage tier re-declares the source instead — as
`conn.virtual('handle')::Shape` plus its dimensions, measures, joins and views —
so it has to carry the filter itself, and today it does not.

RED today: the query is answered from the artifact, unfiltered. GREEN once the
serve shape reproduces the source's filters — or refuses to bind a source that
has any.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.fsf_orders

| order_id:int | amount:num |
| ------------ | ---------- |
| 1            | 100        |
| 2            | 50         |
| 3            | 200        |
| 4            | 25         |

## Model fsf.malloy

A `sql_select` base, which IS a materializable build root, filtered by an
`extend`. (`persist-shape-not-materializable` covers the neighbouring shape — the
same `extend` over a `conn.table(…)` — which Malloy drops from the build plan and
the publisher refuses. That refusal is what makes this shape the reachable one.)

```malloy
##! experimental.persistence

source: raw is orders_pg.sql('SELECT order_id, amount FROM public.fsf_orders')

#@ persist name="fsf_high_value" storage=lake
source: high_value is raw extend {
  where: amount >= 100
}
```

## Publish

expect binding: high_value -> lake

## SQL what the filter selects

The rule, stated in the source warehouse: orders 1 and 3 pass `amount >= 100`,
orders 2 and 4 do not.

```sql
SELECT sum(amount) AS total FROM fsf_orders WHERE amount >= 100;
```

Expect:

| total:num |
| --------- |
| 300       |

## Query high value total

The same 300, whichever tier answers. Today this returns 375 — every row in the
artifact, including the two the filter excludes.

```malloy
run: high_value -> { aggregate: total is amount.sum() }
```

Expect:

| total:num |
| --------- |
| 300       |

## Note (since=2026-09-02)

> A wrong ANSWER, not a lost optimization, and silent: the query succeeds, the
> tier is never mentioned, and the rows are simply the unfiltered relation. The
> whole storage tier is built on falling back to live whenever the shape cannot
> reproduce something, and every other omission does fall back — a view that
> traverses a non-materialized join, an analytic field, an unbound source — because
> the omission makes the shape fail to COMPILE. A dropped `where:` compiles
> perfectly, so nothing catches it.
>
> Nothing gates it upstream either. `assertMaterializationEligible` refuses free
> parameters, given references and authorize gates, and says nothing about
> filters; `assertServesInDuckDB` only asks whether the shape compiles, which it
> does.
>
> The filter is available — it is on the compiled source's `filterList`, the same
> place the authorize walk reads its lifted `where:` from — so carrying it looks
> like another refinement category, with the existing full → drop views → drop
> views+joins → base-only ladder as the safe floor. Refusing to bind a source that
> carries filters is the smaller immediate move and costs only the tier.
>
> The neighbouring shape `X is <table> extend { where … }` is already refused
> (`persist-shape-not-materializable`), so this is reachable specifically over a
> `sql_select` base. Worth checking whether a parameterized filter belongs in the
> same fix: `capped(limit_n::number is 1) is raw extend { where: n <= limit_n }`
> is eligible (a parameter bound to a default counts as bound), and its artifact is
> likewise unfiltered — the only thing keeping an OVERRIDDEN parameter honest today
> is that the shape declares no parameters, so the query fails to compile against
> it and falls back live.
