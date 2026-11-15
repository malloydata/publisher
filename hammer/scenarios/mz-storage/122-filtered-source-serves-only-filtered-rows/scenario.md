---
id: filtered-source-serves-only-filtered-rows
tags: serve-correctness, sql-select
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

The same 300, whichever tier answers. Serving the artifact as-is would return
375 — every row, including the two the filter excludes.

```malloy
run: high_value -> { aggregate: total is amount.sum() }
```

Expect:

| total:num |
| --------- |
| 300       |

## Note (since=2026-09-16)

> Easy to reintroduce, and silent when it is. Every other refinement the shape
> cannot reproduce announces itself by failing the shape's COMPILE, which is what
> triggers the fallback ladder; a dropped `where:` compiles perfectly and simply
> answers with more rows. Nothing upstream catches it either — materialization
> eligibility refuses a given, a `#(partition)`, an authorize gate and a free
> parameter, and says nothing about filters.
>
> Hence the two rules the fix rests on. Filters are carried at EVERY tier of the
> serve-shape ladder, the empty one included: the other kinds are optimizations,
> while a filter is part of what the source means. And an entry that cannot be
> reproduced — one reaching through a join whose target is not materialized, or
> referencing a given — is still emitted, so the shape fails to compile and the
> query serves live, rather than being dropped so the query serves unfiltered.
>
> `filterList` accumulates through `extend`, so a source's own entries already
> carry every filter it inherits. That is why the chained arrangement
> (`filtered-chain-serves-only-filtered-rows`) needs no separate handling.
