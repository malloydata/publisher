---
id: imported-persist-from-flagless-model
tags: eligibility, build-control, needs-attention
package: ipr
---

# An importing model's flag revives a flagless model's `#@ persist`

`persist-without-flag-served-live` proves that a `#@ persist` in a model missing
`##! experimental.persistence` is inert: `compilePackageBuildPlan` skips the model
(it must — Malloy's `getBuildPlan()` throws without the flag), so nothing is built
and the source is served live with no warning.

That is only true while nothing else in the package carries the flag. Here
`base.malloy` is flagless and declares both persist sources, and `main.malloy` —
which declares nothing at all beyond `import "base.malloy"` — carries the flag.
`main`'s plan is walked, the walk reaches into the imported definitions, and BOTH
of `base`'s sources materialize.

So the flag is a property of the model being *walked*, not of the model that
declared the annotation. Whether a `#@ persist` does anything depends on a header
in a different file, and adding an innocuous entry model to a package is enough to
start materializing tables that were previously inert. Neither state warns.

The proof: mutate the warehouse, then re-query. A stale answer means the query is
being served from a frozen snapshot, so the source was materialized.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.ipr_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |

## Model ipr/base.malloy

No `##! experimental.persistence` header — deliberately. On its own (scenario 49)
that makes both annotations inert.

`daily_route` is `#@ -persist`, so it materializes nothing and exists only as the
path from `report` down to `daily` — the walk has to travel through it to find
`daily`, and does.

```malloy
source: orders is orders_pg.table('public.ipr_orders')

#@ persist name="ipr_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}

#@ -persist
source: daily_route is daily extend {
  dimension: doubled is total_amount * 2
}

#@ persist name="ipr_report" storage=lake
source: report is daily_route -> {
  aggregate:
    day_count is count()
    grand_total is total_amount.sum()
}
```

`report` aggregates to a single row on purpose. A same-grain re-projection of
`daily_route` would compile to `daily`'s own SQL and collapse onto its content
address — one table under two names, which is
`extend-persist-materializes-nothing-new`, not this. A different grain is a
different relation and a different address, so the two sources get two tables and
can be observed separately.

## Model ipr/main.malloy

Declares nothing. Its only contribution is the flag.

```malloy
##! experimental.persistence

import "base.malloy"
```

## Publish

Both of the flagless model's sources are build targets.

expect binding: daily -> lake
expect binding: report -> lake

## Query daily

```malloy
run: daily -> { select: order_date, total_amount; order_by: order_date asc }
```

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 200          |

## Query report

```malloy
run: report -> { select: day_count, grand_total }
```

Expect:

| day_count | grand_total |
| --------- | ----------- |
| 2         | 350         |

## Mutate orders_pg.ipr_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 99           | 2026-01-01      | 1000       |

## SQL raw source really changed

```sql
SELECT order_date, sum(amount) AS total FROM ipr_orders GROUP BY order_date ORDER BY order_date;
```

Expect:

| order_date | total:num |
| ---------- | --------- |
| 2026-01-01 | 1150      |
| 2026-01-02 | 200       |

## Query daily (again)

STALE — the mutation is invisible, so `daily` WAS materialized despite its own
model lacking the flag. A live recompute would read 1150.

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 200          |

## Query report (again)

STALE — served from its own lake snapshot, reached through the `#@ -persist`
route. A live recompute would total 1350.

Expect:

| day_count | grand_total |
| --------- | ----------- |
| 2         | 350         |

## Note (since=2026-08-13)

> Two things to decide, and a canary.
>
> **The footgun cuts both ways.** `persist-without-flag-served-live` asks whether a
> flagless model's `#@ persist` should warn instead of being silently ignored. This
> scenario is the other half: it is silently *honored* when some other model in the
> package carries the flag. An author reading `base.malloy` alone cannot tell which
> they will get. Whatever warning that scenario's note proposes should be decided
> against both cases, because the condition is not "the annotation is ignored" but
> "whether it applies is decided elsewhere".
>
> **The route is not severed here.** `malloydata/malloy` #3029 (merged, unreleased —
> publisher is pinned at 0.0.427, npm `latest` is 0.0.430) fixes an import that
> copied only persistent sources, severing the path to dependencies reachable only
> through a `#@ -persist` wrapper. This scenario deliberately builds that shape and
> the walk resolves it anyway, which bounds how much of the publisher the fix can
> move: the severing needs the wrapper referenced by sourceID, where a `->` derived
> source inlines its input instead.
>
> That makes this a useful bump canary even though it is not the fix's own test.
> Should the bump change what a package's plan enumerates, this is the scenario that
> notices — it asserts a build target and a stale serve for a source whose model
> never opted in.
