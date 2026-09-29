---
id: a-refused-source-does-not-stop-its-siblings
tags: eligibility, build-control
package: rss
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A refused source is skipped, and the rest of the package still builds

`by_date` is an ordinary persisted rollup. `scoped_rollup` reads a given inside
its persisted query, so the eligibility gate refuses it — the same refusal as
`givens-refused`. They share one package and one build.

The refusal is a fact about `scoped_rollup` alone, known when the model
compiles, and the build plan already reports it. So it must cost only that
source: the run skips it, records it, and builds everything else. Were it to
fail the run instead, `by_date` would go unbuilt on every run and every
scheduled fire until someone edited `scoped_rollup` — a source it has nothing to
do with.

A run refused only when there is nothing left to build is `givens-refused`'s
case, and still fails there.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.rss_orders

| order_id:int | order_date:date | region:text | amount:num |
| ------------ | --------------- | ----------- | ---------- |
| 1            | 2026-01-01      | US          | 100        |
| 2            | 2026-01-01      | EU          | 50         |
| 3            | 2026-01-02      | US          | 200        |

## Model rss.malloy

```malloy
##! experimental.persistence
##! experimental.givens

given: REGION :: filter<string> is f''

source: orders is orders_pg.table('public.rss_orders')

#@ persist name="rss_by_date" storage=lake
source: by_date is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}

source: scoped is orders extend {
  where: region ~ $REGION
}

#@ persist name="rss_scoped" storage=lake
source: scoped_rollup is scoped -> {
  group_by: order_date
  aggregate: t is amount.sum()
}
```

## Build refusals

The plan reports the refusal before anything runs.

Expect:

| source        | tier    | reason                   |
| ------------- | ------- | ------------------------ |
| scoped_rollup | storage | given_in_persisted_query |

## Publish

The build completes: the refused source is skipped, not fatal.

expect binding: by_date -> lake

## Query date rollup

Served from the table the build wrote, which is the evidence the refusal did not
take this source down with it.

```malloy
run: by_date -> { select: order_date, total_amount; order_by: order_date asc }
```

servedFrom: storage

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 200          |

## Query the refused source

Never built, so it serves live, and correctly for the caller's own region.

```malloy
run: scoped_rollup -> { select: order_date, t; order_by: order_date asc }
```

givens: REGION=US

Expect:

| order_date | t   |
| ---------- | --- |
| 2026-01-01 | 100 |
| 2026-01-02 | 200 |
