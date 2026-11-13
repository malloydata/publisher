---
id: cross-environment-same-name
tags: orchestration, isolation
package: cesame
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Two environments, same persist `name=`, shared destination → the second is refused

Auto-run keeps an explicit `#@ persist name=` verbatim: the author chose it, and a
reader outside Malloy may rely on it. So two environments that persist a source with
the same `name=` into the same DuckLake destination would write the SAME physical
table. Before table ownership, the second build clobbered the first and `env1`
silently served `env2`'s rows. Now every auto-run table at a `name=` carries an
ownership mark (`malloy-publisher:table owner=<env>/<pkg>`), and a build whose
`name=` another package holds is refused before anything is written
(`table-owned-elsewhere`), so that package answers live from its own data. A name
the author did not choose carries the package instead (`<source>__p<hash>`) and
never collides. One server serves both environments here; the `(env=…)` attribute
selects which one each step runs against.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.ce_orders_e1

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 150        |

## Data orders_pg.ce_orders_e2

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 999        |

## Model cesame/ce.malloy (env=default)

Environment 1 reads its own source table; persists as `ce_daily`.

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.ce_orders_e1')

#@ persist name="ce_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}
```

## Model cesame/ce.malloy (env=prod)

Environment 2 reads DIFFERENT data — but persists with the SAME `name="ce_daily"`.

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.ce_orders_e2')

#@ persist name="ce_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}
```

## Publish cesame (env=default)

env1 materializes `lake.ce_daily` = 150.

expect binding: daily -> lake

## Query e1 daily (env=default)

env1 serves its own snapshot.

```malloy
run: daily -> { select: order_date, total_amount; order_by: order_date asc }
```

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |

## Build refused cesame (env=prod)

env2's build of the same `name="ce_daily"` is refused: the table is env1's, by its
mark. Nothing is written, so env1's table still holds 150.

cites: table-owned-elsewhere

## Query e2 daily (env=prod)

env2 answers live from its own source: 999. The only `ce_daily` table holds env1's
150, so this answer cannot have come from it.

```malloy
run: daily -> { select: order_date, total_amount; order_by: order_date asc }
```

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 999          |

## Query e1 daily (again, env=default)

env1 still serves its own table, untouched: 150, from the storage tier.

servedFrom: storage

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
