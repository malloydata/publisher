---
id: cross-environment-same-name
tags: orchestration, isolation, needs-attention
package: cesame
---

# Two environments, same persist `name=`, shared destination → collision

Auto-run assigns the physical table name from `#@ persist name=` VERBATIM — it does
NOT encode the environment. So two environments that persist a source with the same
`name=` into the same DuckLake destination write the SAME physical table, and the
second build clobbers the first. This scenario proves it: `env1` and `env2` each
build `name="ce_daily"` (different source data) into the shared `lake`, and after
`env2` builds, `env1` serves `env2`'s data.

The point: the storage tier does not auto-isolate by environment. Isolation is the
ORCHESTRATOR's job — it must assign environment-distinct physical names (the design's
`<base>__g__<cellEnvPkg8>`, which encodes env+package) or distinct schemas (as in
`two-schema-version-isolation`). One server serves both environments here; the
`(env=…)` attribute selects which one each step runs against.

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

## Publish cesame (env=prod)

env2 materializes into the SAME `lake.ce_daily` — overwriting env1's table
(`CREATE OR REPLACE`), because the physical name has no environment in it.

expect binding: daily -> lake

## Query e2 daily (env=prod)

env2 serves 999.

```malloy
run: daily -> { select: order_date, total_amount; order_by: order_date asc }
```

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 999          |

## Query e1 daily (again, env=default)

COLLISION: env1 now serves `999` — env2's build clobbered env1's table. Auto-run
gave the two environments the same physical name in the same destination, so they
are NOT isolated.

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 999          |

## Note (since=2026-07-23)

> Auto-run physical names come from `name=` verbatim, with no environment
> component, so same-name sources in different environments collide in a shared
> destination. Environment isolation is the orchestrator's responsibility
> (env-decorated generational names, or a per-env schema/destination). Worth
> deciding: should the tier refuse/guard a same-name-same-destination collision, or
> is it purely the orchestrator's contract to assign env-distinct names? Pins
> current behavior so a change flags loudly.
