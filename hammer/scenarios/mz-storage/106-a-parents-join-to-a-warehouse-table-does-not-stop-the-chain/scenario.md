---
id: a-parents-join-to-a-warehouse-table-does-not-stop-the-chain
tags: orchestration, chained, build-control, serve-correctness
package: pjw
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A parent's join to a warehouse table does not stop the chain

A fact source joined to a dimension table is the ordinary shape: `daily` is
stored, and its `extend {}` joins `regions` — a warehouse table nothing
materializes — to name `region_name is r.name`. `rollup` is stored over
`daily` and reads only `total`.

A chained build rebinds `daily`'s table and re-declares what `daily` adds to
it. The join to `regions` cannot be carried into the destination, and a
dimension that reads it names an alias the model does not have — so the
build thins to the kinds that change rows (the `where:` filter) and compiles
again. `rollup` never read `region_name`; it stacks on `daily`'s table and
says `reused`. A downstream that does read it is not built, since its build
cannot be expressed over `daily`'s table and nothing recomputes from raw; it
answers live.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.pjw_orders

| order_id:int | order_date:date | region_id:int | amount:num |
| ------------ | --------------- | ------------- | ---------- |
| 1            | 2026-01-01      | 10            | 100        |
| 2            | 2026-01-01      | 20            | 50         |
| 3            | 2026-01-02      | 10            | 200        |

## Data orders_pg.pjw_regions

| id:int | name:text |
| ------ | --------- |
| 10     | east      |
| 20     | west      |

## Model pjw.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.pjw_orders')
source: regions is orders_pg.table('public.pjw_regions')

#@ persist name="pjw_daily" storage=lake
source: daily is orders -> {
  group_by: order_date, region_id
  aggregate: total is amount.sum()
} extend {
  where: total > 60
  join_one: r is regions on region_id = r.id
  dimension: region_name is r.name
  view: by_region is { group_by: region_name; aggregate: total.sum() }
}

#@ persist name="pjw_rollup" storage=lake
source: rollup is daily -> { aggregate: grand is total.sum() }

#@ persist name="pjw_by_name" storage=lake
source: by_name is daily -> { group_by: region_name; aggregate: grand is total.sum() }
```

## Publish

`rollup` stacks on `daily`; `by_name` reads the join the destination cannot
bind, so the plan refuses it and it is not built.

expect binding: daily -> lake
expect binding: rollup -> lake
expect upstreams: rollup -> reused

## Query rollup

The parent's `where: total > 60` applies on every tier: the 50 is excluded.

```malloy
run: rollup -> { select: grand }
```

Expect:

| grand:num |
| --------- |
| 300       |

## Query by name

`by_name` answers live, with the parent's filter applied.

```malloy
run: by_name -> { select: region_name, grand }
```

Expect:

| region_name | grand:num |
| ----------- | --------- |
| east        | 300       |

## Mutate orders_pg.pjw_orders

| order_id:int | order_date:date | region_id:int | amount:num |
| ------------ | --------------- | ------------- | ---------- |
| 99           | 2026-01-03      | 10            | 1000       |

## Build (orchestrated, strict, pkg=pjw)

Strict: `rollup` alone over the stored `daily`. Stacks on the table.

- rollup -> pjw_rollup__g2 @ lake (reused)
  reference: daily

## Build refusals

`by_name` reads `region_name`, which `daily`'s stored table does not hold: the
join that supplies it reads the warehouse. The plan refuses it, naming the
field, bound or not.

Expect:

| source  | tier    | reason                  |
| ------- | ------- | ----------------------- |
| by_name | storage | upstream_table_unusable |

## Warns

cites: it reads 'region_name' from a table no stored upstream holds

## Bind pjw

## Query rollup (again)

Still 300: `daily`'s stored rows, filtered.

Expect:

| grand:num |
| --------- |
| 300       |
