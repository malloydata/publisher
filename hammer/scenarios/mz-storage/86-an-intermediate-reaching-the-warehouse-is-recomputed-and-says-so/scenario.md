---
id: an-intermediate-reaching-the-warehouse-is-recomputed-and-says-so
tags: serve-correctness, chained, orchestration
package: cir
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A downstream whose intermediate reaches the warehouse is recomputed, under strict too, and its entry says so

`daily_regional` joins the stored `daily` to `regions`, a warehouse table nothing
materializes. The destination holds no such table, so no build of `rollup` over
`daily`'s stored rows exists: the only build there is recomputes `daily` from the
warehouse with the join beside it. The rule has three parts.

The build **succeeds**, under `strictUpstreams` as well. Strict forbids
recomputing a table the orchestrator meant to pin; it does not forbid the only
build a source has, and failing the run would leave a buildable source
unbuildable.

The entry **says what happened**: `upstreamReuse` is `recomputed`, and the reason
names the intermediate that could not be carried. Both paths leave a correct
table, so without this nothing tells them apart.

And the rows **mean what that says**. A recomputed `rollup` reflects the
warehouse at its own build time while `daily` still serves its snapshot, so
after the warehouse changes the two disagree: `rollup` sees the new 1000 and
`daily` does not. That skew is the cost of the recompute, and the field is how a
consumer knows to expect it.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.cir_orders

| order_id:int | order_date:date | region_id:int | amount:num |
| ------------ | --------------- | ------------- | ---------- |
| 1            | 2026-01-01      | 10            | 100        |
| 2            | 2026-01-01      | 20            | 200        |
| 3            | 2026-01-02      | 10            | 400        |

## Data orders_pg.cir_regions

| region_id:int | region:text |
| ------------- | ----------- |
| 10            | east        |
| 20            | west        |

## Model cir.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.cir_orders')
source: regions is orders_pg.table('public.cir_regions')

#@ persist name="cir_daily" storage=lake
source: daily is orders -> {
  group_by: order_date, region_id
  aggregate: total_amount is amount.sum()
}

source: daily_regional is daily extend {
  join_one: r is regions on region_id = r.region_id
}

#@ persist name="cir_rollup" storage=lake
source: rollup is daily_regional -> {
  group_by: region is r.region
  aggregate: grand_total is total_amount.sum()
}
```

## Publish

Both materialize. `rollup` could not be built over `daily`'s stored table, so
it was recomputed, and its entry says so.

expect binding: daily -> lake
expect binding: rollup -> lake
expect upstreams: rollup -> recomputed

## Query rollup

```malloy
run: rollup -> { select: region, grand_total; order_by: region asc }
```

Expect:

| region | grand_total:num |
| ------ | --------------- |
| east   | 500             |
| west   | 200             |

## Mutate orders_pg.cir_orders

| order_id:int | order_date:date | region_id:int | amount:num |
| ------------ | --------------- | ------------- | ---------- |
| 99           | 2026-01-03      | 10            | 1000       |

## Build (orchestrated, strict, pkg=cir)

Rebuild `rollup` alone under strict, reusing the `daily` already built. The run
succeeds, the entry reports the recompute, and the reason names the intermediate
the destination could not stand in for.

- rollup -> cir_rollup__g2 @ lake (recomputed)
  reference: daily

cites: daily_regional

## Bind cir

## Query rollup (again)

Recomputed from the warehouse at build time, so it sees the 1000.

Expect:

| region | grand_total:num |
| ------ | --------------- |
| east   | 1500            |
| west   | 200             |

## Query daily

`daily` still serves the snapshot it was built from. This is the disagreement a
`recomputed` entry warns of.

```malloy
run: daily -> { aggregate: total is total_amount.sum() }
```

Expect:

| total:num |
| --------- |
| 700       |
