---
id: multi-source-partial-build
tags: build-control
package: mspb
---

# Partial build: `sources=` builds one persist source, leaves the others live

A package with two `#@ persist storage=lake` sources. Building with the
`sourceNames` filter (`## Publish (sources=…)`) materializes ONLY the named
source; the other is untouched and continues to serve live. The staleness probe
tells them apart: after the build we mutate the shared source table, then the
built source stays stale (snapshot) while the unbuilt one reflects the mutation.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.mspb_orders

| order_id:int | order_date:date | region:text | amount:num |
| ------------ | --------------- | ----------- | ---------- |
| 1            | 2026-01-01      | US          | 100        |
| 2            | 2026-01-01      | EU          | 50         |
| 3            | 2026-01-02      | US          | 200        |

## Model mspb.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.mspb_orders')

#@ persist name="mspb_by_date" storage=lake
source: by_date is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}

#@ persist name="mspb_by_region" storage=lake
source: by_region is orders -> {
  group_by: region
  aggregate: total_amount is amount.sum()
}
```

## Publish (sources=by_date)

Build ONLY `by_date`. `by_region` is left unbuilt.

expect binding: by_date -> lake

## Query date rollup

Served from storage (the built source).

```malloy
run: by_date -> { select: order_date, total_amount; order_by: order_date asc }
```

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 200          |

## Query region rollup

`by_region` was not built, so it serves live.

```malloy
run: by_region -> { select: region, total_amount; order_by: region asc }
```

Expect:

| region | total_amount |
| ------ | ------------ |
| EU     | 50           |
| US     | 300          |

## Mutate orders_pg.mspb_orders

| order_id:int | order_date:date | region:text | amount:num |
| ------------ | --------------- | ----------- | ---------- |
| 99           | 2026-01-01      | US          | 1000       |

## Query date rollup (again)

Stale ⇒ `by_date` served from the snapshot (unchanged by the mutation).

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 200          |

## Query region rollup (again)

Live ⇒ `by_region` reflects the mutation (US now 1300), proving it was never
materialized.

Expect:

| region | total_amount |
| ------ | ------------ |
| EU     | 50           |
| US     | 1300         |
