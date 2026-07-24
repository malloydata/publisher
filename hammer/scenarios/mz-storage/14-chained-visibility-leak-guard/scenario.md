---
id: chained-visibility-leak-guard
tags: security, visibility, chained
package: chv
---

# Visibility over a chained downstream: hidden column not reachable

Coverage for the chained case — a downstream persist source built from an
upstream's materialized table — where the downstream hides a column with
`except:`: it must not be reachable over storage.

> NOTE: unlike the raw-`.sql()` source case (`visibility-leak-guard`), this does
> NOT reproduce a leak even without the downstream build-side projection —
> verified by temporarily reverting it. A downstream is a QUERY source, and
> Malloy's `getSQL` for a query source already respects `except:` (it projects
> only the public output), so the hidden column is never materialized here. The
> demonstrated leak was specific to raw-`.sql()` sources, whose `getSQL` projects
> the underlying columns verbatim. This scenario is coverage, not a regression
> guard for the (defensive) downstream projection.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.chv_orders

| order_id:int | order_date:date | region:text | amount:num |
| ------------ | --------------- | ----------- | ---------- |
| 1            | 2026-01-01      | US          | 100        |
| 2            | 2026-01-01      | US          | 50         |
| 3            | 2026-01-02      | EU          | 200        |
| 4            | 2026-01-02      | US          | 25         |

## Model chv.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.chv_orders')

#@ persist name="chv_daily" storage=lake
source: daily is orders -> {
  group_by: order_date, region
  aggregate: total_amount is amount.sum()
}

#@ persist name="chv_rollup" storage=lake
source: rollup is daily -> {
  group_by: order_date, region
  aggregate: t is total_amount.sum()
} extend {
  except: region
}
```

## Publish

Both persist; the downstream (`rollup`) builds from `daily`'s materialized table.

expect binding: daily -> lake
expect binding: rollup -> lake

## Query rollup public

```malloy
run: rollup -> { group_by: order_date; aggregate: gt is t.sum(); order_by: order_date asc }
```

Expect:

| order_date | gt:num |
| ---------- | ------ |
| 2026-01-01 | 150    |
| 2026-01-02 | 225    |

## Query rollup hidden region (refused)

`region` is hidden on the downstream and must not be materialized or served.

```malloy
run: rollup -> { group_by: region }
```

## Mutate orders_pg.chv_orders

| order_id:int | order_date:date | region:text | amount:num |
| ------------ | --------------- | ----------- | ---------- |
| 99           | 2026-01-01      | US          | 1000       |

## Query rollup public (again)

Stale ⇒ the downstream is served from its storage snapshot.

Expect:

| order_date | gt:num |
| ---------- | ------ |
| 2026-01-01 | 150    |
| 2026-01-02 | 225    |
