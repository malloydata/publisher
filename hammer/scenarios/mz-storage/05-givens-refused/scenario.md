---
id: givens-refused
tags: eligibility, security
package: f1
---

# Eligibility: a given-referencing source is refused

A source that references a `given` must be REFUSED for a `storage=` destination: a
given binds per query for row-level access control, so a materialized-once table
served to everyone would leak filtered rows across tenants. The build must fail
with a clear reason. This is the security-critical negative case.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.f1_orders

| order_id:int | order_date:date | region:text | amount:num |
| ------------ | --------------- | ----------- | ---------- |
| 1            | 2026-01-01      | US          | 100        |
| 2            | 2026-01-02      | EU          | 200        |

## Model f1.malloy

```malloy
##! experimental.persistence
##! experimental.givens

given: REGION :: filter<string> is f''

source: base is orders_pg.table('public.f1_orders')

source: scoped is base extend {
  where: region ~ $REGION
}

#@ persist name="f1_scoped" storage=lake
source: scoped_rollup is scoped -> {
  group_by: order_date
  aggregate: t is amount.sum()
}
```

## Build refused

The package compiles (givens are valid Malloy), but the build is refused by the
eligibility gate and ends FAILED.

cites: references a given
