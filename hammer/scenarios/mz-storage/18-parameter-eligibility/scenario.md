---
id: parameter-eligibility
tags: eligibility
package: f2
---

# Persisting a parameterized source: bind the argument, or it can't be persisted

`high_value(min_amount)` is a reusable filter template — orders worth at least
`min_amount` — that a modeler normally instantiates per query, e.g.
`high_value(min_amount is 500) -> { ... }`. Someone wants to persist a daily
rollup built on it with `storage=lake`.

- Persist a **concrete instantiation** (the argument is bound to a constant):
  there is one fixed relation, so it materializes the filtered rollup and serves
  it routed.
- Persist the **free template** (the modeler forgot to bind the argument): the
  source cannot be reduced to a single relation, so the package is rejected with
  a clear error, not silently served unfiltered.

This is the real-world shape of the parameter gate: a persisted source must be a
concrete relation, and Malloy forces the argument to be supplied.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.f2_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |
| 4            | 2026-01-02      | 25         |

## Data orders_pg.f2b_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |
| 4            | 2026-01-02      | 25         |

## Model f2/f2.malloy

The modeler forgot to bind `min_amount` — persisting the free template is
rejected at load.

```malloy
##! experimental.persistence
##! experimental.parameters

source: orders is orders_pg.table('public.f2_orders')

source: high_value(min_amount::number) is orders extend {
  where: amount >= min_amount
}

#@ persist name="f2_high_value_daily" storage=lake
source: high_value_daily is high_value -> {
  group_by: order_date
  aggregate: total is amount.sum()
}
```

## Model f2b/f2b.malloy

The threshold is bound to a constant (100), so the persisted rollup is a concrete
relation over the filtered orders.

```malloy
##! experimental.persistence
##! experimental.parameters

source: orders is orders_pg.table('public.f2b_orders')

source: high_value(min_amount::number) is orders extend {
  where: amount >= min_amount
}

#@ persist name="f2b_high_value_daily" storage=lake
source: high_value_daily is high_value(min_amount is 100) -> {
  group_by: order_date
  aggregate: total is amount.sum()
}
```

## Compile free template refused (pkg=f2, refused)

The free-template model is invalid. `## Compile … (refused)` does the full check:
the framework asserts f2 is NOT served (a durable backstop — a non-compiling model
must not also be reported as serving) AND that `/compile` reports the diagnostic.
The scenario doesn't juggle it — it just declares the model invalid.

cites: Argument not provided for required parameter

## Publish f2b

The concrete instantiation is eligible and materializes the filtered rollup.

expect binding: high_value_daily -> lake

## Query high value by day (pkg=f2b)

Only orders with `amount >= 100` are materialized (orders 1 and 3), so order 2
(50) and order 4 (25) never contribute.

```malloy
run: high_value_daily -> { select: order_date, total; order_by: order_date asc }
```

Expect:

| order_date | total:num |
| ---------- | --------- |
| 2026-01-01 | 100       |
| 2026-01-02 | 200       |

## Mutate orders_pg.f2b_orders

Append a large qualifying order to 2026-01-01. Live would push that date to 1100.

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 99           | 2026-01-01      | 1000       |

## SQL raw source really changed

```sql
SELECT order_date, sum(amount) AS total FROM f2b_orders WHERE amount >= 100 GROUP BY order_date ORDER BY order_date;
```

Expect:

| order_date | total:num |
| ---------- | --------- |
| 2026-01-01 | 1100      |
| 2026-01-02 | 200       |

## Query high value by day (again, pkg=f2b)

Stale `100` ⇒ served from the DuckLake snapshot of the filtered rollup.

Expect:

| order_date | total:num |
| ---------- | --------- |
| 2026-01-01 | 100       |
| 2026-01-02 | 200       |
