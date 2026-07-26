---
id: authorize-refused
tags: eligibility, security
package: az
---

# Eligibility: an #(authorize)-gated source is refused

An `#(authorize)` expression is a per-request *who-can-query* gate, evaluated at
query time against the caller's givens. A `storage=` materialization is built
once and served frozen — the served virtual shape carries **no gate to
evaluate** — so materializing an authorize-gated source would serve it to
everyone, bypassing authorization. Like the given refusal (`givens-refused`),
this is a hard security refusal at build time: the source must be served live.

(This refusal was a follow-up gated on the transitive-`#(authorize)` enforcement
in #906; #906 is merged, so it's now enforced. The gate is fail-closed and also
catches an authorize expression reached through a join — see the eligibility
unit tests.)

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.az_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-02      | 200        |

## Model az.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.az_orders')

#(authorize) "true"
#@ persist name="az_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}
```

## Build refused

Materializing an `#(authorize)`-gated source into storage is refused for safety.

cites: authorize
