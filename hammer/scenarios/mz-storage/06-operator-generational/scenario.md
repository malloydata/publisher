---
id: operator-generational
tags: orchestration, operator
package: dop
---

# Operator orchestrated build with generational names

The orchestrator ("operator") drives the build itself via an orchestrated
`## Build`, assigning its own generational physical table names (`dop_daily__g001`,
`__g002`, …) and owning distribution (it binds the build's output manifest with
`## Bind`).

Proves: orchestrator-assigned physical names are honored verbatim (the `## Build`
step verifies the built table echoes the requested name); rebinding to a new
generation moves the served data to that generation; and each generation is a
materialized snapshot (a later source mutation is invisible).

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.dop_orders

| order_id:int | order_date:date | region:text | amount:num |
| ------------ | --------------- | ----------- | ---------- |
| 1            | 2026-01-01      | US          | 100        |
| 2            | 2026-01-01      | US          | 50         |
| 3            | 2026-01-02      | EU          | 200        |
| 4            | 2026-01-02      | US          | 25         |

## Model dop.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.dop_orders')

#@ persist name="dop_daily" storage=lake
source: daily_orders is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}
```

## Build (orchestrated, pkg=dop)

Generation 1 — the operator assigns the physical name `dop_daily__g001`.

- daily_orders -> dop_daily__g001 @ lake

## Bind dop

The operator distributes gen1's manifest (binds the latest build).

## Query gen1

```malloy
run: daily_orders -> { select: order_date, total_amount; order_by: order_date asc }
```

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 225          |

## Mutate orders_pg.dop_orders

| order_id:int | order_date:date | region:text | amount:num |
| ------------ | --------------- | ----------- | ---------- |
| 99           | 2026-01-01      | US          | 1000       |

## Build (orchestrated, pkg=dop)

Generation 2 captures the mutated source into a NEW physical name `dop_daily__g002`.

- daily_orders -> dop_daily__g002 @ lake

## Bind dop

Rebind to gen2's manifest — serving moves to the new generation.

## Query gen1 (again)

Serving now follows gen2 (the new generation captured the mutation).

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 1150         |
| 2026-01-02 | 225          |

## Mutate orders_pg.dop_orders

| order_id:int | order_date:date | region:text | amount:num |
| ------------ | --------------- | ----------- | ---------- |
| 100          | 2026-01-02      | US          | 500        |

## Query gen1 (again)

gen2 is a materialized snapshot — a further source mutation is invisible.

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 1150         |
| 2026-01-02 | 225          |
