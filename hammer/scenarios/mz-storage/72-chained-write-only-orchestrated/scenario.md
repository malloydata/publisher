---
id: chained-write-only-orchestrated
tags: orchestration, chained, kill-switch
package: cwoo
---

# `write-only` must govern serving, not whether an orchestrated chained build resolves

`chained-write-only` proves the self-derived publish path builds a chained pair
at `write-only`. This is the same rule for the ORCHESTRATED path, and the pairing
is the one an operator actually walks through: a host turns writing on before it
turns routing on, so the first orchestrated build of a chained package lands in
exactly this state.

Non-strict on purpose, and paired with `chained-write-only-orchestrated-strict`,
which is byte-identical but for the flag. Kept as a pair because it is what
isolates a cause: if the strict twin ever regresses, this one staying green says
the mode, the chain and the orchestrated path are all still sound and the flag is
the variable. Collapsing them into one strict scenario would lose that.

So what this pins is narrow and worth pinning on its own: `write-only` governs
SERVING. It must not change whether an orchestrated build can resolve a chained
upstream, and it must not leave one source of a chain routed while the other is
live.

## Publisher

Build into the lake, serve live.

- PERSIST_STORAGE_MODE: write-only

## Data orders_pg.cwoo_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |

## Model cwoo.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.cwoo_orders')

#@ persist name="cwoo_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}

#@ persist name="cwoo_rollup" storage=lake
source: rollup is daily -> {
  aggregate: grand_total is total_amount.sum()
}
```

## Build (orchestrated, pkg=cwoo)

Both sources at caller-assigned names, into the lake, with routing off.

- daily -> cwoo_daily__g1 @ lake
- rollup -> cwoo_rollup__g1 @ lake

## Query daily

```malloy
run: daily -> { select: order_date, total_amount; order_by: order_date asc }
```

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 200          |

## Query rollup

```malloy
run: rollup -> { select: grand_total }
```

Expect:

| grand_total:num |
| --------------- |
| 350             |

## Mutate orders_pg.cwoo_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 99           | 2026-01-03      | 1000       |

## Query daily (again)

`write-only` serves live, so the upstream shows the mutation.

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 200          |
| 2026-01-03 | 1000         |

## Query rollup (again)

And so does the downstream. A stale 350 here would mean the chain routed
inconsistently — the dependent reading its table while its upstream read live.

Expect:

| grand_total:num |
| --------------- |
| 1350            |
