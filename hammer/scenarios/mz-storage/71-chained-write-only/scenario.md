---
id: chained-write-only
tags: config, kill-switch, chained
package: cwo
---

# `write-only` must not stop a chained pair building, and must serve both live

`mode-matrix` pins what `write-only` means for ONE source: materialize into the
lake, but do not route reads to the table. `chained-persist` pins that a
downstream builds by reading its upstream's materialized table. Those two rules
meet in a case neither covers — every chained scenario runs at `on` — and the
meeting is not obviously safe: if a downstream builds by READING its upstream,
and `write-only` is precisely the mode that does not route reads to storage, a
chained build could plausibly fail in the one mode an operator steps through on
the way to enabling the tier.

So the rule is that `write-only` governs SERVING, not the build's own upstream
resolution: both sources still materialize, and both still serve live.

That direction matters. `write-only` is the halfway house of a rollout — turn on
writing, confirm tables appear, then turn on routing. If a chained package cannot
be built in that state, the safe rollout order is the one that breaks, and the
only way through is the riskier jump straight to `on`.

## Publisher

Build into the lake, serve live.

- PERSIST_STORAGE_MODE: write-only

## Data orders_pg.cwo_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |
| 3            | 2026-01-02      | 200        |

## Model cwo.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.cwo_orders')

#@ persist name="cwo_daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}

#@ persist name="cwo_rollup" storage=lake
source: rollup is daily -> {
  aggregate: grand_total is total_amount.sum()
}
```

## Publish

Both persist sources materialize. The downstream's upstream resolution must not
depend on reads being routed — a failure here is the build refusing, not a wrong
number.

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

## Mutate orders_pg.cwo_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 99           | 2026-01-03      | 1000       |

## Query daily (again)

`write-only` serves live, so the mutation IS visible — the upstream is not
routed to its table.

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 200          |
| 2026-01-03 | 1000         |

## Query rollup (again)

The downstream serves live too. A stale 350 here would mean the chained source
routed to storage while its upstream did not — the mode applied inconsistently
down a chain, which is worse than either mode applied uniformly.

Expect:

| grand_total:num |
| --------------- |
| 1350            |
