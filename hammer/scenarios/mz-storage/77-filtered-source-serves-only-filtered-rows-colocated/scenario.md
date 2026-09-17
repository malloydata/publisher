---
id: filtered-source-serves-only-filtered-rows-colocated
tags: serve-correctness, sql-select
package: fsc
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A filtered source must never serve rows its filter excludes (colocated)

The control for `filtered-source-serves-only-filtered-rows`: the same model and
the same filter, persisted colocated instead of into a storage destination. The
rule holds here — and holds while the query is genuinely being answered from the
materialized table, so the storage tier's failure is not something inherent to
persisting a filtered source.

The colocated path binds a manifest and lets the compiler rewrite the FROM
underneath the author's own source, which leaves the `where:` attached. The
artifact it reads is the base's UNFILTERED relation; the filter is applied over it
at query time. That is the division of labour the storage tier has to reproduce
for itself, because it re-declares the source rather than swapping its base.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.fsc_orders

| order_id:int | amount:num |
| ------------ | ---------- |
| 1            | 100        |
| 2            | 50         |
| 3            | 200        |
| 4            | 25         |

## Model fsc.malloy

```malloy
##! experimental.persistence

source: raw is orders_pg.sql('SELECT order_id, amount FROM public.fsc_orders')

#@ persist name="fsc_high_value"
source: high_value is raw extend {
  where: amount >= 100
}
```

## Publish

## SQL what the filter selects

Orders 1 and 3 pass `amount >= 100`; orders 2 and 4 do not.

```sql
SELECT sum(amount) AS total FROM fsc_orders WHERE amount >= 100;
```

Expect:

| total:num |
| --------- |
| 300       |

## Query high value total

```malloy
run: high_value -> { aggregate: total is amount.sum() }
```

Expect:

| total:num |
| --------- |
| 300       |

## Mutate orders_pg.fsc_orders

A qualifying order that live would carry into the answer.

| order_id:int | amount:num |
| ------------ | ---------- |
| 5            | 1000       |

## Query high value total (again)

Stale `300` rather than a fresh `1300` ⇒ the answer came from the materialized
table, with the filter applied over it. Both halves matter: a fresh answer would
mean the table was never read and the rule held only because nothing was being
served from storage.

```malloy
run: high_value -> { aggregate: total is amount.sum() }
```

Expect:

| total:num |
| --------- |
| 300       |
