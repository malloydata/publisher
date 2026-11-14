---
id: every-reading-of-a-filtered-parent-applies-its-filter
tags: orchestration, chained, serve-correctness
package: frp
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Every reading of a filtered parent applies its filter

`deals` is stored with `extend { where: not is_deleted }`; its table holds
every deal, deleted ones included, and the filter applies when the table is
read. Four stored downstreams read it four ways: `x` through an extension
that adds a `where:` of its own (`pipeline_now is deals extend { where:
is_open }`), `y` through a rename (`deals_alias is deals`), `z` directly, and
`w` through a `select: *` wrapper. Each must count only the deals the
parent's own filter admits — and `x` only the open ones among them. A chain
that rebinds the parent's table without its filter counts deleted deals and
reports `reused`.

Open, not deleted: 100 + 200 = 300. Not deleted: 100 + 200 + 50 = 350. All
rows: 1350.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.frp_deals

| deal_id:int | amount:num | is_deleted:bool | is_open:bool |
| ----------- | ---------- | --------------- | ------------ |
| 1           | 100        | false           | true         |
| 2           | 200        | false           | true         |
| 3           | 50         | false           | false        |
| 4           | 1000       | true            | true         |

## Model frp.malloy

```malloy
##! experimental.persistence

source: t is orders_pg.table('public.frp_deals')

#@ persist name="frp_deals" storage=lake
source: deals is t -> { select: * } extend {
  where: not is_deleted
}

source: pipeline_now is deals extend { where: is_open }
source: deals_alias is deals
source: deals_wide is deals -> { select: * }

#@ persist name="frp_x" storage=lake
source: x is pipeline_now -> { aggregate: total is amount.sum() }

#@ persist name="frp_y" storage=lake
source: y is deals_alias -> { aggregate: total is amount.sum() }

#@ persist name="frp_z" storage=lake
source: z is deals -> { aggregate: total is amount.sum(), n is count() }

#@ persist name="frp_w" storage=lake
source: w is deals_wide -> { aggregate: total is amount.sum() }
```

## Publish

expect binding: deals -> lake
expect upstreams: x -> reused
expect upstreams: y -> reused
expect upstreams: z -> reused
expect upstreams: w -> reused

## Query x

```malloy
run: x -> { select: total }
```

Expect:

| total:num |
| --------- |
| 300       |

## Query y

```malloy
run: y -> { select: total }
```

Expect:

| total:num |
| --------- |
| 350       |

## Query z

`z` differs from `y` in shape on purpose: a rename is its base, so a `y` and a
`z` with the same query would be one table under two names.

```malloy
run: z -> { select: total, n }
```

Expect:

| total:num | n:num |
| --------- | ----- |
| 350       | 3     |

## Query w

```malloy
run: w -> { select: total }
```

Expect:

| total:num |
| --------- |
| 350       |

## Operator lake

The parent's table holds the deleted deal: the filter is a reading, not a
property of the rows.

```sql
SELECT count(*) AS n FROM lake.frp_deals;
```

Expect:

| n:int |
| ----- |
| 4     |
