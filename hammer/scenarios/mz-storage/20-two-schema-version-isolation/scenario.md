---
id: two-schema-version-isolation
tags: orchestration, isolation
package: mzv1
---

# Two versions, two schemas: isolated snapshots of the same source

Two deployments of the SAME logical source (identical Malloy, identical SQL)
persist into two different DuckLake schemas — `v1.daily` and `v2.daily` — like a
blue/green or per-tenant rollout. This proves two things the single-source
scenarios can't:

1. **No collision at the same content address.** The two persist sources have
   identical SQL over the same connection, so their content address
   (`sourceEntityId`) is the same — but `name=` puts them in different schemas,
   so they materialize as two DISTINCT physical tables, not one shared/deduped
   table.
2. **Independent snapshots.** Each schema captures the source at its own build
   instant. We build `v1` first, mutate the shared source, then build `v2` — and
   `v1` keeps serving its original snapshot while `v2` serves the new data. One
   version's rebuild (or the source changing underneath) never disturbs the
   other.

The two packages read the SAME source table (`mz_orders`), so the only thing
separating them is the schema in `name=`.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.mz_orders

| order_id:int | order_date:date | region:text | amount:num |
| ------------ | --------------- | ----------- | ---------- |
| 1            | 2026-01-01      | US          | 100        |
| 2            | 2026-01-01      | US          | 50         |
| 3            | 2026-01-02      | EU          | 200        |
| 4            | 2026-01-02      | US          | 25         |

## Model mzv1/mzv1.malloy

Version 1 persists into schema `v1`.

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.mz_orders')

#@ persist name="v1.daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}
```

## Model mzv2/mzv2.malloy

Version 2 is byte-for-byte the same source, but persists into schema `v2`.

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.mz_orders')

#@ persist name="v2.daily" storage=lake
source: daily is orders -> {
  group_by: order_date
  aggregate: total_amount is amount.sum()
}
```

## Operator lake

The operator provisions both schemas on the destination (the tier does not
auto-create them) — via its own read-write DuckLake client, external to the
publisher.

```sql
CREATE SCHEMA IF NOT EXISTS v1;
CREATE SCHEMA IF NOT EXISTS v2;
```

## Publish mzv1

Version 1 materializes into `lake.v1.daily`, capturing the initial source.

expect binding: daily -> lake

## Query v1 daily (pkg=mzv1)

```malloy
run: daily -> { select: order_date, total_amount; order_by: order_date asc }
```

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 225          |

## Mutate orders_pg.mz_orders

Append a large order to 2026-01-01, changing the shared source AFTER v1 built.

| order_id:int | order_date:date | region:text | amount:num |
| ------------ | --------------- | ----------- | ---------- |
| 99           | 2026-01-01      | US          | 1000       |

## SQL shared source really changed

```sql
SELECT order_date, sum(amount) AS total FROM mz_orders GROUP BY order_date ORDER BY order_date;
```

Expect:

| order_date | total:num |
| ---------- | --------- |
| 2026-01-01 | 1150      |
| 2026-01-02 | 225       |

## Publish mzv2

Version 2 materializes into `lake.v2.daily` NOW, capturing the mutated source —
a distinct table in a distinct schema.

expect binding: daily -> lake

## Query v2 daily (pkg=mzv2)

Version 2's snapshot reflects the mutation (1150 for 2026-01-01).

```malloy
run: daily -> { select: order_date, total_amount; order_by: order_date asc }
```

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 1150         |
| 2026-01-02 | 225          |

## Query v1 daily (again, pkg=mzv1)

Isolation: version 1 STILL serves its original snapshot (150) — untouched by the
source mutation and by v2's build. Two schemas, two independent snapshots.

Expect:

| order_date | total_amount |
| ---------- | ------------ |
| 2026-01-01 | 150          |
| 2026-01-02 | 225          |
