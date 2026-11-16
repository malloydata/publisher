---
id: a-mixed-case-or-reserved-persist-name-needs-no-quoting
tags: naming, serve-correctness
package: pn
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A mixed-case or reserved-word `#@ persist name=` builds and serves, unquoted

Persist names are plain identifier paths, and Publisher quotes every segment for the
target warehouse itself (`a-quoted-persist-name-is-refused`). So an author never needs
to quote inside the name (the value itself is a quoted string, `name="OrdersDaily"`): a mixed-case name keeps its case, and a reserved word is a valid table name.
One of each here, `OrdersDaily` on the `storage=` tier and `order` colocated in the
source warehouse (their queries differ, so each is its own table). Each is asserted on the table actually written (by its exact,
quoted name) and on a stale answer after the source changes, which holds only if the
serve side found that table.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.pn_orders

| order_id:int | order_date:date | amount:num |
| ------------ | --------------- | ---------- |
| 1            | 2026-01-01      | 100        |
| 2            | 2026-01-01      | 50         |

## Model pn.malloy

```malloy
##! experimental.persistence

source: orders is orders_pg.table('public.pn_orders')

#@ persist name="OrdersDaily" storage=lake
source: mixed is orders -> {
  group_by: order_date
  aggregate: total is amount.sum()
}

#@ persist name="order"
source: reserved is orders -> {
  group_by: order_date
  aggregate: total is amount.sum(), n is count()
}
```

## Build targets

Expect:

| source   | writes      |
| -------- | ----------- |
| mixed    | OrdersDaily |
| reserved | order       |

## Publish pn

expect binding: mixed -> lake

## Operator lake

The `storage=` table is named `OrdersDaily`, case preserved: this quoted lookup fails
if any path folded it.

```sql
SELECT count(*) AS n FROM lake."OrdersDaily";
```

Expect:

| n:int |
| ----- |
| 1     |

## SQL reserved table

The colocated table is the reserved word itself, in the source warehouse.

```sql
SELECT count(*) AS n FROM "order";
```

Expect:

| n:int |
| ----- |
| 1     |

## Mutate orders_pg.pn_orders

```sql
INSERT INTO pn_orders VALUES (3, '2026-01-01', 1000);
```

## Query mixed

Stale (150) means the `storage=` table was read; 1150 would mean a live fallback.

```malloy
run: mixed -> { select: order_date, total }
```

servedFrom: storage

Expect:

| order_date | total |
| ---------- | ----- |
| 2026-01-01 | 150   |

## Query reserved

Stale (150) means the colocated table `order` was read; 1150 would mean the serve side
never found it.

```malloy
run: reserved -> { select: order_date, total, n }
```

Expect:

| order_date | total | n:int |
| ---------- | ----- | ----- |
| 2026-01-01 | 150   | 2     |
