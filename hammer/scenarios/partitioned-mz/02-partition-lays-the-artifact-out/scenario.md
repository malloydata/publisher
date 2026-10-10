---
id: partition-lays-the-artifact-out
tags: build-correctness, partitioning
package: pla
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A declared partition lays the artifact out by that column

`#@ persist partition="org_id"` writes the stored table as one directory per
distinct `org_id`, so a caller's equality term reads only the files their own
value names.

Partitioning is a LAYOUT and carries no isolation: the answers are identical
whether or not it is declared, because every stripped term is re-applied at read
either way (`tenant-scoped-source-serves-per-caller` is that rule). What a
partition changes is how much of the artifact has to be opened to produce them.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.pla_orders

| order_id:int | org_id:int | amount:num |
| ------------ | ---------- | ---------- |
| 1            | 1          | 100        |
| 2            | 2          | 60         |
| 3            | 1          | 200        |
| 4            | 3          | 15         |

## Model pla.malloy

```malloy
##! experimental.persistence
##! experimental.givens

given:
  ORG_ID :: number is 1

source: raw is orders_pg.sql('SELECT order_id, org_id, amount FROM public.pla_orders')

#@ persist name="pla_orders" storage=lake partition="org_id"
source: orders is raw -> { select: * } extend {
  where: org_id = $ORG_ID
}
```

## Publish

expect binding: orders -> lake

## Operator lake

One directory per distinct value of the partition column — three orgs, three
directories. An unpartitioned build writes its files directly under the table
and matches none of these, so an empty result here is the layout silently not
being applied.

```sql
SELECT DISTINCT regexp_extract(data_file, 'org_id=([0-9]+)', 1) AS org_dir
FROM ducklake_list_files('lake', 'pla_orders')
ORDER BY org_dir;
```

Expect:

| org_dir:text |
| ------------ |
| 1            |
| 2            |
| 3            |

## Operator lake

A layout separates the files; it never filters. All four rows are in the
artifact, across those three directories.

```sql
SELECT count(*) AS n FROM lake.pla_orders;
```

Expect:

| n:int |
| ----- |
| 4     |

## Query org 1 total

The answer a partitioned artifact gives is the answer an unpartitioned one
gives. Only the number of files opened differs.

```malloy
run: orders -> { aggregate: total is amount.sum() }
```

givens: ORG_ID=1
servedFrom: storage

Expect:

| total:num |
| --------- |
| 300       |

## Query org 3 total

```malloy
run: orders -> { aggregate: total is amount.sum() }
```

givens: ORG_ID=3
servedFrom: storage

Expect:

| total:num |
| --------- |
| 15        |
