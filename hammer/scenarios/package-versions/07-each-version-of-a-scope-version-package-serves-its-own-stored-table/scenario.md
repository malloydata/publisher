---
id: each-version-of-a-scope-version-package-serves-its-own-stored-table
tags: package-versions, serve-correctness, durability
package: owned
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Under scope: version, each version builds and serves a stored table of its own, across a restart

A package whose materialization scope is `version` gives each published version
tables of its own: a self-assigned name gains the version as a suffix, each
version's build writes its own table, and each version serves from its own
table, including after a restart, when serving is re-established from the
store per version.

## Publisher

- PERSIST_STORAGE_MODE: on

## Model anchor/anchor.malloy

```malloy
source: anchor is orders_pg.sql("SELECT 0 as n")
```

## Version owned@1.0.0 (scope=version)

```malloy
##! experimental.persistence

source: base is orders_pg.sql("SELECT 1 as n")

#@ persist name="summary" storage=lake
source: summary is base -> { group_by: n }
```

## Version owned@1.1.0 (scope=version)

```malloy
##! experimental.persistence

source: base is orders_pg.sql("SELECT 2 as n")

#@ persist name="summary" storage=lake
source: summary is base -> { group_by: n }
```

## Publish (version=1.0.0)

expect binding: summary -> lake

## Publish

expect binding: summary -> lake

## Operator lake

One table per version, each named for it.

```sql
SELECT table_name FROM information_schema.tables
WHERE table_name LIKE 'summary%' ORDER BY table_name
```

Expect:

| table_name      |
| --------------- |
| summary__v1_0_0 |
| summary__v1_1_0 |

## Query older (version=1.0.0)

```malloy
run: summary -> { select: n }
```

Expect:

| n |
| - |
| 1 |

servedFrom: storage

## Query latest

```malloy
run: summary -> { select: n }
```

Expect:

| n |
| - |
| 2 |

servedFrom: storage

## Restart

## Query older (again, version=1.0.0)

Re-established from the store for this version, from this version's table.

Expect:

| n |
| - |
| 1 |

servedFrom: storage

## Query latest (again)

Expect:

| n |
| - |
| 2 |

servedFrom: storage
