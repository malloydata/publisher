---
id: a-source-reading-its-destinations-own-catalog-builds-into-it
tags: security, storage
package: scb
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A source that reads its destination's own catalog builds into it, and only Malloy's SQL runs there

`security-cannot-build-from-lake` holds for a lake a model cannot name. This is the
other case: a connection the model CAN name that reaches the destination's own
catalog. Here that is `lake_probe`, which every environment has and which points at
the same catalog and storage as the `lake` destination under a different name.

Such a source is built with a plain CTAS on one read-write attach of the catalog,
with no passthrough and no egress. The publisher decides "same catalog" by what
locates the catalog (host, port, database, metadata schema, data path), never by
name, so the two names here are the point. The two names also mean the compiled SQL
qualifies its table with the CONNECTION's name, which the build session has to
resolve.

The invariant `security-cannot-build-from-lake` states still stands, in its own words:
no SQL its author wrote runs on a read-write lake session. So a persist over
`lake_probe.sql(…)` with `storage=lake` is refused, while a persist over
`lake_probe.table(…)`, all of whose SQL Malloy wrote, builds.

## Publisher

- PERSIST_STORAGE_MODE: on

## Operator lake

The table a file load would have put in the store.

```sql
CREATE TABLE main.scb_events AS
SELECT * FROM (VALUES (1, 'click'), (2, 'click'), (3, 'view')) AS t(id, kind)
```

## Model scb/scb.malloy

```malloy
##! experimental.persistence

source: events is lake_probe.table('lake_probe.main.scb_events')

#@ persist name="scb_by_kind" storage=lake
source: by_kind is events -> {
  group_by: kind
  aggregate: n is count()
}
```

## Model scbsql/scbsql.malloy

```malloy
##! experimental.persistence

#@ persist name="scbsql_out" storage=lake
source: by_kind_sql is lake_probe.sql('SELECT kind FROM lake_probe.main.scb_events') -> {
  group_by: kind
}
```

## Publish scb

expect binding: by_kind -> lake

## Query by kind

```malloy
run: by_kind -> { select: kind, n; order_by: kind asc }
```

Expect:

| kind  | n |
| ----- | - |
| click | 2 |
| view  | 1 |

## Operator lake

Change the store's table. A query recomputed live would now count two views.

```sql
INSERT INTO main.scb_events VALUES (4, 'view')
```

## Query by kind (again)

The unchanged count proves the answer came from the built table, not the live one.

Expect:

| kind  | n |
| ----- | - |
| click | 2 |
| view  | 1 |

## Build refused scbsql

cites: carries SQL its author wrote
