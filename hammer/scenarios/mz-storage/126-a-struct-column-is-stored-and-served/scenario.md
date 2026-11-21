---
id: a-struct-column-is-stored-and-served
tags: materialize, serve-correctness, data-fidelity
package: mzr
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A `#(materialize) storage=` source over a struct column stores the struct and serves from it

`events` is the shape of a GA4-style export: a row per event with a `device`
struct, a `geo` struct holding a struct, and a `params` struct holding a list,
the way a BigQuery `STRUCT` arrives. Its table holds each as a struct, and the
source is served from it: a query that reads no struct, one that groups by a
struct's field, one through a struct inside a struct, and one that unnests a
struct's list.

A build used to project the stored table to the source's atomic fields and its
lists, and Malloy types a struct column as a source of its own, so the table held
only `event_id`. The served copy then found declared columns the table lacked and
sent every query on `events` live, `count()` included.

Postgres has no column Malloy reads as a struct, so the rows are in the
destination's own catalog, read through `lake_probe`, as
`a-source-reading-its-destinations-own-catalog-builds-into-it` does. The rows
added after the build prove which tier answered: every query keeps answering the
stored rows.

## Publisher

- PERSIST_STORAGE_MODE: on
- PUBLISHER_MATERIALIZE: true

## Operator lake

The table a GA4-style export would have put in the store.

```sql
CREATE TABLE main.mzr_events AS
SELECT * FROM (VALUES
  (1, {'category': 'mobile', 'os': 'ios'}, {'country': 'US', 'loc': {'city': 'nyc'}}, {'tags': ['a', 'b'], 'src': 'ads'}),
  (2, {'category': 'desktop', 'os': 'mac'}, {'country': 'US', 'loc': {'city': 'sf'}}, {'tags': ['b'], 'src': 'mail'}),
  (3, {'category': 'mobile', 'os': 'android'}, {'country': 'FR', 'loc': {'city': 'paris'}}, {'tags': ['a'], 'src': 'ads'})
) AS t(event_id, device, geo, params)
```

## Model mzr.malloy

```malloy
#(materialize) storage="lake"
source: events is lake_probe.table('lake_probe.main.mzr_events') extend {
  measure: n is count()
}
```

## Publish

expect binding: events -> lake

## Operator lake

A fourth event after the build. A query recomputed live would count it.

```sql
INSERT INTO main.mzr_events VALUES
  (4, {'category': 'mobile', 'os': 'ios'}, {'country': 'DE', 'loc': {'city': 'berlin'}}, {'tags': ['c'], 'src': 'ads'})
```

## Query events

A query that reads no struct: the stored three events, not the store's four.

```malloy
run: events -> { aggregate: n }
```

servedFrom: storage

Expect:

| n:num |
| ----- |
| 3     |

## Query by category

A group-by on a struct's field, from the table.

```malloy
run: events -> {
  group_by: device.category
  aggregate: n
  order_by: category asc
}
```

servedFrom: storage

Expect:

| category | n:num |
| -------- | ----- |
| desktop  | 1     |
| mobile   | 2     |

## Query by city

A struct inside a struct, filtered and grouped, from the table: no `berlin`.

```malloy
run: events -> {
  where: geo.country != 'FR'
  group_by: geo.loc.city
  aggregate: n
  order_by: city asc
}
```

servedFrom: storage

Expect:

| city | n:num |
| ---- | ----- |
| nyc  | 1     |
| sf   | 1     |

## Query by tag

A struct's list, unnested, from the table: no `c`.

```malloy
run: events -> {
  group_by: tag is params.tags.each
  aggregate: events is count()
  order_by: tag asc
}
```

servedFrom: storage

Expect:

| tag | events:num |
| --- | ---------- |
| a   | 2          |
| b   | 2          |
