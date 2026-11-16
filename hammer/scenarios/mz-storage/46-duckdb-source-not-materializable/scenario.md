---
id: duckdb-source-not-materializable
tags: eligibility, build-control
package: dk
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A DuckDB (local) source can't be materialized into storage

The storage tier builds by native per-engine **query-passthrough**
(`postgres_query` / `bigquery_query` / `snowflake_query`) — the source warehouse
computes and only results cross into the store. That passthrough is defined only
for the foreign engines (BigQuery, Snowflake, Postgres). A DuckDB
source is already local to the build engine, so there is nothing to push down and
no passthrough for it; the build refuses rather than silently doing the wrong
thing.

This pins that guardrail: a `#@ persist storage=lake` source reading a declared
DuckDB connection is refused when the build is planned
(`storage_source_unsupported`), before anything runs, and the build fails naming
the connection and the fix. (A genuine cross-dialect *success*
case — Postgres alongside BigQuery/Snowflake — needs foreign-engine credentials
the harness doesn't wire, so it's out of this harness's reach; only Postgres is
available here.)

## Connection dk (type=duckdb)

A DuckDB source connection, declared via the `## Connection` handler.

## Publisher

- PERSIST_STORAGE_MODE: on

## Model dk.malloy

```malloy
##! experimental.persistence

source: dk_orders is dk.sql("""
  SELECT 'US' AS region, 100 AS amount
  UNION ALL SELECT 'EU', 200
""")

#@ persist name="dk_daily" storage=lake
source: dk_daily is dk_orders -> {
  group_by: region
  aggregate: total is amount.sum()
}
```

## Build refusals

The plan examines the source and refuses it under the storage tier, with the
reason.

Expect:

| source   | tier    | reason                     |
| -------- | ------- | -------------------------- |
| dk_daily | storage | storage_source_unsupported |

## Build refused

A DuckDB source cannot be passed through into the store, so materializing it
into `storage=lake` is refused.

cites: its connection 'dk' is a 'duckdb' connection, which cannot be passed through to a 'storage=' build

## Build refused

The refusal names the fix.

cites: drop 'storage=' to build it in its own connection

## Query the refused source

Nothing was built and nothing is bound, so the query is answered live from the
DuckDB connection. (A live answer with no binding carries no `servedFrom`; that
tag only names a bound tier or its fallback.)

```malloy
run: dk_daily -> { select: region, total; order_by: region asc }
```

Expect:

| region | total |
| ------ | ----- |
| EU     | 200   |
| US     | 100   |
