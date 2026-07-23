---
id: duckdb-source-not-materializable
tags: eligibility, build-control
package: dk
---

# A DuckDB (local) source can't be materialized into storage

The storage tier builds by native per-engine **query-passthrough**
(`postgres_query` / `bigquery_query` / `snowflake_query`) — the source warehouse
computes and only results cross into the store. That passthrough is defined only
for the foreign engines (BigQuery, Snowflake, Postgres). A DuckDB / DuckLake
source is already local to the build engine, so there is nothing to push down and
no passthrough for it; the build refuses rather than silently doing the wrong
thing.

This pins that guardrail: a `#@ persist storage=lake` source reading a declared
DuckDB connection is refused at build time. (A genuine cross-dialect *success*
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

## Build refused

A DuckDB source has no query-passthrough into the store, so materializing it into
`storage=lake` is refused.

cites: passthrough
