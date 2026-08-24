---
name: malloy-connections
description: Author and debug the database connections a Publisher environment serves, in publisher.config.json. Use when pointing a package at BigQuery, Snowflake, Postgres or another warehouse, when a connection will not load, when a server comes up serving nothing, or when deciding where a credential should live. Covers the reserved duckdb name, the per-package sandbox, environment variable substitution, and the failures that report no error.
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Connections

A Publisher package reads its rows from a connection. There are two kinds and they behave
differently, which is the source of most confusion here.

- **The per-package DuckDB sandbox.** Every loaded package gets one automatically, named `duckdb`.
  A model reaches it as `duckdb.table('data/orders.csv')`, relative to the package directory. A
  package that reads local CSV, Parquet, JSON or XLSX files needs **no connection configuration at
  all**. Do not add one.
- **Environment-level connections.** Declared in `publisher.config.json` under an environment's
  `connections` array, and shared by every package in that environment. This is how a package
  reaches a warehouse. A model reaches it by name, `my_warehouse.table('public.orders')`.

## The shape

```json
{
  "environments": [
    {
      "name": "default",
      "packages": [{ "name": "analytics", "location": "./analytics" }],
      "connections": [
        {
          "name": "warehouse",
          "type": "postgres",
          "postgresConnection": {
            "host": "db.example.com",
            "port": 5432,
            "databaseName": "analytics",
            "userName": "reader",
            "password": "${MALLOY_WAREHOUSE_PASSWORD}"
          }
        }
      ]
    }
  ]
}
```

Three parts, always: a `name`, a `type`, and one payload object whose key is the type plus
`Connection`. The supported types are `postgres`, `bigquery`, `snowflake`, `trino`, `databricks`,
`mysql`, `duckdb`, `motherduck`, `ducklake` and `publisher`.

## Rules that will bite you

### The name `duckdb` is reserved

It belongs to the per-package sandbox. An environment-level connection using it is refused **before**
the type is even looked at, so it applies to a Postgres connection called `duckdb` exactly as much as
to a DuckDB one.

The whole **environment** is then skipped, not just that connection. On a config with one
environment, that means the server starts, reports ready, and serves nothing. This one is at least
diagnosed properly: the reason appears in `loadErrors` and on the packages endpoint verbatim. Name it
something else (`warehouse`, `shared_duckdb`, the warehouse type).

### An environment-level DuckDB connection cannot point at a local file

It accepts only `attachedDatabases`, for attaching a remote database (BigQuery, Snowflake, Postgres,
GCS, S3, Azure) through DuckDB. There is no field for a local `.duckdb` file and no way to add one.
If you want local files, you do not want this connection at all: you want the per-package sandbox,
which is already there.

### Credentials belong in the environment, not the file

Any string value in `publisher.config.json` may be a `${VARIABLE}` reference, substituted from the
process environment when the config is read. Use it for every secret.

**The pattern is uppercase-only**: `${MALLOY_WAREHOUSE_PASSWORD}` works, `${malloy_warehouse_password}`
does not. A lowercase reference is not an error. It is not substituted either. It travels to the
driver as those literal characters and fails as an authentication error that names nothing.

### An unset variable produces a healthy-looking server that serves nothing

This is the one worth reading twice, because every instinct about it is wrong.

If a `${VAR}` in the config is not set in the environment, the server does **not** refuse to start.
The environment simply does not load, so the server boots, binds its ports, prints
`PUBLISHER_READY`, and serves nothing:

```
environments=0 packages=0 load_errors=<see below>
```

**How that is reported depends on the server version, so diagnose from the behaviour rather than
from the number.** Measured on 0.0.250, `load_errors` reads `0` and the variable name appears
nowhere in the output, the underlying error reaching the log as an empty object: the field you would
check to find out what went wrong is empty, precisely in the case it exists to describe. A pending
change makes the server name the unset variable in `loadErrors` instead, which is a strict
improvement. Both are the same underlying failure.

So the durable rule, true on either: **a Publisher that comes up reporting ready with no
environments and no packages is very often an unset environment variable.** Check that before
anything else, whatever `load_errors` says. `grep` the config for `${` and confirm every name it
finds is exported in the shell that starts the server.

## Per-dialect fields

Only the common ones. The server's OpenAPI spec at `/api-doc.yaml` on a running Publisher is the
complete reference.

| type | identity fields | credential |
| --- | --- | --- |
| `postgres` | `host`, `port`, `databaseName`, `userName` | `password`, or a whole `connectionString` |
| `bigquery` | `defaultProjectId`, `billingProjectId`, `location` | `serviceAccountKeyJson`, **or omit it entirely** |
| `snowflake` | `account`, `username`, `warehouse`, `database`, `schema`, `role` | `password`, or `privateKey` plus `privateKeyPass` |

BigQuery is the one worth knowing: `serviceAccountKeyJson` is optional, and leaving it out falls
through to Application Default Credentials. So `gcloud auth application-default login` once, and the
config needs no credential in it at all. That is the cleanest option available on any dialect, and it
is the default the scaffolder writes.

When you do supply `serviceAccountKeyJson`, it is the **contents** of the key file as a JSON string,
not a path to it. Put it in an environment variable and reference it.

## Naming a connection so it stays portable

Publisher itself accepts almost any name. Credible's `cred add connection` does not: it validates
each name against `^[a-zA-Z_][a-zA-Z0-9_]+$`. On a name that fails it logs an error naming that
connection and **carries on to the next one** rather than failing. So a connection called
`my-warehouse` works perfectly locally, does not arrive after you publish, and the command still
exits successfully having created the others. The error is there to be read, but it is one line in
the output of a run that otherwise worked, which is an easy thing to miss and a hard thing to connect
back to a name you chose weeks earlier.

Stay inside the intersection and the question never comes up: letters, digits and underscores, at
least two characters, starting with a letter or underscore, never `duckdb`. Write `my_warehouse`, not
`my-warehouse`.

## Testing a connection

On a running server, `POST /api/v0/connections/test` validates a connection configuration without
adding it to anything. The body is one connection object, exactly as it appears in the config.

```bash
curl -s -X POST http://localhost:4000/api/v0/connections/test \
  -H 'Content-Type: application/json' \
  -d '{"name":"warehouse","type":"postgres","postgresConnection":{"host":"db.example.com","port":5432,"databaseName":"analytics","userName":"reader","password":"'"$MALLOY_WAREHOUSE_PASSWORD"'"}}'
```

Note this takes the resolved value, not a `${VAR}` reference: it is an API call, so nothing
substitutes for you.

## Finding something to model

Once a connection loads, use `malloy_searchDatabaseSchema` to discover what is in it. It takes a
plain-English description ("orders", "anything with revenue") and returns matching tables with the
`source:` line to start from. It returns names and types only, never row values. This is the right
first step against a warehouse you have not seen; do not guess table names into a model.

Over REST the equivalent is `GET /api/v0/environments/{env}/connections/{conn}/schemas`, then
`.../schemas/{schema}/tables`, with the ranking left to you.

## Diagnosing a connection that will not load

In order, because each step rules out the one below it.

1. `GET /api/v0/status`. If `operationalState` is not `serving`, nothing else matters yet.
2. Check `loadErrors` in that same response. When it names your package or environment, the reason
   is there verbatim, and you are done.
3. **If there are no environments and no load errors, suspect an unset `${VAR}`** (see above). This
   is the case with no diagnostic, so it has to be checked by inspection rather than by reading an
   error.
4. If the environment loaded but a query fails, the configuration is being read and the warehouse is
   rejecting it. Test the credential outside Publisher (`psql`, `bq`, `snowsql`) before changing the
   config.

### Postgres, `PGSSLMODE`, and why `sslmode` is not the fix

One Postgres failure has a cause nowhere near where it appears. If `PGSSLMODE` is set in the
environment the server runs in, Publisher stops using your structured `host`/`port`/`databaseName`
fields directly and builds a connection string with `sslmode` set from that variable. Against a
Postgres with SSL off, the connection then fails with:

```
Error fetching schema for public.orders: The server does not support SSL connections
```

Measured on 0.0.250 against a local Postgres with `ssl` off: unset `PGSSLMODE` loads and queries
fine; `PGSSLMODE=allow` gives `packages=0 load_errors=1` and that error.

**Do not reach for the connection's own `sslmode` field.** It is only valid on a connection reached
through a `proxy`, and on a direct connection it is rejected outright, taking the whole environment
with it:

```
sslmode is only supported for proxied connections (direct connections use the deployment PGSSLMODE)
```

So adding it turns a broken package into a broken environment. The two things that do work are
unsetting `PGSSLMODE` for the server process, or giving the connection a full `connectionString`,
which Publisher returns verbatim without injecting anything.

This only bites when `PGSSLMODE` is set. A plain local Postgres with it unset, which is the ordinary
case, needs none of this.

## A note on what `${VAR}` does and does not protect

It keeps the secret out of the config file, and out of your shell history. It does not make the
credential private in general: a running Publisher returns connection configuration, with values
already substituted, over its unauthenticated REST API. Keep the API on localhost, or behind a
gateway that authenticates, and treat "not in the file" as the scope of what the indirection buys.
