# Connections

Publisher uses **connections** to reach databases and query engines. Connections are defined per-environment in `publisher.config.json`; each one has a unique `name` and a `type` (`bigquery`, `snowflake`, `postgres`, `mysql`, `duckdb`, `trino`, etc.) plus type-specific configuration under a matching `*Connection` key.

For full setup details per connection type, see [docs.malloydata.dev/documentation/user_guides/publishing/connections](https://docs.malloydata.dev/documentation/user_guides/publishing/connections).

## Per-package DuckDB sandboxes

Each loaded package automatically gets its own DuckDB connection named `duckdb`. These per-package sandboxes are how the bundled samples (`ecommerce`, `imdb`, `faa`) query the Parquet/CSV files in the sample repositories without needing any user-defined connection.

You do not have to declare these sandboxes — they're created on package load.

## Environment-level DuckDB connections

You can also declare a top-level DuckDB connection at the environment level. Publisher intentionally exposes only data-source intent for these — database files, working directories, filesystem/network policy, extension loading, temp directories, and resource knobs are all owned by Publisher. The only configuration available is **attached databases**, where you declare foreign databases (BigQuery, Snowflake, Postgres, GCS, S3, Azure) that the DuckDB instance should `ATTACH` so queries can reference them.

An env-level DuckDB connection must declare at least one attached database. If you don't need to attach any foreign databases, you don't need to declare an env-level DuckDB connection at all — each loaded package already gets a per-package `duckdb` sandbox automatically (see above), which covers the plain in-memory use case.

## Connection naming rules

A few names are reserved or have special meaning. Picking the wrong name causes a clear error at server startup:

### `duckdb` (reserved)

You cannot define a top-level connection named `duckdb`. The name is claimed by the per-package sandbox described above. Publisher errors at startup:

```
Connection name 'duckdb' is reserved for per-package sandboxes. Choose a different name
for environment-level DuckDB connections (e.g. 'shared_duckdb').
```

Use any other name for an environment-level DuckDB connection.

### Uniqueness within an environment

Connection names must be unique within a single environment. Duplicate names after the first are silently ignored (later definitions don't override earlier ones), so prefer distinct, descriptive names.

## Publisher proxy connections (`type: "publisher"`)

A `publisher` connection does not talk to a warehouse directly. Instead it
**proxies SQL to a remote Publisher dataplane** (e.g. a hosted Credible
environment), which runs the query against its own connection and returns the
rows. This is the local-dev authoring loop: run a local Publisher with
`--watch-env` to serve a package's `public/` app with live-reload, while queries
proxy to your real remote connection — no need to replicate warehouse
credentials locally.

It is the same connection type the Malloy CLI and the VS Code extensions use.

> **Disabled by default.** Because a `publisher` connection makes the server
> issue outbound HTTP requests to a configured `connectionUri`, it is a
> server-side request forgery (SSRF) surface in a hosted, multi-tenant
> deployment. The type is therefore **denied unless explicitly enabled**: set
> the environment variable `PUBLISHER_ALLOW_PROXY_CONNECTIONS=true` on the
> server process. Enable it only for trusted local `--watch-env` authoring —
> never in a shared/hosted deployment. With the flag unset, a `publisher`
> connection fails at config load with an actionable error.

To use it from the server, set `PUBLISHER_ALLOW_PROXY_CONNECTIONS=true` and add a
`publisher` connection block to the environment's `connections` in
`publisher.config.json`:

```json
{
  "name": "analytics",
  "type": "publisher",
  "publisherConnection": {
    "connectionUri": "https://org.data.credibledata.com/api/v0/environments/proj/connections/analytics",
    "accessToken": "<jwt>"
  }
}
```

- `connectionUri` (**required**) — the full URI of the remote connection.
- `accessToken` (optional) — Bearer token for the remote dataplane.

The remote dataplane owns authentication, access control, and read-only
enforcement; the proxy itself does not reject writes. A missing `connectionUri`
fails at startup with an actionable error rather than a generic
`Unsupported connection type`.

**Known limitation:** the `accessToken` is user-scoped and short-lived. The
server uses the token as configured and does not refresh it, so a long-running
`--watch` session can outlive the token. Token refresh/expiry is owned by the
CLI/extension today; re-issue the token and restart if queries start failing
auth.

## Example: mixed connections

```json
{
  "environments": [
    {
      "name": "my-env",
      "packages": [...],
      "connections": [
        {
          "name": "shared_duckdb",
          "type": "duckdb",
          "duckdbConnection": {
            "attachedDatabases": [
              {
                "name": "warehouse_pg",
                "type": "postgres",
                "postgresConnection": {
                  "host": "warehouse.example.com",
                  "port": 5432,
                  "userName": "publisher",
                  "databaseName": "analytics"
                }
              }
            ]
          }
        },
        {
          "name": "warehouse_bq",
          "type": "bigquery",
          "bigqueryConnection": { "defaultProjectId": "my-gcp-project" }
        }
      ]
    }
  ]
}
```

The package's own DuckDB sandbox (`duckdb`) remains available alongside `shared_duckdb` and `warehouse_bq`.
