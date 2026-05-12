# Connections

Publisher uses **connections** to reach databases and query engines. Connections are defined per-environment in `publisher.config.json`; each one has a unique `name` and a `type` (`bigquery`, `snowflake`, `postgres`, `mysql`, `duckdb`, `trino`, etc.) plus type-specific configuration under a matching `*Connection` key.

For full setup details per connection type, see [docs.malloydata.dev/documentation/user_guides/publishing/connections](https://docs.malloydata.dev/documentation/user_guides/publishing/connections).

## Per-package DuckDB sandboxes

Each loaded package automatically gets its own DuckDB connection named `duckdb`. These per-package sandboxes are how the bundled samples (`ecommerce`, `imdb`, `faa`) query the Parquet/CSV files in the sample repositories without needing any user-defined connection.

You do not have to declare these sandboxes — they're created on package load.

## Environment-level DuckDB connections

You can also declare a top-level DuckDB connection at the environment level. Publisher intentionally exposes only data-source intent for these — database files, working directories, filesystem/network policy, extension loading, temp directories, and resource knobs are all owned by Publisher. The only configuration available is **attached databases**, where you declare foreign databases (BigQuery, Snowflake, Postgres, GCS, S3, Azure) that the DuckDB instance should `ATTACH` so queries can reference them.

`attachedDatabases` may be an empty array — in that case Publisher creates a plain DuckDB instance with no foreign attachments. This is useful for ad-hoc analytical queries against in-memory tables or local files referenced via SQL.

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
