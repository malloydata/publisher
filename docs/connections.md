# Connections

Publisher uses **connections** to reach databases and query engines. Connections are defined per-environment in `publisher.config.json`; each one has a unique `name` and a `type` (`bigquery`, `snowflake`, `postgres`, `mysql`, `duckdb`, `trino`, etc.) plus type-specific configuration under a matching `*Connection` key.

For full setup details per connection type, see [docs.malloydata.dev/documentation/user_guides/publishing/connections](https://docs.malloydata.dev/documentation/user_guides/publishing/connections).

## Per-package DuckDB sandboxes

Each loaded package automatically gets its own DuckDB connection named `duckdb`. These per-package sandboxes are how the bundled samples (`ecommerce`, `imdb`, `faa`) query the Parquet/CSV files in the sample repositories without needing any user-defined connection.

You do not have to declare these sandboxes — they're created on package load.

## Connection naming rules

A few names are reserved or have special meaning. Picking the wrong name causes a clear error at server startup:

### `duckdb` (reserved)

You cannot define a top-level connection named `duckdb`. The name is claimed by the per-package sandbox described above. Publisher errors at startup:

```
Connection name 'duckdb' is reserved for per-package sandboxes. Choose a different name
for project-level DuckDB connections (e.g. 'duckdb_main').
```

For a project-level DuckDB connection — e.g. one pointing at a shared `.duckdb` file across packages — use any other name. Common choice: `duckdb_main`.

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
          "name": "duckdb_main",
          "type": "duckdb",
          "duckdbConnection": { "databasePath": "data/warehouse.duckdb" }
        },
        {
          "name": "warehouse",
          "type": "bigquery",
          "bigqueryConnection": { "defaultProjectId": "my-gcp-project" }
        }
      ]
    }
  ]
}
```

The package's own DuckDB sandbox (`duckdb`) remains available alongside `duckdb_main` and `warehouse`.
