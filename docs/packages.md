# The package format

> What this is: what a Malloy Publisher package is on disk. The files, the `publisher.json`
> manifest fields, where a package's data comes from, and how a package gets served. For the
> server's own config file, see [configuration.md](configuration.md); for running a server, see
> [deployment.md](deployment.md).

## A package is a directory with two files

A package is a directory holding a `publisher.json` manifest and at least one `.malloy` model.
That is the whole format. Data files sit alongside the models:

```
sales/
  publisher.json        # the manifest ({} is valid)
  sales.malloy          # a Malloy model
  data/
    sales.csv           # data the model reads through the built-in duckdb connection
```

A working minimal pair:

```json
{ "name": "sales", "description": "Sales by region" }
```

```malloy
source: sales is duckdb.table('data/sales.csv') extend {
  measure: total_rows is count()

  view: by_region is {
    group_by: region
    aggregate: total_rows
  }
}
```

The manifest must exist and parse as JSON: a directory without a `publisher.json` is not a
package, and a manifest that fails to parse fails the package load. Beyond that, every field is
optional; `{}` is a valid manifest.

## The manifest: publisher.json

The fields the server reads:

| Field | Purpose |
| --- | --- |
| `name` | Conventionally the package's name, but the server never surfaces it: the registered name (config entry or API call) wins in API URLs and responses. |
| `description` | Shown in the Publisher UI and in API responses. |
| `explores` | Curates which models are discoverable. See [discovery-and-access.md](discovery-and-access.md). |
| `queryableSources` | `"declared"` (the default) or `"all"`: the query boundary. See [discovery-and-access.md](discovery-and-access.md). |
| `materialization` | Persisted-source build policy (`schedule`, `freshness`). Package root only. See [materialization.md](materialization.md). |
| `scope` | `"package"` (the default) or `"version"`. Any other value fails the package load. |

Unknown keys are ignored and preserved. The bundled examples carry a `version` field as a
convention, but nothing reads it. (One more field, `manifestLocation`, exists for orchestrated
control-plane deployments; a locally authored package never needs it.)

## Where the data comes from

Every loaded package automatically gets a DuckDB connection named `duckdb`, rooted at the package
directory. `duckdb.table('data/sales.csv')` and `duckdb.table('data/sales.parquet')` both work
with zero configuration, and relative paths resolve against the package root. It is also the
default connection, so a model that names no connection gets it.

A package cannot declare its own warehouse connection. Connections to BigQuery, Snowflake,
Postgres, and the rest are defined per environment, in the server's config; the name `duckdb` is
reserved for the per-package sandbox. See [connections.md](connections.md).

## Serving a package

There is no directory scan: a package on disk serves only once it is registered. Two main ways:

- A `{ "name": "...", "location": "..." }` entry under an environment in `publisher.config.json`.
  The `location` can be a local path (absolute, `~/`, or relative to the config file, written
  `./sales`) or a `https://github.com/...`, `gs://`, or `s3://` URL. See
  [configuration.md](configuration.md#bring-your-own-config) for the full recipe.
- On a running server, `POST /api/v0/environments/{env}/packages` with
  `{"name": "...", "location": "/absolute/path"}`. See [api-overview.md](api-overview.md). Like
  the rest of the API this endpoint is unauthenticated, so keep the server on localhost or behind
  a gateway.

## Lifecycle: the served copy

When an environment is first created, Publisher copies each configured package into its own
storage at `publisher_data/<env>/<pkg>/` (in the server root: the directory the server was
launched from, unless `--server_root` set another) and compiles it, then serves that copy.
Consequences:

- Editing your original source directory afterwards changes nothing, unless the environment runs
  in watch mode (`--watch-env <env>`), which mounts local packages in place as symlinks instead of
  copying them.
- After editing the served copy, reload the package to recompile it:
  `GET /api/v0/environments/{env}/packages/{pkg}?reload=true` over REST, or the
  `malloy_reloadPackage` MCP tool. The reload is in place, unless the package's metadata was given
  a `location` through the API (a PATCH), in which case it re-fetches from that location and
  overwrites local edits. A reload that fails to compile leaves the files alone and keeps serving
  the previous model.
- `--init` deletes `publisher_data/` and re-copies everything from the configured locations. Keep
  your source of truth outside `publisher_data/`.

The agent workflow built on this lifecycle is in [AGENTS.md](../AGENTS.md); load failures and how
to read them are in [deployment.md](deployment.md#serving-does-not-mean-everything-loaded).

## See also

- A `public/` directory of HTML pages makes the package a data app, no build step:
  [html-data-apps.md](html-data-apps.md).
- Runtime parameters and access control, declared in the models:
  [givens.md](givens.md), [authorize.md](authorize.md), [row-level-access.md](row-level-access.md).
