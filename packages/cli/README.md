# Malloy Publisher CLI

Command-line interface for managing Malloy Publisher resources.

## Installation

```bash
npm install -g @malloydata/publisher-cli
```

## Usage

```bash
# Set the Publisher server URL (optional, defaults to http://localhost:4000)
export MALLOY_PUBLISHER_URL=http://localhost:4000

# Environments, packages, connections
malloy-pub list environment
malloy-pub create environment my-environment
malloy-pub update environment my-environment --readme "Updated readme"
malloy-pub list package --environment my-environment
malloy-pub create connection --environment my-environment --file connections.json

# Materializations (build a package's persisted sources)
malloy-pub list materialization --environment my-environment --package my-package
malloy-pub materialize --environment my-environment --package my-package --wait
malloy-pub get materialization <id> --environment my-environment --package my-package
malloy-pub stop-materialization <id> --environment my-environment --package my-package
malloy-pub delete materialization <id> --environment my-environment --package my-package --drop-tables

# Build manifest (the tables a materialization produced)
malloy-pub get manifest --environment my-environment --package my-package
malloy-pub reload-manifest --environment my-environment --package my-package

# Read-only package contents
malloy-pub list model --environment my-environment --package my-package
malloy-pub get model flights.malloy --environment my-environment --package my-package
malloy-pub list notebook --environment my-environment --package my-package
malloy-pub list database --environment my-environment --package my-package
```

## Development

```bash
# Install dependencies
npm install

# Generate API client from OpenAPI spec
npm run generate:api

# Build
npm run build

# Link for local development
npm link

# Test
malloy-pub list environment --url http://localhost:4000
```

## Commands

Resource (CRUD) verbs:

- `malloy-pub list <resource>` - List resources (environment, package, connection, materialization, model, notebook, database). `list materialization` also accepts `--limit <n>` and `--offset <n>` for pagination.
- `malloy-pub get <resource> [name]` - Get resource details (environment, package, connection, materialization, model, notebook, manifest)
- `malloy-pub create <resource> [name]` - Create a resource (environment, package, connection)
- `malloy-pub update <resource> [name]` - Update a resource (environment, package, connection)
- `malloy-pub delete <resource> [name]` - Delete a resource (environment, package, connection, materialization). `delete materialization <id> [--drop-tables]` also drops the materialized physical tables — including tables built into a `storage=` DuckDB/DuckLake destination (a destination-aware drop), not just in-warehouse tables.

Action commands:

- `malloy-pub materialize --environment <env> --package <pkg> [--force-refresh] [--auto-load-manifest] [--wait] [--timeout <seconds>] [--poll-interval <seconds>]` - Create and start a materialization build. With `--wait`, poll until it reaches a terminal state and exit non-zero if it fails, is cancelled, or times out; otherwise it returns the id so you can check status with `get materialization`. `--wait` defaults to a 120-second timeout and a 2-second poll interval; raise `--timeout` for long builds.
- `malloy-pub stop-materialization <id> --environment <env> --package <pkg>` - Stop a pending or running materialization.
- `malloy-pub reload-manifest --environment <env> --package <pkg>` - Reload the build manifest and recompile the package's models.

Models, notebooks, and databases are deployed package artifacts and are read-only. Models and notebooks support `list` and `get`; databases support `list` only (the API exposes no get-by-id for databases).

Common options:

- `--url <server>` - Publisher server URL (overrides `MALLOY_PUBLISHER_URL`)
- `--environment <name>` - Environment name (required for every resource except `environment` itself)
- `--package <name>` - Package name (required for materialization, model, notebook, database, and manifest)
