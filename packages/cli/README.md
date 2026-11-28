<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Malloy Publisher CLI

Command-line interface for managing Malloy Publisher resources. The binary is `malloy-pub`.

## Installation

The CLI is published to npm as `@malloy-publisher/cli` and needs Node.js 20 or newer (no Bun). Run it
without installing:

```bash
npx @malloy-publisher/cli@latest list environment
```

or install `malloy-pub` globally:

```bash
npm i -g @malloy-publisher/cli
malloy-pub --version
```

It is released in lockstep with `@malloy-publisher/server`, so the two share a version number, and
`malloy-pub` speaks the REST API of the server released beside it. Keep the `@latest` on `npx` for the
same reason the server's docs give: `npx` reuses whatever version it cached last.

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
malloy-pub delete materialization <id> --environment my-environment --package my-package

# Build manifest (the tables a materialization produced)
malloy-pub get manifest --environment my-environment --package my-package
malloy-pub reload-manifest --environment my-environment --package my-package

# Read-only package contents
malloy-pub list model --environment my-environment --package my-package
malloy-pub get model flights.malloy --environment my-environment --package my-package
malloy-pub list database --environment my-environment --package my-package
```

## Development

From a clone of the repository (Bun 1.3.13 or newer, as for the rest of the repo; the API client
generator also needs Java):

```bash
# Install dependencies (at the repository root)
bun install

# Generate the API client from ../../api-doc.yaml
bun run generate-api-types

# Build dist/index.js (runs generate-api-types first)
bun run build

# Run from source, or the built bundle under Node
bun run dev -- list environment --url http://localhost:4000
node dist/index.js list environment --url http://localhost:4000

# Unit tests
bun run test
```

## Commands

Resource (CRUD) verbs:

- `malloy-pub list <resource>` - List resources (environment, package, connection, materialization, model, database). `list materialization` also accepts `--limit <n>` and `--offset <n>` for pagination.
- `malloy-pub get <resource> [name]` - Get resource details (environment, package, connection, materialization, model, manifest)
- `malloy-pub create <resource> [name]` - Create a resource (environment, package, connection)
- `malloy-pub update <resource> [name]` - Update a resource (environment, package, connection)
- `malloy-pub delete <resource> [name]` - Delete a resource (environment, package, connection, materialization). `delete materialization <id>` deletes the run's record only; the tables it built stay, so drop any you no longer need through their connection after checking that no remaining run, of any version, names a table with that name in any place.

Action commands:

- `malloy-pub materialize --environment <env> --package <pkg> [--force-refresh] [--auto-load-manifest] [--wait] [--timeout <seconds>] [--poll-interval <seconds>]` - Create and start a materialization build. With `--wait`, poll until it reaches a terminal state and exit non-zero if it fails, is cancelled, or times out; otherwise it returns the id so you can check status with `get materialization`. `--wait` defaults to a 120-second timeout and a 2-second poll interval; raise `--timeout` for long builds.
- `malloy-pub stop-materialization <id> --environment <env> --package <pkg>` - Stop a pending or running materialization.
- `malloy-pub reload-manifest --environment <env> --package <pkg>` - Reload the build manifest and recompile the package's models.

Models and databases are deployed package artifacts and are read-only. Models support `list` and `get`; databases support `list` only (the API exposes no get-by-id for databases).

Common options:

- `--url <server>` - Publisher server URL (overrides `MALLOY_PUBLISHER_URL`)
- `--environment <name>` - Environment name (required for every resource except `environment` itself)
- `--package <name>` - Package name (required for materialization, model, database, and manifest)
