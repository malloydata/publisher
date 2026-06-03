# Publisher

<a href="https://github.com/malloydata/publisher/actions/workflows/build.yml">![build](https://github.com/malloydata/publisher/actions/workflows/build.yml/badge.svg)</a>

**Publisher** is the open-source semantic model server for [Malloy](https://malloydata.dev). It serves Malloy models through REST and MCP APIs, enabling consistent data access for applications, tools, and AI agents.

## Prerequisites

| Tool                              | Version                     | Required for                                                                  |
| --------------------------------- | --------------------------- | ----------------------------------------------------------------------------- |
| [Bun](https://bun.sh/)            | ≥ 1.3.13                    | Primary runtime + package manager                                             |
| [Node.js](https://nodejs.org/)    | ≥ 20                        | DuckDB postinstall scripts and the `npx @malloy-publisher/server` bin shebang |
| [Python](https://www.python.org/) | ≥ 3.12                      | Only if you build the Python client (`packages/python-client`)                |
| Java                              | ≥ 21 (Corretto recommended) | Only if you regenerate API clients via `bun run generate-api-types`           |

The repo ships a `.tool-versions` file compatible with [mise](https://mise.jdx.dev/) and [asdf](https://asdf-vm.com/), so `mise install` (or `asdf install`) provisions all four versions at once.

## Quick Start

```bash
npx @malloy-publisher/server --port 4000
```

Open http://localhost:4000 to explore the sample models. Three DuckDB-backed samples (`ecommerce`, `imdb`, `faa`) are cloned from GitHub on first launch — expect a 30–60s wait before `operationalState` reports `serving`. No credentials required.

> **Heads up — npx + DuckDB native binding.** On some Node 24 setups, `npx` does not install DuckDB's native binding (`node_modules/duckdb/lib/binding/duckdb.node`), so the server exits at startup with `Cannot find module ...duckdb.node`. This is an upstream `duckdb` install-script issue tracked separately. Workaround until that's fixed: clone this repo and run `make start-init` (or `bun run build && bun run start`) from the repo root — the workspace's `install-duckdb-bindings` script handles the binding install during `bun run build`.

### Bring your own config

Pass `--config <path>` to point the server at a specific `publisher.config.json`, or place a `publisher.config.json` in the directory you launch from. Both forms override the bundled default.

```bash
# Existing repo of Malloy samples or your own packages
git clone https://github.com/credibledata/malloy-samples.git
npx @malloy-publisher/server --port 4000 --config malloy-samples/publisher.config.json

# Or cd in and rely on the implicit lookup
cd malloy-samples && npx @malloy-publisher/server --port 4000
```

To enable the BigQuery samples (`bigquery-hackernews`, etc.), copy [`packages/server/publisher.config.example.bigquery.json`](packages/server/publisher.config.example.bigquery.json) over your `publisher.config.json` and set `GOOGLE_APPLICATION_CREDENTIALS`.

### Verify it's working

```bash
curl -s http://localhost:4000/api/v0/status | jq .operationalState   # → "serving"
curl -s http://localhost:4000/api/v0/environments | jq '.[].name'    # → list of environments
```

`operationalState` reports the current server lifecycle:

- **`serving`** — ready to handle requests.
- **`initializing`** — loading packages and connections from `publisher.config.json`. Normal on boot, and especially noticeable on the first run when sample packages need to be cloned from GitHub. Wait for `serving`.
- **`draining`** — graceful shutdown in progress: the server is waiting for in-flight requests to finish before closing. Controlled by `SHUTDOWN_DRAIN_DURATION_SECONDS` and `SHUTDOWN_GRACEFUL_CLOSE_TIMEOUT_SECONDS` (see [Configuration](#configuration)).
- **`throttled`** — the memory governor has hit its back-pressure limit and is rejecting new package loads and queries to stay under `PUBLISHER_MAX_MEMORY_BYTES`. Already-loaded packages remain serviceable; the control plane should treat the worker as unhealthy for new load until memory drops. Only reported when the memory governor is enabled.

---

## Docker

Two ways to run the Publisher in Docker: build the image from source, or pull the pre-built image from Docker Hub. Either way, the container's `WORKDIR` is `/publisher` (mount your `publisher.config.json` there), REST is on `:4000`, and MCP is on `:4040`.

### Build from source

```bash
docker build -t malloy-publisher .
docker run -d \
  -p 4000:4000 -p 4040:4040 \
  -v $(pwd)/publisher.config.json:/publisher/publisher.config.json:ro \
  malloy-publisher
```

If you don't have a config yet, copy [`packages/server/publisher.config.example.duckdb.json`](packages/server/publisher.config.example.duckdb.json) (DuckDB-only samples, no credentials needed) as a starting point.

### Pre-built image

The official pre-built image is published to Docker Hub at [`ms2data/malloy-publisher`](https://hub.docker.com/r/ms2data/malloy-publisher).

```bash
docker pull ms2data/malloy-publisher
docker run -d \
  -p 4000:4000 -p 4040:4040 \
  -v $(pwd)/publisher.config.json:/publisher/publisher.config.json:ro \
  ms2data/malloy-publisher
```

**Tags:**

- `:latest` — most recent stable release.
- `:X.Y.Z` — pinned to a specific release; recommended for production.
- `:next` — pre-release builds; not recommended for production.

`*-dev` tags (e.g. `:0.0.198-dev`) are frozen — no new ones are being published, and `:next` is the current pre-release channel. Existing `*-dev` tags still resolve in the registry; don't use them for new deployments.

### Docker Compose

A ready-to-use Compose file lives at [`docker-compose.example.yml`](docker-compose.example.yml) — it runs the pre-built image with both ports mapped, a healthcheck against `/api/v0/status`, and a named volume for `publisher_data/` so first-boot package clones survive restarts. To use it:

1. Copy it into your project: `cp docker-compose.example.yml docker-compose.yml`.
2. Place a `publisher.config.json` next to it (or change the volume mount). No config of your own yet? Copy [`packages/server/publisher.config.example.duckdb.json`](packages/server/publisher.config.example.duckdb.json) — DuckDB-only samples, no credentials needed.
3. `docker compose up -d`

For env-var configuration, persistent `publisher_data/` volumes, and advanced options, see [`packages/server/README.docker.md`](packages/server/README.docker.md).

## Documentation

Full documentation is available at **[docs.malloydata.dev/documentation/user_guides/publishing](https://docs.malloydata.dev/documentation/user_guides/publishing/publishing)**:

- [Getting Started](https://docs.malloydata.dev/documentation/user_guides/publishing/publishing) - Setup, deployment options, configuration
- [Database Connections](https://docs.malloydata.dev/documentation/user_guides/publishing/connections) - BigQuery, Snowflake, Postgres, DuckDB, and more
- [Explorer](https://docs.malloydata.dev/documentation/user_guides/publishing/explorer) - No-code visual query builder
- [REST API](https://docs.malloydata.dev/documentation/user_guides/publishing/rest_api) - Build custom applications
- [Publisher SDK](https://docs.malloydata.dev/documentation/user_guides/publishing/publisher_sdk) - Embed analytics in React apps
- [MCP for AI Agents](https://docs.malloydata.dev/documentation/user_guides/publishing/mcp_agents) - Connect Claude and other AI assistants

## How the Pieces Fit Together

### Malloy

The core compiler and query execution engine. Malloy compiles `.malloy` files into SQL, executes queries against databases, and returns structured `Result` objects. Malloy is a pure JavaScript/TypeScript library with no UI or serving capabilities—it's the foundation everything else builds on.

**Repository:** [github.com/malloydata/malloy](https://github.com/malloydata/malloy)

### Malloy Render

A visualization library that transforms Malloy `Result` objects into interactive tables, charts, and dashboards.

When Malloy executes a query, the result includes both **data** and **rendering hints**—tags like `# bar_chart` or `# line_chart` that indicate how the data should be displayed. Malloy Render interprets these tags and produces the appropriate visualization.

**Built with:** SolidJS and Vega/Vega-Lite. Available as both a JavaScript API (`MalloyRenderer`) and a `<malloy-render>` web component.

**Repository:** [github.com/malloydata/malloy/packages/malloy-render](https://github.com/malloydata/malloy/tree/main/packages/malloy-render)

### Publisher

An open-source semantic model server for Malloy. Publisher makes Malloy models accessible over the network and provides a professional UI for data exploration.

- **Server:** REST API for listing content, managing database connections, compiling models, and executing queries. Also provides an MCP API for AI agent integration. Supports [source filters](docs/filters.md) for model-driven, server-side query filtering.
- **App:** Web interface for browsing Malloy content, exploring models with a no-code query builder, and viewing results.

### Publisher SDK

A React component library for building custom data applications powered by Publisher:

- **API communication** — Talks to the Publisher Server via REST
- **Query execution** — Submits queries and retrieves results
- **Result visualization** — Integrates Malloy Render to display results
- **UI components** — Pre-built pages for browsing environments, packages, models, and notebooks
- **Source filters** — Automatically renders filter widgets for models with [`#(filter)` annotations](docs/filters.md)

The Publisher App is built entirely with the SDK, but the SDK is a standalone NPM package for building your own applications.

## Architecture

Publisher consists of four packages:

| Package                                               | Description                                                                                                                                     |
| ----------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| **[packages/server](packages/server/)**               | Express.js backend providing REST API (port 4000) and MCP API (port 4040). Loads Malloy packages, compiles queries, executes against databases. |
| **[packages/sdk](packages/sdk/)**                     | React component library for building UIs that consume Publisher's REST API.                                                                     |
| **[packages/app](packages/app/)**                     | Reference implementation and production-ready data exploration tool built with the SDK.                                                         |
| **[packages/python-client](packages/python-client/)** | Auto-generated Python SDK for the REST API.                                                                                                     |

## Development

This project uses [bun](https://bun.sh/) as the JavaScript runtime. Sample packages are fetched at runtime per [`publisher.config.json`](packages/server/publisher.config.json) — no submodule checkout needed.

The bundled `publisher.config.json` ships three samples (`ecommerce`, `imdb`, `faa`) that run via per-package DuckDB sandboxes — no GCP credentials needed. To enable the BigQuery-required `bigquery-hackernews` sample, copy [`publisher.config.example.bigquery.json`](packages/server/publisher.config.example.bigquery.json) over `publisher.config.json` (or point `--server_root` at a directory containing it) and set `GOOGLE_APPLICATION_CREDENTIALS`.

### Makefile shortcuts

A top-level `Makefile` wraps the common workflows so you don't have to remember script names or `cd` into individual packages. Run `make help` for the full list. The most useful targets:

| Target                                                       | What it does                                                                                            |
| ------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------- |
| `make install`                                               | `bun install` at the repo root                                                                          |
| `make build`                                                 | Production build: SDK → app → server bundle                                                             |
| `make start` / `make start-init`                             | Run the built server (`--init` clears persisted storage on boot)                                        |
| `make stop`                                                  | Kill anything on ports `:4000` or `:4040`                                                               |
| `make dev`                                                   | **Express + Vite together** in one terminal with prefixed `[server]`/`[react]` logs (Ctrl+C kills both) |
| `make dev-server` / `make dev-react`                         | Same dev workflow, split into two terminals                                                             |
| `make status` / `make environments` / `make packages`        | Quick API smoke checks                                                                                  |
| `make test` / `make lint` / `make typecheck` / `make format` | Quality gates                                                                                           |
| `make regen-api`                                             | Regenerate server + SDK clients from `api-doc.yaml` (needs Java)                                        |

### Production build

One command builds the SDK, app, and server bundle in order:

```bash
make install
make build
make start                # Run the built server (REST on :4000, MCP on :4040)
```

Or run the underlying `bun` scripts directly: `bun install && bun run build:server-deploy && bun run start`.

### Dev mode

Express and Vite run as separate processes. Express on `:4000` proxies non-API traffic to Vite on `:5173` when `NODE_ENV=development`, so visit `http://localhost:4000` for the full app — `:5173` won't have API access.

**One terminal (recommended):**

```bash
make dev
```

This runs both servers with combined, color-prefixed logs (`[server]` / `[react]`). Ctrl+C stops both cleanly.

**Two terminals (if you prefer split logs):**

```bash
make dev-server          # Express (REST :4000 + MCP :4040, watch mode)
```

```bash
make dev-react           # Vite dev server (:5173, proxied through :4000)
```

Open http://localhost:4000.

### Tests and quality gates

```bash
make test                # unit + integration server tests
make lint && make format # eslint + prettier
make typecheck           # tsc --noEmit across sdk/app/server
```

`make typecheck` (and the underlying `bun run typecheck`) depends on the SDK's emitted `.d.ts` files, which in turn depend on the OpenAPI codegen. On a fresh clone, build first — either with `make build` (full SDK + app + server bundle), or with the targeted minimum:

```bash
bun install
bun run generate-api-types
bun run build:sdk
bun run typecheck
```

After that, `bun run typecheck` works on its own as long as the SDK build artifacts stay current:

- After editing `api-doc.yaml` → re-run `bun run generate-api-types && bun run build:sdk`.
- After editing SDK source → re-run `bun run build:sdk`.

## Configuration

Publisher reads its runtime configuration from `publisher.config.json` (see [Development](#development) for the BigQuery opt-in) and a handful of environment variables. Every CLI flag below has an env-var equivalent; pass either.

| Env var                                   | CLI flag                                        | Default   | Meaning                                                                                                                                                                                                |
| ----------------------------------------- | ----------------------------------------------- | --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `PUBLISHER_PORT`                          | `--port <n>`                                    | `4000`    | REST + static-app HTTP port.                                                                                                                                                                           |
| `PUBLISHER_HOST`                          | `--host <addr>`                                 | `0.0.0.0` | Host binding for the main server.                                                                                                                                                                      |
| `MCP_PORT`                                | `--mcp_port <n>`                                | `4040`    | MCP HTTP port.                                                                                                                                                                                         |
| `SERVER_ROOT`                             | `--server_root <dir>`                           | `.` (cwd) | Directory containing `publisher.config.json`.                                                                                                                                                          |
| `INITIALIZE_STORAGE`                      | `--init`                                        | _unset_   | Set to `true` (or pass `--init`) to initialize storage on boot. Set on the first run with new persistent storage; safe to omit afterward. Also exposed as the `start:init` / `start:dev:init` scripts. |
| `SHUTDOWN_DRAIN_DURATION_SECONDS`         | `--shutdown_drain_duration_seconds <s>`         | `0`       | Time to keep `/health` returning OK after SIGTERM before refusing new traffic.                                                                                                                         |
| `SHUTDOWN_GRACEFUL_CLOSE_TIMEOUT_SECONDS` | `--shutdown_graceful_close_timeout_seconds <s>` | `0`       | Time to wait for in-flight requests to drain before forcing close.                                                                                                                                     |
| `NODE_ENV`                                | —                                               | _unset_   | Set to `development` to proxy non-API traffic to the Vite dev server on `:5173`.                                                                                                                       |
| `LOG_LEVEL`                               | —                                               | `debug`   | One of `error`, `warn`, `info`, `verbose`, `debug`, `silly`.                                                                                                                                           |
| `DISABLE_RESPONSE_LOGGING`                | —                                               | _unset_   | Set to `true` or `1` to suppress response-body logging.                                                                                                                                                |
| `OTEL_EXPORTER_OTLP_ENDPOINT`             | —                                               | _unset_   | OpenTelemetry collector endpoint.                                                                                                                                                                      |
| `GOOGLE_APPLICATION_CREDENTIALS`          | —                                               | _unset_   | Fallback path to a GCP service-account JSON for BigQuery connections that don't include inline auth. Ignored when the connection config provides its own credentials.                                  |
| `PG_CONNECT_TIMEOUT_SECONDS`              | —                                               | `5`       | Connection timeout (seconds) for Postgres-backed DuckLake manifest catalogs (`materializationStorage`). Bad credentials or an unreachable host return HTTP 422 in ~5s rather than hanging the publisher. No effect on user-facing Postgres connections or non-PG catalogs (SQLite, MySQL). |
| `PUBLISHER_MAX_QUERY_ROWS`                | —                                               | `100000`  | Maximum rows returned per query on every query surface (`/connections/.../sqlQuery`, model query, notebook cell, MCP `executeQuery`). Forwarded to the connector / Malloy `runnable.run` as the effective row limit; queries that exceed the cap fail with HTTP 413. Set to `0` to disable. A caller-supplied `rowLimit` smaller than the cap is preserved. |
| `PUBLISHER_MAX_RESPONSE_BYTES`            | —                                               | `50000000` (50 MB) | Maximum JSON-serialized response size for ad-hoc SQL and model queries. Streaming-capable connections (Postgres, DuckDB) enforce mid-stream and abort the driver immediately; non-streaming connections enforce post-buffer. Exceeding the cap fails with HTTP 413. Set to `0` to disable.                                                              |
| `PUBLISHER_DEFAULT_QUERY_ROW_LIMIT`       | —                                               | `1000`    | Default `LIMIT` applied to model queries that don't include their own. Always ≤ `PUBLISHER_MAX_QUERY_ROWS`. `0` is rejected.                                                                                                                                                                                                                            |
| `PUBLISHER_QUERY_TIMEOUT_MS`              | —                                               | `300000` (5 min) | Wall-clock timeout per query (all surfaces). Wired to the underlying SDK via `AbortSignal`; queries that exceed the budget are aborted and return HTTP 504. Set to `0` to disable.                                                                                                                                                                |
| `PUBLISHER_MAX_CONCURRENT_QUERIES`        | —                                               | `32`      | Per-pod cap on simultaneous in-flight queries (HTTP + MCP share the same slot pool). When the cap is reached, new queries fail fast with HTTP 503 (or the MCP-error equivalent). Tune higher under load; set to `0` to disable.                                                                                                                          |
| `PUBLISHER_MAX_MEMORY_BYTES`              | —                                               | _unset_   | Enables the RSS-based memory governor. When set, the governor samples process RSS every `PUBLISHER_MEMORY_CHECK_INTERVAL_MS` ms and rejects new package loads and queries with HTTP 503 once RSS crosses `PUBLISHER_MEMORY_HIGH_WATER_FRACTION × PUBLISHER_MAX_MEMORY_BYTES`, until it drops below `PUBLISHER_MEMORY_LOW_WATER_FRACTION ×`. Unset or `0` disables.       |
| `PUBLISHER_MEMORY_HIGH_WATER_FRACTION`    | —                                               | `0.8`     | High-water mark (fraction of `PUBLISHER_MAX_MEMORY_BYTES`). Must be in `(0, 1)` and strictly above the low-water mark.                                                                                                                                                                                                                                |
| `PUBLISHER_MEMORY_LOW_WATER_FRACTION`     | —                                               | `0.7`     | Low-water mark (fraction of `PUBLISHER_MAX_MEMORY_BYTES`). Hysteresis: back-pressure clears when RSS dips below this value.                                                                                                                                                                                                                            |
| `PUBLISHER_MEMORY_CHECK_INTERVAL_MS`      | —                                               | `5000`    | RSS sampling interval (ms). Minimum 100.                                                                                                                                                                                                                                                                                                              |
| `PUBLISHER_MEMORY_BACKPRESSURE`           | —                                               | `true`    | Set to `false` to disable the 503 behavior while keeping RSS monitoring — useful for a metrics-only rollout before enabling enforcement.                                                                                                                                                                                                              |
| —                                         | `--help`, `-h`                                  | —         | Print the full flag list.                                                                                                                                                                              |

PostgreSQL and other database-specific connections may also honor their respective driver env vars (e.g. `PGSSLMODE`).

### Operational tuning — OOM guards

The publisher exports OpenTelemetry metrics (under the `publisher` meter) so the OOM guardrails above can be observed and tuned in production. The most useful series for this work:

| Metric                                                                       | Type             | Use                                                                                                          |
| ---------------------------------------------------------------------------- | ---------------- | ------------------------------------------------------------------------------------------------------------ |
| `publisher_query_cap_exceeded_total{cap_type,source}`                        | Counter          | Per-cap 413 firings. Pivot by `cap_type` (`rows`/`bytes`) to know which knob to raise; by `source` for surface. |
| `publisher_max_query_rows`, `publisher_max_response_bytes`                   | Gauges           | Live values of the corresponding env vars (and `-1` on misconfig).                                           |
| `publisher_query_admission_rejections_total{environment}`                    | Counter          | 503s from the memory governor at the query layer. Hot environments stand out via the label.                  |
| `publisher_package_admission_rejections_total{environment,reason}`           | Counter          | 503s from the memory governor at the package-load layer.                                                     |
| `publisher_query_timeout_total{timeout_ms}`, `publisher_query_timeout_ms`    | Counter, gauge   | 504 firings and the live `PUBLISHER_QUERY_TIMEOUT_MS` value.                                                 |
| `publisher_query_concurrency_rejections_total{http.route,limit}`             | Counter          | 503s from the per-pod query concurrency cap, labeled by hot route (HTTP) or `mcp:executeQuery`.              |
| `publisher_query_active_slots`, `publisher_query_max_slots`                  | Gauges           | Live in-flight slot count and cap — render utilization as `active / max`.                                    |
| `publisher_process_rss_bytes`, `publisher_heap_size_limit_bytes`, `publisher_heap_used_bytes` | Gauges | Process RSS, V8 heap ceiling (`--max-old-space-size`), V8 used heap.                                       |
| `publisher_memory_backpressure_active`, `_activations_total`                 | Gauge, counter   | Current governor state and historical activations.                                                            |
| `http_server_requests_total{http.status_code}`                               | Counter          | Coarse 413/503/504 totals — pair with the dedicated counters above for per-cause breakdown.                  |

## Community

- Join the [Malloy Slack](https://join.slack.com/t/malloy-community/shared_invite/zt-1kgfwgi5g-CrsdaRqs81QY67QW0~t_uw)
- Report issues on [GitHub](https://github.com/malloydata/publisher/issues)
