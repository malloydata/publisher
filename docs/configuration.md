# Configuration

> What this is: the complete runtime configuration reference — the config file, every environment
> variable and CLI flag, the OOM/operational-tuning knobs, and the metrics that observe them. For how
> to *run* the server, see [deployment.md](deployment.md).

Publisher reads its runtime configuration from `publisher.config.json` and a handful of environment
variables. Every CLI flag below has an env-var equivalent; pass either.

## Bring your own config

Pass `--config <path>` to point the server at a specific `publisher.config.json`, or place a
`publisher.config.json` in the directory you launch from. Both forms override the bundled default.

```bash
# Point at a directory that holds your own publisher.config.json + packages
npx @malloy-publisher/server --port 4000 --config /path/to/your/publisher.config.json

# Or cd into that directory and rely on the implicit lookup
cd /path/to/your/project && npx @malloy-publisher/server --port 4000
```

Your packages can live anywhere; they do not have to be inside this repo. A package `location` may be
absolute, start with `~/`, or be relative to the directory holding the config it appears in. Keeping
a config next to the packages it points at means the two move together, which is what makes the
config worth committing:

```
my-data/
  publisher.config.json
  sales/                    # your package: publisher.json + sales.malloy
```

```json
{
   "environments": [
      {
         "name": "local",
         "packages": [{ "name": "sales", "location": "./sales" }]
      }
   ]
}
```

The package directory format itself (`publisher.json` fields, models, data files) is documented in
[packages.md](packages.md).

Do not author packages under `publisher_data/`. That is storage Publisher manages for itself: it
copies each configured package in (or symlinks it, under `--watch-env`), and `--init` deletes the
whole tree.

A `location` can also be a `https://github.com/...`, `gs://`, or `s3://` URL, which Publisher
downloads. Only local directories are eligible for `--watch-env`.

To add a BigQuery-backed sample (`bigquery-hackernews`) alongside the bundled examples, copy
[`packages/server/publisher.config.example.bigquery.json`](../packages/server/publisher.config.example.bigquery.json)
over your `publisher.config.json` and set `GOOGLE_APPLICATION_CREDENTIALS`. For the database
connection reference (BigQuery, Snowflake, Postgres, DuckDB, and more), see
[connections.md](connections.md).

## Environment variables & CLI flags

| Env var | CLI flag | Default | Meaning |
| --- | --- | --- | --- |
| `PUBLISHER_PORT` | `--port <n>` | `4000` | REST + static-app HTTP port. |
| `PUBLISHER_HOST` | `--host <addr>` | `0.0.0.0` | Host binding for both the REST and MCP servers. Set `127.0.0.1` to keep them loopback-only. |
| `MCP_PORT` | `--mcp_port <n>` | `4040` | MCP HTTP port. Serves the five MCP tools (`malloy_getContext`, `malloy_executeQuery`, `malloy_compile`, `malloy_reloadPackage`, `malloy_searchDocs`) and the agent skills as MCP prompts. |
| `SERVER_ROOT` | `--server_root <dir>` | `.` (cwd) | Where Publisher keeps its own storage (`publisher_data/`, `publisher.db`), and where it looks for `publisher.config.json` when `--config` is not passed. |
| `PUBLISHER_USE_BUNDLED_DEFAULT` | — | _unset_ | Set to `true` to fall back to the sample config bundled inside the installed package when neither `--config` is passed nor a `publisher.config.json` exists at the server root. The server sets this itself on a zero-flag start (so a bare `npx @malloy-publisher/server` boots the samples); passing `--config` or `--server_root` leaves it unset. Because the bundled config lives inside the install, relative package locations resolve against the server root in this mode rather than the config's directory. |
| `INITIALIZE_STORAGE` | `--init` | _unset_ | Set to `true` (or pass `--init`) to **wipe persisted storage** (`publisher_data/`) and re-sync it from the config on boot. A first boot with empty storage loads the config automatically, so you only need this to reset state or pick up config changes. Also exposed as the `start:init` / `start:dev:init` scripts. |
| `SHUTDOWN_DRAIN_DURATION_SECONDS` | `--shutdown_drain_duration_seconds <s>` | `0` | After SIGTERM, how long to keep serving in-flight and new requests (readiness reports not-ready immediately) before the server starts refusing new traffic. |
| `SHUTDOWN_GRACEFUL_CLOSE_TIMEOUT_SECONDS` | `--shutdown_graceful_close_timeout_seconds <s>` | `0` | Time to wait for in-flight requests to drain before forcing close. |
| `NODE_ENV` | — | _unset_ | Set to `development` to proxy non-API traffic to the Vite dev server on `:5173`. |
| `PUBLISHER_WATCH` | `--watch-env <name>` | _unset_ | Dev only. Mount the named environment's local-dir packages in place (a symlink, not a copy) and watch them, so edits to your source recompile that package and live-reload any open pages. Repeat the flag or use a comma-separated list to mount several in place; only the first one auto-reloads. Leave unset in production, where packages are copied and stay decoupled from their source. |
| `PUBLISHER_FRAME_ANCESTORS` | — | `*` | `Content-Security-Policy: frame-ancestors` value sent on served HTML pages, controlling which origins may embed a page in an iframe. Defaults to any origin. |
| `LOG_LEVEL` | — | `debug` | One of `error`, `warn`, `info`, `verbose`, `debug`, `silly`. |
| `DISABLE_RESPONSE_LOGGING` | — | _unset_ | Set to `true` or `1` to suppress response-body logging. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | _unset_ | OpenTelemetry collector endpoint. |
| `GOOGLE_APPLICATION_CREDENTIALS` | — | _unset_ | Fallback path to a GCP service-account JSON for BigQuery connections that don't include inline auth. Ignored when the connection config provides its own credentials. |
| `PUBLISHER_ALLOW_PROXY_CONNECTIONS` | — | _unset_ | Set to `true` to allow `publisher`-type proxy connections (Publisher-to-Publisher). See [connections.md](connections.md). |
| `PACKAGE_LOAD_WORKERS` | — | `1` | Worker processes for package compilation. Must be ≥ 1. |
| `PACKAGE_LOAD_JOB_TIMEOUT_MS` | — | `120000` (2 min) | Timeout per package-load job before the worker is recycled. |
| `EXTENSION_FETCH_POLICY` | — | `on-demand` | Whether the server may fetch DuckDB extensions from the network at runtime. `on-demand`: a missing extension is installed on first use (unchanged prior behavior). `local-only`: never install, and disable DuckDB's implicit auto-install — a locally-present (e.g. image-baked) extension still loads, but a missing one fails with an actionable error naming the extension. Use `local-only` for air-gapped / pinned-image deployments. See [ducklake.md](ducklake.md#duckdb-extension-provisioning). |
| `PUBLISHER_MAX_QUERY_ROWS` | — | `100000` | Maximum rows returned per query on every query surface (`/connections/.../sqlQuery`, model query, notebook cell, MCP `executeQuery`). Forwarded to the connector / Malloy `runnable.run` as the effective row limit; queries that exceed the cap fail with HTTP 413. Set to `0` to disable. A caller-supplied `rowLimit` smaller than the cap is preserved. |
| `PUBLISHER_MAX_RESPONSE_BYTES` | — | `50000000` (50 MB) | Maximum JSON-serialized response size for ad-hoc SQL and model queries. Streaming-capable connections (Postgres, DuckDB) enforce mid-stream and abort the driver immediately; non-streaming connections enforce post-buffer. Exceeding the cap fails with HTTP 413. Set to `0` to disable. |
| `PUBLISHER_DEFAULT_QUERY_ROW_LIMIT` | — | `1000` | Default `LIMIT` applied to model queries that don't include their own. Always ≤ `PUBLISHER_MAX_QUERY_ROWS`. `0` is rejected. |
| `PUBLISHER_QUERY_TIMEOUT_MS` | — | `300000` (5 min) | Wall-clock timeout per query (all surfaces). Wired to the underlying SDK via `AbortSignal`; queries that exceed the budget are aborted and return HTTP 504. Set to `0` to disable. |
| `PUBLISHER_MAX_CONCURRENT_QUERIES` | — | `32` | Per-pod cap on simultaneous in-flight queries (HTTP + MCP share the same slot pool). When the cap is reached, new queries fail fast with HTTP 503 (or the MCP-error equivalent). Tune higher under load; set to `0` to disable. |
| `PUBLISHER_MAX_MEMORY_BYTES` | — | _unset_ | Enables the RSS-based memory governor. When set, the governor samples process RSS every `PUBLISHER_MEMORY_CHECK_INTERVAL_MS` ms and rejects new package loads and queries with HTTP 503 once RSS crosses `PUBLISHER_MEMORY_HIGH_WATER_FRACTION × PUBLISHER_MAX_MEMORY_BYTES`, until it drops below `PUBLISHER_MEMORY_LOW_WATER_FRACTION ×`. Unset or `0` disables. |
| `PUBLISHER_MEMORY_HIGH_WATER_FRACTION` | — | `0.8` | High-water mark (fraction of `PUBLISHER_MAX_MEMORY_BYTES`). Must be in `(0, 1)` and strictly above the low-water mark. |
| `PUBLISHER_MEMORY_LOW_WATER_FRACTION` | — | `0.7` | Low-water mark (fraction of `PUBLISHER_MAX_MEMORY_BYTES`). Hysteresis: back-pressure clears when RSS dips below this value. |
| `PUBLISHER_MEMORY_CHECK_INTERVAL_MS` | — | `5000` | RSS sampling interval (ms). Minimum 100. |
| `PUBLISHER_MEMORY_BACKPRESSURE` | — | `true` | Set to `false` to disable the 503 behavior while keeping RSS monitoring — useful for a metrics-only rollout before enabling enforcement. |
| `PUBLISHER_LOCAL_MATERIALIZATION_SCHEDULER` | — | `false` | Opt-in: enable the standalone materialization scheduler, which fires each loaded package's `materialization.schedule` cron so a self-hosted Publisher rebuilds on a cadence with no control plane. **Never set this on a control-plane-driven (orchestrated) worker** — it is the primary guard against double-driving refresh. See [materialization.md](materialization.md). |
| `PUBLISHER_MATERIALIZATION_SCHEDULER_INTERVAL_MS` | — | `60000` (1 min) | How often the scheduler sweeps for due schedules, in ms. Minimum `1000`. Only read when the scheduler is enabled. |
| `PUBLISHER_MATERIALIZATION_SCHEDULER_MAX_FIRES_PER_TICK` | — | `10` | Stampede guard: max packages fired per sweep. A capped package fires on a later tick. Must be a positive integer. |
| `EMBEDDING_API_KEY` | — | _unset_ | Enables semantic (embedding-based) ranking for `malloy_getContext` question retrieval. Sent as a bearer token to the embedding endpoint. Unset: retrieval stays lexical (lunr/BM25), unchanged. Must be set explicitly; an ambient `OPENAI_API_KEY` is deliberately not read. See "Semantic retrieval for malloy_getContext" below. |
| `EMBEDDING_MODEL` | — | `text-embedding-3-small` | Embedding model name sent to the endpoint. |
| `EMBEDDING_API_BASE` | — | `https://api.openai.com/v1` | Base URL of an OpenAI-compatible embeddings API (`POST <base>/embeddings`). Point at any compatible endpoint (e.g. a local Ollama or vLLM server). |
| `EMBEDDING_DIMENSIONS` | — | _unset_ | Optional `dimensions` request parameter (e.g. `512` to shrink `text-embedding-3-small` vectors). When unset the parameter is omitted, which suits providers that do not support it. |
| — | `--help`, `-h` | — | Print the full flag list. |

PostgreSQL and other database-specific connections may also honor their respective driver env vars
(e.g. `PGSSLMODE`).

## Semantic retrieval for malloy_getContext

By default, `malloy_getContext`'s question retrieval is lexical (lunr/BM25) over the model's own
text, so a query only matches entities that share tokens with it (`departure delay` never finds a
field named `dep_delay`). Setting `EMBEDDING_API_KEY` switches ranking to embedding similarity:
each package's entities (source, view, named query, dimension, and measure names plus their
`#(doc)` text) are embedded once and searched by cosine similarity, so synonyms and `snake_case`
names match by meaning.

What to know before turning it on:

- **What leaves the machine.** Entity names, their `#(doc)` annotation text, and the query strings
  agents pass to `malloy_getContext` are sent to the configured embedding endpoint. Model source
  code, data, and query results are never sent. Point `EMBEDDING_API_BASE` at a local
  OpenAI-compatible server (Ollama, vLLM) to keep everything on-machine.
- **Storage.** Vectors are cached in the server's own `publisher.db` (keyed by a content hash, so
  only new or changed entities re-embed, across restarts too). `--init` wipes the cache along with
  the rest of persisted storage; the only cost of a wipe is re-embedding.
- **First query per package.** The first question against a package kicks off the embedding sync in
  the background and answers lexically; questions after that are semantic. Responses carry a
  `retrieval` field (`"semantic"` or `"lexical"`) whenever the provider is configured.
- **Failure behavior.** If the endpoint is down, times out, or rejects the key, retrieval falls
  back to lexical (with a warning in the server log) and retries after a cool-down. A package with
  more than 5,000 entities stays lexical.
- To measure the difference on your own models, see the eval script header in
  `packages/server/src/mcp/tools/get_context_eval.ts`.

## Operational tuning: OOM guards

The publisher exports OpenTelemetry metrics (under the `publisher` meter) so the OOM guardrails above
can be observed and tuned in production. The most useful series for this work:

| Metric | Type | Use |
| --- | --- | --- |
| `publisher_query_cap_exceeded_total{cap_type,source}` | Counter | Per-cap 413 firings. Pivot by `cap_type` (`rows`/`bytes`) to know which knob to raise; by `source` for surface. |
| `publisher_max_query_rows`, `publisher_max_response_bytes` | Gauges | Live values of the corresponding env vars (and `-1` on misconfig). |
| `publisher_query_admission_rejections_total{environment}` | Counter | 503s from the memory governor at the query layer. Hot environments stand out via the label. |
| `publisher_package_admission_rejections_total{environment,reason}` | Counter | 503s from the memory governor at the package-load layer. |
| `publisher_query_timeout_total{timeout_ms}`, `publisher_query_timeout_ms` | Counter, gauge | 504 firings and the live `PUBLISHER_QUERY_TIMEOUT_MS` value. |
| `publisher_query_concurrency_rejections_total{http.route,limit}` | Counter | 503s from the per-pod query concurrency cap, labeled by hot route (HTTP) or `mcp:executeQuery`. |
| `publisher_query_active_slots`, `publisher_query_max_slots` | Gauges | Live in-flight slot count and cap — render utilization as `active / max`. |
| `publisher_process_rss_bytes`, `publisher_heap_size_limit_bytes`, `publisher_heap_used_bytes` | Gauges | Process RSS, V8 heap ceiling (`--max-old-space-size`), V8 used heap. |
| `publisher_memory_backpressure_active`, `_activations_total` | Gauge, counter | Current governor state and historical activations. |
| `http_server_requests_total{http.status_code}` | Counter | Coarse 413/503/504 totals — pair with the dedicated counters above for per-cause breakdown. |

## Theming

Publisher renders charts, tables, and dashboard tiles with a configurable light/dark theme. See
[theming.md](theming.md) for the config-file, editor, and per-chart annotation layers.
