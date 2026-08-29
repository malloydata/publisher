<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Configuration

> What this is: the complete runtime configuration reference — the config file, every environment
> variable and CLI flag, the OOM/operational-tuning knobs, and the metrics that observe them. For how
> to _run_ the server, see [deployment.md](deployment.md).

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

The copy is the part that surprises people: **without `--watch-env`, a local package is served
from its boot-time copy, so edits to your source directory are never read**, however many times
you save or reload — a `?reload=true` still answers 200, recompiled from the copy. Local authoring
means starting the server with `--watch-env <env>`, which mounts the environment's local packages
in place and live-reloads them.

Adding `--watch-env` to a *later* boot is not enough on its own, and this is the one to watch for:
the mount is decided when an environment is first loaded from config, so a package already copied
into `publisher_data/` stays a copy, and the boot still logs `Watch mode active` over it. Pass
`--watch-env <env> --init` once to re-mount (`--init` alone re-copies). After that, plain
`--watch-env` boots keep watching. `publisher_data/<env>/<pkg>` shows which you got: a symlink is
mounted, a real directory is a copy.

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
| `PUBLISHER_RATE_LIMIT` | _none_ | _unset_ | Maximum requests per minute one client may make to the REST server; over it, requests get `429` with `RateLimit-*` headers until the minute rolls over. Unset or `0` means no limit; any other value must be a non-negative integer or startup fails. `/health*` and `/metrics` are never limited. Clients are told apart by the connection's peer address, so behind a reverse proxy every client shares one bucket: rate-limit at the proxy there, or leave this unset. The MCP port is not covered. |
| `MCP_PORT` | `--mcp_port <n>` | `4040` | MCP HTTP port. Serves the six MCP tools (`malloy_getContext`, `malloy_executeQuery`, `malloy_compile`, `malloy_reloadPackage`, `malloy_searchDocs`, `malloy_searchDatabaseSchema`) and the agent skills as MCP prompts. |
| `SERVER_ROOT` | `--server_root <dir>` | `.` (cwd) | Where Publisher keeps its own storage (`publisher_data/`, `publisher.db`), and where it looks for `publisher.config.json` when `--config` is not passed. |
| `PUBLISHER_NO_MCP_CONFIG` | `--no-mcp-config` | _unset_ | Stops the server writing a `.mcp.json` into its working directory on startup. Accepts `1`/`true`/`yes`/`on` to disable and `0`/`false`/`no`/`off`/empty to leave on; anything else, including a value an env file left quotes around, is a startup error rather than a disable. See [The `.mcp.json` the server writes](#the-mcpjson-the-server-writes). |
| `PUBLISHER_NO_MCP_APPS` | none | _unset_ | Stops the server offering the MCP Apps widget, so a query result renders as JSON in the chat rather than as a chart. Turns off all four surfaces together: the `io.modelcontextprotocol/ui` extension, the `ui://` resource, the tool's widget metadata, and the sentence about it in the server instructions. Same boolean spellings and the same startup error on anything else as `PUBLISHER_NO_MCP_CONFIG` above. Useful because the alternative is rebuilding without the widget, which whoever runs the published package or the image cannot do. See [Inline result rendering](ai-agents.md#inline-result-rendering-mcp-apps). |
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
| `PUBLISHER_MAX_RESPONSE_BYTES` | — | `50000000` (50 MB) | Maximum JSON-serialized response size, for ad-hoc SQL, model queries and notebook cells. Streaming-capable connections (Postgres, DuckDB) enforce mid-stream and abort the driver immediately; non-streaming connections enforce post-buffer. Exceeding the cap fails with HTTP 413. Set to `0` to disable. See the note below the table. |
| `PUBLISHER_DEFAULT_QUERY_ROW_LIMIT` | — | `1000` | Default `LIMIT` applied to model queries that don't include their own. Always ≤ `PUBLISHER_MAX_QUERY_ROWS`. `0` is rejected. |
| `PUBLISHER_QUERY_TIMEOUT_MS` | — | `300000` (5 min) | Wall-clock timeout per query (all surfaces). Wired to the underlying SDK via `AbortSignal`; queries that exceed the budget are aborted and return HTTP 504. Set to `0` to disable. |
| `PUBLISHER_MAX_CONCURRENT_QUERIES` | — | `32` | Per-pod cap on simultaneous in-flight queries (HTTP + MCP share the same slot pool). When the cap is reached, new queries fail fast with HTTP 503 (or the MCP-error equivalent). Tune higher under load; set to `0` to disable. |
| `PUBLISHER_MAX_MEMORY_BYTES` | — | _unset_ | Enables the RSS-based memory governor. When set, the governor samples process RSS every `PUBLISHER_MEMORY_CHECK_INTERVAL_MS` ms and rejects new package loads and queries with HTTP 503 once RSS crosses `PUBLISHER_MEMORY_HIGH_WATER_FRACTION × PUBLISHER_MAX_MEMORY_BYTES`, until it drops below `PUBLISHER_MEMORY_LOW_WATER_FRACTION ×`. Unset or `0` disables. |
| `PUBLISHER_MEMORY_HIGH_WATER_FRACTION` | — | `0.8` | High-water mark (fraction of `PUBLISHER_MAX_MEMORY_BYTES`). Must be in `(0, 1)` and strictly above the low-water mark. |
| `PUBLISHER_MEMORY_LOW_WATER_FRACTION` | — | `0.7` | Low-water mark (fraction of `PUBLISHER_MAX_MEMORY_BYTES`). Hysteresis: back-pressure clears when RSS dips below this value. |
| `PUBLISHER_MEMORY_CHECK_INTERVAL_MS` | — | `5000` | RSS sampling interval (ms). Minimum 100. |
| `PUBLISHER_MEMORY_BACKPRESSURE` | — | `true` | Set to `false` to disable the 503 behavior while keeping RSS monitoring — useful for a metrics-only rollout before enabling enforcement. |
| `PUBLISHER_DUCKDB_MEMORY_LIMIT` | — | _unset_ | DuckDB `memory_limit` applied to **every** DuckDB session and instance Publisher owns, as a flat value (`1GB`, `512MB`) passed to DuckDB verbatim. `off` or unset leaves DuckDB's own default, which is roughly 80% of the container's memory computed **per instance** — so N instances in one process commit N times that share, and the kernel kills the process while each of them still believes it is inside its budget. Measured in a 3 GiB container: three instances each reported a 2.3 GiB limit, 6.9 GiB of committed budget against 3 GiB of real memory. **Sizing:** the divisor is not a number of builds. Count 1 metadata store + 1 serve-shape gate session + 1 environment lookup funnel + one sandbox per loaded package × `PACKAGE_LOAD_WORKERS` (package loads run in `worker_threads`, so same address space and same cgroup) + one per in-flight materialization build, and nothing bounds concurrent builds. Budget roughly (container memory − resident baseline) ÷ that count. DuckDB does not reserve the limit up front, so an idle instance costs far less than its budget — the limit caps what it may reach, and it is the sum of the caps that has to fit. Too low fails a query with a DuckDB out-of-memory error and leaves the process up, which is the intended trade against losing the pod and every package on it. Validated at startup. |
| `PUBLISHER_DUCKDB_TEMP_DIRECTORY` | — | _unset_ | Where DuckDB spills. A materialization build overrides this with its own disposable working directory (unique per build, removed with the build); every other session and instance uses this. Unset leaves DuckDB's default of `.tmp` relative to the process working directory, whose default `max_temp_directory_size` is 90% of whatever filesystem that lands on — on a container with no ephemeral-storage limit, the node's shared disk. Created at startup if missing, since `SET temp_directory` accepts a path that does not exist and only fails at the first spill. Unlike the memory limit there is no `off` sentinel, because a directory named `off` is a legal path. Note that setting `PUBLISHER_DUCKDB_MEMORY_LIMIT` does not by itself create spill on the `storage=` build path: that pipeline pushes its SQL to the source warehouse and streams the result into the destination, with no join, sort or aggregation to spill. Local compute, and therefore spill, is the chained-build and serve paths. |
| `PUBLISHER_LOCAL_MATERIALIZATION_SCHEDULER` | — | `false` | Opt-in: enable the standalone materialization scheduler, which fires each loaded package's `materialization.schedule` cron so a self-hosted Publisher rebuilds on a cadence with no control plane. **Never set this on a control-plane-driven (orchestrated) worker** — it is the primary guard against double-driving refresh. See [materialization.md](materialization.md). |
| `PUBLISHER_MATERIALIZATION_SCHEDULER_INTERVAL_MS` | — | `60000` (1 min) | How often the scheduler sweeps for due schedules, in ms. Minimum `1000`. Only read when the scheduler is enabled. |
| `PUBLISHER_MATERIALIZATION_SCHEDULER_MAX_FIRES_PER_TICK` | — | `10` | Stampede guard: max packages fired per sweep. A capped package fires on a later tick. Must be a positive integer. |
| `PERSIST_STORAGE_MODE` | — | `off` | Controls the `#@ persist storage=<name>` materialization tier (materialize a source into a registered [storage destination](connections.md#storage-destinations) and serve it from there — a destination is declared alongside `connections`, not in it, and is not nameable from a model). `off`: the `storage=` annotation is inert — sources build and serve from their own warehouse exactly as without it. `write-only`: materialize into the storage destination but still serve live (the measurement rung). `on`: build **and** serve from the storage table via the virtual-source transform. Read at startup. A kill switch: moving it **down** never fails a loaded package — a `storage=` source just reverts to serving live and surfaces as a package warning. See [persist-storage-tutorial.md](persist-storage-tutorial.md). |
| `EMBEDDING_API_KEY` | — | _unset_ | Enables semantic (embedding-based) ranking for `malloy_getContext` question retrieval. Sent as a bearer token to the embedding endpoint. Unset: retrieval stays lexical (lunr/BM25), unchanged. Must be set explicitly; an ambient `OPENAI_API_KEY` is deliberately not read. See "Semantic retrieval for malloy_getContext" below. |
| `EMBEDDING_MODEL` | — | `text-embedding-3-small` | Embedding model name sent to the endpoint. |
| `EMBEDDING_API_BASE` | — | `https://api.openai.com/v1` | Base URL of an OpenAI-compatible embeddings API (`POST <base>/embeddings`). Point at any compatible endpoint (e.g. a local Ollama or vLLM server). |
| `EMBEDDING_DIMENSIONS` | — | _unset_ | Optional `dimensions` request parameter (e.g. `512` to shrink `text-embedding-3-small` vectors). When unset the parameter is omitted, which suits providers that do not support it. |
| `EMBEDDING_INDEX_CONNECTION_SCHEMA` | — | `false` | Allows `malloy_searchDatabaseSchema` to send a connection's schema name, table names, column names and column types, plus the agent's search text, to the embedding endpoint for semantic ranking. Never row values. A second switch on top of `EMBEDDING_API_KEY`, which alone covers only your own model text; unset, schema search still works and ranks lexically. Accepts `1`/`true`/`yes`/`on` and `0`/`false`/`no`/`off`. It is read when the tool is called, not at startup, so an unrecognised value does not stop the server: the tool logs a warning and ranks lexically for that call. See "Semantic ranking for malloy_searchDatabaseSchema" below. |
| — | `--help`, `-h` | — | Print the full flag list. |

`PUBLISHER_MAX_RESPONSE_BYTES` measures the shape the caller asked for, not one fixed shape.
On the model-query endpoint that means two requests differing only in `compactJson` reach the
cap at different result sizes. Over MCP it measures the compact rows the tool's envelope is
built from, which is not what the agent receives: the envelope is capped separately at 90k
characters by its own budget.

PostgreSQL and other database-specific connections may also honor their respective driver env vars
(e.g. `PGSSLMODE`).

### The `.mcp.json` the server writes

On startup the server writes a `.mcp.json` into the directory it was run in, naming the MCP endpoint it
actually bound, so an agent started in that directory finds the `malloy_*` tools with no registration
step. This is the one place these rules are written down; the README, `AGENTS.md` and
[deployment.md](deployment.md) point here rather than repeating them.

It lands in the **working** directory, not `--server_root`, because the file is for whoever opens an
agent there. It only ever creates, and never reads: an existing file may hold other servers and their
credentials, and rewriting it on every boot would churn version-controlled files.

It does not always write one. Whenever it does not, it says so at `info` and prints the `claude mcp add`
command that connects an agent anyway, so the startup log is the authority rather than this list. The one
exception is turning it off with `PUBLISHER_NO_MCP_CONFIG` or `--no-mcp-config`, which is deliberately
silent: you asked for nothing, so it says nothing. Everything below is announced.

| Case | Why |
| --- | --- |
| The directory already has a `.mcp.json` | Only ever created, never edited. Yours is left exactly as it is, unread. |
| The directory is inside a git working tree | An untracked file in a checkout is a surprise in `git status` and gets committed by accident. Your own project is usually a repo, so this is the common case, not an exotic one. |
| The directory is your home directory, or the filesystem root | Nobody opens an agent in either, and a process manager that leaves the working directory unset lands in `/`. |
| The MCP port bound is not the one requested | `--mcp_port 0`, or a non-numeric `MCP_PORT` under Bun (Node refuses to bind at all). That port changes every run, so a saved file would be wrong from the next boot. |
| The write failed | For instance a read-only directory. Nothing is broken: the server is serving, only the convenience file is missing. |

The command it prints uses the default local scope, which is the scope that outranks a project
`.mcp.json`. `-s user` would be shadowed by the very file that caused the message.

The URL names the address the server bound, using the loopback literal of that family when the bind is
a wildcard, rather than `localhost`, which is ambiguous across IPv4 and IPv6. That is for correctness,
not isolation: any process running as you can bind the same port.

**The file outlives the server and is never corrected.** A stale one does not simply fail. If something
else holds that port later, perhaps a second Publisher serving different data, an agent started there
connects to it and answers from the wrong model. `malloy_getContext` names the environment and packages
an agent is actually talking to, which is the way to settle it. That detects the accident, which is the
realistic case; it is not proof against a process deliberately holding the port, since that process can
answer too.

The Docker image sets `PUBLISHER_NO_MCP_CONFIG=1`, since no agent session starts inside a container.

## Semantic retrieval for malloy_getContext

By default, `malloy_getContext`'s question retrieval is lexical (lunr/BM25) over the model's own
text, so a query only matches entities that share tokens with it (`departure delay` never finds a
field named `dep_delay`). Setting `EMBEDDING_API_KEY` switches ranking to embedding similarity:
each package's entities (source, view, named query, dimension, and measure names plus their
annotation text) are embedded once and searched by cosine similarity, so synonyms and `snake_case`
names match by meaning.

What to know before turning it on:

- What leaves the machine: entity names, their annotation text (the `#(doc)` docs, or an entity's
  other `#` annotation lines when it has no `#(doc)`), and the query strings agents pass to
  `malloy_getContext`. Model source code, data, and query results are never sent. Point
  `EMBEDDING_API_BASE` at a local OpenAI-compatible server (Ollama, vLLM) to keep everything
  on-machine; with a local server, also set `EMBEDDING_MODEL` to a model that server actually
  serves, since the default names an OpenAI model.
- Storage: vectors are cached in the server's own `publisher.db`, keyed by a content hash, so only
  new or changed entities re-embed, across restarts too. `--init` wipes the cache along with the
  rest of persisted storage; the only cost of a wipe is re-embedding.
- First query per package: the first question kicks off the embedding sync in the background and
  answers lexically; once the sync lands, later questions are ranked semantically. Responses carry
  a `retrieval` field (`"semantic"` or `"lexical"`) whenever the provider is configured.
- Failure behavior: if the endpoint is down, times out, or rejects the key, retrieval falls back
  to lexical (with a warning in the server log) and retries after a cool-down. A package with more
  than 5,000 entities stays lexical.
- To measure the difference on your own models, see the eval script header in
  `packages/server/src/mcp/tools/get_context_eval.ts`.

## Semantic ranking for `malloy_searchDatabaseSchema`

`malloy_searchDatabaseSchema` searches a configured connection's schema, so an agent can find the
right tables in a database it has never seen and start a model from them. Its ranking is lexical by
default and needs no configuration, no API key, and no network.

Semantic ranking is opt-in and needs **two** variables set, not one: `EMBEDDING_API_KEY` **and**
`EMBEDDING_INDEX_CONNECTION_SCHEMA=true`. That is deliberate. The API key on its own authorises
embedding your own model text, which is already on your disk. A database's table and column names are
your customer's, and turning on semantic `malloy_getContext` should not quietly start sending them to
a third party. Setting the second variable is how you say you meant to.

What to know before turning it on:

- What leaves the machine: the schema (or dataset) name, table names, column names, column types,
  and the query strings agents pass to the tool. **Never any row values**: no value from a row is
  returned, logged or embedded. (One nuance, since this section is the one an auditor quotes: for a
  DuckDB connection over CSV or JSON files, DuckDB's own type sniffer reads the head of the file to
  infer column types. That inference happens in your warehouse and only its column names and types
  leave; no cell value is returned or sent anywhere.) Point `EMBEDDING_API_BASE` at a local OpenAI-compatible server
  (Ollama, vLLM) to keep even the names on-machine.
- What it buys you: lexical ranking only matches tables that share words with the question, so
  "website visits" does not find `web_session_events` and "newsletter blasts" does not find
  `marketing_email_campaigns`. Embedding similarity matches those by meaning. On the eval fixture,
  lexical scores 6/10 at recall@3, missing exactly the four questions that share no words with
  their table.
- Storage: schema vectors are held **in memory only**, keyed per connection and schema and
  invalidated by a fingerprint over the tables and their columns. The key is the environment,
  connection and schema (plus the package, for the per-package `duckdb` sandbox). At most a few
  schemas are held at once, least-recently-used evicted. They are not written to
  `publisher.db`, so a restart re-embeds a schema the first time it is searched again.
- Failure behavior: if the endpoint is down, times out, or rejects the key, ranking falls back to
  lexical (with a warning in the server log) and retries after a cool-down. A schema with more than
  5,000 tables is not embedded and stays lexical; lexical is not the cheaper branch, it just
  needs no provider. Its index is built once per schema and cached, so the cost lands on the first
  search after a schema changes rather than on every search. A search response carries a `ranking` field (`"semantic"` or `"lexical"`) so you can tell which
  one answered; a plain listing does no ranking and carries none.
- To measure the difference yourself, including the A/B against the lexical baseline, see the eval
  script header in `packages/server/src/mcp/tools/schema_search_eval.ts`.

## Operational tuning: OOM guards

The publisher exports OpenTelemetry metrics (under the `publisher` meter) so the OOM guardrails above
can be observed and tuned in production. The most useful series for this work:

| Metric                                                                                        | Type           | Use                                                                                                             |
| --------------------------------------------------------------------------------------------- | -------------- | --------------------------------------------------------------------------------------------------------------- |
| `publisher_query_cap_exceeded_total{cap_type,source}`                                         | Counter        | 413 firings for an oversized response. Pivot by `cap_type` and `source`; see the note below. |
| `publisher_max_query_rows`, `publisher_max_response_bytes`                                    | Gauges         | Live values of the corresponding env vars (and `-1` on misconfig).                                              |
| `publisher_query_admission_rejections_total{environment}`                                     | Counter        | 503s from the memory governor at the query layer. Hot environments stand out via the label.                     |
| `publisher_package_admission_rejections_total{environment,reason}`                            | Counter        | 503s from the memory governor at the package-load layer.                                                        |
| `publisher_query_timeout_total{timeout_ms}`, `publisher_query_timeout_ms`                     | Counter, gauge | 504 firings and the live `PUBLISHER_QUERY_TIMEOUT_MS` value.                                                    |
| `publisher_query_concurrency_rejections_total{http.route,limit}`                              | Counter        | 503s from the per-pod query concurrency cap, labeled by hot route (HTTP) or `mcp:executeQuery`.                 |
| `publisher_query_active_slots`, `publisher_query_max_slots`                                   | Gauges         | Live in-flight slot count and cap — render utilization as `active / max`.                                       |
| `publisher_process_rss_bytes`, `publisher_heap_size_limit_bytes`, `publisher_heap_used_bytes` | Gauges         | Process RSS, V8 heap ceiling (`--max-old-space-size`), V8 used heap.                                            |
| `publisher_memory_backpressure_active`, `_activations_total`                                  | Gauge, counter | Current governor state and historical activations.                                                              |
| `http_server_requests_total{http.status_code}`                                                | Counter        | Coarse 413/503/504 totals — pair with the dedicated counters above for per-cause breakdown.                     |

`cap_type` says what to do about a 413: `rows` and `bytes` mean that cap was exceeded, so
raising it is an option, while `unserializable` means the response could not be turned into JSON
at all and no cap raise fixes it. `unserializable` is never emitted for `source: connection_sql`,
which has no such guard on its own path.

## Theming

Publisher renders charts, tables, and dashboard tiles with a configurable light/dark theme. See
[theming.md](theming.md) for the config-file, editor, and per-chart annotation layers.
