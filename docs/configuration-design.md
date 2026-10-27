<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Design: one configuration model

**Status: proposed.** Nothing here is built. The user-facing reference is
[configuration.md](configuration.md), which this design would generate rather than hand-maintain.
The decisions are under [Decisions](#decisions), and the questions still open under
[Open questions](#open-questions).

## Goal

Every Publisher setting is one entry in one registry, with one name, one type and one default, and can
be given in any of three places: the config file, an environment variable, or a command-line flag. Where
a value comes from is the operator's choice, not an accident of when the setting was added.

## What exists today

Publisher reads its settings from three places, and which place a setting lives in follows no rule:

- **`publisher.config.json`** began as the list of what to serve, and grew server-wide settings one at a
  time: `frozenConfig`, `theme`, `mcp.includeHiddenFilesAndSources` and the `retrieval` block. With the
  admin API, `servingApiEnabled` and `adminApiEnabled` arrive already following this design: each is a
  key in the file, a derived variable and a derived flag, read with the precedence below.
- **Environment variables** carry everything else, about 60 of them, most added for container
  deployments.
- **Command-line flags** exist for about a dozen of those variables, the ones a local user reaches for:
  ports, hosts, the server root, the config path, shutdown timing, watch mode and `--init`.

Every string in the config file may reference a variable as `${VAR}`, which is how a file names a
credential without holding it.

### What that costs

| Problem | Example |
|---|---|
| The same kind of setting lives in different places | MCP's port and host are variables, but what MCP exposes (`mcp.includeHiddenFilesAndSources`) is in the file |
| A setting can be given in two places with no general rule for which wins | the `retrieval.embedding` keys override the `EMBEDDING_*` variables; nothing else overlaps |
| Booleans are read five ways | most variables accept `1/true/yes/on` and `0/false/no/off` and refuse anything else; `INITIALIZE_STORAGE`, `PUBLISHER_USE_BUNDLED_DEFAULT` and `PUBLISHER_ALLOW_PROXY_CONNECTIONS` are on only for exactly `true`; `DISABLE_RESPONSE_LOGGING` for `true` or `1`; `PUBLISHER_ALLOW_UNVERIFIED_SSH_HOST_KEY` reads an unrecognised value as off where it is used, because one reader runs inside ssh2's synchronous host-key callback, where a throw would escape; and `frozenConfig` is read with `Boolean()`, so `"frozenConfig": "false"` freezes the server |
| A typo is silent | an unknown key in the file is ignored (only `retrieval.llm`, `retrieval.embedding` and `retrieval.egress` refuse one), so a misspelled `adminApiEnable` runs the API it meant to turn off; an unknown flag, and a flag given without its value, are ignored too |
| A `${VAR}` cannot supply a number or a boolean | substitution always yields a string |
| Names do not follow a pattern | about 20 Publisher settings lack the `PUBLISHER_` prefix (`MCP_PORT`, `SERVER_ROOT`, `LOG_LEVEL`, `EMBEDDING_MODEL`, …); flags mix `--mcp_port` with `--watch-env`; a flag and its variable can differ (`--watch-env` is `PUBLISHER_WATCH`, `--init` is `INITIALIZE_STORAGE`); two switches are negative (`--no-mcp-config`, `DISABLE_RESPONSE_LOGGING`) |
| The reference drifts from the code | `--config`, `PERSIST_COLLISION_ENFORCE` and `S3_CREDENTIAL_CHAIN_POLICY` are not in the configuration table |

## Design

### One registry

A single module declares every setting:

```ts
setting({
   key: "mcp.port",           // its path in publisher.config.json
   type: integer({ min: 0, max: 65535 }),
   default: 4040,
   description: "MCP HTTP port.",
   aliases: { env: ["MCP_PORT"], flags: ["--mcp_port"] },  // deprecated names, read during migration
});
```

The registry is the only place a setting is defined. Parsing, validation, `--help`, the configuration
reference and the startup log are all produced from it, and a test fails if the checked-in reference
differs from what the registry generates, if two settings derive the same variable or flag, or if one
setting's derived name is another's alias. Code reads a setting through one typed accessor
(`settings.get("mcp.port")`), never from `process.env`.

The module can be one file per section of [the mapping](#the-mapping), exported as one list; what
matters is that one list holds every setting. Settings are read once, at startup, before any module
that uses one is loaded, and a lint rule refuses `process.env` outside the registry module, which also
hands out the variables Publisher does not own (below), unparsed. A worker thread receives the parsed
settings from the thread that started it rather than reading the environment again, and a test builds
its settings with the same loader rather than writing `process.env`.

A default can be another setting rather than a constant, so `default` also takes a function of the
settings already resolved:

- `mcp.host` defaults to `host` when `host` is given and to `127.0.0.1` otherwise, while `admin.host`
  defaults to `127.0.0.1` whatever `host` is.
- `retrieval.fcj.judgeLlm.apiKey`, which
  [#1469](https://github.com/malloydata/publisher/pull/1469) adds as `JUDGE_LLM_API_KEY`, defaults to
  `retrieval.llm.apiKey`.
- `useBundledDefault` is on when neither `root` nor `config` is given. Today only the two flags count,
  so `--server_root /x` with no config at `/x` serves nothing, while `SERVER_ROOT=/x` serves the bundled
  samples. Once a setting has one value whatever its source, the variable counts too, which changes the
  second case in step 2.

### Three sources, one precedence

Each setting may be given as:

| Source | Form | Example |
|---|---|---|
| Config file | its key | `{ "mcp": { "port": 4041 } }` |
| Environment variable | `PUBLISHER_` + the key in upper snake case, a dot becoming `_` | `PUBLISHER_MCP_PORT=4041` |
| Command-line flag | `--` + the key in kebab case, a dot becoming `-` | `--mcp-port 4041` |

The variable and flag names are derived from the key and are never chosen by hand. Precedence is the
same for every setting: **flag, then variable, then file, then default.** Startup logs one line naming
each setting that is not at its default and where its value came from, with secrets shown as
`(set)`.

Three kinds of setting are exceptions, and the registry marks them:

- **Structured settings** (`environments`, `theme`, and objects such as `retrieval.llm` whose fields
  depend on each other) are file-only as a whole. Their scalar leaves (`retrieval.embedding.model`)
  still get a variable and a flag, so one object's leaves can come from different sources. Where one
  field decides what the others mean (in `retrieval.llm` and `retrieval.embedding`, `provider` decides
  which fields apply and what `baseUrl` defaults to), the object is assembled from every source first
  and validated as one, and an error names the source of each field it cites. A variable that switches
  `provider` is therefore checked against the file's other fields. A mix can still validate (a `model`
  written for the old provider), so the startup line lists that object's leaves with their sources.
- **Launch settings** (`root`, `config`, `useBundledDefault`, `initializeStorage`,
  `watchEnvironments`) have a variable and a flag but cannot be in the file: the first three decide
  which file is read, `initializeStorage` wipes storage and must not happen on every boot because a file
  said so, and `watchEnvironments` describes one development run.
- **Secrets** have no flag, because a flag is visible in a process listing. They come from a variable, or
  from the file through a `${VAR}` reference, and are never logged or reported. A literal secret in the
  file fails startup. Telling a literal from a reference means checking the file's raw value before
  `${VAR}` substitution, because a substituted value looks like a literal. The `retrieval` API keys are
  new file keys: today they come only from `EMBEDDING_API_KEY` and `LLM_API_KEY`, and `retrieval.llm`
  and `retrieval.embedding` refuse an `apiKey`.

Variables Publisher does not own keep their names and stay outside the registry: `NODE_ENV`,
`NODE_OPTIONS`, `NODE_EXTRA_CA_CERTS` (which the Docker image sets to its RDS CA bundle), `OTEL_*`,
`GOOGLE_APPLICATION_CREDENTIALS`, libpq's `PGSSLMODE` and `PGSERVICE` (which Publisher's Postgres
connection setup also reads), and the `ADBC_*` variables the Docker image's driver shim reads.

### Types, read the same way from every source

| Type | In the file | As a variable or flag |
|---|---|---|
| boolean | `true` or `false` | `true/false`, `1/0`, `yes/no`, `on/off`, any case; a bare `--x` is true and `--no-x` is false |
| integer, number | a JSON number, within the registered range | the same, parsed |
| enum | one of the registered strings | the same, any case |
| list | a JSON array | comma-separated; a flag may repeat |
| string | a JSON string | the same |

An empty variable is unset, as it already is for nearly every variable (`PACKAGE_LOAD_WORKERS=""`
fails startup today), so `-e NAME=` undoes a value an image sets.

Units stay in the name (`timeoutMs`, `maxResponseBytes`, `drainDurationSeconds`) and values are plain
integers in that unit. The one exception is `duckdb.memoryLimit`, which DuckDB parses (`1GB`), so it is
a string and says so.

A `${VAR}` in the file is substituted first and then read as the setting's type, so
`"adminApiEnabled": "${ADMIN_API_ENABLED}"` works.

### Validation

A value that does not parse as its type, an unknown key at any registered level of the file, an unknown
flag or a flag given without its value, or the same setting given under an old and a new name with
different values fails startup, with one message naming
the setting, the source and what was expected (`mcp.port from PUBLISHER_MCP_PORT: expected an integer
from 0 to 65535, got "40a0"`). An unknown key gets the nearest registered key as a suggestion. An old
and a new name are compared after parsing, so a negated alias agrees with its new name
(`PUBLISHER_NO_MCP_CONFIG=1` and `PUBLISHER_MCP_CONFIG_FILE_ENABLED=false`). A file that cannot be read
or parsed fails startup before any setting is read, so no setting falls back to its default because the
file was unreadable.
Cross-setting rules stay where they are today (the admin API's checks, the memory governor's
high-water above low-water) but run on the parsed settings.

### Names

The top level of the file holds the server's own settings, so the most used names do not change:
`port` is `PUBLISHER_PORT` and `--port`, and `frozenConfig` gains `PUBLISHER_FROZEN_CONFIG`. Other
settings sit in sections named for the subsystem they configure. A boolean that turns a feature on or
off ends in `Enabled` and defaults to whichever state is safe; the `allow` section holds permissions,
each off by default except `allow.s3CredentialChain`, which keeps today's default `any`. Deployments that
read S3 through the host's own role (an EKS pod's IRSA role) depend on that default, so changing it
would be a breaking change with its own migration step.

## The mapping

Every setting Publisher reads today, with its key. **Unchanged** means today's variable and flag already
match the derived names.

### Server (top level)

| Today | Key | Variable | Flag | Notes |
|---|---|---|---|---|
| `frozenConfig` (file) | `frozenConfig` | `PUBLISHER_FROZEN_CONFIG` | `--frozen-config` | strict boolean |
| `servingApiEnabled`, `PUBLISHER_SERVING_API_ENABLED`, `--[no-]serving-api-enabled` | `servingApiEnabled` | unchanged | unchanged | already follows this design |
| `adminApiEnabled`, `PUBLISHER_ADMIN_API_ENABLED`, `--[no-]admin-api-enabled` | `adminApiEnabled` | unchanged | unchanged | already follows this design |
| `PUBLISHER_PORT`, `--port` | `port` | unchanged | unchanged | |
| `PUBLISHER_HOST`, `--host` | `host` | unchanged | unchanged | |
| `PUBLISHER_RATE_LIMIT` | `rateLimit` | unchanged | `--rate-limit` | requests per minute |
| `PUBLISHER_FRAME_ANCESTORS` | `frameAncestors` | unchanged | `--frame-ancestors` | |
| `theme` (file) | `theme` | — | — | structured |
| `environments` (file) | `environments` | — | — | structured |

### Launch settings (no file key)

| Today | Key | Variable | Flag |
|---|---|---|---|
| `SERVER_ROOT`, `--server_root` | `root` | `PUBLISHER_ROOT` | `--root` |
| `PUBLISHER_CONFIG_PATH`, `--config` | `config` | `PUBLISHER_CONFIG` | `--config` |
| `PUBLISHER_USE_BUNDLED_DEFAULT` | `useBundledDefault` | unchanged | `--use-bundled-default` |
| `INITIALIZE_STORAGE`, `--init` | `initializeStorage` | `PUBLISHER_INITIALIZE_STORAGE` | `--initialize-storage` (`--init` kept as a short form) |
| `PUBLISHER_WATCH`, `--watch-env` | `watchEnvironments` | `PUBLISHER_WATCH_ENVIRONMENTS` | `--watch-environments` (`--watch-env` kept as a short form) |

`watchEnvironments` is a list. Today `--watch-env` adds to `PUBLISHER_WATCH` rather than replacing it.
Under one precedence the flag replaces the variable, as it does for every other list.

### `mcp`

| Today | Key | Variable | Flag |
|---|---|---|---|
| `MCP_PORT`, `--mcp_port` | `mcp.port` | `PUBLISHER_MCP_PORT` | `--mcp-port` |
| `MCP_HOST`, `--mcp_host` | `mcp.host` | `PUBLISHER_MCP_HOST` | `--mcp-host` |
| `MCP_CORS_ORIGINS` | `mcp.corsOrigins` (list) | `PUBLISHER_MCP_CORS_ORIGINS` | `--mcp-cors-origins` |
| `PUBLISHER_NO_MCP_CONFIG`, `--no-mcp-config` | `mcp.configFileEnabled` (default `true`) | `PUBLISHER_MCP_CONFIG_FILE_ENABLED` | `--mcp-config-file-enabled` / `--no-mcp-config-file-enabled` |
| `mcp.includeHiddenFilesAndSources` (file) | unchanged | `PUBLISHER_MCP_INCLUDE_HIDDEN_FILES_AND_SOURCES` | `--mcp-include-hidden-files-and-sources` |
| `PUBLISHER_MCP_TRACE` | `mcp.trace` (enum: `retrieval`) | unchanged | `--mcp-trace` |

### `admin` (the admin API's settings)

| Today | Key | Variable | Flag |
|---|---|---|---|
| `PUBLISHER_ADMIN_PORT`, `--admin_port` | `admin.port` | unchanged | `--admin-port` |
| `PUBLISHER_ADMIN_HOST`, `--admin_host` | `admin.host` | unchanged | `--admin-host` |
| `PUBLISHER_ADMIN_CATALOG_DATABASE` | `admin.catalogDatabase` | unchanged | — (secret) |
| `PUBLISHER_ADMIN_STORAGE_PROVIDER` | `admin.storage.provider` | unchanged | `--admin-storage-provider` |
| `PUBLISHER_ADMIN_STORAGE_KEY_ID` | `admin.storage.keyId` | unchanged | `--admin-storage-key-id` |
| `PUBLISHER_ADMIN_STORAGE_SECRET` | `admin.storage.secret` | unchanged | — (secret) |
| `PUBLISHER_ADMIN_STORAGE_REGION` | `admin.storage.region` | unchanged | `--admin-storage-region` |
| `PUBLISHER_ADMIN_STORAGE_ENDPOINT` | `admin.storage.endpoint` | unchanged | `--admin-storage-endpoint` |

### `query`, `postgres`, `memory`, `packageLoad`

| Today | Key | Variable |
|---|---|---|
| `PUBLISHER_MAX_QUERY_ROWS` | `query.maxRows` | `PUBLISHER_QUERY_MAX_ROWS` |
| `PUBLISHER_MAX_RESPONSE_BYTES` | `query.maxResponseBytes` | `PUBLISHER_QUERY_MAX_RESPONSE_BYTES` |
| `PUBLISHER_DEFAULT_QUERY_ROW_LIMIT` | `query.defaultRowLimit` | `PUBLISHER_QUERY_DEFAULT_ROW_LIMIT` |
| `PUBLISHER_QUERY_TIMEOUT_MS` | `query.timeoutMs` | unchanged |
| `PUBLISHER_MAX_CONCURRENT_QUERIES` | `query.maxConcurrent` | `PUBLISHER_QUERY_MAX_CONCURRENT` |
| `PUBLISHER_QUERY_METADATA` | `query.metadataEnabled` (default `false`) | `PUBLISHER_QUERY_METADATA_ENABLED` |
| `PUBLISHER_POSTGRES_POOL_MAX` | `postgres.poolMax` | unchanged |
| `PUBLISHER_MAX_MEMORY_BYTES` | `memory.maxBytes` | `PUBLISHER_MEMORY_MAX_BYTES` |
| `PUBLISHER_MEMORY_HIGH_WATER_FRACTION` | `memory.highWaterFraction` | unchanged |
| `PUBLISHER_MEMORY_LOW_WATER_FRACTION` | `memory.lowWaterFraction` | unchanged |
| `PUBLISHER_MEMORY_CHECK_INTERVAL_MS` | `memory.checkIntervalMs` | unchanged |
| `PUBLISHER_MEMORY_BACKPRESSURE` | `memory.backpressureEnabled` | `PUBLISHER_MEMORY_BACKPRESSURE_ENABLED` |
| `PACKAGE_LOAD_WORKERS` | `packageLoad.workers` | `PUBLISHER_PACKAGE_LOAD_WORKERS` |
| `PACKAGE_LOAD_JOB_TIMEOUT_MS` | `packageLoad.jobTimeoutMs` | `PUBLISHER_PACKAGE_LOAD_JOB_TIMEOUT_MS` |
| `PACKAGE_LOAD_SCHEMA_CACHE_BYTES` | `packageLoad.schemaCacheBytes` | `PUBLISHER_PACKAGE_LOAD_SCHEMA_CACHE_BYTES` |

Every row here also gets the derived flag (`--query-max-rows`, …). `PUBLISHER_QUERY_METADATA` takes only
`on` and `off`, so it is a boolean under the `Enabled` rule, and today's two values are boolean spellings
the alias keeps reading.

### `duckdb`, `ducklake`, `persist`, `materialization`

| Today | Key | Variable |
|---|---|---|
| `EXTENSION_FETCH_POLICY` | `duckdb.extensionFetchPolicy` | `PUBLISHER_DUCKDB_EXTENSION_FETCH_POLICY` |
| `PUBLISHER_DUCKDB_MEMORY_LIMIT` | `duckdb.memoryLimit` | unchanged |
| `PUBLISHER_DUCKDB_TEMP_DIRECTORY` | `duckdb.tempDirectory` | unchanged |
| `PUBLISHER_PARTITIONED_WRITE_FLUSH_THRESHOLD` | `duckdb.partitionedWriteFlushThreshold` | `PUBLISHER_DUCKDB_PARTITIONED_WRITE_FLUSH_THRESHOLD` |
| `PUBLISHER_DUCKLAKE_ROW_GROUP_SIZE_BYTES` | `ducklake.rowGroupSizeBytes` | unchanged |
| `PUBLISHER_DUCKLAKE_TARGET_FILE_SIZE_BYTES` | `ducklake.targetFileSizeBytes` | unchanged |
| `PERSIST_STORAGE_MODE` | `persist.storageMode` | `PUBLISHER_PERSIST_STORAGE_MODE` |
| `PERSIST_COLLISION_ENFORCE` | `persist.collisionEnforcementEnabled` | `PUBLISHER_PERSIST_COLLISION_ENFORCEMENT_ENABLED` |
| `PUBLISHER_LOCAL_MATERIALIZATION_SCHEDULER` | `materialization.schedulerEnabled` | `PUBLISHER_MATERIALIZATION_SCHEDULER_ENABLED` |
| `PUBLISHER_MATERIALIZATION_SCHEDULER_INTERVAL_MS` | `materialization.schedulerIntervalMs` | unchanged |
| `PUBLISHER_MATERIALIZATION_SCHEDULER_MAX_FIRES_PER_TICK` | `materialization.schedulerMaxFiresPerTick` | unchanged |

### `allow`, `authorize`, `logging`, `shutdown`

| Today | Key | Variable |
|---|---|---|
| `PUBLISHER_ALLOW_PROXY_CONNECTIONS` | `allow.proxyConnections` | unchanged |
| `PUBLISHER_ALLOW_DUCKDB_SETUP_SQL` | `allow.duckdbSetupSql` | unchanged |
| `PUBLISHER_ALLOW_UNVERIFIED_SSH_HOST_KEY` | `allow.unverifiedSshHostKey` | unchanged |
| `S3_CREDENTIAL_CHAIN_POLICY` | `allow.s3CredentialChain` (enum: `any`, `destinations-only`, `off`) | `PUBLISHER_ALLOW_S3_CREDENTIAL_CHAIN` |
| `PUBLISHER_BYPASS_AUTHORIZE_SECRET` | `authorize.bypassSecret` (secret) | `PUBLISHER_AUTHORIZE_BYPASS_SECRET` |
| `LOG_LEVEL` | `logging.level` | `PUBLISHER_LOGGING_LEVEL` |
| `DISABLE_RESPONSE_LOGGING` | `logging.responseBodiesEnabled` (default `true`) | `PUBLISHER_LOGGING_RESPONSE_BODIES_ENABLED` |
| `SHUTDOWN_DRAIN_DURATION_SECONDS`, `--shutdown_drain_duration_seconds` | `shutdown.drainDurationSeconds` | `PUBLISHER_SHUTDOWN_DRAIN_DURATION_SECONDS` |
| `SHUTDOWN_GRACEFUL_CLOSE_TIMEOUT_SECONDS`, `--shutdown_graceful_close_timeout_seconds` | `shutdown.gracefulCloseTimeoutSeconds` | `PUBLISHER_SHUTDOWN_GRACEFUL_CLOSE_TIMEOUT_SECONDS` |

`authorize.bypassSecret` is a secret and has no flag.

### `retrieval`

The block keeps its shape. Its scalar leaves gain a variable and a flag, and the `EMBEDDING_*` and
`LLM_API_KEY` variables become aliases of them:

| Today | Key | Variable |
|---|---|---|
| `EMBEDDING_API_KEY` | `retrieval.embedding.apiKey` (secret) | `PUBLISHER_RETRIEVAL_EMBEDDING_API_KEY` |
| `EMBEDDING_MODEL` | `retrieval.embedding.model` | `PUBLISHER_RETRIEVAL_EMBEDDING_MODEL` |
| `EMBEDDING_API_BASE` | `retrieval.embedding.baseUrl` | `PUBLISHER_RETRIEVAL_EMBEDDING_BASE_URL` |
| `EMBEDDING_DIMENSIONS` | `retrieval.embedding.dimensions` | `PUBLISHER_RETRIEVAL_EMBEDDING_DIMENSIONS` |
| `EMBEDDING_MIN_SIMILARITY` | `retrieval.embedding.minSimilarity` | `PUBLISHER_RETRIEVAL_EMBEDDING_MIN_SIMILARITY` |
| `EMBEDDING_INDEX_CONNECTION_SCHEMA` | `retrieval.embedding.connectionSchemaIndexingEnabled` | `PUBLISHER_RETRIEVAL_EMBEDDING_CONNECTION_SCHEMA_INDEXING_ENABLED` |
| `LLM_API_KEY` | `retrieval.llm.apiKey` (secret) | `PUBLISHER_RETRIEVAL_LLM_API_KEY` |

The other `retrieval` keys (`indexing.maxEntities`, `llm.provider`, `llm.model`, …, `egress.preset`)
keep their file paths and gain derived variables and flags.

### Settings in open pull requests

| Today | Key | Variable | Flag | Notes |
|---|---|---|---|---|
| `versionPromotion` (file), `PUBLISHER_VERSION_PROMOTION` ([#1354](https://github.com/malloydata/publisher/pull/1354)) | `versionPromotion` (enum: `on-publish`, `explicit`) | unchanged | `--version-promotion` | the key and variable already follow this design; the flag is new |
| `PUBLISHER_PACKAGE_VERSIONING` (#1354) | `packageVersioning` (enum: `on`, `off`) | unchanged | — | a transition setting that [#1381](https://github.com/malloydata/publisher/issues/1381) removes, so it keeps its name and is exempt from the `Enabled` rule |
| `JUDGE_LLM_API_KEY` (#1469) | `retrieval.fcj.judgeLlm.apiKey` (secret, default `retrieval.llm.apiKey`) | `PUBLISHER_RETRIEVAL_FCJ_JUDGE_LLM_API_KEY` | — (secret) | `retrieval.fcj.judgeLlm` is provisional |

Until step 2 ships, a new setting takes its derived name from the start, as `PUBLISHER_VERSION_PROMOTION`
does, so it never needs an alias.

## Migration

1. **The registry, behavior unchanged.** Every setting is registered under its key, with today's
   variable and flag as its names and today's precedence; code reads through the accessor; the reference
   and `--help` are generated. The boolean readers become one, but no value changes meaning in this
   step: a value that the common reader and today's reader read differently keeps today's meaning and
   is warned about. Those values include `INITIALIZE_STORAGE=1`, which is off today and would wipe
   storage on every boot; `PUBLISHER_ALLOW_PROXY_CONNECTIONS=yes`, which is refused today and would
   allow proxy connections; and `"frozenConfig": "false"`, which freezes the server today. A value no
   reader accepts (`enabled`, say), which some of them read as off today, fails startup. An unknown file
   key, an unknown flag, a flag given without its value and an invalid `LOG_LEVEL` (today it falls back
   to `debug`) are warned about, not yet refused. Each reading this step keeps and a later step ends (an
   exact-`true` boolean, the `retrieval.embedding` keys winning over their variables, `--watch-env`
   adding to `PUBLISHER_WATCH`, only the flags turning `useBundledDefault` off) is recorded in the
   registry next to the alias or default it belongs to, so the later step removes it from one place.
   Some settings are exceptions to "behavior unchanged". Today they accept values of more than one
   type, and the registry gives each setting a single type (a boolean, an integer, an enum), so a value
   of any other type fails startup from this step. For example, `MCP_PORT` takes a port number, `0` for
   an ephemeral port, or, under Bun, any non-numeric string, which also binds an ephemeral port.
   Registered as an integer, the non-numeric form fails startup. The release notes name each exception.
   Ships alone.
2. **The new names.** The derived variables and flags become the names, today's become aliases, and
   each alias in use logs one deprecation warning at startup and counts in
   `publisher_deprecated_usage_total{feature="config:<old name>"}`. Precedence becomes flag, variable,
   file for every setting. An old and new name given different values fails startup.
3. **Strict.** A release later: the warnings from step 1 become refusals, so a boolean value the two
   readers disagree on is refused before it can change meaning. The aliases are removed in the release
   after that, announced in the changelog with the mapping table above.

An orchestrator moves to the new names when it adopts step 2's release. Nothing it sets today stops
working before step 3, which refuses only the values step 1 warned about; the old names keep working
until step 3's second release.

## Decisions

- **The file and the environment are equals, not layers with different jobs.** The alternative, a
  written rule ("the file says what the server serves, variables say how this process runs"), keeps
  two places to look and still leaves the borderline cases (MCP's exposure, retrieval) to argue about.
  With every setting available everywhere, the only rule is precedence.
- **The file's top level is the server's own section.** It keeps `port`, `host` and `frozenConfig`
  where operators know them, and keeps their variables unchanged.
- **One precedence, flag over variable over file.** It reverses one case: the `retrieval.embedding`
  keys win over the `EMBEDDING_*` variables today. During step 2 a deployment that sets both to
  different values fails startup rather than silently changing which one is used.
- **Positive booleans.** `--no-mcp-config` and `DISABLE_RESPONSE_LOGGING` become `Enabled` settings that
  default to on; a bare `--no-x` still turns any boolean off.
- **Secrets never take a flag**, and are never logged, reported, or echoed in an error.

## Open questions

1. Should `GET /status` report the effective non-secret settings and their sources? It would make a
   misconfigured deployment visible from outside, but the main API is unauthenticated and it would
   publish ports, hosts and limits. The startup log line covers the operator's need without it.
2. Should the file accept JSON with comments? A commented config is easier to keep, and every reader
   of the file is ours.
3. Should the registry also emit a JSON Schema for `publisher.config.json`, for editor completion and
   CI validation of a deployment's file?

## Not in scope

- What an environment, package or connection looks like in `environments`; that is the config's
  content, not a setting.
- Package-level settings in `publisher.json`, which are per package and have their own reference.
- The connection credentials a config names through `${VAR}`; they keep working as they do.
