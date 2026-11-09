<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# API overview

> What this is: the shape of Publisher's programmatic surfaces — the resource model, the REST API at `/api/v1`, the MCP
> tools, and where to find the live API explorer. For connecting an AI agent, see [ai-agents.md](ai-agents.md); for the
> Console, see [console.md](console.md). The design decisions behind v1 are recorded in
> [issue #1562](https://github.com/malloydata/publisher/issues/1562).
> The plan that builds this API, moves every client to it and retires v0 in Publisher 1.0.0 is [publisher-v1.md](publisher-v1.md).

## Two surfaces

| Surface      | Port                         | For                                                                                                                                                                       |
| ------------ | ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **REST API** | `4000` (base path `/api/v1`) | Applications, reports, scripts, and unattended agents: list content, compile models, run queries. See the [REST loop](ai-agents.md#unattended-and-one-shot-agents-the-rest-loop). |
| **MCP API**  | `4040` (`/mcp`)              | AI agents in interactive sessions: discovery, query, and authoring over the [Model Context Protocol](https://modelcontextprotocol.io). See [ai-agents.md](ai-agents.md).   |

Both read and write the same resources, in the same vocabulary. Neither surface authenticates callers — put the server
behind your own gateway before exposing it beyond localhost ([security-posture.md](security-posture.md)).

**The previous API, `/api/v0`, is deprecated as a whole.** It keeps working, takes bug fixes only, and every response
carries a `Deprecation` header. Its retirement is announced in the release notes once every first-party client has
moved. Its spec is served at `/api-doc.v0.yaml`. `GET /status` answers on both base paths: in the v1 shape here, and in v0's
shape, with additive fields only, on v0.

## The model

```
Environment                     a namespace for packages and connections; the unit of tenancy
├── Connection                  a warehouse handle the models read
│   ├── schemas / tables        what the connection can read
│   └── :run-sql                ad-hoc SQL: run, or describe a statement's columns
└── Package                     a record with pointers (source, description, latestVersionId); holds no content
    ├── status                  how this server holds the package's versions
    ├── runs                    attempts to build persisted sources, each with a manifest
    └── Version                 an immutable snapshot of the package's content; `latest` is a reserved segment
        ├── models              compiled sources, views and givens; query; compile
        ├── reports             a `.malloy` file with an `## artifact` tag (grid or linear); the report write
        ├── data-apps, public/  HTML data apps and the static files they load
        ├── data-files          embedded CSV, Parquet, JSON and Excel files
        ├── agents, skills      what the package declares for agents
        ├── entities, classification, index
        ├── build-plan, status; :reload
        └── connections         the version's own `duckdb` sandbox over its data files
```

Every content read is of exactly one version, named in the path. A publish, a report write and a re-fetch of the
source that finds it changed each make a version; nothing edits one in place, and undo is pointing `latestVersionId` back at the
parent.

## The grammar

- **Names.** Paths are kebab-case; parameters and fields are camelCase. `{path}` is a file inside a version; `{xName}` a
  named resource. `versionId` is the one spelling of a version, everywhere.
- **Reads never change state.** `POST …/versions/{v}:reload` recompiles a version from its files against current
  warehouse schemas and waits: a live package's edit loop, or a published version's schema refresh. Publishing a
  version is `POST …/versions`; loading one is `PATCH …/versions/{v}` `{"loaded": true}`.
- **Which reads load.** `/status`, a version's `status`, the versions list, the version record and `…/index` never load a
  version that is not in memory. Every other content read loads it on demand (a hydrate when the compiled-model store has
  it); read `VersionStatus.loaded` first to know what a read will cost.
- **State changes are fields.** `PATCH …/versions/{versionId}` with `archived`, `loaded`, `pinned` or
  `manifestLocation`; `PATCH …/packages/{packageName}` with `latestVersionId`; `PATCH …/runs/{runId}` with
  `status: "cancelled"`. Actions are `POST` with a `:verb` suffix on the resource they act on (`:reload`, `:query`, `:compile`, `:run-sql`), so an action can never collide with a child's name; an unsaved connection is tested with `?dryRun=true` on create or update.
- **One mutation shape.** `PATCH` is JSON merge-patch: present fields are set, `null` unsets, absent fields are untouched,
  lists replace, so an orchestrator pushes an environment's desired state as a `PATCH` of its `connections`. `PUT` is
  used only by the report write, which replaces a file.
  `POST` on a collection answers 201 with the resource and a `Location`; `DELETE` answers 204.
  A create or a write takes its own resource, with only its writable fields set; an action (`:query`, `:compile`, `:run-sql`, `:reload`) takes an `…Request` body, since it creates no resource.
- **Field behavior.** What only the server sets is `readOnly` (`resource`, `revision`, timestamps, a run's every field but `status`), and a write that sends it back as read ignores it, so a client can `PATCH` a resource it just read. A `name` is set on create and never changes; a write that changes one answers 400.
- **Idempotent creates.** Every `POST` that creates or starts something (an environment, a connection, a package, a version, a run) accepts an `Idempotency-Key` header: a retry with the same key and body within 24 hours answers with the first response and does nothing again, and the same key with a different body answers 409 `IDEMPOTENCY_KEY_REUSED`.
- **Guarded deletes.** A delete that would delete other resources with it (an environment's packages and connections, a package's versions) requires `?force=true`, and answers 409 `RESOURCE_NOT_EMPTY` without it.
- **Timestamps.** Every timestamp is an RFC 3339 `date-time` in UTC, read-only, and absent until it has a value, never `null`. An event's time ends in `At` (`createdAt`, `startedAt`, `failedAt`); the moment some data reflects ends in `AsOf` (`dataAsOf`, `schemaAsOf`). Every stored resource, one with a `revision`, carries `createdAt` and `updatedAt`. Durations are integers whose name carries the unit (`durationMs`, `windowSeconds`).
- **Resource paths.** Every object a `GET` can read carries `resource`, its canonical path relative to the API's base: no leading slash, and the resolved version id in place of `latest` (`environments/acme/packages/sales/versions/1.4.0/models/orders.malloy`). A list item carries it when its item has a `GET` of its own. Relative, so it stays right behind a gateway that serves the API under another prefix; a link and a stable key, never parsed.
- **Names.** A resource read two ways is `X` from its own `GET` and `XSummary` in its list, and its list is `XList` (`Model` and `ModelSummary`, `Report` and `ReportSummary`, `Table` and `TableSummary`).
- **Field names.** camelCase; a reference is `xName` or `xPath`; a file's path is `path` and its text `content`; a hash is `…Hash`; a duration's unit ends its name; no `is` prefix on a boolean. A connection's own settings keep their warehouse's words. Every embedded error is an object with a `message`; only a writable field may be `null`.
- **Conditional reads.** Every read but `/status` answers with an `ETag` and takes `If-None-Match`, answering 304 with no body when nothing changed. `Cache-Control` is `private, max-age=31536000, immutable` only on raw file reads under a concrete version id (`…/public/{path}`), whose bytes never change; every other read is `private, no-cache`, because a compiled model under a concrete version can still change with `:reload`, and a read through `latest` or `live` can resolve differently next time.
- **Back-pressure.** Every operation but `/status` may answer 429 `RATE_LIMITED`; 429 and 503 carry `Retry-After` in seconds. `/status` is never rate-limited, because it is the orchestrator's heartbeat.
- **Request ids.** The server takes the caller's `X-Request-Id`, else the trace id of a W3C `traceparent`, else makes one; every response carries it as `X-Request-Id`, and every error body repeats it as `requestId`.
- **Enum values.** Lowercase snake_case, except a `reason`, which is always a code from a closed list in UPPER_SNAKE (an error's, an index's, a refused source's; free text is a `message`), values that name a response field (`include: ["malloyResult"]`), and values that echo another vocabulary (a setting's value, Postgres's `sslmode`).
- **Concurrency.** Every mutable resource carries a `revision`, returned as `ETag` on reads. Send it back as `If-Match` on
  a write; 412 `REVISION_MISMATCH` when the resource has moved on. The report write's precondition is the report's `sourceHash`, sent back as read.
- **Lists.** Every list is `{items, nextCursor}`, paged with `limit` (default 100, at most 1000) and `after`. `/status` is
  the one unpaged snapshot, narrowable with `?environmentName=`.
- **Typed responses.** A query answers `{rows, schema, metadata}`, plus `malloyResult` when asked for with
  `include: ["malloyResult"]`. Nothing is a JSON string inside JSON.
- **Errors.** Every error is `{status, reason, message, details?}`, `reason` from one closed enum. Branch on `status` and
  `reason`, never on `message`. 400 is your request or your Malloy (`COMPILE_ERROR` carries `details.problems`); 424 is
  your connection (`CONNECTION_AUTH_FAILED`, `CONNECTION_NOT_FOUND`); 502 is the database (`CONNECTION_FAILED`).
- **Booleans** are JSON `true`/`false` in bodies and `true`/`false` in query strings; anything else is 400.

## Key endpoints

| Method & path                                                              | Does                                                                                                                                                                                      |
| -------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `GET /status`                                                              | `operationalState`, `version`, `hostname`, `loadErrors` (with `stale` for a version serving a model older than its content), and every version this server holds with its `VersionStatus`. |
| `GET /environments`, `POST /environments`                                  | List and create. `PATCH /environments/{env}` merges (its `connections` list replaces whole), `DELETE` removes it and everything in it.                                                                 |
| `GET …/connections`                                                        | Connections. `POST` creates (`?dryRun=true` tests without storing); `PATCH` merges. Destinations, where builds write, are the environment's `destinations` list, not connections. |
| `GET …/connections/{connectionName}/schemas`, `…/schemas/{s}/tables`, `POST …:run-sql`| Browse what a connection can read; run or describe SQL. The same routes exist under a version for its `duckdb` sandbox.                                                                     |
| `GET …/packages`, `POST …/packages`                                        | List records; create a package from `source.location` and publish its first version (`?dryRun=true` checks without storing). Later versions are `POST …/versions`. |
| `PATCH …/packages/{pkg}`                                                   | `description`, `latestVersionId` (a compare-and-swap under `If-Match`; moving it back is undo), `source`. Scope and schedule come from a version's `publisher.json`.                     |
| `GET …/versions/{v}/status`                                                | The `VersionStatus` of one version, with the fields `/status` leaves out (`servedRevision`, `contentHash`, `bindings`). Loads nothing. |
| `GET …/versions`, `GET …/versions/{v}`, `PATCH`, `DELETE`                  | The package's versions, newest first; one version; its lifecycle fields (`loaded: true` loads it); delete for good.                                                                                           |
| `POST …/versions`                                                          | Publish a new version: from a new `source`, or with no body from the recorded one. 201 with the version it made; 200 with the current version when nothing changed. |
| `POST …/versions/{v}:reload`                                               | Recompile the version from its files against current warehouse schemas, and wait: a live package's edit loop, or a published version's schema refresh. Makes no version. |
| `GET …/versions/{v}/models`, `GET …/models/{path}`                         | Model files; one model's sources, views and givens (`?include=content,modelInfo,sourceInfos` for the larger parts).                                                                   |
| `POST …/versions/{v}/models/{path}:query`                                  | Run a query. See [request shapes](#query-request-shapes).                                                                                                                                 |
| `POST …/versions/{v}/models/{path}:compile`                                | Compile-check Malloy text at `append`, `file` or `package` scope.                                                                                                                         |
| `PUT …/versions/{v}/reports/{path}`                                        | The builder's save: write a report's text, which makes version `<parent>+edit.N` and moves `latestVersionId`. Create (201) without `sourceHash`, replace (200) with the `sourceHash` you read, sending a `Report` with its `content`. Answers the `Report`. |
| `GET …/versions/{v}/reports`, `…/reports/{path}`                           | Reports by `path` and `layout`; one report's manifest (its tiles and controls). Run a tile through `query`.                                                                          |
| `GET …/versions/{v}/data-apps`, `…/public/{path}`                          | HTML data apps and the static files they load, served from the version they were published with.                                                                                        |
| `GET …/versions/{v}/data-files`                                            | Embedded data files and their schemas.                                                                                                                                                    |
| `GET …/versions/{v}/agents`, `…/agents/{agentName}`, `…/skills`, `…/skills/{skillName}` | What the version declares for agents, and the skills in force for it. Adopt an agent only when the user names it.                                                                        |
| `GET …/versions/{v}/entities`, `…/classification`, `…/index`               | What each model declares for an indexer; the classification summary; the discovery index's state.                                                                                        |
| `GET …/versions/{v}/build-plan`                              | The persist build plan (`state`: `ready`, `empty`, `unavailable`). What the version serves per source is `bindings` on the package's `status`.                                                                   |
| `GET …/packages/{pkg}/runs`, `POST`, `GET …/runs/{id}`, `PATCH`, `DELETE`  | Materialization runs (`status`, `trigger`, `mode`, `scope`); start one (auto, or orchestrated with `buildInstructions`); cancel with `{"status": "cancelled"}`.                            |
| `GET …/runs/{id}/manifest`                                                 | What a run produced: one entry per `sourceEntityId`, each naming its table (`tableId`, and `tableName` in the warehouse).                                                                                                           |

Example — run a query against the bundled `storefront` package's latest version:

```bash
curl -s -X POST \
  http://localhost:4000/api/v1/environments/examples/packages/storefront/versions/latest/models/storefront.malloy/query \
  -H 'content-type: application/json' \
  -d '{"query":"run: order_items -> by_category"}' | jq .rows
```

### Query request shapes

The body takes one of two shapes: `query` alone (ad-hoc Malloy, compiled in the model's context), or `queryName`
without `query` (a view when `sourceName` is set, a model-level named query when it is not). Names are names, not
Malloy code: send one name per field, bare, exactly as the model's `sources` listing returns it. Anything richer goes in
`query`. `givens` ride on either shape to supply [runtime parameters](givens.md); and `rowLimit` caps the rows.

The response is `{rows, schema, metadata}`: plain row objects, the column schema, and `metadata` with `rowLimit`,
`rowLimitSource`, `rowLimitHit`, `elapsedMs`, `servedFrom`, `costBytes`, `queryCorrelationId` and `renderLogs`. Add
`"include": ["malloyResult"]` for the full Malloy result the renderer consumes, or `["sql"]` for the SQL that ran.

A query that fails says whose problem it is. Branch on `reason`:

- **400** `COMPILE_ERROR` or `QUERY_REJECTED`: your Malloy, or the database rejected the statement; `details.problems`
  carries the compiler's diagnostics. Fix what the message names.
- **424** `CONNECTION_AUTH_FAILED` or `CONNECTION_NOT_FOUND`: the connection is misconfigured; the query never ran. Do
  not rewrite it or retry until the connection is fixed.
- **502** `CONNECTION_FAILED`: the database could not be reached; the query never ran. Retry once, then report it.

## Vocabulary

The spec's nouns are the vocabulary, and the Console and the docs follow them:

| Noun             | Means                                                                                                   |
| ---------------- | ------------------------------------------------------------------------------------------------------- |
| environment      | a namespace for packages and connections                                                                |
| connection       | a warehouse handle the models read                                                                      |
| destination      | where `#@ persist storage=` builds write; set on the environment, never named by a model                 |
| package          | a record with pointers; its content is a version                                                        |
| version          | an immutable snapshot of a package's content; `latest` is the package's pointer to one of them          |
| model            | a `.malloy` file; compiled, it yields sources, views, named queries and givens                          |
| report           | a `.malloy` file with an `## artifact` tag, `layout` grid or linear                                     |
| data app         | an HTML page under `public/`                                                                            |
| data file        | a CSV, Parquet, JSON or Excel file embedded in a version                                                |
| agent, skill     | what a package declares for agents; the guides in force for it                                          |
| run              | one attempt to build a slot's persisted sources; "materialization" is the feature's name                |
| manifest entry   | what a run produced for one source; `tableId` names the physical table                                  |
| binding          | what a version serves for one source right now                                                          |
| table            | a physical table the server created or serves, with `state` and `generation`                            |
| given            | a runtime parameter a model declares; reports render the ones their tiles read as filter controls       |
| index            | the discovery index `get_context` ranks against                                                         |
| stale            | a version serving a model older than its content because the newer content failed to compile           |

## Live API explorer

The running server hosts the interactive **Swagger UI** and the OpenAPI 3.1 specs:

| URL                                       | What                                                      |
| ----------------------------------------- | --------------------------------------------------------- |
| **http://localhost:4000/api-doc.html**    | Interactive explorer over the v1 spec.                    |
| **http://localhost:4000/api-doc.yaml**    | The v1 OpenAPI spec (feed it to codegen or Postman).      |
| **http://localhost:4000/api-doc.v0.yaml** | The deprecated v0 spec, for clients that have not moved.  |

Without a running server, the same specs are at
`https://raw.githubusercontent.com/malloydata/publisher/main/api-doc.v1.yaml` and `…/api-doc.yaml` (v0).

## Generated clients

The v1 spec drives the TypeScript client the SDK and CLI use and the generated Python client
([`packages/python-client`](../packages/python-client)). Regenerate them from `api-doc.v1.yaml` after any spec change —
see [CONTRIBUTING.md](../CONTRIBUTING.md).
