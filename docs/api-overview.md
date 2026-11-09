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
├── Connection                  a warehouse handle with a role: source, destination, or both
│   ├── schemas / tables        what the connection can read
│   └── :run-sql                ad-hoc SQL: run, or describe a statement's columns
└── Package                     a record with pointers (source, description, latestVersionId); holds no content
    ├── status                  how this server holds the package's versions
    ├── runs                    attempts to build persisted sources, each with a manifest
    └── Version                 an immutable snapshot of the package's content; `latest` is a reserved segment
        ├── models              compiled sources, views and givens; query; compile; the report write
        ├── reports             a `.malloy` file with an `## artifact` tag (grid or linear)
        ├── data-apps, public/  HTML data apps and the static files they load
        ├── data-files          embedded CSV, Parquet, JSON and Excel files
        ├── agents, skills      what the package declares for agents
        ├── entities, classification, index
        ├── build-plan; :refresh-schema
        └── connections         the version's own `duckdb` sandbox over its data files
```

Every content read is of exactly one version, named in the path. A publish, a report write and a re-fetch of the
source that finds it changed each make a version; nothing edits one in place, and undo is pointing `latestVersionId` back at the
parent.

## The grammar

- **Names.** Paths are kebab-case; parameters and fields are camelCase. `{path}` is a file inside a version; `{name}` a
  named resource. `versionId` is the one spelling of a version, everywhere.
- **Reads never change state, and an immutable version has nothing to reload.** `POST …/packages/{pkg}:reload`
  recompiles a live package and waits for the result. Re-fetching the source is `POST …/packages/{pkg}/versions`;
  loading a version is `PATCH …/versions/{v}` `{"loaded": true}`; recompiling against current warehouse schemas is
  `POST …/versions/{v}:refresh-schema`.
- **Which reads load.** `/status`, a package's `status`, the versions list, the version record and `…/index` never load a
  version that is not in memory. Every other content read loads it on demand (a hydrate when the compiled-model store has
  it); read `ServingState.loaded` first to know what a read will cost.
- **State changes are fields.** `PATCH …/versions/{versionId}` with `archived`, `loaded`, `pinned` or
  `manifestLocation`; `PATCH …/packages/{packageName}` with `latestVersionId`; `PATCH …/runs/{runId}` with
  `status: "cancelled"`. Actions are `POST` with a `:verb` suffix on the resource they act on (`:reload`, `:refresh-schema`, `:query`, `:compile`, `:run-sql`), so an action can never collide with a child's name; an unsaved connection is tested with `?dryRun=true` on create or update.
- **One mutation shape.** `PATCH` is JSON merge-patch: present fields are set, `null` unsets, absent fields are untouched,
  lists replace, so an orchestrator pushes an environment's desired state as a `PATCH` of its `connections`. `PUT` is
  used only by the report write, which replaces a file.
  `POST` on a collection answers 201 with the resource and a `Location`; `DELETE` answers 204.
- **Field behavior.** What only the server sets is `readOnly` (`resource`, `revision`, timestamps, a run's every field but `status`), and a write that sends it back as read ignores it, so a client can `PATCH` a resource it just read. A `name` is set on create and never changes; a write that changes one answers 400.
- **Idempotent creates.** Every `POST` that creates or starts something (an environment, a connection, a publish, a re-fetch, a run) accepts an `Idempotency-Key` header: a retry with the same key and body within 24 hours answers with the first response and does nothing again, and the same key with a different body answers 409 `IDEMPOTENCY_KEY_REUSED`.
- **Guarded deletes.** A delete that would delete other resources with it (an environment's packages and connections, a package's versions) requires `?force=true`, and answers 409 `RESOURCE_NOT_EMPTY` without it.
- **Timestamps.** Every timestamp is an RFC 3339 `date-time` in UTC, read-only, and absent until it has a value, never `null`. An event's time ends in `At` (`createdAt`, `startedAt`, `failedAt`); the moment some data reflects ends in `AsOf` (`dataAsOf`, `schemaAsOf`). Every stored resource, one with a `revision`, carries `createdAt` and `updatedAt`. Durations are integers whose name carries the unit (`durationMs`, `freshnessWindowSeconds`).
- **Resource paths.** Every object a `GET` can read carries `resource`, its canonical path relative to the API's base: no leading slash, and the resolved version id in place of `latest` (`environments/acme/packages/sales/versions/1.4.0/models/orders.malloy`). A list item carries it when its item has a `GET` of its own. Relative, so it stays right behind a gateway that serves the API under another prefix; a link and a stable key, never parsed.
- **Concurrency.** Every mutable resource carries a `revision`, returned as `ETag` on reads. Send it back as `If-Match` on
  a write; 412 `REVISION_MISMATCH` when the resource has moved on. The report write also takes `expectedHash`, the
  SHA-256 of the text you read.
- **Lists.** Every list is `{items, nextCursor}`, paged with `limit` (default 100, at most 1000) and `after`. `/status` is
  the one unpaged snapshot, narrowable with `?environment=`.
- **Typed responses.** A query answers `{rows, schema, metadata}`, plus `malloyResult` when asked for with
  `include: ["malloyResult"]`. Nothing is a JSON string inside JSON.
- **Errors.** Every error is `{status, reason, message, details?}`, `reason` from one closed enum. Branch on `status` and
  `reason`, never on `message`. 400 is your request or your Malloy (`COMPILE_ERROR` carries `details.problems`); 424 is
  your connection (`CONNECTION_AUTH_FAILED`, `CONNECTION_NOT_FOUND`); 502 is the database (`CONNECTION_FAILED`).
- **Booleans** are JSON `true`/`false` in bodies and `true`/`false` in query strings; anything else is 400.

## Key endpoints

| Method & path                                                              | Does                                                                                                                                                                                      |
| -------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `GET /status`                                                              | `operationalState`, `version`, `hostname`, `loadErrors` (with `stale` for a version serving a model older than its content), and every version this server holds with its `ServingState`. |
| `GET /environments`, `POST /environments`                                  | List and create. `PATCH /environments/{env}` merges (its `connections` list replaces whole), `DELETE` removes it and everything in it.                                                                 |
| `GET …/connections?role=destination`                                       | Connections by role. `POST` creates (`?dryRun=true` tests without storing); `PATCH` merges (`?dryRun=true` tests the change).                                                      |
| `GET …/connections/{name}/schemas`, `…/schemas/{s}/tables`, `POST …:run-sql`| Browse what a connection can read; run or describe SQL. The same routes exist under a version for its `duckdb` sandbox.                                                                     |
| `GET …/packages`, `POST …/packages`                                        | List records; publish a version from `source.location` (`?dryRun=true` checks without storing). Publish answers the `Version`.                                                             |
| `PATCH …/packages/{pkg}`                                                   | `description`, `latestVersionId` (a compare-and-swap under `If-Match`; moving it back is undo), `source`. Scope and schedule come from a version's `publisher.json`.                     |
| `POST …/packages/{pkg}:reload`                                             | A live package only: recompile the author's files and wait; 409 `PACKAGE_NOT_LIVE` otherwise. |
| `GET …/packages/{pkg}/status`                                              | The `ServingState` of one version (`?versionId=`, `latest` by default), with the per-package fields (`servedRevision`, `sourceContentSha`, `bindings`). Loads nothing.                   |
| `GET …/versions`, `GET …/versions/{v}`, `PATCH`, `DELETE`                  | The package's versions, newest first; one version; its lifecycle fields (`loaded: true` loads it); delete for good.                                                                                           |
| `POST …/versions`                                                          | Re-fetch the recorded source; publish `<parent>+reload.N` if it changed (201), else `unchanged` (200). |
| `POST …/versions/{v}:refresh-schema`                                       | Recompile the version against the warehouse's current schemas; makes no version. |
| `GET …/versions/{v}/models`, `GET …/models/{path}`                         | Model files; one model's sources, views and givens (`?include=sourceText,modelInfo,sourceInfos` for the larger parts).                                                                   |
| `POST …/versions/{v}/models/{path}:query`                                  | Run a query. See [request shapes](#query-request-shapes).                                                                                                                                 |
| `POST …/versions/{v}/models/{path}:compile`                                | Compile-check Malloy text at `append`, `file` or `package` scope.                                                                                                                         |
| `PUT …/versions/{v}/models/{path}`                                         | The builder's save: write a report file, which makes version `<parent>+edit.N` and moves `latestVersionId`. Create (201) without `expectedHash`, replace (200) with it.                  |
| `GET …/versions/{v}/reports`, `…/reports/{path}`                           | Reports by `path` and `layout`; one report's manifest (its tiles and controls). Run a tile through `query`.                                                                          |
| `GET …/versions/{v}/data-apps`, `…/public/{path}`                          | HTML data apps and the static files they load, served from the version they were published with.                                                                                        |
| `GET …/versions/{v}/data-files`                                            | Embedded data files and their schemas.                                                                                                                                                    |
| `GET …/versions/{v}/agents`, `…/agents/{name}`, `…/skills`, `…/skills/{name}` | What the version declares for agents, and the skills in force for it. Adopt an agent only when the user names it.                                                                        |
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
| connection       | a warehouse handle; its `role` says whether models read it, builds write to it, or both                 |
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
