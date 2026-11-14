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
carries `Deprecation` and `Link` headers, and from release R a `Sunset` header with the date it is removed in Publisher
1.0.0, 90 days after release R; the release notes announce that date once every first-party client has moved. Its spec is served at `/api-doc.v0.yaml`. `GET /status` answers on both base paths: in the v1 shape here, and in v0's
shape, with additive fields only, on v0.

## The model

```
Environment                     a namespace for packages and connections; the unit of tenancy
├── Connection                  a warehouse handle the models read; its settings in a block named by its `type`
│   ├── schemas / tables        what the connection can read
│   └── :run-sql                ad-hoc SQL: run, or describe a statement's columns
├── destinations                where materialization builds write; a field of the environment, not a connection
└── Package                     a record with pointers (source, description, latestVersionId); holds no content
    ├── runs                    attempts to build a slot's persisted sources, each with a manifest
    └── Version                 an immutable snapshot of the package's content; `latest` is a reserved segment
        ├── models              compiled sources, views and givens; :query; :compile
        ├── reports             a `.malloy` file with an `## artifact` tag (grid or linear); the report write
        ├── data-apps           the catalog of HTML data apps; pages are served outside the API (`DataApp.url`)
        ├── data-files          embedded CSV, Parquet, JSON and Excel files
        ├── agents, skills      what the package declares for agents
        ├── entities, classification, index
        ├── build-plan, status  the persist build plan; this server's serving state, with its bindings
        ├── :load, :unload, :reload
        └── connections         the version's own `duckdb` sandbox over its data files
```

Every content read is of exactly one version, named in the path. A live (watched) package has one pseudo-version,
`live`, which names content and has no record. A publish (a re-fetch of the recorded source that finds it changed is
one) and a report write each make a version; nothing edits one in place, and undo is pointing `latestVersionId` back at
the version's `parentVersionId`. A slot is what one run at a time may build: the package under `scope: package`, one
version under `scope: version`.

## The grammar

These are the rules the spec's `info` states, in the same words where they can be.

- **Path names.** Paths are kebab-case; parameters and fields are camelCase. `{xName}` is a named resource,
  `{versionId}` and `{runId}` are ids, and `{path}` is a file's version-relative path (relative to the version's root,
  `/`-separated; every path in a body is version-relative too, but a data app's `path`, relative to `public/`, and a
  skill file's, relative to its skill's directory). `{path}` spans segments: send its `/` literally, never
  as `%2F`, and a `:verb` follows its last segment. A path has no empty, `.` or `..` segment, no leading `/`, and no
  `\`, `:` or control character.
- **Reads change no record.** A `GET` may load the version, or start work a `profile=light` publish deferred, which only
  `VersionStatus` and the deferred part's own `status` show. `POST …/versions/{versionId}:reload` recompiles a version from its files against the
  warehouse's current schemas and makes no version: on a live package it is the edit loop, on any other version a schema
  refresh. Publishing a version, from the recorded source or a new one, is `POST …/versions`; loading and unloading a
  version on this server are `:load` and `:unload`, which change no record. Memory state is `VersionStatus.status`,
  never a field of `Version`.
- **Which reads load.** `/status`, a version's `status`, the versions list, the version record, `:unload` and
  `…/index` never load a version that is not loaded. Every other content read loads it on demand (a hydrate when the
  compiled-model store has it), as each such operation's description says; read `VersionStatus.status` first to know
  what a read will cost. A version that cannot load (its models no longer compile, or its stored content cannot be
  read) answers 424 `VERSION_LOAD_FAILED` with `details.problems` on every operation that loads it, `:load` and a run
  included, and its status reads `failed`; the next such operation tries the load again. `:reload` is the exception: a
  compile that fails is its 400 `COMPILE_ERROR`, and the version keeps serving its last good compile, its status
  `stale` with `error` set. `:load`, `:unload` and `:reload` each answer the version's `VersionStatus`, whose
  `warnings` are the load's non-fatal findings.
- **State changes are fields.** `PATCH …/versions/{versionId}` with `archived`, `pinned` or `manifestLocation`;
  `PATCH …/packages/{packageName}` with `latestVersionId`; `PATCH …/runs/{runId}` with `status: "cancelled"`. What is
  not a field of a record is an action, `POST` with a `:verb` suffix on the resource it acts on (`:load`, `:unload`,
  `:reload`, `:query`, `:compile`, `:run-sql`), so an action can never collide with a child's name. An unsaved
  connection is tested with `?dryRun=true` on create or update.
- **One mutation shape.** `PATCH` is JSON merge-patch: present fields are set, `null` unsets, absent fields are
  untouched, lists replace. A merge-patch body, its nested objects included, is not checked against the schema's
  `required`, and its `null` clears an optional field although no schema spells `null`. An orchestrator pushes an environment's desired state as a `PATCH` whose `connections` list
  replaces the stored one. `PUT` is used only by the report write, which replaces a file. `POST` on a collection answers
  201 with the resource and a `Location`; `DELETE` answers 204. A create or a write takes its own resource's writable
  fields; an action that takes input (`:query`, `:compile`, `:run-sql`) takes an `…Request` body, since it creates no
  resource, and `:reload`, `:load` and `:unload` take none.
- **Concurrency.** Every mutable resource but a report carries a `revision`, returned as `ETag` on reads. Every `PATCH`
  and `DELETE` accepts `If-Match` and answers 412 `REVISION_MISMATCH` (with `details.currentRevision`) when the resource
  has moved on. A report is revisioned by its `contentHash`, which is the report write's precondition in place of
  `If-Match`; a stale one answers 409 `CONTENT_CHANGED` with `details.currentContentHash`.
- **Field behavior.** What only the server sets is `readOnly` (`resource`, `revision`, timestamps, and every run field
  but `versionId`, `trigger`, `callerRunId`, `options`, `buildInstructions` and `status`), and a write that sends it
  back as read ignores it, so a client can `PATCH` a resource it just read. A field set on create is writable on create
  and never changes after (`x-immutable`): a `name`, a package's and a version's `source`, and a run's `versionId`,
  `trigger`, `callerRunId`, `options` and `buildInstructions`. A write that sends an `x-immutable` field with a different
  value answers 400 `INVALID_REQUEST` naming it; the same value is accepted, so a read can be sent back. A field with a
  `default` need not be sent on create. A run's `status` is the one field a client changes later. One field is both set
  by the server and sent back: a report's `contentHash`. An object schema no request can carry is `readOnly` as a whole.
- **One schema per resource.** A write body is the resource itself, one schema for reading and writing it. `required`
  describes a read: what every read of the object carries. In a request, a `readOnly` field is ignored and never
  required (OpenAPI 3.0's rule, adopted here because 3.1 dropped it), a field with a `default` is never `required` and a request may leave it out, though every
  read carries it, and a write-only field is never in `required`: a field a create needs says "Required on create." in its description. A
  `PATCH` body is a JSON merge-patch of the resource: any subset of its writable fields, `null` clearing an optional
  one; no schema spells `null`, since a read never returns it. Generated clients get request and response types from the
  one schema (openapi-typescript's `--read-write-markers --default-non-nullable false`).
- **Idempotent creates.** Every `POST` that creates or starts something (an environment, a connection, a package, a
  version, a run), and the report write, accepts an `Idempotency-Key` header: a retry with the same key and body within
  24 hours answers with the first response and does nothing again, and the same key with a different body answers 409
  `IDEMPOTENCY_KEY_REUSED`.
- **Guarded deletes.** A delete that would delete other resources with it (an environment's packages, connections and
  destinations, a package's versions) requires `?force=true`, and answers 409 `RESOURCE_NOT_EMPTY` without it.
- **Field names.** camelCase. A reference is `xName` or `xPath`; a file's path is `path` and its text `content`, while
  Malloy text that is not a file (a compile check's or a query's) is `source` or `query`. A hash is `…Hash` of what it
  hashes (a file's `contentHash`); a count ends in `Count` and a size in `Bytes`; a boolean has no `is` prefix. One word,
  one meaning: `origin` is provenance, `kind` is what a thing is (a persisted source's `persist` or `preaggregate`),
  `live` is only the pseudo-version (serving from the source warehouse is `warehouse`), a version other than `live` is a
  stored version, a source discovery shows is exposed, and `manifest` is a run's manifest, or an external file of the
  same entries that `manifestLocation` names; a report's structure is its `layout`. A connection's own settings keep
  their warehouse's words.
- **Timestamps.** Every timestamp is an RFC 3339 `date-time` in UTC, read-only, and absent until it has a value, never
  `null`. An event's time ends in `At` (`createdAt`, `startedAt`, `failedAt`); the moment some data reflects ends in
  `AsOf` (`dataAsOf`, `schemaAsOf`). Every stored resource, one with a `revision`, carries `createdAt` and `updatedAt`.
  Durations are integers whose name carries the unit (`elapsedMs`, `windowSeconds`).
- **Resource paths.** Every resource a `GET` reads carries `resource`, its canonical path relative to the API's base: no
  leading slash, and the resolved version id in place of `latest`
  (`environments/acme/packages/sales/versions/1.4.0/models/orders.malloy`). The exceptions are `/status`, a snapshot of
  the server, list envelopes, and an object embedded in another that has no read of its own (an agent's own skill). A
  list item carries it when its item has a `GET` of its own, so a data app, whose page is served outside the API,
  carries none. Relative, so it stays right behind a gateway that serves the API under another prefix; a
  link and a stable key, never parsed.
- **Schema names.** A resource read two ways is `X` from its own `GET` and `XSummary` in its list, and its list is
  `XList` (`Model` and `ModelSummary`, `Report` and `ReportSummary`); nothing else is named `…Summary`.
- **Conditional reads.** Every read but `/status` answers with an `ETag` and takes `If-None-Match`, answering 304 with no
  body when nothing changed. `Cache-Control` is `no-store` on `/status` and, on every other read, `private, no-cache`, because a compiled model under a concrete version can still change with `:reload`, and a
  read through `latest` or `live` can resolve differently next time.
- **Back-pressure.** Every operation but `/status` may answer 429 `RATE_LIMITED`; 429 and 503 carry `Retry-After` in
  seconds. `/status` is never rate-limited and never answers 503, because it is the orchestrator's heartbeat: it reports
  `initializing` and `draining` in its body.
- **Request ids.** The server takes the caller's `X-Request-Id`, else the trace id of a W3C `traceparent`, else makes
  one; every response carries it as `X-Request-Id`, and every error body repeats it as `requestId`.
- **Enum values.** Lowercase snake_case, except a `reason`, which is always a code from a closed list in UPPER_SNAKE (an
  error's, an index's, a refused source's; free text is a `message`), a warning's or problem's `code`, a stable
  kebab-case name of the check that made the finding (`render-tag`, `report-layout-unknown`; the set is open), a value
  that echoes a setting's own value (`versionPromotion: on-publish`), values that name a response field
  (`include: ["malloyResult"]`), and values that echo another vocabulary (a setting's value, Postgres's `sslmode`).
- **Lists.** Every list is `{items, nextCursor}`, paged with `limit` (default 100, at most 1000) and `after`, in a stable
  order its operation states: by name or path ascending, except versions and runs, newest first, and a connection's
  schemas and tables, in the warehouse's order. `/status` is the one unpaged snapshot, narrowable with
  `?environmentName=`.
- **Typed responses.** A query answers `{rows, metadata}`, and `metadata.versionId` names the version that answered.
  Malloy's full result, with column types and render metadata, comes only with `include: ["malloyResult"]`, and the SQL
  that ran only with `include: ["sql"]`. Nothing is a JSON string inside JSON.
- **Errors.** Every error is `{status, reason, message, requestId, details?}`. Branch on `status` and `reason`, never on
  `message`; treat a reason you do not know as the status's generic case. Each operation's error responses list the
  reasons it answers, and `details` has one shape per reason a client acts on (`COMPILE_ERROR`: `problems`;
  `REVISION_MISMATCH`: `currentRevision`; `INVALID_TRANSITION`: `currentStatus`; `TABLE_OWNED_ELSEWHERE`:
  `ownerPackageName` and `ownerVersionId`; `INVALID_REQUEST`: `field`; `RESULT_TOO_LARGE`: `capType`; `CONTENT_CHANGED`: `currentContentHash`; `VERSION_LOAD_FAILED`: `problems`). An error inside a successful
  response is an `EmbeddedError` (`message`, and a `reason` from that use's own list); every diagnostic, a compile
  problem, a load warning or a render log message, is a `Problem`, located by `modelPath` and a 0-based `range`.
- **Dry runs.** `?dryRun=true` runs every check and stores nothing, and answers 200 with no `Location`, ignoring
  `Idempotency-Key`: a connection's answers its test result (`ok: false` on any failure), a publish's the version it
  would make (a `Version` with no `resource`, `revision` or timestamps, although `Version` requires them, and no
  `ETag`).
- **Booleans** are JSON `true`/`false` in bodies and `true`/`false` in query strings; anything else is 400
  `INVALID_REQUEST`.
- **Status codes.** No operation answers 401; a gateway may answer 401 or 403 before a request reaches Publisher, in its
  own body shape. Each operation declares only the codes its shape can produce: 404 when it names a resource; 409 on
  writes and on `:reload`, `:load` and `:unload`; 412 when it takes `If-Match`; 413 when it takes a body
  (`PAYLOAD_TOO_LARGE`), or for a query or SQL result over a cap (`RESULT_TOO_LARGE`); 403 on a resource write under
  `resources.readOnly` (`READ_ONLY`) and on an `#(authorize)` gate (`FORBIDDEN`); 410 `VERSION_ARCHIVED` when it uses
  an archived version's content (a content read, an action on it, the report write, a run, or `latestVersionId`'s
  target), never on that version's record, its status, its `PATCH`, `DELETE` or `:unload`, or the index; and 424, 502 and 504 when it loads a version or reaches a warehouse. 400 is your request or your Malloy; 424
  is something the operation depends on (your connection: `CONNECTION_AUTH_FAILED`, `CONNECTION_NOT_FOUND`; an
  external manifest: `MANIFEST_UNAVAILABLE`; or the version itself, which cannot load: `VERSION_LOAD_FAILED`), and
  retrying fails the same way; 502 is the database (`CONNECTION_FAILED`);
  504 is the database not answering in time (`GATEWAY_TIMEOUT`).

## Key endpoints

Each row names its operations by `operationId`, which the generated clients use.

| Method & path | Does |
| --- | --- |
| `GET /status` | `get-status`: `status`, `serverVersion`, `hostname`, `bootId`, `readOnly`, `versionPromotion`, `loadErrors` (with `stale` for a version serving a model older than its content), and per environment its connections and destinations by `name`, `type`, `fingerprint` and `configStamp` (never their configuration) and every version this server holds with its `VersionStatus`. Loads nothing; `no-store`; never rate-limited. |
| `GET /environments`, `POST /environments` | `list-environments`, `create-environment`. `PATCH /environments/{environmentName}` (`update-environment`) merges: its `connections` and `destinations` lists each replace whole, and an entry with a name and no configuration keeps the stored one, so a push need not resend withheld credentials. `DELETE` removes it and everything in it (`?force=true`). |
| `GET …/connections`, `POST`, `GET …/connections/{connectionName}`, `PATCH`, `DELETE` | `list-connections`, `create-connection`, `get-connection`, `update-connection`, `delete-connection`. A connection's settings are a block named by its `type` (`"type": "postgres", "postgres": {…}`); a block refuses a field it does not define with 400 naming it. Credentials are write-only and listed in `withheldFields`. `?dryRun=true` on create or update tests without storing, answering 200 with a `ConnectionTestResult`. Destinations are the environment's `destinations` list, not connections. |
| `GET …/connections/{connectionName}/schemas`, `…/schemas/{schemaName}/tables`, `…/tables/{tableName}`, `POST …/connections/{connectionName}:run-sql` | `list-schemas`, `list-tables`, `get-table`, `run-sql`. Browse what a connection can read; run or describe SQL. The same routes exist under a version for its `duckdb` sandbox (`…-in-version`), beside `list-connections-in-version` and `get-connection-in-version`, which list and read that sandbox. A missing schema or table is 404 `TABLE_NOT_FOUND`. |
| `GET …/packages`, `POST …/packages` | `list-packages`, `create-package`. List the records; create a record with its `source` (no `latestVersionId`: 400). Every version, the first included, is `POST …/versions`. |
| `GET …/packages/{packageName}`, `PATCH`, `DELETE` | `get-package`, `update-package`, `delete-package`. `PATCH` sets `description` and `latestVersionId` (send `If-Match` to make the move a compare-and-swap; moving it back is undo). `source` moves only with a publish from a new source. Scope and schedule come from a version's `publisher.json` (`materialization`). |
| `GET …/versions`, `GET …/versions/{versionId}`, `PATCH`, `DELETE` | `list-versions`, `get-version`, `update-version`, `delete-version`. The package's versions, newest first (`?archived=` narrows); one version (`live` has no record: 404); its lifecycle fields (`archived`, `pinned`, `manifestLocation`; 424 `MANIFEST_UNAVAILABLE` when the manifest cannot be bound); delete for good. |
| `POST …/versions` | `publish-version`: fetch a source and publish the version its `publisher.json` declares, or `<parent>+republish.N` (`origin: republish`) when that version exists with other content. From a new `source`, which becomes the package's recorded one, or with no body from the recorded one. 201 with the version it made; 200 with the current version when nothing changed. `?dryRun=true` always answers 200, with the `Version` it would make (no `resource`, `revision` or timestamps) and no `ETag`; a `publisher.json` with no `version` is 400 `PACKAGE_VERSION_MISSING`; `?profile=light` defers what a draft does not need (the build plan, report lint, the discovery index, data-file row counts) to its first read. |
| `GET …/versions/{versionId}/status` | `get-version-status`: the `VersionStatus` of one version, with the fields `/status` leaves out (`loadId`, `contentHash`, `bindings`, `warnings`). Loads nothing. |
| `POST …/versions/{versionId}:load`, `POST …/versions/{versionId}:unload` | `load-version`, `unload-version`: load the version on this server and wait, or drain and unload it (409 `VERSION_BUILDING` while a run builds it); each answers the `VersionStatus`. Changes no record. A version that cannot load answers 424 `VERSION_LOAD_FAILED`. |
| `POST …/versions/{versionId}:reload` | `reload-version`: recompile the version from its files against current warehouse schemas, and wait: a live package's edit loop, or a stored version's schema refresh. Makes no version and changes no record. Answers the `VersionStatus` (on `live`, the receipt that the edit is served: `contentHash`, `loadId`, `warnings`), with no `ETag`; a failed compile is 400 `COMPILE_ERROR`. |
| `GET …/versions/{versionId}/models`, `GET …/models/{path}` | `list-models`, `get-model`: model files; one model's sources, views and givens (`?include=content,modelInfo,sourceInfos` for the larger parts). |
| `POST …/versions/{versionId}/models/{path}:query` | `query-model`: run a query. See [request shapes](#query-request-shapes). |
| `POST …/versions/{versionId}/models/{path}:compile` | `compile-model`: compile-check Malloy text at `append`, `file` or `package` scope. Answers `problems` and the `versionId` compiled against; the compile failed exactly when a problem has `severity` `error`. |
| `GET …/versions/{versionId}/reports`, `…/reports/{path}` | `list-reports`, `get-report`: reports by `path` and `layout` (`?layout=` narrows); one report, with its tiles, givens and layout (`?include=content` for its text). Run a `query` tile's `query` through `…/models/{path}:query`, prefixed with `run:`; a `text` tile runs nothing. |
| `PUT …/versions/{versionId}/reports/{path}` | `write-report`, the builder's save. The body is a `Report`: `content`, and `contentHash` as read; the rest is read-only and ignored. Without `contentHash` it creates the file (201; 409 `FILE_EXISTS`); with it, it replaces the file (200; 409 `CONTENT_CHANGED` when the file no longer hashes to it). On a stored version it makes version `<parent>+edit.N` and moves `latestVersionId` unless `versionPromotion` is `explicit`; on a live package it writes through to the author's directory. Identical `content` answers 200 and makes no version, so a retry is safe; takes `Idempotency-Key`. Only the latest version can be written (409 `VERSION_NOT_LATEST`); `index.malloy`, `public/`, a non-report and a `readOnly` report answer 409 `FILE_NOT_WRITABLE`. Answers the `Report`. |
| `GET …/versions/{versionId}/data-apps` | `list-data-apps`: the catalog of HTML data apps, each with the `url` its page is served at. Pages and the files they load are served outside the API, at `/environments/{environmentName}/packages/{packageName}/versions/{versionId}/public/{path}` (immutable under a stored version id; `private, no-cache` through `latest` or `live`, whose files change), with v0's `/environments/{environmentName}/packages/{packageName}/{path}` as the latest version's short form. |
| `GET …/versions/{versionId}/data-files` | `list-data-files`: embedded data files and their schemas (`rowCountStatus` is `deferred` after a `profile=light` publish, until the count this read starts finishes). |
| `GET …/versions/{versionId}/agents`, `…/agents/{agentName}`, `…/skills`, `…/skills/{skillName}` | `list-agents`, `get-agent`, `list-skills`, `get-skill`: what the version declares for agents, and the skills in force for it (`origin`: `bundled`, `package` or `agent`); a skill's `SKILL.md` text comes on both reads, its files' content with `?include=files`. Adopt an agent only when the user names it. |
| `GET …/versions/{versionId}/entities`, `…/classification`, `…/index` | `list-entities`, `get-classification`, `get-discovery-index`: what each model declares for an indexer; the classification summary; the discovery index's `status` (`lexical`, `deferred`, `indexing`, `ready`, `failed`) and the version it is for. |
| `GET …/versions/{versionId}/build-plan` | `get-build-plan`: the persist build plan (`status`: `ready`, `empty`, `deferred`, `unavailable`). What the version serves per source is `bindings` on its `…/status`. |
| `GET …/packages/{packageName}/runs`, `POST`, `GET …/runs/{runId}`, `PATCH`, `DELETE` | `list-runs`, `create-run`, `get-run`, `update-run`, `delete-run`: materialization runs (`status`, `trigger`, `mode`, `scope`; `?status=` narrows); start one for a `versionId` (auto, or orchestrated with `buildInstructions`; `trigger` and `callerRunId` set on create); cancel with `{"status": "cancelled"}`; delete with `?dropTables=true` to drop the tables only it references. |
| `GET …/runs/{runId}/manifest` | `get-run-manifest`: what a run produced: one entry per `sourceEntityId`, each naming its table (`tableId`, and `tableName` in the warehouse, with its `connectionName` or `destinationName`), and the sources that failed. |

Example — run a query against the bundled `storefront` package's latest version:

```bash
curl -s -X POST \
  http://localhost:4000/api/v1/environments/examples/packages/storefront/versions/latest/models/storefront.malloy:query \
  -H 'content-type: application/json' \
  -d '{"query":"run: order_items -> by_category"}' | jq .rows
```

### Query request shapes

The body, a `QueryRequest`, takes one of two shapes: `query` alone (ad-hoc Malloy, compiled in the model's context), or
`queryName` without `query` (a view when `sourceName` is set, a model-level named query when it is not). Names are
names, not Malloy code: send one name per field, bare, exactly as the model's `sources` listing returns it. Anything
richer goes in `query`. `givens` ride on either shape to supply [runtime parameters](givens.md); and `rowLimit` caps the
rows.

The response is a `QueryResult`, `{rows, metadata}`: plain row objects, and `metadata` with `versionId` (the version
that answered, `latest` resolved), `rowLimit`, `rowLimitSource`, `rowLimitHit`, `elapsedMs`, `path`,
`scannedBytes`, `queryCorrelationId` and `renderLogs`. Add `"include": ["malloyResult"]` for the full Malloy result,
with column types, that the renderer consumes, or `["sql"]` for the SQL that ran.

`metadata.path` is the path report: what answered the query and why. `servedBy` names the table that answered and
its `dataAsOf`, or `kind: live`. `considered` lists every materialized table the router tried, in order, each `used`
or `skipped` with a `reason` from a closed list (`NOT_BUILT`, `WARNED`, `MISSING_FIELD`, `CONTRACT`,
`GRAIN_TOO_COARSE`, `TIME_ZONE`, `CROSS_CONNECTION`, `COUNT_SEMANTICS`, `STALE`, `RUN_FAILED`). `notConsidered` names nearby tables
the router does not try for this query (a sibling's, or one on a source extending it), nearest first. `joins` says
how each join resolved (`inline`, `table` or `live`), and `warnings` carries the publish warnings on the
annotations the query touched. `query` is the query's shape as routing reads it. It replaces v0's `servedFrom`,
which said where an answer came from but never why.

A query that fails says whose problem it is. Branch on `reason`:

- **400** `COMPILE_ERROR`: your Malloy; `details.problems` carries the compiler's diagnostics. Fix what they name.
- **400** `QUERY_REJECTED`: the database refused the statement. Fix what the message names.
- **404** `TABLE_NOT_FOUND`: the database lacks a table the model reads.
- **413** `RESULT_TOO_LARGE`: the result passed a cap (`details.capType`: `rows`, `bytes` or `unserializable`).
  Narrow the query or lower `rowLimit`; resending it fails the same way.
- **403** `FORBIDDEN`: a gate refused this caller; supply the givens it reads (`Source.gates`).
- **410** `VERSION_ARCHIVED`: the version is archived; query another, or restore it with `PATCH` `{"archived": false}`.
- **424** `CONNECTION_AUTH_FAILED` or `CONNECTION_NOT_FOUND`: the connection is misconfigured; the query never ran. Do
  not rewrite it or retry until the connection is fixed.
- **424** `VERSION_LOAD_FAILED`: the version itself cannot load (`details.problems` says why); the query never ran.
  Query another version, or fix the cause and `:reload` or publish.
- **502** `CONNECTION_FAILED`: the database could not be reached; the query never ran. Retry once, then report it.
- **504** `GATEWAY_TIMEOUT`: the database did not answer in time.

## Vocabulary

The spec's nouns are the vocabulary, and the Console and the docs follow them:

| Noun             | Means                                                                                                   |
| ---------------- | ------------------------------------------------------------------------------------------------------- |
| environment      | a namespace for packages and connections                                                                |
| connection       | a warehouse handle the models read                                                                      |
| destination      | where `#@ persist storage=` builds write; set on the environment, never named by a model                 |
| package          | a record with pointers; its content is a version                                                        |
| version          | an immutable snapshot of a package's content; `latest` is the package's pointer to one of them          |
| stored version   | any version but `live`, the pseudo-version a live (watched) package serves                              |
| model            | a `.malloy` file; compiled, it yields sources, views, named queries and givens                          |
| report           | a `.malloy` file with an `## artifact` tag, `layout` grid or linear                                     |
| data app         | an HTML page under `public/`                                                                            |
| data file        | a CSV, Parquet, JSON or Excel file embedded in a version                                                |
| agent, skill     | what a package declares for agents; the guides in force for it                                          |
| run              | one attempt to build a slot's persisted sources; "materialization" is the feature's name                |
| slot             | what one run at a time may build: the package under `scope: package`, one version under `scope: version` |
| manifest entry   | what a run produced for one source; `tableId` names the physical table                                  |
| binding          | what a version serves for one source right now                                                          |
| table            | a warehouse table, browsed under a connection; a built one is identified by its manifest entry's `tableId` and named by its `tableName` |
| warehouse        | the source's own database; a query whose `path.servedBy.kind` is `live` read no materialized table      |
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

Without a running server, the v1 spec is at
`https://raw.githubusercontent.com/malloydata/publisher/main/api-doc.v1.yaml`. The v0 spec is `api-doc.yaml` in the
repository until v1 ships and it is renamed `api-doc.v0.yaml`; the server serves it as `/api-doc.v0.yaml` either way.

## Generated clients

The v1 spec drives the TypeScript client the SDK and CLI use and the generated Python client
([`packages/python-client`](../packages/python-client)). Regenerate them from `api-doc.v1.yaml` after any spec change —
see [CONTRIBUTING.md](../CONTRIBUTING.md).
