# Release Notes

Curated release notes for `@malloy-publisher/sdk`, `@malloy-publisher/app`, and `@malloy-publisher/server` (versioned in lockstep).

## How this file is used

The `Release (NPM + Docker)` workflow (`.github/workflows/release.yml`) creates GitHub releases automatically with a standard header (NPM/Docker links) plus an auto-generated "What's Changed" PR list via `gh release create --generate-notes`. That auto list is sufficient for routine patch releases.

For releases that warrant narrative — redesigns, breaking changes, migration steps — copy every `## [Unreleased]` section below into the GitHub release page after CI publishes it, and stamp each one with the version that shipped it. There is regularly more than one, because unrelated narratives accumulate between releases: they are separate entries in the same release rather than alternatives, so reading "the relevant section" as singular ships one and silently drops the others. The future workflow change to read this file directly is documented in #2 of the May 2026 review.

## Packages that version on their own line

`@malloy-publisher/skills` and `@malloy-publisher/create-malloy-package` are not part of the lockstep version above, and their notes do not belong in this file. The release workflow still publishes them: for each one it reads the version from `main` and, when that version is not yet on npm, dispatches that package's own publish workflow (`skills-npm.yml`, `create-malloy-package-npm.yml`). A package whose version is unchanged is skipped, so a release that touched neither is unaffected.

To ship one of them, bump its `package.json` on `main` and run an ordinary release. The bump is what triggers the publish and nothing else in CI requires it, so a change that lands without one is skipped and the release stays green. A prerelease skips both packages outright, since their own versions carry no hyphen and would take over the `latest` tag. Either way the release's job summary says what it skipped and why. Each package can also still be published on its own by dispatching its workflow directly, which is the point of keeping them separate: a skill edit should not need a server release.

One behaviour change to know about: `skills-npm.yml` now publishes only from `main`, matching the guard `create-malloy-package-npm.yml` already had. Dispatching it from a branch still runs `check_pack`, but the publish job is skipped, and a skipped job reports success, so check the job list rather than the run's green tick if you expected a publish. See [.github/workflows/CONTEXT.md](.github/workflows/CONTEXT.md) for the publishing rules that are easy to get wrong.

---

## [Unreleased] — queries report how they were served, and what they cost

The server measured several things and then discarded them, and the query
histogram carried two labels that grew without bound. Both are addressed.

### What changed

- **`malloy_model_query_duration` no longer labels by query text or row count.** Both are unbounded — ad-hoc text yields a new series per distinct query, a row count one per distinct result size — and a histogram label multiplies by the bucket count, so the metric grew for as long as a process served traffic. On one deployment serving real traffic the query-text label alone carried ~637 distinct values across ~14.9k series for this histogram. They remain on the request log. `environment` and `package` take their place: the only identity on the metric was previously a bare model path, which is not unique across packages.
- **`malloy_model_query_duration` now spans execution, not just preparation.** The timer stopped before the warehouse round trip, so the histogram excluded the one part its own description ("how long it takes to execute a Malloy model query") named. It now covers compile, authorize, routing, prepare and execution. **Expect every existing p95 panel to step up at deploy** — that is the metric starting to measure what it always claimed, not a regression.
- **`QueryResult` gains `servedFrom`, `executionTimeMs` and `queryCostBytes`.** A storage-served answer is byte-identical to a live one, so `servedFrom` is the only way a caller can tell a materialized source did anything; `live_fallback` is reported separately from `storage` because it is a success answered by the live warehouse, and counting it as a hit would report a healthy hit rate for a broken store. `queryCostBytes` comes from `runStats`, which the BigQuery connector already populated and nothing read.
- **`ManifestEntry` gains `buildDurationMs` and `queryCostBytes`.** The duration was already measured for the build histogram and sent upward as null.
- **`malloy_model_query_scanned_bytes`**, a counter of bytes scanned by served queries, where the backend reports them — BigQuery today.

### Reading the cost numbers

**Bytes scanned is not bytes billed.** BigQuery rounds up to a 10MB minimum per query, so a small read bills an order of magnitude above what it scanned — and materialization refreshes are mostly small reads. Every figure reported here is SCANNED. Use it to compare queries against each other; it is not a spend number.

**Null is not zero, and on the build side it is null more often than not.** `ManifestEntry.queryCostBytes` is populated only for a COLOCATED build, from the Malloy connection's own statistics. An incremental delta reports null because its statements do not run through a single call whose result reaches the manifest, and a chained `storage=` build reports null because it read its parent's already-materialized table and touched no warehouse at all.

A plain `storage=` build also reports null, and that one is a gap rather than a property: its read goes through DuckDB's native query-passthrough, where no Malloy connector is in the call path to report statistics. Recovering the figure means reading it back from the warehouse's own accounting, which needs an identifier for the job — and the passthrough does not return one for a rows-returning call. Obtaining that identifier restructures how the build issues its read, so it is deliberately left to the change that does so rather than approximated here.

On the serve side, check `servedFrom` before reading a null as "free": a `storage`-served query touched no warehouse, while a Snowflake or Postgres query touched one and simply reported nothing.

### For consumers generating clients from this spec

`QueryResult` and `ManifestEntry` both gain fields. Strict generated clients reject unknown properties — openapi-generator's Java/Gson `validateJsonElement` throws on any field absent from the client's `openapiFields` — so a consumer running this server against a client generated from an older spec fails at deserialize on every affected response. **Regenerate clients in the same change as the version bump**, not after it.

---

## [Unreleased] — `storage=` builds from a Snowflake source now work (Docker image)

The 0.0.236 notes below list `snowflake_query` among the native query-passthroughs a `storage=` source is materialized through. That was true of the code and never true of the published image: **materializing a Snowflake source into a storage destination has not worked at all.** Two independent faults, both fixed.

### What changed

- **The image now carries the ADBC Snowflake driver.** The Snowflake extension is a wrapper over it, and `INSTALL snowflake FROM community` does not bring it — so every `snowflake_query()` failed at run time with `ADBC Snowflake driver (libadbc_driver_snowflake.so) not found`. It is now fetched at a pinned version, for the image's architecture, into the extension directory the runtime reads.
- **A key-pair Snowflake connection can be federated.** The passthrough required a password and emitted `PASSWORD`, so a connection authenticating by key pair — which queries fine on the live path — could not be built from at all. Key pair or password is now accepted, with a private-key passphrase when supplied.
- **`ROLE` and `SCHEMA` travel with the federated connection.** Both are part of what identifies a Malloy connection; without them a build ran under the user's _default_ role while live queries on the same connection used the configured one.

### Why it went unnoticed

Both guards were blind for the same reason. The image build verified Snowflake with `SELECT snowflake_version()` — a scalar that never touches the driver and passes without it — and the offline extension smoke test asserted only that extensions `LOAD`. The driver is a query-time dependency, so nothing that ran at build time could see it missing.

### Scope, and what is still missing

The driver is installed by the **Docker image**. A local clone or `npx @malloy-publisher/server` still has no driver, so `storage=` builds from Snowflake continue to fail there with the same error — installing it is a manual step (`dbc install snowflake`, or the extension's installer script). Closing that properly means the bake step owning the driver alongside the extensions, which is worth doing and is not this change.

### Operational notes

A failed driver fetch now **fails the image build** rather than warning and continuing. An image without the driver cannot answer a Snowflake query, so it should not leave the builder reporting success — which is how this shipped. A release built from an unchecked ref is covered by the same assertion, since it lives in the Dockerfile rather than only in CI.

---

## [0.0.236] — DuckDB/DuckLake materialization tier (`storage=`)

This section describes the tier as it stands at 0.0.236. It first shipped in 0.0.232; the disjoint-set semantics between `storageDestinations` and `connections` landed in 0.0.236.

A `#@ persist` source can now be materialized into a **storage destination** — a DuckLake declared in the environment's `storageDestinations`, a disjoint set from `connections` — instead of its own warehouse, and served back from that materialized table cross-dialect, with no model change. Off by default; see [docs/persist-storage-tutorial.md](docs/persist-storage-tutorial.md).

### What changed

- **`storageDestinations`**, a per-environment list declared alongside `connections`, holds the warehouses a `storage=` source is materialized into and served from. It is a disjoint set: a destination is not resolvable by name from a model, a notebook cell, or query text, is absent from the connection endpoints (404), and its name is independent of the connection namespace — so the same name may appear in both lists and mean two different warehouses. Only the build and materialized-serve paths resolve one. Writing the list is all-or-nothing: a create or update carrying a value that is not a list of usable destinations is refused with 400 naming every defect and applies none of it, so a destination the server could not read is never silently left unregistered; an unusable entry in the config file, or in a row restored at boot, is instead dropped with a warning so one bad entry cannot take an environment offline. See [docs/connections.md](docs/connections.md#storage-destinations).
- **`#@ persist storage=<destination>`** materializes a source into that storage destination via native per-engine query-passthrough (`postgres_query`/`bigquery_query`/`snowflake_query`); absent or `storage=source` is the unchanged in-warehouse path. The reserved connection name `source` is rejected at registration.
- **`PERSIST_STORAGE_MODE`** deployment switch (`off` default | `write-only` | `on`): a kill switch that ships dark — `off` is a no-op, and moving it down never fails a loaded package (a `storage=` source reverts to serving live and surfaces a package warning). See [docs/configuration.md](docs/configuration.md).
- **Serve from storage:** when `on`, a query against a materialized source is served from the stored table via a virtual-source transform (its dimensions, measures, materialized-target joins, and views re-declared over the stored columns); anything not reproducible falls back to serving live, so turning it on can never make a query wrong.
- **Physical tables named by `name=` verbatim.** The auto-run server names a `storage=` table by its `#@ persist name=` value (or the source name) verbatim — exactly as the in-warehouse path does — and a rebuild atomically replaces it in place (DuckLake's catalog swap is transactional). No hashed suffix, no coexisting generations, and no operator convenience view. Assigning distinct physical names per generation (for immutable generations, safe schema evolution, or rollback) is the caller's responsibility on the orchestrated build path, where the caller supplies `physicalTableName` and distributes serve bindings via `manifestLocation`. `DELETE …/materializations/{id}?dropTables=true` reclaims a storage table (destination-aware drop).
- **Chained sources reuse the parent.** A `storage=` source that reads another `storage=` source in the same destination is built by **reading the parent's materialized table** (rolled up in DuckDB), so it reuses the parent's work and is consistent-by-construction. If it can't (a parent field that isn't a stored column, a live join, or a cross-destination parent) it falls back to recomputing the upstream from raw — refused instead under `strictUpstreams`. Reported by `publisher_storage_chained_build_total{outcome}`.
- **Eligibility gate (HTTP 422 / failed build):** a `storage=` source with an unbound free parameter, a given reference (a security refusal — a frozen given-filtered table would leak rows across tenants), or a non-DuckDB-portable served shape is refused. A source protected by `#(authorize)` should also not be materialized (the served shape carries no gate); that refusal lands alongside the upstream transitive-`#(authorize)` enforcement it reuses — until then, serve authorize-gated sources live.
- **Connection type `ducklake`** (catalog + `bucketUrl` storage) — see [docs/connections.md](docs/connections.md).
- **Observability:** `storageServeBindings` on package status; `publisher_storage_serve_routing_total{outcome=storage|live_fallback|runtime_live_fallback}`, `publisher_storage_chained_build_total{outcome=parent_reuse|inline_fallback|strict_refused|infra_failure}`, and a `served_from=storage|live_fallback` attribute on `malloy_model_query_duration`, plus build/GC/eligibility counters under the `publisher` meter. `runtime_live_fallback` is the signal that the tier is broken while queries still succeed — the hit rate alone won't show it.
- **A run-time store failure honours `freshnessFallback=live`.** If a routed query fails against the stored table (a reclaimed generation a binding hasn't caught up with), a source whose binding declares `live` is recomputed live rather than erroring — the same answer the compile-time fallback ladder already gives. `fail` and the `stale_ok` default keep surfacing the error, and the decision is read from the bindings actually serving the query, so a stale sibling can't veto it.

### Operational notes

- **Multi-replica serving via the manifest.** A `storage=` source can be served across a fleet by carrying its serve binding in the same manifest the publisher already fetches from a package's `manifestLocation`: a manifest entry that names a `storageDestinationName` (with the captured `schema` and `sourceName`) binds as a cross-connection serve binding applied to the already-compiled models (no recompile); entries without it remain same-connection `tableName` substitutions (which do recompile). A refresh is the usual manifest-rebind — rewrite the manifest and re-`PATCH` `manifestLocation` — and a storage-only refresh costs no recompile. Entries are keyed by the build's content `sourceEntityId` (= the serve handle), so a freshness refresh keeps the handle and only swaps the table path, while a schema-changing generation gets a new handle. Standalone (no `manifestLocation`), serve bindings are still re-derived per-replica from the local materialization store on package load; run that single-replica. When a `manifestLocation` is set the host is authoritative and the local-store rebind is skipped, so the two binding sources never fight.
- **Roll back cleanly.** Deleting a package's materializations before rolling back to a publisher version without this tier avoids a wedge: an older build reuses/binds a persisted `storage=` manifest entry as a same-connection table it can't resolve. Building with `storage=` only ever affects deployments that turned the mode on.

## [Unreleased] — A package's `index.malloy` is its published surface

A package whose root holds an `index.malloy` now takes its discovery surface from that file, with no `explores` key in `publisher.json`. Listings return that file and whatever it exports; the other models become building blocks. This matches Malloyyo, whose project entry point is the same fixed filename, so the two products now agree on what a package's front door looks like. `@malloy-publisher/create-malloy-package` scaffolds one, so a new package is curated from its first boot.

**This changes what an existing package lists**, so read the migration note below if any of your packages already has a root `index.malloy`.

### What changed

- **`index.malloy` defaults the discovery surface.** With no `explores` key and an `index.malloy` at the package root, `listModels()` returns that file alone and `export { … }` curation applies inside it. An explicit `explores` always wins. A package with both, disagreeing, keeps the explicit key and carries a warning rather than the server guessing. `"explores": []` is a third, explicit state meaning "do not curate", and it suppresses the convention.
- **The convention curates discovery only. It never enables the query boundary.** `queryableSources` still decides whether the surface also gates queries, and it still takes effect only alongside an `explores` the author declared. Every source in a convention-curated package stays queryable by name. This is deliberate: the boundary denies with a `404` indistinguishable from "does not exist", so enforcing it because a file with a particular name appeared would revoke query access on a deployed package with no config edit and no actionable error. Adding an `index.malloy` can hide a source from listings; it can never make one unreachable.
- **`explores` may now be a value the server derived** rather than one the author wrote, and the API response does not distinguish the two. Two consequences for a client. A response carrying `explores` alongside `queryableSources: "declared"` does not by itself mean unlisted sources are refused. And on a `PATCH`, an `explores` identical to the derived value is read as a client echoing back what it read, so it neither becomes a declaration nor is written to the manifest; the package reports a warning saying so. Send a different surface to declare one, or edit `publisher.json`.
- **Neither `explores` nor `queryableSources` is removed or deprecated in the breaking sense.** Both keep working exactly as before and remain the way to curate a surface the convention cannot express, such as one spanning several files. A package that has an `index.malloy` and declares them anyway gets a warning saying which parts are now redundant, and what deleting them would cost.

### Migration

- **A package with a root `index.malloy` and no `explores` will list fewer models than before.** Its surface becomes that one file, and `export { … }` curation starts applying inside it. Nothing becomes unreachable, because the convention does not gate queries, so anything you did not mean to hide is still queryable by name while you sort it out.
- **That reassurance is about query access, not about listings.** Anything downstream that reads the listing rather than querying by name does narrow with it: a catalog, a chat surface, MCP `malloy_getContext`, or a search indexer will see only the surface at its next pass, and a source outside it drops out even if it carries an `#(index)` tag. The `x-publisher-bypass-authorize` header does not help here, because that lifts an identity gate and this is the discovery axis. If you run an indexer, re-check its coverage after upgrading.
- **To keep exactly the old behavior, add `"explores": []`.** One key, no rename, no boundary. Renaming the file also restores the listings but changes the model's identity, so `…/models/index.malloy` starts returning 404 and any sibling that `import`s `"index.malloy"` fails to compile, which takes the whole package out of service. Declaring `explores` with your old file list does not restore the old behavior either: it turns on the query boundary.
- **Watch for the aggregator shape.** An `index.malloy` that is all `import`s and no `export { … }` exports nothing, so the package serves one model with no sources and looks empty. Add `export { … }` naming what you want published, or take the `"explores": []` route.

## [0.0.242]: one meaning for `givens` across the API

`givens` had come to mean four different things: declarations, typed values, string-encoded values, and a bare list of names. It now always means a collection of `Given` declarations, and the other three have names of their own. Renames and spec corrections only; no endpoint changes what it does.

### What changed

- **`Givens` is renamed `GivenValues`,** and the string-encoded form that survives a URL is a new named `EncodedGivenValues`. `Givens` read as the plural of `Given` and was not: `Given` describes what a model _accepts_ and is always carried in a plain array, while these two are the values a caller _sends_, decoded and string-encoded respectively. No field, shape, or wire format changed on any endpoint, but **the symbol rename is breaking for a generated client that names it**, so regenerate before upgrading. Which clients those are depends on the generator, and the two we run disagree: `openapi-typescript` (our server types) emits a named `Givens` and now emits `GivenValues` and `EncodedGivenValues` instead, and the Python generator likewise emits `given_values.py` and `encoded_given_values.py` in place of `givens.py`. The axios generator behind `@malloy-publisher/sdk` inlines the map and names nothing, so an SDK consumer is unaffected.
- **`Given` now carries the control contract** wherever it is returned: `label`, `control`, `rangeMin`, `rangeMax`, and `suggest` (a new `GivenSuggest`), all optional. How a given should be presented belongs to the given rather than to any one surface, which is what lets two surfaces render the same control without restating it. The server does not populate them yet; this release lands the contract so the readers that follow have one place to write to.
- **Package warnings name their subject `subject` rather than `target`.** `target` meant the opposite of a `# drill` target: it named where a finding sits, not where anything points. Every producer feeding `Package.warnings` now uses the new key, including the materialization-config findings, whose own `MaterializationConfigWarning` type carried the old one.
- **`RawNotebook` declares what the notebook endpoint actually returns.** `type`, `modelPath`, `modelInfo`, and `queries` are on every response and were undeclared, which forced a blanket cast in the server that would have accepted a stale field name after a rename. `resource` and `path` were declared and have never been sent. It also gains `startingGivens` (`EncodedGivenValues`), the name for a document's declared starting values; nothing emits it yet.
- **A `versionId` request answers 501, not 500.** Every route that declares the parameter has documented `501 Not Implemented` all along, but `NotImplementedError` had no mapping and fell through to the 500 default.

### Migration

- Regenerate clients against `api-doc.yaml`. If your generator names `Givens`, that symbol becomes `GivenValues`, and the string-encoded form becomes `EncodedGivenValues`.
- `warnings[].target` becomes `warnings[].subject`.
- `RawNotebook.path` becomes `RawNotebook.modelPath`. `path` was declared but never populated, so anything reading it was already getting `undefined`; `modelPath` is the value it wanted.
- A caller that treated a `versionId` request's 500 as a server fault should expect 501.

## [0.0.242] — `PageViewer` is now `DataAppViewer`

The SDK component that embeds an in-package HTML data app is renamed, along with the docs page for the built-in web UI. No behavior changes.

### What changed

- **`PageViewer` → `DataAppViewer`**, exported from `components/DataAppViewer`. Props are unchanged (`resourceUri`). There is no alias, so an external consumer importing `PageViewer` will fail to build.
- **`utils/pageEmbed` → `utils/dataAppEmbed`**, same contents (`PUBLISHER_RESIZE_MESSAGE_TYPE`, `PublisherResizeMessage`, `isPublisherResizeMessage`, `serverBaseUrl`, `packageFileUrl`). The move itself is invisible to consumers: the module has no `./utils/*` subpath in `exports`, so it can only be reached through the package root, and the one symbol the root re-exports, `packageFileUrl`, keeps its name and its root export. Only the path behind it changed.
- **The package view's "Governed Reports" section is now labelled "Notebooks."** Label only; the same `.malloynb` files are listed, and no prop or route changed.
- **`docs/publisher-app.md` is now [docs/console.md](docs/console.md)**, and the built-in web UI is called the **Publisher Console** throughout the docs. The `packages/app` package name is unchanged.

### Migration

- Rename the import: `import { DataAppViewer } from "@malloy-publisher/sdk"`. That is the only change an embedder needs. `packageFileUrl` is the other symbol in this area reachable from the package root, and it is untouched.

The REST `/pages` endpoint is untouched by this change and still answers at its existing path. Renaming it to `/data-apps` is a separate, breaking change with its own release note.

## [0.0.242] — Breaking: `/pages` is now `/data-apps`

The endpoint that lists a package's in-package HTML data apps is renamed, along with its schema and the SPA route that opens one. **There is no alias and no deprecation period: a caller still requesting `/pages` stops getting the listing.**

A production deployment answers it **404 as JSON**, so a client sees a clean error rather than a surprise. That is worth stating because it was not true until recently: an unmatched path under `/api/v0/` used to fall through to the SPA's catch-all and answer 200 with `index.html` on any deployment serving the bundled web UI, which would have handed a migrating client an HTML body instead of an error. #962 fixed that catch-all. (Under `NODE_ENV=development` the JSON fallback is not mounted, so the same request gets an HTML 404 from Express instead.)

### What changed

- **`GET …/packages/{pkg}/pages` → `GET …/packages/{pkg}/data-apps`.** Same response shape, same query parameters, same status codes.
- **Schema `Page` → `DataApp`**, and the OpenAPI `operationId` `list-pages` → `list-data-apps` under a `data-apps` tag. Anything generated from `api-doc.yaml` changes accordingly: in the TypeScript client, `PagesApi.listPages` becomes `DataAppsApi.listDataApps`, and `apiClients.pages` on `<ServerProvider>`'s context becomes `apiClients.dataApps`.
- **The SPA route `/{env}/{pkg}/pages/{file}` → `/{env}/{pkg}/data-apps/{file}`, and the old form still works for one release.** A bookmark or shared link using `pages/` opens the data app as before and rewrites itself to the new URL in the address bar, carrying any query string or fragment with it so the rewrite never silently drops state a caller supplied. **This alias is deprecated and comes out one release after this one.** Update stored links now rather than relying on it. The standalone URL (`/environments/{env}/packages/{pkg}/{file}`) never changed. One thing the alias does take away: a model or notebook is excluded from the rewrite, so a `.malloy` or `.malloynb` living in a package's `pages/` directory still opens in the model viewer, but a non-model file under `public/pages/` is no longer reachable at `/{env}/{pkg}/pages/<file>` and is addressed as `/{env}/{pkg}/data-apps/pages/<file>` instead. That is the same collision described below for `public/data-apps/`, and nothing in this repo ships either directory.
- **The package view's "Pages" section is now labelled "Data Apps."**

### Migration

- Change the request path to `/data-apps`. If you generate a client from the spec, regenerate it.
- If you use the SDK's API clients directly, `apiClients.pages.listPages(env, pkg)` becomes `apiClients.dataApps.listDataApps(env, pkg)`.
- Update any stored link of the form `/{env}/{pkg}/pages/{file}`. It still works in this release and stops working in the next one.

Why the REST path breaks cleanly while the browser URL gets a grace period: the two have different costs and different owners. Carrying both spellings in the spec would mean two paths, two operationIds and two generated client methods for one listing, with every future change to it made twice, and the one known consumer of the endpoint reviewed this change and chose the clean break, having already accepted the short window during a rollout where some of its machines answer 404. A bookmark has no owner to consult, and the person who saved it is not reading these notes, so that surface redirects for one release rather than failing. The endpoint is documented (in [docs/html-data-apps.md](docs/html-data-apps.md) and [docs/api-overview.md](docs/api-overview.md), both updated here), so the REST break is a real one for anyone who took it up rather than a quiet one. If that trade is wrong for your deployment, say so on the PR.

One more consequence of the SPA route move, easy to miss: the app now claims the `data-apps` segment, so `/{env}/{pkg}/data-apps/<file>` is no longer redirected to the static route. Clicking a data app in the Console is unaffected, because the listing already includes the file's path relative to `public/`. What changes is a hand-written URL of that shape: it opens the embedded viewer one segment down, on `public/<file>`, rather than redirecting. A package that itself ships a `public/data-apps/` directory is the case to know about, since its files are addressed as `/{env}/{pkg}/data-apps/data-apps/<file>`; the standalone URL `/environments/{env}/packages/{pkg}/data-apps/<file>` serves them unchanged either way. This mirrors what `public/pages/` had before, so it is not a new class of collision, but `data-apps` is a likelier directory name than `pages` was.

## [0.0.208] — Single-call materialization (plan-as-artifact)

**Breaking change to the materialization API.** Materialization moves from the two-round (compile-then-build) protocol to a single call. The build plan is now a compile-time property of the package, and a build is requested in one request.

### What changed

- **New `Package.buildPlan`.** `GET …/packages/{name}` (and every endpoint/MCP resource that returns package metadata) now includes a `buildPlan` describing the package's persist sources and their dependencies. It is `null` when the package has no persist sources. This is the artifact callers read to assemble build instructions.
- **Single-call builds via `buildInstructions`.** `POST …/materializations` accepts an optional `buildInstructions` body. With no instructions the publisher self-assigns names and runs the full build, auto-loading the resulting manifest (auto-run). With `buildInstructions` (validated against the live `Package.buildPlan` at create time) it builds directly into the caller-assigned names and does **not** auto-load — the caller distributes via `manifestLocation` (orchestrated).
- **Streamlined state machine.** `PENDING → MANIFEST_ROWS_READY → MANIFEST_FILE_READY` (terminal), or `FAILED` / `CANCELLED`. The transient `BUILD_PLAN_READY` status is removed.

### Removed (breaking)

- `pauseBetweenPhases` on `CreateMaterializationRequest`.
- The `BUILD_PLAN_READY` value from `MaterializationStatus`.
- `POST …/materializations/{id}?action=build` — `stop` is now the only supported action.
- `Materialization.buildPlan` — read the plan from `Package.buildPlan` instead.

### Client / UI impact

- **CLI:** the `--pause-between-phases` flag is gone; `malloy-pub materialize --wait` settles on `MANIFEST_FILE_READY` / `FAILED` / `CANCELLED`.
- **SDK UI:** the materialization detail dialog drops the "Mode" field and now renders its build-plan view from `Package.buildPlan`.
- Regenerate any SDK/Python/k6 clients against the updated `api-doc.yaml`.

## [0.0.229] — Package locations: `~/` expands, and relative paths anchor at the config

**A relative package `location` now resolves against the directory holding the config it appears in, not the server root.** Those are the same directory whenever the config is found at `<SERVER_ROOT>/publisher.config.json`, which covers the bundled samples, every Docker recipe in [docs/deployment.md](docs/deployment.md), and any setup that `cd`s to the config before starting. Nothing changes for them. Two cases keep the server root as the anchor: the config bundled inside the published package (a zero-arg `npx @malloy-publisher/server`), and a `--config` naming a directory rather than a file.

**Who is affected:** anyone whose `--config <path>` names a file in a directory other than the server root, including a subdirectory of it, and whose packages use a relative `location`. Those packages previously resolved against the server root (the working directory, unless `--server_root` was also passed) and now resolve next to the config. Fix either way: make the `location` absolute, or move the config next to the packages it points at, which is the arrangement this change exists to support.

**The symptom is quiet.** A location that cannot be mounted is not fatal to the process: the server still reports `serving`. It does fail the whole environment the location belongs to, so that environment is skipped and none of its packages load, including the ones that resolved fine. The reason is in the log: `Error initializing environment "<name>"; skipping environment`.

**`~/` in a `location` now works.** It was accepted and then never expanded, so it resolved to a literal `~` directory under the server root and failed to mount. Expansion is unconditional and happens before any anchor applies.

See [docs/configuration.md](docs/configuration.md) for the rule and the recommended layout.

## [0.0.205] — Source access gates (`#(authorize)`)

**Sources can now gate query access on givens.** A `#(authorize) "<bool expr>"` annotation (source-level) or `##(authorize)` (file-level) is evaluated against the request's [givens](docs/givens.md) before any query that reads the source runs; access is denied with **HTTP 403** unless at least one in-scope expression is `true` (OR semantics). Enforced on `POST /…/query`, the notebook-cell `GET`, `POST /…/compile`, and the MCP `malloy_executeQuery` tool. Malformed or invalid annotations fail model load with **424**.

**Important — this is a trusted-tier boundary, not end-user authn.** Givens are caller-asserted, so `#(authorize)` enforces policy only when Publisher sits behind a trusted tier that sets givens from verified context and the query API is network-isolated from untrusted callers. See [docs/authorize.md](docs/authorize.md) (Security model) for the deployment contract, the locked-base + curated-extension pattern, and known limitations.

## [0.0.201] — Givens

**Givens are now the recommended way to supply runtime parameters.** Models declare `given:` blocks (per [Malloy's experimental givens feature](https://docs.malloydata.dev/documentation/experiments/givens)); callers send values via the new `givens` body field on `POST /…/query` and `POST /…/compile`, the `givens` query parameter on the notebook-cell GET, or the `givens` argument on the MCP `malloy_executeQuery` tool. The notebook UI automatically renders a Parameters panel for any model that declares givens.

`filterParams`, `bypassFilters`, the matching `filter_params` / `bypass_filters` query parameters, and `#(filter)` annotations are **deprecated** and will be removed in a future release after a coordinated migration with current users. Models that use `#(filter)` will continue to work unchanged during the deprecation window; affected responses now carry a `Deprecation: true` header (per RFC 8594) pointing at `docs/givens.md`, and the server logs a one-time migration notice when such a model is loaded. See [docs/givens.md](docs/givens.md) for the migration recipe.

## [0.0.197] — SDK and app UI redesign

UI redesign of the SDK's pages and shell. Type-level public APIs are unchanged; rendered DOM, CSS, and visual treatment have changed across `Home`, `Project`, `Package`, `AddPackageDialog`, and the per-cell wrappers used by `Notebook` and `Model`. External embedders should review side-by-side before upgrading.

### Component visual changes

- **`<Home />`** — left-aligned hero, three feature columns (no icons, no chips), Credible-style project list. Same `onClickProject` prop.
- **`<Project />`** — h4 page title + "Packages" section heading, compact icon-tile cards (no underline, weight 600). Same `onSelectPackage`, `resourceUri` props.
- **`<Package />`** — replaces the 3-column grid (Config / Notebooks / Models / Databases / Connections) with a sectioned list (Governed Reports / Semantic Models / Package Data) plus a back link, h4 title, and inline README. Same `onClickPackageFile`, `resourceUri`, `retrievalFn` props. Subcomponents `Config`, `Connections`, `Databases`, `Models`, `Notebooks` under `components/Package/` are no longer rendered by `<Package>` (still importable; will be removed in a future release).
- **`<AddPackageDialog />`** — outlined text fields, pill buttons, refreshed copy. Same `resourceUri` prop.
- **`CleanMetricCard`** (used to wrap `<NotebookCell>` and `<ModelCell>` query results) — border, shadow, and white background removed; cells now flow without card chrome.
- **`<Notebook />` Filter Panel** — border + shadow removed.

### Theme token cleanup

- Replaced 16 hardcoded `color: "#666666"` instances across `Notebook`, `NotebookCell`, `Model`, `ModelCell`, `ResultsDialog`, and `ModelExplorerDialog` with `color: "text.secondary"`. Icons and section titles now follow the consumer's MUI theme.
- `PackageSectionTitle` (in `styles.ts`) refactored to read `theme.palette.text.secondary` and `theme.palette.divider`. Dropped uppercase + 0.5px letterspacing.

### App shell

- The top-bar `Header` in `packages/app` is replaced with a permanent left sidebar (260/64 collapse) + 56px content header with breadcrumb chips and a `#header-actions-portal` slot. Mobile navigation moves to a drawer.
- Theme: black/off-white palette, Inter + JetBrains Mono fonts (loaded from Google Fonts), pill button shape (20px radius), 4px card radius. ABC Diatype (paid commercial) is not used.
- MUI's click ripple animation is disabled globally via `MuiButtonBase` defaultProps (deliberate, matches the flat button aesthetic). Affects only consumers wrapped by Publisher's exported `theme` (i.e. `<MalloyPublisherApp />` users); embedders rendering individual SDK components inside their own `<ThemeProvider>` keep their own ripple defaults.
- Package-detail icon tiles use Malloy brand colors sampled from `public/logo.svg`: teal `#14b3cb` (reports), orange `#e47404` (models), dark blue `#1474a4` (data).

### New internal surface

- `Package/ContentTypeIcon.tsx` — inline-SVG icon component (`type: "report" | "model" | "data"`) for branded tiles. Not exported from the package root.

### Migration

- If you embed `<Notebook>` or `<Model>` and rely on the bordered card around each result, you'll need to add your own wrapper.
- If you provide a custom MUI theme, verify `palette.text.secondary` is defined — it now drives muted icon and text colors that were previously hardcoded.
- The `MalloyPublisherApp({ headerProps })` API is unchanged at the type level (`logoHeader?: ReactElement`, `endCap?: ReactElement`), but the slots render in different DOM positions with different size constraints than they did in 0.0.x:
  - **`logoHeader`** previously rendered on the left of a horizontal top bar. It now renders in the **sidebar header** (56 px tall, 260 px wide expanded, 64 px wide collapsed). Wide horizontal wordmarks designed for a top bar may crop or disappear in the collapsed sidebar — prefer a compact mark + short label, or an icon that reads alone at 64 px.
  - **`endCap`** previously rendered on the right of the top bar next to the doc links. It now renders into the **content header portal** (right-aligned slot in a 56 px content header above the page content). The portal is global across routes, so it's intended for cross-route primary actions (e.g. a sign-in or settings button), not per-page actions.
- The `app` package now declares `@tanstack/react-query` as a direct dependency. Consumers who rely on hoisting from the SDK's peerDep are unaffected; consumers installing `app` standalone will now resolve the dependency cleanly.
