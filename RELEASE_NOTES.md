# Release Notes

Curated release notes for `@malloy-publisher/sdk`, `@malloy-publisher/app`, and `@malloy-publisher/server` (versioned in lockstep).

## How this file is used

The `Release (NPM + Docker)` workflow (`.github/workflows/release.yml`) creates GitHub releases automatically with a standard header (NPM/Docker links) plus an auto-generated "What's Changed" PR list via `gh release create --generate-notes`. That auto list is sufficient for routine patch releases.

For releases that warrant narrative — redesigns, breaking changes, migration steps — copy the relevant section below into the GitHub release page after CI publishes it. The future workflow change to read this file directly is documented in #2 of the May 2026 review.

---

## [Unreleased] — DuckDB/DuckLake materialization tier (`storage=`)

A `#@ persist` source can now be materialized into a **registered DuckDB or DuckLake connection** instead of its own warehouse, and served back from that materialized table — cross-dialect, with no model change. Off by default; see [docs/persist-storage-tutorial.md](docs/persist-storage-tutorial.md).

### What changed

- **`#@ persist storage=<connection>`** materializes a source into that connection (a DuckDB/DuckLake destination) via native per-engine query-passthrough (`postgres_query`/`bigquery_query`/`snowflake_query`); absent or `storage=source` is the unchanged in-warehouse path. The reserved connection name `source` is rejected at registration.
- **`PERSIST_STORAGE_MODE`** deployment switch (`off` default | `write-only` | `on`): a kill switch that ships dark — `off` is a no-op, and moving it down never fails a loaded package (a `storage=` source reverts to serving live and surfaces a package warning). See [docs/configuration.md](docs/configuration.md).
- **Serve from storage:** when `on`, a query against a materialized source is served from the stored table via a virtual-source transform (its dimensions, measures, materialized-target joins, and views re-declared over the stored columns); anything not reproducible falls back to serving live, so turning it on can never make a query wrong.
- **Physical tables named by `name=` verbatim.** The auto-run server names a `storage=` table by its `#@ persist name=` value (or the source name) verbatim — exactly as the in-warehouse path does — and a rebuild atomically replaces it in place (DuckLake's catalog swap is transactional). No hashed suffix, no coexisting generations, and no operator convenience view. Assigning distinct physical names per generation (for immutable generations, safe schema evolution, or rollback) is the caller's responsibility on the orchestrated build path, where the caller supplies `physicalTableName` and distributes serve bindings via `manifestLocation`. `DELETE …/materializations/{id}?dropTables=true` reclaims a storage table (destination-aware drop).
- **Chained sources reuse the parent.** A `storage=` source that reads another `storage=` source in the same destination is built by **reading the parent's materialized table** (rolled up in DuckDB), so it reuses the parent's work and is consistent-by-construction. If it can't (a parent field that isn't a stored column, a live join, or a cross-destination parent) it falls back to recomputing the upstream from raw — refused instead under `strictUpstreams`. Reported by `publisher_storage_chained_build_total{outcome}`.
- **Eligibility gate (HTTP 422 / failed build):** a `storage=` source with an unbound free parameter, a given reference (a security refusal — a frozen given-filtered table would leak rows across tenants), or a non-DuckDB-portable served shape is refused. A source protected by `#(authorize)` should also not be materialized (the served shape carries no gate); that refusal lands alongside the upstream transitive-`#(authorize)` enforcement it reuses — until then, serve authorize-gated sources live.
- **Connection type `ducklake`** (catalog + `bucketUrl` storage) — see [docs/connections.md](docs/connections.md).
- **Observability:** `storageServeBindings` on package status; `publisher_storage_serve_routing_total`, `publisher_storage_chained_build_total`, and a `served_from=storage` attribute on `malloy_model_query_duration`, plus build/GC/eligibility counters under the `publisher` meter.

### Operational notes

- **Multi-replica serving via the manifest.** A `storage=` source can be served across a fleet by carrying its serve binding in the same manifest the publisher already fetches from a package's `manifestLocation`: a manifest entry that names a `storageConnectionName` (with the captured `schema` and `sourceName`) binds as a cross-connection serve binding applied to the already-compiled models (no recompile); entries without it remain same-connection `tableName` substitutions (which do recompile). A refresh is the usual manifest-rebind — rewrite the manifest and re-`PATCH` `manifestLocation` — and a storage-only refresh costs no recompile. Entries are keyed by the build's content `sourceEntityId` (= the serve handle), so a freshness refresh keeps the handle and only swaps the table path, while a schema-changing generation gets a new handle. Standalone (no `manifestLocation`), serve bindings are still re-derived per-replica from the local materialization store on package load; run that single-replica. When a `manifestLocation` is set the host is authoritative and the local-store rebind is skipped, so the two binding sources never fight.
- **Roll back cleanly.** Deleting a package's materializations before rolling back to a publisher version without this tier avoids a wedge: an older build reuses/binds a persisted `storage=` manifest entry as a same-connection table it can't resolve. Building with `storage=` only ever affects deployments that turned the mode on.

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

## [Unreleased] — Package locations: `~/` expands, and relative paths anchor at the config

**A relative package `location` now resolves against the directory holding the config it appears in, not the server root.** Those are the same directory whenever the config is found at `<SERVER_ROOT>/publisher.config.json`, which covers the bundled samples, every Docker recipe in [docs/deployment.md](docs/deployment.md), and any setup that `cd`s to the config before starting. Nothing changes for them. Two cases keep the server root as the anchor: the config bundled inside the published package (a zero-arg `npx @malloy-publisher/server`), and a `--config` naming a directory rather than a file.

**Who is affected:** anyone whose `--config <path>` names a file in a directory other than the server root, including a subdirectory of it, and whose packages use a relative `location`. Those packages previously resolved against the server root (the working directory, unless `--server_root` was also passed) and now resolve next to the config. Fix either way: make the `location` absolute, or move the config next to the packages it points at, which is the arrangement this change exists to support.

**The symptom is quiet.** A location that cannot be mounted is not fatal to the process: the server still reports `serving`. It does fail the whole environment the location belongs to, so that environment is skipped and none of its packages load, including the ones that resolved fine. The reason is in the log: `Error initializing environment "<name>"; skipping environment`.

**`~/` in a `location` now works.** It was accepted and then never expanded, so it resolved to a literal `~` directory under the server root and failed to mount. Expansion is unconditional and happens before any anchor applies.

See [docs/configuration.md](docs/configuration.md) for the rule and the recommended layout.

## [Unreleased] — Source access gates (`#(authorize)`)

**Sources can now gate query access on givens.** A `#(authorize) "<bool expr>"` annotation (source-level) or `##(authorize)` (file-level) is evaluated against the request's [givens](docs/givens.md) before any query that reads the source runs; access is denied with **HTTP 403** unless at least one in-scope expression is `true` (OR semantics). Enforced on `POST /…/query`, the notebook-cell `GET`, `POST /…/compile`, and the MCP `malloy_executeQuery` tool. Malformed or invalid annotations fail model load with **424**.

**Important — this is a trusted-tier boundary, not end-user authn.** Givens are caller-asserted, so `#(authorize)` enforces policy only when Publisher sits behind a trusted tier that sets givens from verified context and the query API is network-isolated from untrusted callers. See [docs/authorize.md](docs/authorize.md) (Security model) for the deployment contract, the locked-base + curated-extension pattern, and known limitations.

## [Unreleased] — planned (post-givens-migration)

**Givens are now the recommended way to supply runtime parameters.** Models declare `given:` blocks (per [Malloy's experimental givens feature](https://docs.malloydata.dev/documentation/experiments/givens)); callers send values via the new `givens` body field on `POST /…/query` and `POST /…/compile`, the `givens` query parameter on the notebook-cell GET, or the `givens` argument on the MCP `malloy_executeQuery` tool. The notebook UI automatically renders a Parameters panel for any model that declares givens.

`filterParams`, `bypassFilters`, the matching `filter_params` / `bypass_filters` query parameters, and `#(filter)` annotations are **deprecated** and will be removed in a future release after a coordinated migration with current users. Models that use `#(filter)` will continue to work unchanged during the deprecation window; affected responses now carry a `Deprecation: true` header (per RFC 8594) pointing at `docs/givens.md`, and the server logs a one-time migration notice when such a model is loaded. See [docs/givens.md](docs/givens.md) for the migration recipe.

## [Unreleased] — planned 0.0.195

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
