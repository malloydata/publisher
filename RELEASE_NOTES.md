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
- **Observability:** `storageServeBindings` on package status; `publisher_storage_serve_routing_total{outcome=storage|live_fallback|runtime_live_fallback}`, `publisher_storage_chained_build_total{outcome=parent_reuse|inline_fallback|strict_refused|infra_failure}`, and a `served_from=storage|live_fallback` attribute on `malloy_model_query_duration`, plus build/GC/eligibility counters under the `publisher` meter. `runtime_live_fallback` is the signal that the tier is broken while queries still succeed — the hit rate alone won't show it.
- **A run-time store failure honours `freshnessFallback=live`.** If a routed query fails against the stored table (a reclaimed generation a binding hasn't caught up with), a source whose binding declares `live` is recomputed live rather than erroring — the same answer the compile-time fallback ladder already gives. `fail` and the `stale_ok` default keep surfacing the error, and the decision is read from the bindings actually serving the query, so a stale sibling can't veto it.

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

## [Unreleased] — The Console home page catches up, and content types get their own colors

**The home page still advertised the three things Publisher did a year ago.** Its feature list read "Ad-hoc analysis / Notebook dashboards / AI data agents" — no dashboards, no data apps, nothing about access control — while the package page below it was listing all of them.

- **Six features instead of three**, in the order someone meets them: Explorer, then the three artifacts a package can hold (notebooks, dashboards, data apps), then the MCP endpoint agents come in through and the model-declared rules that govern all of it. Each links to the doc that covers it, and the ones documented only in the repo (dashboards, data apps, givens, materialization, connections) now have entries in `docLinks.ts` rather than pointing at a docs-site page that does not describe them.
- **The closing paragraph carries what did not earn a card** — connections to BigQuery, Snowflake, Postgres, MySQL, Trino, and DuckDB; materialized tables on a schedule; and the REST API this console is built on, linked to the spec the server already serves at `/api-doc.html`.
- **Every content type on the package page has its own color.** Six kinds were sharing three: dashboards and models were both orange, notebooks and data apps and materializations all teal. Models take the logo's dark blue, and data apps, package data, and materializations take three new accents. The logo's three stay with the three things a package has always held.
- **New `MALLOY_ACCENT` in the SDK's `styles.ts`**, kept separate from the chart series it echoes because the two have opposite constraints: a series color fills a large area and can be light, while these sit behind a white icon and have to clear 3:1 against white. The series' violet, pink, and sage land nearer 1.7:1 — the reason they were not simply reused.
- **A row cannot mismatch its icon and its color any more.** `PackageItemRow` took an icon element and a tint hex as separate props; it now takes the content type and derives both from `CONTENT_TINT`.

`docs/screenshots/console.png` is recaptured, and taller: at its old 900px viewport it cut off after Semantic Models, which is four of the six sections and a poor illustration of the sentence in `docs/console.md` that now points at it.

## [Unreleased] — The storefront data app, rebuilt on the model

**The HTML data app looked like a different product and knew things the model already knew.** It carried its own palette and 12px radii next to the Console's, hardcoded four KPI labels and a table's headings, filtered by pasting dropdown values into a `where:` string, and had no answer to a reader who wanted to go from a category to that category's page. The page is now one tab per dashboard in the package, and everything it shows about the model, it reads from the model.

- **A tab per dashboard**, sharing their slugs — which is the whole reason a `# drill { to=category }` can land: the tag names a dashboard, and the page has a tab of that name. Tiles sit on the same 12-column grid the dashboards use, and a chart grows to its row so it ends level with the table beside it.
- **Drill, hand-rolled to match.** A drillable cell is ordinary text with a pointer, and the link color plus an underline on hover; two destinations pop a menu headed by the clicked value; one navigates straight away; `to=self` filters in place; an aggregate never drills, even when the tag reaches it. The destinations, the given to seed, and the value encoding are all read off the result's field annotations, so making a new column clickable is a model edit.
- **A chart over a drillable dimension clicks like the table does**, by its axis label as much as by its bar — the brands chart on the Category tab. `# drill` belongs to the field, not to one way of drawing it, so the chart needed no tag of its own. Canvas text takes no CSS and Chart.js reports clicks only inside the plot area, so the page hit-tests the axis strip itself and paints the hovered tick in the link color; `charts.js` carries the three rules that make that safe, including why only a categorical axis gets it.
- **Axis labels stopped overprinting.** Three years of months ran together across the bottom of the trend charts. The cause was a `maxTicksLimit` of 10: Chart.js's auto-skip *replaces* its own measurement with that number rather than capping it, so ten labels were forced through a space that fit six. Dropping it lets the library measure, and a bar axis now keeps every label — rotating if it must — because a skipped one leaves a bar standing over nothing. Pinned by a test that measures the gap between neighbouring ticks against their widths.
- **Nothing about a field is written twice.** Table headings come from `# label`, number formats from `# currency` / `# percent` / `# number=`, alignment from Malloy's `calculation` marker, and the control row from the model's `given:` declarations, fetched from the model endpoint — add a given and the widget appears, the same promise a dashboard makes.
- **Filters travel as givens, not as string building.** New `data_app.malloy` applies all five givens in one `scoped_orders` source, so the page's filters mean what the dashboards' mean, and a value with a quote in it cannot change what a query says. State lands in the URL in the Console's shape (`?tab=regions&REGION=West`): a link someone can send, with Back undoing a drill.
- **Publisher's own look:** Inter, the warm-grey Console shell, 4px radii, the Malloy brand series palette for the charts, and a dark-mode block. Color is reserved for data, which is also why a drillable cell is not blue until you hover it.
- **`Publisher.query`'s `givens` option was already there and documented as absent** — [docs/html-data-apps.md](docs/html-data-apps.md) and the package's `AGENTS.md` both said the runtime could not pass per-query givens. Corrected, with the filter-syntax shapes each given type takes.
- New `brand_performance` view on `order_items` (the brand counterpart to `category_performance`), and `packages/app/tests/playwright/package-data-app.spec.ts` covering the tabs, the control row, the affordance, the menu, and the URL contract.

## [Unreleased] — Dashboards that line up

**The bundled dashboards came out lopsided.** KPI cards took their natural widths beside the tiles rather than a share of the row, the first tile flowed into whatever columns were left over, and inside the tiles a choropleth drew at a hardcoded 343px against a 1050px edge while a five-column table stopped at its content width. Every one of those is a tag the example was not using, so the page read as a rendering bug when it was a layout the model never asked for.

- **All four `storefront` dashboards, and the model's `business_overview` view, now use one recipe:** `columns=12`, a `# colspan` on every card and tile summing to 12 per row, and a `# break` so the tiles get a row of their own. The view is what the notebook's first cell runs, so the notebook gets the same page.
- **A tile's contents reach its edges.** A `shape_map`'s Vega SVG scales to the tile (it carries a viewBox, so this is exact), a root table's columns share the slack with their content width as the floor, and a chart shorter than its row is centred rather than sitting against the top with the gap below.
- **Notebook cells size by what they hold.** A chart cell renders at a chart's height instead of the table cap, so a three-bar chart is no longer 700px of mural; a table cell hugs its rows; a `# dashboard` cell keeps its natural height. Tables fill the cell width, the way a tile's table fills its tile.
- **Everything on the page is labelled.** Every field a `storefront` view groups by or aggregates carries a `# label`, tiles are labelled on the nest, and a composite dashboard's tile heading reads "By brand" rather than `scoped_sales -> by_brand`, which stays as its tooltip. A ratio card gets a number format, so orders per customer reads `10.7` and not `10.695`.
- **Two renderer labels come from a field's name, not its `# label`**, which no amount of labelling fixes: a `shape_map`'s color legend, and a series legend that sizes itself from the longer of the label and its widest value and then truncates both (4-digit years under a 4-character label clipped to `20…`). Worked around in the example by renaming the measure and lengthening the series label, and written down in [docs/dashboards.md](docs/dashboards.md#laying-out-the-grid) with the rest of the recipe.

The layout is asserted, not eyeballed: a fixture dashboard built to the recipe has its card and tile boxes measured in the browser, so a row that stops ending flush fails a test. [docs/dashboards.md](docs/dashboards.md) and the `malloy-dashboards` skill carry the recipe and the traps.

### SDK impact

`RenderedResult`'s `onSizeChange` now reports what it measured (`(height, kind)` where kind is `"dashboard" | "chart" | "other"`), and `ResultContainer` takes an optional `chartHeight` for the height a chart should be drawn at. Left unset, a chart gets `maxHeight` as before, so a host that passes neither is unaffected.

## [Unreleased] — The guided-tour notebook stops repeating itself

**Three of the tour's eleven result cells were the dashboard's own tiles shown again a screen later** — the state map, the category bar chart, and the top-products table — and two of the sections that were new carried no prose at all. A reader scrolling it met the same numbers twice and was told very little about any of them.

- **Every cell after the dashboard now shows something the dashboard didn't.** The three duplicates are gone. Geography is carried by `sales_by_region` (the South alone is 37% of revenue), and the category section by `category_performance`, where the margin-rate column holds what a revenue ranking hides: Outerwear leads on 22% of revenue at one of the thinnest rates in the catalog.
- **The prose now says what the data says.** "The business is growing" is $503K → $682K → $913K; "the holiday quarter lifts" is 2025 taking $237K of its $913K in November and December. Every figure was read off a query, and the example data comes from a seeded PRNG, so they stay true across a regeneration.
- **The two silent sections got a narrative.** Brands are a near dead heat — the top four within $2,800 of one another — so their ranking is noise and the rate is the finding; and the top-products-by-revenue list is all outerwear, which the same question asked by units answers with an almost entirely different ten.
- **One cell is an ad-hoc query rather than a named view** — that products-by-units cut — so the tour shows querying the model, not only running what was defined in it.
- **`brand_performance` joins the tour**, and with it the other shape a drill takes: `brand` names one destination (`to=self`), so a click filters in place with no menu, a few cells below `category`'s two-destination menu. Pinned by a new case in `packages/app/tests/playwright/package-notebooks.spec.ts`.

The README hero (`docs/malloy-publisher-demo.png`) and `docs/screenshots/storefront-dashboard.png` are recaptured: both still showed a five-card KPI stack the model stopped producing, and the hero's capture step no longer lifts the dashboard's height caps, which had been reflowing the category tile until its axis labels clipped.

## [Unreleased] — The storefront notebook gets the filter controls its dashboards have

**The guided-tour notebook had no Parameters panel**, so the flagship example showed givens on dashboards only, and the one place a reader meets Publisher's notebooks first looked like a surface without filters. It also cost that notebook the `to=self` half of `# drill`: with no control for `CATEGORY`, "Filter this notebook" had nowhere to write and was correctly not offered — which read as the two surfaces behaving differently when it was the example, not the code.

- **[`examples/storefront/storefront.malloynb`](examples/storefront/storefront.malloynb) imports four of the givens its dashboards use** — Category, Brand, Ordered since, Minimum line total — and runs every cell against one source that applies them. Four controls scope the whole document; the widgets, the suggest lists, the URL state, and the Reset are the same code the dashboards' filter row runs.
- **Clicking a category cell there now offers both destinations**, "Category Detail" and "Filter this notebook", the same menu the equivalent dashboard tile offers.
- **`REGION` is declared in `givens.malloy` and deliberately not imported.** A notebook renders a control per given it imports, where a dashboard renders one per given its query references, so importing an unused given is how you get a control that moves nothing. [docs/choosing-a-surface.md](docs/choosing-a-surface.md) now states that difference instead of calling the two rows identical.
- **[docs/givens.md](docs/givens.md) gains the other authoring shape**: givens in their own file, imported by documents that each decide what to filter — next to the existing `governed-analytics` walkthrough, where the source arrives pre-filtered. Including the `##!` flag being per file, which is what a notebook writing `$CATEGORY` in its own cell trips over.

Covered end to end in `packages/app/tests/playwright/package-notebooks.spec.ts`, against the shipped example rather than a fixture: a panel that loses its controls is a silent regression no other test sees.

### Added: a notebook can say where its controls start

`## givens { REGION=f'US' SINCE="2024-03-01" }` at a notebook's file level is the notebook spelling of a dashboard's `# artifact { givens { … } }` — the last piece of the two surfaces that was not shared, next to `## autorun=false`. Both go through one `readStartingGivens`, and arrive as one field with one name: `givens` on `RawNotebook` as on `DashboardManifest`, carrying `EncodedGivenValues`.

The precedence is the same as a dashboard's, because it is the same code: a URL parameter beats the file's starting values, which beat the declaration's defaults. Applied values go into the URL on load, so what a reader copies out of the address bar is what they are looking at.

### Fixed

- **A `control=multiselect` no longer closes its list after every pick.** MUI's `Autocomplete` closes on select by default, so choosing three brands meant reopening the list twice. Single-value pickers still close, which is what a single choice should do. Both surfaces render the same control, so both get this.

## [Unreleased] — The storefront example stops overriding the chart palette

**`examples/storefront` shipped a `## theme.palette.series` of its own**, an indigo-to-cyan ramp inherited from the HTML data app's accent color. It won over both the instance config and the Theme Editor, for every view in the model — so the bundled dashboards rendered indigo bars and lines beside a teal choropleth (the map reads `mapColor`, which the override did not touch) and a Console themed in Malloy's brand colors. It read as Publisher failing to apply its own theme; it was one line of the example asking for something else.

- The model-level override is gone, so storefront's charts inherit the configured palette like everything else. Out of the box that is Malloy's teal, which the choropleth and the chrome already used.
- The HTML data app in `storefront/public/` draws its own Chart.js charts, so its colors are hardcoded by definition; they now start from the same default, in two named constants a deployment can repoint.
- [docs/theming.md](docs/theming.md) documents the model-level layer, which it previously described only as "per-chart", and says plainly that a `##` palette beats the config and the editor with nothing in the UI to say so.

Doc screenshots and recordings were recaptured. The README's hero GIF is a hand-recorded screencast and still shows the old palette.

## [Unreleased] — Drillable cells look drillable

**`# drill` had no affordance.** A cell that navigated rendered exactly like one that did nothing: no cursor change, no hover, no link styling, on dashboards and in notebooks alike. The clicks worked; there was no way to discover them short of clicking around. This brings the reader-facing half of drill in line with Malloyyo, which the click behavior already matched.

- **Drillable table cells read as links.** A pointer cursor at rest, and blue plus underlined under the pointer — plain text otherwise, so a whole column does not compete with the data. Ported from Malloyyo's own marking and styling, so a model repo behaves the same in both.
- **The affordance and the click share one capability filter.** A destination the surface cannot reach is neither marked nor offered: a `to=self` in a document that declares no control for its given stays plain text instead of swallowing the click. A dead link is worse than none.
- **Destinations read as sentences in the menu.** `category_detail` renders as "Category detail" rather than as a filename, matching Malloyyo.
- **`to=self` names the surface it would filter.** "Filter this notebook" in a notebook, "Filter this dashboard" on a dashboard. It previously said "dashboard" in both, from a tag that cannot know where it fired.
- **A measure that inherited a drill tag no longer navigates.** Only grouped dimensions drill, matching Malloyyo: an aggregate cell holds a total, not the value the filter would name.

Charts are unchanged and still carry no affordance in either product, so a dashboard meant to be drilled wants at least one table tile. See [docs/dashboards.md](docs/dashboards.md#drill).

### SDK impact

`RenderedResult`, `ResultContainer`, and `DashboardTile` take a single `drill` prop (a `DrillBinding` of `{ onClick, canDrill }`) in place of the former `onDrill` callback, and `useDrill` returns `{ drill, drillMenu }` rather than `{ onDrill, drillMenu }`. The two travelled together anyway: passing only the handler is what produced the missing affordance. `useDrill` also takes an optional `selfLabel`. A host that renders results without drill passes nothing and is unaffected.

## [Unreleased] — Two example packages instead of three

**`examples/html-data-app` is removed; `storefront` is the one data-app example.** The bundled `examples` environment now serves `storefront` and `governed-analytics`. Nothing was lost, because `storefront` grew its own `public/index.html` when it became the package that carries every analytics surface over one dataset — which left two near-identical HTML data apps in the tree, down to a byte-identical 208 KB vendored copy of Chart.js.

The two things only the deleted package had moved into `storefront`:

- `public/embed-test.html`, the host page that mounts the data app with `Publisher.embed` and shows the iframe auto-sizing. It now appears in `storefront`'s **Data Apps** list beside `index.html`.
- `AGENTS.md`, package-scoped guidance for agent clients that read `AGENTS.md` rather than Anthropic Agent Skills. Rewritten for `storefront`, so it covers all four surfaces and then goes deep on the data-app runtime.

`governed-analytics` stays, and is not a candidate for merging: `#(authorize)`, row-level access, and `explores` / `queryableSources` curation cannot move into `storefront` without breaking it, since a gated source answers 403 to a caller with no identity given and the flagship example is meant to work the moment you open it. It did get freshened in this release (below).

### Migration

- Any URL or config naming the `html-data-app` package. The equivalent is `storefront`: `http://localhost:4000/environments/examples/packages/storefront/`.
- A `publisher.config.json` copied from `packages/server/publisher.config.json` (or either `publisher.config.example.*.json`) that lists `html-data-app`. Drop the entry; nothing else in the file changes.
- `docs/screenshots/html-data-app-dashboard.png` is deleted (it was referenced by no doc) and `html-data-app-filtering.gif` is now `data-app-filtering.gif`, recaptured against `storefront`.
- `scripts/generate-example-data.mjs` no longer writes `subscriptions.parquet`. The other datasets are byte-identical: subscriptions was generated last, so removing it does not shift the shared PRNG stream.

## [Unreleased] — `governed-analytics` gets real controls and a dashboard

**The governance example now demonstrates governance through the current surfaces**, rather than looking like it predates them.

- **Control tags on the filter givens.** `REGION` renders as a dropdown and `MIN_AMOUNT` as a slider, and a new `STATUS` given is a multi-select. Two named option-list queries (`region_suggest`, `status_suggest`) back the dropdowns, reading the _unfiltered_ base source so the choices stay complete. The identity givens, `ROLE` and `TENANT`, are deliberately left untagged: they are supplied by a trusted tier from verified identity, not typed by a reader.
- **A dashboard.** `dashboards/governed-overview.malloy` puts the same three declarations in a control row over the same governed source, which is the point of putting the control contract on the declaration rather than on a surface.
- **A documented interaction between curation and dashboards.** In a package that curates its surface (`explores` plus `queryableSources: "declared"`), a dashboard file must be listed in `explores`, and must `export` both its own query and anything its controls read: an import resolves at compile time but is not runnable under curation. Because an explicit `export { … }` replaces the default "everything top-level", the dashboard's own query belongs on the list too. Omit a suggest and only that dropdown comes up empty; omit the dashboard's query and the grid stops loading. Written up in [docs/dashboards.md](docs/dashboards.md) and demonstrated in the file. A package with no `explores`, like `storefront`, needs none of this.

## [Unreleased] — "Pages" are now "Data Apps" (breaking)

**One name for one thing.** The in-package HTML surface was called a _data app_ in every doc and skill, and a _page_ in the API, the generated client, and the Console. "Page" was also the wrong word twice over: it collides with the Console's own screens (the package page, a notebook page), and it undersells an artifact that queries a semantic model and holds filter state. Everything now says **data app**. [docs/html-data-apps.md](docs/html-data-apps.md) is unchanged in substance; only the names it quotes moved.

Nothing about authoring changes: a data app is still a `public/` directory of HTML that Publisher serves with no build step, `Publisher.query` and `Publisher.embed` are untouched, and `<meta name="publisher:fit">` still works. What changed is what the surface is _called_.

### Breaking

- **REST:** `GET …/packages/{pkg}/pages` → `GET …/packages/{pkg}/data-apps`. The response shape is unchanged. The old path is **removed**, not aliased — and note how it fails: like any unmatched path on this server it falls through to the SPA fallback, so it answers **200 with `text/html`** rather than a 404. A client that parses the body will fail; one that only checks the status code will not. Check `content-type`, or just move the path.
- **OpenAPI:** schema `Page` → `DataApp` (with `Page.fit` → `DataApp.fit`), tag `pages` → `data-apps`, operationId `list-pages` → `list-data-apps`. Regenerate clients against the updated `api-doc.yaml`.
- **Generated TypeScript client:** `PagesApi` → `DataAppsApi`, `listPages` → `listDataApps`, `Page` → `DataApp`, `PageFitEnum` → `DataAppFitEnum`. On the `ServerProvider` context, `apiClients.pages` → `apiClients.dataApps`.
- **SDK:** the `PageViewer` component is now `DataAppViewer`, and `ContentTypeIcon`'s `type="page"` is `type="dataApp"`. Same props, same rendering.
- **Console URL:** the embedded viewer route is `/{env}/{pkg}/data-apps/{file}` rather than `/{env}/{pkg}/pages/{file}`. An old bookmark falls through to "Unrecognized file type" rather than redirecting. The standalone URL, `/environments/{env}/packages/{pkg}/{file}`, is **unchanged** — that is the one an embed or an external link is most likely to hold, and it never carried the word.
- **UI:** the package page's **Pages** section is now **Data Apps**.

## [Unreleased] — Dashboards: `dashboards/*.malloy`

**A package can now ship dashboards, and a dashboard is just a tagged `.malloy` file.** Drop a file in a package's `dashboards/` directory, tag its query with `# artifact`, and Publisher discovers it at load, lists it on the package page, and serves it at `/<env>/<pkg>/dashboards/<name>`. Filter controls are rendered from the `given:` declarations the query references, the grid comes from the standard `# dashboard { columns=N }` renderer tag, and `# drill` on a model dimension makes cells click through to another dashboard. No code, no build step.

The grammar is [Malloyyo](https://github.com/malloydata/malloyyo)'s, kept byte-compatible on purpose, so a model repo with a `dashboards/` directory works unchanged in either. [docs/dashboards.md](docs/dashboards.md) is the guide; [docs/choosing-a-surface.md](docs/choosing-a-surface.md) covers when to reach for one over a notebook or an HTML data app.

### What changed

- **Discovery and two endpoints.** `GET …/packages/{pkg}/dashboards` lists them; `GET …/packages/{pkg}/dashboards/{name}` returns the manifest — title, autorun, columns, control specs, tiles. A dashboard's query runs through the ordinary query endpoint with givens in the body, so there is no dashboard-specific execution path and authorization, row limits, and query caps behave as they do everywhere else.
- **Two forms.** A single tagged query whose result is the page, or a **composite**: a model-level `## artifact { tiles=[…] }` naming views that already exist, each run separately so a broken tile shows its error in place instead of blanking the page. A `dashboards/*.malloy` with no artifact tag is a shared include, skipped by discovery.
- **`# drill` on a dimension, not on a dashboard.** `to=<slug>` navigates with the clicked value written into the named given, `to=self` filters in place, and two destinations pop a menu. Because the tag is on the dimension, every result grouping by it is clickable — in a dashboard tile and in a **notebook cell** alike, with no per-document wiring.
- **Control state is URL state**, so a filtered dashboard is a shareable link, and `# artifact { autorun=false }` batches changes behind an Apply button.
- **Load-time lint.** Drill targets that name no dashboard, a `to=self` whose given nothing declares, a `suggest` naming a source the file cannot see, an unresolvable composite tile, a bad `dashboard_columns`, and rejected renderer tags all surface as package warnings — the failures that are otherwise silent. A dashboard that fails to compile is still listed, with its error, rather than disappearing.
- **`Dashboard` is a public SDK export**, props-driven and host-agnostic: it reads no router, taking `givens` / `onGivensChange` and `onNavigate` instead. The Publisher Console is one consumer of it, not its home. [`examples/data-app`](examples/data-app) renders the same component in a standalone React app with no router at all.
- **The `storefront` example ships four of them**, in [`examples/storefront/dashboards/`](examples/storefront/dashboards) — every form, over the same data as that package's notebook and HTML data app, so the three surfaces can be compared side by side. Open `http://localhost:4000/examples/storefront/dashboards/overview`.
- **New agent skill `malloy-dashboards`**, for building one from a model.

### Not included

- **No code escape hatch.** A sandboxed custom-JSX component type was built and then cut: it was not a security boundary while `public/` stays open, and it duplicated [HTML data apps](docs/html-data-apps.md), which already do that job at whole-page scope with a real runtime and a real embedding story. A `dashboards/*.jsx` is ignored with a load-time warning pointing there. The reasoning is in [docs/malloyyo-dashboards-design.md](docs/malloyyo-dashboards-design.md#custom-jsx-components-cut).
- **Embedding a dashboard in a non-React host page** is a follow-up ([#931](https://github.com/malloydata/publisher/issues/931)). `Publisher.embed` cannot usefully target a dashboard route yet.

## [Unreleased] — Notebooks and dashboards share one interactivity model

**A notebook's parameters now behave exactly like a dashboard's, because they are the same code.** Notebooks previously had a simpler Parameters panel: plain inputs, immediate re-run, and no way to link to a filtered view. They now get everything the dashboard control row has, with no change to any `.malloynb` file.

### What changed

- **Parameter state is in the URL.** Applied values become query parameters on the notebook route, so a filtered notebook is a shareable, bookmarkable link, and loading one restores the controls. Changing a value replaces the history entry rather than pushing one, so Back leaves the notebook instead of walking through every value the reader tried.
- **Richer controls, declared on the given.** `# label`, `# control=select|multiselect`, `# suggest { source=… dimension=… }` / `suggest { query=… }`, and `# range_min` / `# range_max` now drive a notebook's Parameters panel as well as a dashboard's control row. These are properties of the `given:` declaration, so a given already tagged for a dashboard needs nothing further. See [docs/givens.md](docs/givens.md).
- **`## autorun=false` batches behind Apply.** A file-level tag on a notebook, the counterpart of `# artifact { autorun=false }` on a dashboard. It matters more here: one parameter change re-runs every cell in the document. Notebooks default to autorun, as before.
- **`# drill { to=self }` filters a notebook in place**, in addition to the cross-dashboard drill that already worked from notebook cells.
- **Notebooks are listed by title, not filename.** A notebook row on the package page now shows a human title with the filename beside it, the way dashboard and data app rows do. The title is `## title="…"`, then the file's `#" ` doc comment — the chain a dashboard's title follows — and then the notebook's first markdown heading, which most notebooks already have, so existing files get a real title without being edited. `Notebook.title` and `Notebook.description` are new on the notebooks API response.
- **The load-time lint covers drills wherever they are declared.** `# drill` is a tag on a model dimension, so the scan now reads every compiled model rather than only the files in `dashboards/`: a broken target reachable only from a notebook, or in a package with no dashboards at all, is now a package warning instead of a dead-end at click time. New finding: a `to=self` whose given no model in the package declares, which cannot fire on any surface.
- **`RawNotebook.autorun`** is a new boolean on the notebook API response.
- **`Given`** now carries the control contract (`label`, `control`, `rangeMin`, `rangeMax`, `suggest`) wherever it is returned. `DashboardManifest.givens` is a list of plain `Given`s; the separate `DashboardGivenSpec` and `DashboardSuggest` schemas are gone. Regenerate clients against the updated `api-doc.yaml`.
- **One meaning for `givens` across the API.** It had come to mean four things at once — declarations, typed values, string-encoded values, and a list of names. It now always means a collection of `Given` *declarations*. Values keyed by given name are `GivenValues` (`EncodedGivenValues` in the URL form), the starting values a dashboard or notebook declares are `startingGivens` (was `givens` on `DashboardManifest` and `RawNotebook`), the dashboard control row is `givens` (was `givenSpecs`), and a tile's list of names is `givenNames` (was `givens`). A composite tile's run expression is `query` rather than `tile`, matching `DashboardManifest.query`. Package-warning entries name their subject `subject` rather than `target`, which previously meant the opposite of a `# drill` target. Regenerate clients.
- **The `Givens` schema is renamed `GivenValues`,** and the string-encoded form it shares with URL parameters is now a named `EncodedGivenValues` rather than an anonymous map inlined on `DashboardManifest.givens`. `Givens` read as the plural of `Given` and was not: `Given` describes what a model _accepts_ and is always carried in a plain array, while these two are the values a caller _sends_, decoded and string-encoded respectively. No field, shape, or wire format changed. Our TypeScript generator emits no symbol for any of the three (they are `additionalProperties`-only, so they inline), but a generator for another language may name them — regenerate and check before upgrading.

### Fixed

- **A `Date` given was encoded wrongly for two of the three time types.** The three take three different spellings and each rejects the other two: `date` wants a bare `YYYY-MM-DD`, `timestamp` is naive and rejects a trailing `Z`, and only `timestamptz` takes a full ISO string. Dashboards sent the ISO form for all three, so a `date` or `timestamp` given set from a dashboard control failed with a 400. The encoder now reads the declared type.

### Removed (breaking)

- **The notebook's Filters panel is gone.** It drove `filter_params` and `#(filter)` annotations, deprecated in favour of givens (see the givens-migration note below). The server side is unchanged and still honours the deprecation window; only the UI panel is removed. Models using `#(filter)` should migrate to `given:` to get controls back — [docs/givens.md](docs/givens.md) has the recipe.
- **SDK:** the `useGivensForm` hook (use `useGivensState`), the `GivenControlSpec` type exported from `components/given` (its fields are on `Given` now), the `spec` prop on `GivenInput`, the `specs` prop on `GivensPanel`, and the `retrievalFn` prop on `Notebook` and `Package`. `givensToRequest` takes the declared-type map as its second argument. `GivenValue` moves to `hooks/givenValue` and is still re-exported from `hooks`.
- **SDK `Notebook`** takes `givens` and `onGivensChange` props, matching `Dashboard`. A host that renders `Notebook` directly and wants URL-addressable parameters must wire them; without them the panel still works, just without URL state.

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
