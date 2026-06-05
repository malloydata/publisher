# Release Notes

Curated release notes for `@malloy-publisher/sdk`, `@malloy-publisher/app`, and `@malloy-publisher/server` (versioned in lockstep).

## How this file is used

The `Release (NPM + Docker)` workflow (`.github/workflows/release.yml`) creates GitHub releases automatically with a standard header (NPM/Docker links) plus an auto-generated "What's Changed" PR list via `gh release create --generate-notes`. That auto list is sufficient for routine patch releases.

For releases that warrant narrative — redesigns, breaking changes, migration steps — copy the relevant section below into the GitHub release page after CI publishes it. The future workflow change to read this file directly is documented in #2 of the May 2026 review.

---

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
