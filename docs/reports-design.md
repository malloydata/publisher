<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Design: reports, anywhere in a package

**Status: shipped, except ending the deprecations.** This is the design record; the user-facing
reference is [reports.md](reports.md). The decisions are under [Decisions](#decisions), and there
are no open questions.

- **Shipped** (Delivery steps 1–4): the report rule and layout reader on the server and in the SDK,
  path identity, `GET …/reports` and `…/reports/{path}`, one report lint, the curation edges, the
  write path, the builder with its layout switch, New report at the package root, drafts keyed by
  path, drill by path, the Console's path routes, and the docs and skills. `kind=`, the query-level
  `# artifact` and the bare-name drill fallback are removed, not deprecated: `kind=` is the
  `report-kind-removed` error.
- **Deferred** (Delivery step 5): removing what stays deprecated behind its own seam — the
  `…/dashboards*` and `…/notebooks*` routes and the basename alias (`routes/deprecated.ts`,
  `controller/legacy_report.controller.ts`, `service/legacy_report_views.ts`), the Console
  `/dashboards/<name>` alias, and the SDK `Dashboard`/`Notebook` wrappers and kind-typed props
  (`components/legacy/`), once Credible has moved; and `.malloynb`, `explores`,
  `queryableSources` and `#(filter)` (`service/deprecated/`).

[Where the split lives today](#where-the-split-lives-today) records the code as it stood before
this change, which is why it names files and symbols that no longer exist.

## Goal

Publisher has two document types today, the Malloyyo **dashboard** and the **notebook**, with
separate folders, listings, routes, viewers, docs, and skills. They are already nearly the same
thing: both are a `.malloy` file whose `## artifact` tag lists tiles, and the notebook skill says so
outright, "a notebook is a dashboard with one column". This design makes that official and removes
what is left of the split:

1. **There is one document type, the report.** A report is a `.malloy` file carrying an artifact
   tag. It has a **layout**: **linear**, rendered top to bottom like a notebook, or **grid**,
   rendered as a two-dimensional grid of tiles. Layout is the only difference. Everything else
   (filters and givens, autorun, text tiles, drill, the builder, curation, the write path, lint) is
   one feature set that applies to both.
2. **A report can live anywhere in the package.** Where a file lives carries no meaning:
   `dashboards/` and `notebooks/` are not required, not a default, and not a convention. They become
   ordinary directory names. Tags alone decide whether a file is a report and which layout it has.

Existing packages keep working, with one exception: every file in a `dashboards/` or `notebooks/`
directory today is classified the same way by its tags alone (checked against every example and
fixture in this repo, §Compatibility) except a kindless model-level tag that lists tiles in
`notebooks/`, which was a notebook and is now a grid report. Every URL and REST route that answers
today still answers, the old ones as deprecated aliases.

## Decisions

| Question                                                  | Decision                                                                                                                                                                                                                                                                                                                            |
| --------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Are dashboards and notebooks different things?            | No. Both are **reports**; a report's **layout** is linear or grid, and that is the only difference (§1, §2).                                                                                                                                                                                                                        |
| Do `dashboards/` and `notebooks/` mean anything?          | No. Not a requirement, a default, or a convention (§1, §12).                                                                                                                                                                                                                                                                        |
| Where does the create dialog put a new report by default? | The package root (§10).                                                                                                                                                                                                                                                                                                             |
| Propose the change to Malloyyo upstream first?            | No. Publisher diverges: a report outside `dashboards/`, a path drill target, or the new vocabulary does not carry over to Malloyyo. Recorded in `malloyyo-dashboards-design.md`'s drift section.                                                                                                                                    |
| One reader for the tag?                                   | Yes. `readReportTag` over a file's model-level notes decides whether it is a report and its layout, for the compiled file, the same notes read off its text (a file that did not compile, the compile check, the write path, the `explores` advice), and the lint; `reportLayoutParity.json` holds every reader to one answer (§2). |
| How is a report identified?                               | By its package path, only. The basename is a display label; `# drill` targets a path from the package root, and a bare name is a root file (§3).                                                                                                                                                                                    |
| How does the tag name a report's layout?                  | `layout=grid` / `layout=linear`. `kind=` is not read: reports have no users yet, so the old spelling was removed rather than deprecated. A tag that sets it makes no report, reported as `report-kind-removed` (error) with the `layout=` to write (§2, §8).                                                                        |
| What happens to the query-level `# artifact` form?        | Removed: only a model-level `## artifact` makes a report. A `# artifact` on a `query:` is not read, and the file is an ordinary model; the rewrite is mechanical ([The query-level form](#the-query-level-artifact-form)).                                                                                                          |

## Vocabulary

| Old                                            | New                                                   |
| ---------------------------------------------- | ----------------------------------------------------- |
| dashboard, notebook, document                  | **report**                                            |
| a dashboard (2-D tiles)                        | a report with **grid** layout                         |
| a notebook (tiles top to bottom)               | a report with **linear** layout                       |
| `kind=dashboard` / `kind=notebook` in the tag  | `layout=grid` / `layout=linear` (`kind=` is not read) |
| `GET …/dashboards`, `…/notebooks`              | `GET …/reports`, `GET …/reports/{path}` (§4)          |
| skills `malloy-dashboards`, `malloy-notebooks` | one skill, `malloy-reports` (§12)                     |

User-facing text, docs, skills, API descriptions, and new code use "report". Existing code
identifiers (`DashboardManifest`, `discoverDashboards`, lint codes such as
`notebook-tile-layout-ignored`) are renamed only where this change touches them anyway; a
repository-wide identifier rename is not part of it. Legacy `.malloynb` files and `run:`-cell
notebooks are linear reports in this vocabulary, and stay read-only and deprecated as today.

## Where the split lives today

| Where                                                                                                                                                                        | What it decides today                                                                                                                                                                                 |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `notebook.ts`: `NOTEBOOKS_DIR`, `DASHBOARDS_DIR`, `isNotebookModelPath`, `isDocumentModelPath`, `documentKind`                                                               | A candidate is a `.malloy` exactly one level under `notebooks/` or `dashboards/`. A kindless tag takes its folder's kind.                                                                             |
| `dashboard.ts`: `isDashboardModelPath`, `dashboardSlug`                                                                                                                      | Same rule for `dashboards/`. Nesting is excluded because `dashboards/a/x.malloy` and `dashboards/b/x.malloy` would collide on one URL and one `# drill`.                                              |
| `package.ts`: `classifyDocument`, `discoverDashboards`, `dashboards` / `layoutNotebooks` / `notebookFileText`                                                                | Two discovery outputs and two text maps. An untagged file is a shared include under `notebooks/` and a dashboard candidate under `dashboards/`. A second dashboard with a taken basename is shadowed. |
| `dashboard.controller.ts`: `DASHBOARD_FILE`                                                                                                                                  | `PUT` accepts `^(dashboards\|notebooks)/[^/]+\.malloy$` only, with a separate untagged-file gate per folder and a 409 for a taken basename.                                                           |
| `notebook_lint.ts`, `notebook.ts` (`notebookReaderProblem`), `environment.ts` (lint text), `dashboard.ts` (render-log filter), `package_manifest.ts` (`exploresDeprecation`) | Lint runs only for candidates, split into a notebook lint and a dashboard lint that de-duplicate against each other; `notebook-other-folder` tells an author to move a file to its kind's folder.     |
| `server.ts`, `api-doc.yaml`                                                                                                                                                  | Two resources: `…/dashboards` (by basename) and `…/notebooks` (by path), with different schemas (`Dashboard`, `DashboardManifest`, `Notebook`, `RawNotebook`).                                        |
| `spa_fallback.ts`: `SPA_OWNED_SEGMENTS`                                                                                                                                      | `dashboards/` and `notebooks/` always reach the Console, at the cost of a package's own `public/dashboards/` files.                                                                                   |
| SDK `documentLocation.ts`, `catalog.ts`, `readDocument.ts`, `spliceDocument.ts`, `createDocument.ts`, `documentPath.ts`, `newDashboard.ts`, `newNotebook.ts`, `Package.tsx`  | Slug from path by regex; kind inferred from folder; new files go to `<kind>s/<slug>.malloy`; templates import `"../<model>"`; two package-page sections; drafts keyed by `<kind>s/<slug>`.            |
| Console `ModelPage.tsx`, `RoutedDocument.tsx`, `DashboardPage`, `NotebookPage`, `DocumentModeButton.tsx`, `useDrillNavigate.ts`, `drill/resolveDrill.ts`                     | Two route shapes and two pages; drill targets a basename and always navigates to `/dashboards/<slug>`.                                                                                                |
| Docs, skills, `AGENTS.md`                                                                                                                                                    | Two skills, two reference pages (`docs/dashboards.md` and the notebook sections), and `docs/choosing-a-surface.md` presenting them as separate surfaces.                                              |

Two pieces of precedent make this smaller than it looks:

- **Most of the server already keys on discovery, not paths.** The query boundary asks
  `isServedDashboard` and `isServedNotebook`, which read discovery's maps, and `listModels` drops
  served reports by the same maps.
- **A tile-layout notebook is already built as a dashboard manifest with one column**
  (`buildDashboardManifest` returns `kind: "notebook"`; `RawNotebook.dashboard` carries it), and
  the builder (`DashboardEditor`) already edits both with a `kind` prop.

## Design

### 1. What makes a file a report

A file is a **report** when all of these hold:

- it is a `.malloy` file in the package, outside `public/`. The loader compiles every `.malloy`
  file, `public/` included (`filterModelPaths` in `package_load_worker.ts`), but `public/` is also
  served as raw static files to anyone who can reach the server, so a report there would be
  readable two ways under different rules. Discovery skips it and the write path refuses it (§7).
  This is about what Publisher does with `public/`, not a location rule for reports;
- it carries a model-level `## artifact` tag that parses and does not set `kind=` (§2). A
  `# artifact` on a `query:` or a `view:` is not read ([The query-level form](#the-query-level-artifact-form)).
  The tag is read by one reader, `readReportTag`, over the file's model-level notes: a compiled
  file's own notes, or, for a file that did not compile and for text not yet saved, the same notes
  read off its parse tree (`modelNotesOfText`), so the two cannot disagree;
- it is not the package's root `index.malloy` (§6).

A legacy `.malloynb` is also a report (linear, read-only), as it is a notebook today, wherever it is.

Everything else is an ordinary model, which a report may import. There is no "candidate" or
"shared include" state tied to a directory any more.

The path predicates and the `NOTEBOOKS_DIR` / `DASHBOARDS_DIR` constants are deleted, and
discovery produces **one** map keyed by path of every file that carries a tag, each with a status
(`served`, `tileless`, `invalid`, `dropped`) and, when served, its layout, in place of
`dashboards`, `layoutNotebooks`, `dashboardFileText`, and `notebookFileText`. A reload discovers
against the models it is about to install and swaps both in together, so no query sees new models
with the old reports' boundary or the reverse. `isServedDashboard`
and `isServedNotebook` collapse into `isServedReport`; every caller already wants "is this a
report", not which kind.

### 2. Which layout a report has

In order, first match wins:

1. `layout=grid` or `layout=linear` in the tag.
2. A model-level `## artifact` that lists `tiles` or a model-level `dashboard {…}` grid tag (on the
   artifact line or a line of its own) is **grid**.
3. Anything else is **linear**.

   _Later:_ a `.malloy` tag that comes out linear with no `tiles` key was the `run:`-cell form, which was
   retired; such a file is no report (`report-cells-removed`).

A tag that sets `kind=` is no report at all: `kind=` was the old spelling of the layout, removed
rather than deprecated because reports have no users yet, and reading it silently as a layout, or
silently not, would both hide the change from an author. The lint reports it as
`report-kind-removed` (error) with the `layout=` to write, and the compile check and the write path
refuse the text for that error.

Steps 2 and 3 are the rule `compile_model` already applies to submitted text, which has no
folder (`api-doc.yaml`, the `compile` endpoint and `CompiledDocument`); this makes it the only
rule. A tempting alternative, "grid only when the file states a grid width or a tile span", was
checked and rejected: seven kindless dashboards in the fixtures list tiles with no width or span at
all (the grid's default width applies), and that rule would turn them linear. So a **linear report
that lists tiles must say so**, with `layout=linear`. A hand-written one in
`notebooks/` that lists tiles and names no kind took the folder's kind and was a notebook; it is
now grid (§Compatibility).

The builder and the create templates **always write `layout=`**, so a file never depends on steps
2 and 3 after its first save, and `spliceDocument.ts` drops its `dashboards/` special case. Changing a
report's layout in the builder is a one-property edit, not a conversion between document types.

**What layout changes, and what it does not.** Layout decides how tiles are placed: linear renders
them one column wide, top to bottom, and ignores `colspan` / `break` (today's
`notebook-tile-layout-ignored` warning stays); grid places them by `columns`, `colspan`, and
`break`. Nothing else depends on layout. In particular, every feature one kind has today and the
other lacks becomes available to both, as part of this change or as a listed follow-up:

| Feature                                               | Today                                 | Reports                          |
| ----------------------------------------------------- | ------------------------------------- | -------------------------------- |
| Filter bar from givens, URL-synced, Apply / `autorun` | both                                  | both                             |
| Text tiles                                            | both                                  | both                             |
| `# drill` click-through                               | both render it; target is a dashboard | target is any report (§3)        |
| Opened in the builder                                 | both (`DashboardEditor` with `kind`)  | one editor, with a layout switch |
| `run:` cells, definitions in the file                 | notebook only                         | retired: rewrite as tiles        |
| Package-page section, viewer page, route              | separate per kind                     | one each (§9, §10)               |

### 3. Identity: the path, only

**The package path is a report's identity, and nothing else is.** Everything that must pick one
report keys on `path`: the editor, drafts, the package-page rows, Console routes (§9),
`GET …/reports/{path}` (§4), `# drill`, and telemetry. Two reports may share a basename in
different directories, and neither shadows the other. The basename survives on the listing as a
**display label only**. Discovery's shadowing and the write path's name-collision 409 are removed.

**`# drill` targets a report by path, of either layout.** `to="sales/overview"` is the package path
without `.malloy`, read from the package root, so a drill tag on a dimension means the same thing
whichever report renders it. `to=self` is unchanged.

**A bare name is a root file.** A target with no `/` (`to=overview`) is a path like any other:
`overview.malloy` at the package root. It is never looked up as a basename elsewhere; that fallback
existed only for the new format and was removed. A target that is not a plain package path (a `.`
or `..` segment, an empty segment, a leading `/`) resolves to nothing before any lookup.

### 4. REST API

One resource replaces two.

| Route                                              | Change                                                                                                                                                                                                                                                                                                                                                          |
| -------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **new** `GET …/reports`                            | Every report in the package: `path`, `name` (label), `layout`, `title`, `description`, `editable` (false for `.malloynb` and `run:`-cell reports), and the load `error` for one that does not compile.                                                                                                                                                          |
| **new** `GET …/reports/{path}`                     | One report's manifest, where `{path}` is the package path including `.malloy` (or `.malloynb`), exactly as `GET …/reports` returns it, the convention `…/notebooks/{path}` already uses: today's `DashboardManifest` fields with `layout` in place of `kind`; for a `run:`-cell or `.malloynb` report, its cells, as `RawNotebook` returns them today.          |
| (no cell route)                                    | A `.malloy` report's tiles run through `POST …/models/{path}/query`; a `.malloynb`'s cells through the deprecated `…/notebooks/{path}/cells/{i}`. A `…/reports/{path}/cells/{i}` route was built and removed before release: the query endpoint already applies the same surface, `#(authorize)` and row-level gates (`cell_query_parity.integration.spec.ts`). |
| `GET …/dashboards`, `GET …/dashboards/{name}`      | **Deprecated aliases.** The listing returns the grid reports; the item resolves a unique basename and answers 409 naming every match when it is ambiguous.                                                                                                                                                                                                      |
| `GET …/notebooks`, `…/notebooks/{path}`, `…/cells` | **Deprecated aliases** for the linear reports; already path-addressed.                                                                                                                                                                                                                                                                                          |
| `PUT …/models/{path}`                              | Any location; one rule for both layouts (§7).                                                                                                                                                                                                                                                                                                                   |
| `GET …/models`                                     | Unchanged rule, by discovery: served reports are excluded under a curated surface, linear reports always (as served notebooks are today). Open to simplifying to "reports are never models" in a follow-up.                                                                                                                                                     |

The deprecated routes are removed in a later release (Delivery step 5); they live behind one
registration (`routes/deprecated.ts`) over projections of the report API
(`service/legacy_report_views.ts`), which synthesize `kind` and own the basename alias.
`api-doc.yaml` gains a `Report` and a `ReportManifest` schema, marks the old
schemas and routes deprecated, rewrites every description that states the folder rule or the
dashboard/notebook split, and the clients are regenerated.

### 5. Imports

An untagged `.malloy` file is a model like any other, wherever it lives, and any report may import
it. A report's imports are relative to its own location, as Malloy imports always are, so moving a
report between directories means updating its import paths. The builder computes them with the
existing `importPathOf`, which already handles any depth.

### 6. Curation and the query boundary

A report reads the surface; it never adds to it. That is already enforced by discovery's maps, not
by paths, so it carries over unchanged. Two edges need stating because reports anywhere make them
reachable:

- **`index.malloy` at the package root is never a report.** It _is_ the surface. If it carries an
  artifact tag, the tag is ignored and the package reports an `error` finding, rather than the
  surface silently vanishing.
- **A model that `index.malloy` imports, or that `explores` lists, and that is also tagged as a
  report** is served as a report and contributes nothing to the surface, exactly as a tagged file in
  `dashboards/` that `explores` lists does today. With reports anywhere this is how an author could
  empty their own surface by tagging a model, so it gets a `warning` finding naming the importer.
  The importer may be `index.malloy` or any file it reaches through imports: the check follows the
  same transitive closure the surface does.

`exploresDeprecation` in `package_manifest.ts` recognized dashboards and notebooks "by path alone";
it reads each listed file by the same tag reader discovery uses, so following its advice cannot
move a file on or off the surface.

### 7. The write path

`PUT …/models/{path}` (the builder's save) today accepts two path shapes, and
`docs/security-posture.md` names that confinement as part of what makes the endpoint safe. The new
rule replaces the confinement with an equivalent guarantee rather than dropping it:

- **Create** is allowed at any `.malloy` path in the package that does not exist yet, under
  `assertSafeRelativeModelPath` as today, outside `public/`, never at the root `index.malloy`. The
  text must carry an artifact tag (§1). Today's two per-folder gates become this one rule.
- **Replace** is allowed only for a file the package currently serves as an editable report. Today
  that check exists only for `notebooks/`; it becomes the rule everywhere. So the endpoint still
  cannot overwrite a model, an untagged file, a `.malloynb`, or `index.malloy`, wherever they live.
- The name-collision 409 is removed (§3). Everything else is unchanged: `frozenConfig` 403,
  compile-first at `scope: "file"`, `expectedHash` 409 and create-if-absent 409, atomic write under
  the package lock, the post-reload verify (now "the file is a served report"), and the rollback.

The endpoint can add files in more places, but can modify only what it could already modify: served
reports. An attacker who can reach it can already register a package (the existing argument in
`security-posture.md`), so creating a file in a new directory opens no door that was shut.
`security-posture.md` is rewritten to state the new guarantee. Missing directories are created on
create, inside the package root only.

### 8. Lint

The notebook lint and the dashboard lint become **one report lint**. Today they run separately and
de-duplicate against each other (`relintDashboards` runs first so the notebook lint can drop its
copy of a shared finding); with one report map there is one pass per report, plus the
layout-specific checks (grid: `columns`, `colspan`; linear: `notebook-tile-layout-ignored`,
`run:`-cell readability).

| Finding                        | Change                                                                                                                                                                  |
| ------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `notebook-other-folder`        | **Retired.** It tells an author to move a file to its kind's folder.                                                                                                    |
| `notebook-kind-missing`        | **Replaced** by `report-layout-implicit`.                                                                                                                               |
| the shadowed-dashboard finding | **Retired.** Two reports may share a basename (§3).                                                                                                                     |
| drill-target lint              | Resolves `to=` as a path from the package root to a report of either layout; a bare name is a root file. A target that is not a plain package path is reported as such. |

| **new** `report-kind-removed` (error) | A tag that sets `kind=`, which is not read: the file is no report. Gives the `layout=` to write. |
| **new** `report-layout-implicit` (warning) | A model-level `## artifact` with no `layout=` and no tiles, read as linear by §2 step 3. In `dashboards/` that shape was an unserved empty dashboard, so this is the one file that changes meaning; none exists in the repo. Fix: add `layout=`. |
| **new** `report-on-surface` (warning) | §6's imported-or-listed report. |
| **new** `index-is-not-a-report` (error) | §6's tagged root `index.malloy`. |

The lint, `notebookReaderProblem`, the lint-text read in `environment.ts`, and the package-warning
scope all switch from the path predicates to discovery's report map.

### 9. Console

**Routes.** A report's route is its package path with `.malloy` removed: `/<env>/<pkg>/sales/q3`
for `sales/q3.malloy`, with the query parameter `?edit` for the builder (`/<env>/<pkg>/sales/q3?edit`).
A parameter, not a trailing `/edit` segment, because a segment is also a path: `sales/edit.malloy` is
a report whose route is `/<env>/<pkg>/sales/edit`. A trailing `/edit` (the edit route before this
change) stays a legacy alias: it is looked up as a path first, and only when no report is at
`<path>/edit.malloy` does it redirect to `<path>?edit`. `<path>.malloy` keeps opening the Model view.
Every existing URL keeps working, because a file at `dashboards/overview.malloy` already lives at
`/<env>/<pkg>/dashboards/overview`.

**One page.** `DashboardPage` and `NotebookPage` merge into a `ReportPage` that fetches
`GET …/reports/{path}` and renders by layout. `ModelPage` stops deciding "is this a report?" by
prefix: for a splat with no `.malloy` / `.malloynb` extension it asks the report listing for
`<splat>.malloy`. If there is none and the splat is exactly `dashboards/<x>` or `notebooks/<x>`, it
tries the basename `<x>` (a grid report preferred for `dashboards/`, a linear one for `notebooks/`)
and redirects to the report's path route. That
is a **compatibility alias only**; nothing new generates it. The report lookup comes first, so a
report under `data-apps/` opens as a report; `data-apps/<file>` that is not a report keeps opening
the data-app viewer.

**Drill** navigates to the target report's path route.

**`spa_fallback.ts`** drops `dashboards` and `notebooks` from `SPA_OWNED_SEGMENTS`. Before
redirecting an asset-shaped path to the static route, it checks whether `<path>.malloy` is a served
report (the fallback already confirms candidates against loaded state), and, for exactly
`dashboards/<x>` or `notebooks/<x>`, whether a served report has the basename `<x>`, which is what
the Console's alias resolves. That covers a report anywhere whose name ends in an asset extension,
and its old by-name link, and gives back a package's own `public/dashboards/` and
`public/notebooks/` files whenever no report has that name. `data-apps` stays: it is a Console viewer route.

`DocumentModeButton` reads the resolved report. Telemetry and editor keys switch from slug to path.

### 10. SDK

- **One component.** A new `Report` export renders a `.malloy` report by layout from
  `GET …/reports/{path}`. `Dashboard` and `Notebook` stay exported as thin wrappers over the same
  renderers (`components/legacy/`), because Credible's app imports both today; they are deprecated,
  not removed, until Credible has moved. A `.malloynb` is `Notebook`'s to render, through the
  deprecated notebook routes, so `Report` imports nothing deprecated.
- **Location resolution** (`documentLocation.ts`, `useDocumentLocation.ts`): one listing, resolved
  by exact path. The basename lookup survives only behind the compatibility alias and reports
  ambiguity rather than taking the first match.
- **Package page** (`Package.tsx`): the Dashboards and Notebooks sections merge into one **Reports**
  section, each row marked with its layout and opened by path; README detection becomes "a report at
  the package root whose basename is `README`", replacing the `notebooks/readme.malloy` check; the
  new-report model picker excludes listed report paths instead of two prefixes.
- **Builder** (`catalog.ts`, `EditorSurface.tsx`, `ReportEditor`): one editor, keyed by `path`,
  with a layout switch that rewrites `layout=` and nothing else. Reports are excluded from the
  source catalog by the listed paths, not by prefix. The drill editor writes path targets.
- **Create** (`DocumentCreate/*`, `newDashboard.ts`, `newNotebook.ts`): one **New report** action
  that asks for a title and a layout, defaulting to linear, and writes `<slug>.malloy` at the
  **package root**; the author can type a different location. No `dashboards/` or `notebooks/`
  directory is ever created. The two templates merge into one that differs only in `layout=` and
  the grid tag, computes imports with `importPathOf` (at the root, `"<model>"`), and always writes
  `layout=`. The collision check is by path only.
- **Drafts and storage** (`DocumentStorage.ts`, `Package.tsx`): drafts key on the report path, which
  `locatorFor` already builds; the `<kind>s/` prefix filter and the per-kind query keys go.
- **Notebook prose links** (`Prose.tsx`) resolve relative to the report's own location, already
  correct at any depth; the docs should say that moving a report changes what its relative links
  mean.

### 11. MCP

No tool lists reports, and `get_context` indexes what `listModels` returns, so it already follows
discovery. Two small changes: the render-log suppression in `execute_query` switches to the report
map, and the `compile_model` description's statement of the kind rule becomes the layout rule
(§2), with `CompiledDocument` carrying `layout`.

### 12. Docs, skills, and examples

- **Docs.** `docs/dashboards.md` and the notebook material merge into `docs/reports.md`: one
  reference with a section per layout. `docs/choosing-a-surface.md` stops presenting dashboards
  and notebooks as separate surfaces; the choice becomes report (and which layout) vs data app.
  Every statement of the folder rule is removed, not softened, and examples sit at the package
  root with path drill targets. Also affected: `docs/discovery-and-access.md`,
  `docs/security-posture.md`, `docs/api-overview.md`, `docs/console.md`, `docs/ai-agents.md`,
  `docs/README.md`, `README.md`, and `AGENTS.md`.
- **Skills.** `malloy-dashboards` and `malloy-notebooks` merge into **`malloy-reports`**, with a
  short "which layout" section and per-layout references. `malloy-analysis-report` already speaks
  of reports and points at it. `malloy-publish`, `malloy-charts`, and the `malloy-review` structure
  rubric (which exempts a tagged report of either layout wherever it lives) are updated. Skill text
  tells an agent to write a report at the package root unless the author asks otherwise. The
  skills text is bundled into the server and the scaffolder, so it ships with the behavior.
- **History.** `docs/malloyyo-dashboards-design.md` and `docs/dashboard-builder-plan.md` get a
  pointer to this page rather than a rewrite; the Malloyyo drift section records the divergence.
- **Examples.** The `storefront` package moves its reports to the package root, writes `layout=`,
  and rewrites its `# drill` tags to paths, so the reference package teaches the new model. The
  test fixtures stay where they are, to pin compatibility.

## Compatibility

- **Every existing file in this repo classifies the same; not every possible file does.** Checked
  against every `.malloy` under a `dashboards/` or `notebooks/` directory in `examples/` and
  `packages/server/tests/fixtures/`: each states its layout, or is a model-level tag in
  `dashboards/` that lists tiles (grid by §2 step 2, a dashboard before), or is a model-level tag in
  `notebooks/` with no tiles (linear by step 3, a notebook before). Fixtures that used the removed
  `kind=` or query-level `# artifact` moved to `layout=` and to a model-level tag naming the query as
  its one tile; the baseline changes only where they did. The untagged
  `dashboards/_shared.malloy` stays a model. That check is a test
  (`classification_compat.integration.spec.ts`). Two shapes change meaning, neither in the repo:
  - A **kindless model-level tag that lists tiles in `notebooks/x.malloy`**. Before, the folder
    supplied `kind=notebook`, so it was a linear notebook. Folders mean nothing now, so §2 step 2
    makes it **grid**: it moves from `GET …/notebooks` to `GET …/dashboards` (and is `grid` in
    `GET …/reports`), and renders as a grid. Nothing warns about it; the author adds
    `layout=linear` to keep it linear.
  - A kindless, tileless model-level tag in `dashboards/` (an unserved empty dashboard before) is
    now linear (`report-layout-implicit`).
- **Every existing URL and REST route answers.** Path routes cover every file already in the two
  directories; the old routes are deprecated aliases (§4, §9).
- **A `# drill { to=<name> }` names a root file.** A bare name that named a dashboard under
  `dashboards/` must be written as its path (`to="dashboards/overview"`); the load lint reports one
  that resolves to nothing.
- **A model with an artifact tag outside the two directories becomes a report.** Today the tag
  means nothing there. Nothing in the repo has one; `report-on-surface` states it whenever it
  removes something from the surface. It needs a release note.
- **Malloyyo** reads dashboards only from `dashboards/`, by bare names, with `kind=`. Reports
  elsewhere, path drill targets, and `layout=` work in Publisher and not in Malloyyo. Decided.
- **SDK consumers** keep `Dashboard`, `Notebook`, and the old client calls through the deprecation;
  they will not open a report outside the two directories by path until they upgrade.

## Delivery

Each step is shippable alone and leaves the system consistent.

1. **Server: one report model.** The report rule (§1), layout resolution (§2), path identity
   (§3), one report map and one lint (§8), the curation edges (§6),
   `GET …/reports` and `…/reports/{path}`, the deprecated aliases, and the `api-doc.yaml` rewrite.
2. **Console and SDK reading.** `Report`, `ReportPage`, one package-page section, path routes, the
   alias, drill by path, and the SPA fallback (§9, §10 minus create).
3. **Write path, builder, and create.** §7, the one editor with its layout switch, New report at
   the root, and drafts by path, with the `security-posture.md` rewrite in the same pull request.
4. **Docs, skills, and storefront.** §12, plus the release note.
5. **Later release: end the deprecations.** The `…/dashboards` and `…/notebooks` routes and the
   name alias go (delete `routes/deprecated.ts`, its controller and `legacy_report_views.ts`);
   `Dashboard` and `Notebook` SDK exports go once Credible has moved. `kind=`, the query-level
   `# artifact` and bare-name drill fallback are already gone: they existed only for this format.

## Test plan

- Port the folder-pinning tests rather than delete them: nested-path rejections become acceptance
  tests at any depth, the shadowing and name-collision tests become "two reports share a basename
  and both are served", and the retired findings' tests are replaced by the new ones'.
- **A classification test** that loads `dashboards-test`, `dashboards-curated`, `dashboards-lint`,
  `dashboards-convention`, `notebooks-lint`, `notebooks-malloyyo`, `notebooks-malloyyo-surface`,
  and `storefront` and asserts that every report's layout equals its kind today, and that manifests
  and drill resolution are unchanged apart from the deliberate changes it lists.
- A fixture package with reports at the root and two levels deep, a grid report in `notebooks/`
  and a linear one in `dashboards/`, two reports with the same basename, drills by path and by bare
  names that are and are not root files, drill from a grid report to a linear one, a tagged model that
  `index.malloy` imports, a tagged `index.malloy`, and a report under `public/`.
- Write-path tests: create at the root and nested; switch a report's layout; refuse replacing a
  model, an untagged file, a `.malloynb`, and `index.malloy`; refuse `public/`.
- SPA fallback: `public/dashboards/x.csv` is served statically again; `sales/report.csv.malloy`
  reaches the Console.
- Console e2e (Playwright): one Reports section; open, edit, switch layout, and drill a nested
  report; New report lands at the root; an old `/dashboards/<name>` link redirects.

## The query-level artifact form

`# artifact` on a `query:` is Malloyyo's original dashboard form: one rendered Malloy query, laid
out by its own `# dashboard` tag. Publisher already treats it as legacy (`docs/dashboards.md`: "do
not author new ones; the tiles form is the only authored one"); the builder cannot open it, and the
create flow never writes it. Its properties (`title`, `givens`, `autorun`, `kind`) are a strict
subset of the model-level tag's, and a tile can be a bare model-level named query, so every
query-level report has a mechanical rewrite:

```malloy
// before
# artifact { title="Overview" givens { REGION=f'US' } }
query: overview is orders -> { … }

// after
## artifact { layout=grid title="Overview" givens { REGION=f'US' } tiles=["overview"] } dashboard { columns=12 }
query: overview is orders -> { … }
```

**Decided: removed.** Only a model-level `## artifact` makes a report. A file whose only tag is a
`# artifact` on a query is an ordinary model, wherever it lives and whatever the surface is, so the
tag can never take a model off the curated surface. Nothing warns about it, because the tag is not
read; the rewrite above is the migration.

The alternative, keeping it and recognizing it everywhere, was rejected because any model anywhere
would become a report by carrying one tag on one query, which takes it out of the model list under a
curated surface and stops it contributing to that surface. Restricting it to `dashboards/` instead
would make that directory meaningful again. Dropping it removes the accidental-report case and
leaves one report grammar, which is what the reports unification asks for.
