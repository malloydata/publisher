# Dashboard and notebook builders: research, gaps, and plan

_Design and planning doc. The dashboard builder is the SDK component that opens a
`dashboards/*.malloy` file, lets a person arrange and filter it visually, and
writes the file back; the notebook builder (§7, planned) does the same for a
`.malloynb` file's linear cells. This document records what was learned building
the first,
measures it against the state of the art in dashboard builders, records the
decision to defer any change to the Malloy renderer or the Malloyyo format, and
lays out the steps for each remaining gap. It is the reference for scoping the
next releases; the grammar and runtime of dashboards themselves are in
[malloyyo-dashboards-design.md](malloyyo-dashboards-design.md) and
[dashboards.md](dashboards.md)._

Status as of 2026-09-15: the builder is wired into the Console (PR #1158,
branch `sdk/dashboard-document`) at `…/dashboards/<slug>/edit`, edits real
package dashboards, saves into the browser's document storage, and exports the
file. Every item in §5 that needs no API, renderer or format change has
shipped; the rest is deferred by decision, with the reason recorded beside it.

## 1. What the research established

The research phase (September 2026) compared the leading dashboard builders —
the drag-and-drop, filter-mapped, drill-through model that has become the
industry standard — with the Malloyyo dashboard format Publisher implements,
then probed a running Publisher to settle every question that could be settled
by measurement rather than reading. The conclusions that shape everything below:

**Build on the Malloyyo format.** It is already Publisher's dashboard format, it
is git-native and agent-authorable, and its filters are governed givens rather
than ad hoc widgets. The runtime half of a builder (discovery, manifest, control
row, grid, drill) existed; the authoring half existed nowhere, on either side.

**The file is the product.** Dashboards may be stored anywhere a storage provider
can put them, so whatever crosses the storage API has to be standard Malloy. The
builder therefore holds a JSON document only as editor state, never persists it,
and both reads and writes the `.malloy` file. That ruled out a JSON sidecar and
made a Malloy reader necessary.

**Read with the parser, write by splicing.** `Malloy.parse` is synchronous and
needs no connection or schema, so a file can be read into a document in the
browser. The writer patches only the byte ranges it owns and leaves every other
byte alone, so comments, formatting and unmodelled Malloy survive an edit. Its
safety gate is a semantic round-trip, not a byte one: splice, read back, refuse
the write if the result is not the document that was asked for.

**A dashboard is an editing projection, not a schema.** The document need not
represent the whole file. Anything it does not model survives a splice untouched;
a file is refused only when an edit would be unsafe. Every composite dashboard in
the repository is opened by the test suite and written back unchanged byte for
byte, so a real dashboard that stops opening fails CI rather than a user.

**Filters belong in the dashboard file.** Measured on 2026-09-15: a given
declared in the dashboard compiles, reaches the manifest with its control tags,
lands in each tile's `givenNames`, and binds. A local given does _not_ drive a
model-level `where:` of the same name: binding is per declaration, not per name.
So the convention became: the dashboard declares its own givens and binds them
per tile with `+ { where: field ~ $GIVEN }` refinements. The builder adds and
removes filters by editing that one file and never edits imports or model files.
A shared `givens.malloy` remains for controls the data app and notebooks share;
a dashboard can import and bind those, but the builder cannot change them. A
plain `date` or `number` given binds with `>=` (`~` does not compile against a
`date`).

**Two other measured facts the builder rests on.** The renderer's own
`sizingStrategy` says whether a result fills its box, and a chart is redrawn by
the renderer's own size observer only if its box actually shrinks, which a CSS
grid item with the default `min-width: auto` does not do. And the parser's symbol
tree misreads a refinement spelled `+ { limit: 5, where: … }` (it compiles) by
dropping the next view declaration, so the reader finds views, dimensions and
givens textually under their `source:` line rather than trusting the tree.

## 2. Where the builder stands

| Capability                 | Today                                                                                                                                                                       |
| -------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Open an existing dashboard | Any composite `## artifact { tiles=[…] }` file the reader can fully represent. Refused with a reason and a line otherwise.                                                  |
| Save                       | Splices the file; the round-trip gate refuses a write it cannot read back. Comments and unmodelled Malloy survive. A save that adds or removes a tile shows the diff first. |
| Layout                     | Drag to reorder (whole tile, `@dnd-kit/react`, keyboard included), drag the right edge for width, drop into the empty end of a row to move up.                              |
| Row structure              | `# break` is treated as positional: a move keeps the rows' shape; a drop into a gap is the one move that changes it.                                                        |
| Sizing aids                | Column guides and a width badge while dragging; width presets (full, ½, ⅓, ¼) on the tile's menu; a quick layout that sets every tile to one width.                         |
| Tile presentation          | Title and subtitle from the tile's own menu. Inherited tiles (declared on the model) are movable but not restyled.                                                          |
| Add or remove a tile       | Added from the package catalog (source → view, correct by construction); removed from the tile's menu. Both preview the file's diff before saving.                          |
| Filters                    | Declared in the dashboard and bound per tile from one place, the strip under the header, with a tiles-to-update mapping, per-tile comparison and a field picker.            |
| Filters from the model     | Bindable and removable from the dashboard; not editable, since the declaration is the model's.                                                                              |
| Clickable cells            | A `# drill` on any dimension the dashboard's own extension declares: destinations (this dashboard, the package's others) and the control a click sets, from the tile menu.  |
| Page settings              | Title, markdown description, grid width, run-as-controls-change (`autorun`) and starting values, from the edit bar.                                                         |
| Live view                  | Tiles run the document's bindings on the dashboard's own extension, so an edit is visible before it is saved; the control row follows the document.                         |
| Undo/redo                  | Whole-document history, one entry per gesture. Keyboard: ⌘Z / ⌘⇧Z, ⌘S, Esc, ←/→ to nudge width.                                                                             |
| Validation                 | A binding to a field the source does not have, or of a type the given cannot compare, is marked and blocks Apply when the catalog is known.                                 |
| Viewer                     | Every grouped value opens the rows behind it (`drill:` through the tile's view); each tile has "Explore from here" into the model explorer.                                 |
| Telemetry                  | `onEvent` on the viewer, builder and editor: opened, saved, refused, exported, rows shown, explored — each with outcome and duration. The Console logs them structured.     |
| Notebooks                  | A viewer only (`Notebook`): cells run and render, parameters surface, drills work. No builder yet; see §7.                                                                  |
| Where it lives             | The SDK's lazy `builder` entry; the Console's `dashboards/<slug>/edit` page; saves go to the browser's `DocumentStorage`, Export hands the file back for the package.       |

## 3. Gaps against the state of the art

Each gap is classed by where the fix lives. **Format** means the Malloyyo
grammar cannot express it; **Renderer** means `@malloydata/render`; **Runtime**
means the SDK or server, with no format change; **Platform** means storage,
access and delivery. "State of the art" is what the leading commercial builders
offer today.

| Area                     | State of the art                                                                             | Builder today                                                                                                | Class                     |
| ------------------------ | -------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ | ------------------------- |
| Placement                | Dense column grid; row, col, width, height per tile; free drag and drop                      | Flow grid; order and `colspan`; a drop into a row's empty end. No row index, no height.                      | Format (G1)               |
| Sizing aids              | Width presets, gridlines while dragging, quick layout                                        | All three; one grid width per page                                                                           | Parity                    |
| Tabs                     | Tabbed dashboards                                                                            | None                                                                                                         | Format (G2)               |
| Tile kinds               | Query, text, markdown, button, image, embedded content, filter tile, merged results          | Query tiles only; one markdown header per page                                                               | Format (G2)               |
| Query authoring          | Each tile owns an inline query edited in an explore UI                                       | A tile names an existing view, optionally refined by a filter                                                | Runtime, then Format (G3) |
| Add / remove a tile      | Yes                                                                                          | Yes, from the package catalog, with a diff preview                                                           | Parity                    |
| Filter declaration       | On the dashboard, from any field                                                             | On the dashboard, from a field of the tiles' source                                                          | Parity                    |
| Filter to tile binding   | Per-tile field mapping, including "do not filter"                                            | Per-tile field mapping, per-tile comparison, including untick                                                | Parity                    |
| Control types            | A dozen or so                                                                                | Six: search, select, multiselect, range slider, time range, date picker                                      | Runtime plus tags         |
| Required, curated values | Present                                                                                      | None                                                                                                         | Runtime plus tags         |
| Linked filters           | A parent narrows a child's options                                                           | None                                                                                                         | Runtime plus tags         |
| Cross-filtering          | Click any mark to filter every other tile                                                    | `# drill { to=self }` on a dimension the dashboard declares; authorable from a tile's menu                   | Runtime, then Format      |
| Drill                    | Overlay of the rows behind a value, further drill, explore from here                         | Dimension drill to a dashboard or self; rows behind any grouped value; explore from a tile; no measure drill | Runtime                   |
| Chart types              | Twenty or so, with a configuration escape hatch                                              | Twelve, from the view's own tag; the builder does not choose one                                             | Renderer                  |
| Vis options              | Series colours, reference and trend lines, value labels, axis ranges, conditional formatting | None                                                                                                         | Renderer                  |
| Dashboard settings       | Timezone, run on load, auto-refresh, download defaults, themes, mobile layout                | Title, description, starting values, `autorun`, grid width; all editable in the builder                      | Runtime                   |
| Editing model            | Explicit edit mode, explicit save, typically no undo                                         | Explicit save, undo/redo, diff before a structural save                                                      | Ahead                     |
| What editing does        | Rewrites a database record; a code form, where one exists, is converted                      | Splices the authored file; comments survive                                                                  | Ahead                     |
| Governance               | Access filters and user attributes through embedding                                         | Givens, row-level access and `#(authorize)` apply to every tile with no wiring                               | Ahead                     |
| Storage and access       | Database with folder ACLs                                                                    | A storage provider seam; browser storage today; the package-file provider needs a write API                  | Platform                  |
| Delivery                 | Schedules, alerts, PDF/CSV/PNG, signed embed                                                 | Export of the Malloy file                                                                                    | Platform                  |
| Observability            | Usage and performance telemetry                                                              | Per-operation events with outcome and duration; the host chooses the sink                                    | Parity                    |

The six structural gaps identified in the research, by number: **G1** no
positional layout or tile heights; **G2** no non-query tiles or tabs; **G3** tile
layout lives on the view, not the tile entry; **G4** filters declared in the model
(now closed by the convention in §1); **G5** no round-trippable document model
(now closed by the reader and the splice writer); **G6** two dialects (Publisher's
`# dashboard { columns }` and per-tile view tags against Malloyyo's
`dashboard_columns`).

## 4. The deferral: no renderer or format changes near term

Decided 2026-09-13 and reaffirmed since: **the builder makes no change to
`@malloydata/render` and no extension to the Malloyyo grammar in the near term.**

Why:

- Every grammar extension widens G6 unless the grammar has one home. The shared
  grammar package with Malloyyo is decided but not started, and the conversation
  with its maintainers is not happening yet. An extension shipped first in
  Publisher is a third dialect.
- Renderer asks (chart types, sorting, conditional formatting, reference lines,
  KPI comparison) are upstream work with its own cadence. Both projects tried and
  cut a JavaScript escape hatch for bespoke charts, so a second charting layer in
  the builder is not the answer either.
- The builder's value is already distinct without them: a GUI that opens the
  authored file, changes it, and gives it back with the comments intact. No
  commercial builder does that.

What that parks, explicitly (and what it does **not** block; see §5):

1. **Positional layout and tile heights (G1).** Reorder-plus-width is the whole
   layout vocabulary until the grammar says otherwise. The grid is Publisher's own
   CSS grid, so the extension stays a Publisher-side change when it comes.
2. **Tile kinds and tabs (G2).** Text tiles are also how narrative layouts fold
   into dashboards, which is why this is the first extension to propose.
3. **Renderer asks.** Including one concrete defect the builder exposes
   constantly: a root `# shape_map` is drawn at a fixed 500×350 (588px with its
   legend) that no tag reads, so it clips in any narrower tile.
4. **Query authoring for a tile.** Whether the add-tile flow reuses
   `ModelExplorer` or a lighter picker is undecided.

## 5. What could be done with no Malloy, renderer or Malloyyo change

Each is SDK or server work on the existing format. Everything not marked
deferred shipped on 2026-09-15.

1. **Console wiring and lazy loading.** The SDK's `builder` entry (`React.lazy`
   boundary so the parser, 440 KB gzipped, loads only when the builder opens; the
   `globalThis.process` shim its dependencies need) and the Console's
   `dashboards/<slug>/edit` page. One static `import { Malloy }` anywhere
   reachable from the entry chunk defeats this, so the reader keeps its
   `await import`.
2. **Saving through the storage seam.** `DocumentStorage` stores Malloy text
   under `<env>/<package>/dashboards/<slug>.malloy`. A package dashboard is a
   read-only origin: edit copies it into the provider (the Console's default is
   this browser), save writes the copy, a draft that was there when the editor
   opened is offered on the next visit, and Export hands the file back for the
   package. **Deferred, by decision: any server write path.** Writing back into
   the package, and with it a Publisher package-file provider, means a new REST
   endpoint, and API changes are out of scope. When it is taken up, the shape
   that was prototyped and rolled back is worth keeping: compile first as the
   file, write atomically, reload the package _in place_ (a location-based
   reinstall would overwrite the write), restore the previous file on a failed
   reload, refuse under `frozenConfig`, and track the origin (`contentHash` at
   copy time, re-fetch and compare on open, never auto-merge).
3. **Add and remove tiles.** The picker is built from the package catalog
   (`buildCatalog`), so every tile expression is correct by construction; the
   emitted view name is assigned once and never recomputed from position. A
   removed tile loses its declaration and `#` tags; a `//` comment above it
   stays, because the file cannot say whether it belonged to the tile, the row or
   the page — and the diff preview makes that the author's call.
4. **Dashboard settings.** Title, `##"` description, grid width, `autorun` and
   starting givens, from a popover off the edit bar, committed on close as one
   history entry.
5. **Drill authoring.** "Clickable cells" on a tile's menu: every dimension the
   tile's source declares in this file, with its destinations and the control a
   click sets. The builder writes the `# drill` tag, never the dimension — a
   dimension no view groups by is a dead drill, and views are the author's. This
   is also the format's only cross-filtering primitive (`to=self`).
6. **Filter control tags.** The operator UI (a binding's comparison, chosen in
   the filter window) shipped. **Deferred:** `required`, curated option lists,
   display modes and a "linked to" parent — the server reads a control's
   contract into the manifest's `Given` schema, so a new tag is an API change;
   they should also be proposed to Malloyyo so the vocabulary stays one.
7. **Rows behind a value, and explore from here.** Every grouped value in a
   composite tile is clickable: one with no `# drill` opens the rows behind it
   (Malloy's `drill:` through the tile's view, so the tile's own filters and the
   controls apply); one with a drill offers the rows beside its destinations.
   Each tile's heading opens the model explorer on the tile's source with its
   view as the query.
8. **Export.** The Malloy file, from the editor's bar. CSV and PNG per tile are
   not started.
9. **Sizing aids.** Width presets and quick layout shipped; run-on-load is
   `autorun`. **Deferred** with the format work in §7: auto-refresh and a
   timezone setting, which have no tag to write (§8).
10. **Upstream bug reports.** Skipped by decision; the reader works around the
    parser's symbol tree textually.

## 6. How it is built

The shape after the end-to-end quality pass (2026-09-15), recorded so the next
change lands in the right place.

**Modules** (`packages/sdk/src/components/DashboardBuilder`):

- `document.ts` — the editing projection: tiles, sources and their own
  dimensions, local givens with their control contract, drills, settings; one
  `tileKey` for the grid, the drag and the history.
- `malloyText.ts` — the one textual reading of a file the reader and writer
  share: declarations under a source, `given:` in both spellings, the artifact
  line, a tile expression's steps.
- `readDocument.ts` — parser symbols for structure, text for content; refuses
  with a reason and a line.
- `spliceDocument.ts` — `checkShape` (what comes and goes, and whether that is
  writable), seven planners over one context (order, settings, givens, drills,
  removed tiles, added tiles, tile presentation), then the round-trip gate.
- `controls.ts` — pure edits to the document for filters; `useFilterForm.ts`
  holds the filter window's state and every derivation, MUI-free.
- `layout.ts` — what a reorder does to the rows; `useTileResize.ts` and
  `useTileReorder.ts` — one gesture each, previewing and committing once;
  `TileFrame.tsx` — what is drawn around a tile; `FilterStrip.tsx`.
- `useDashboardEditor.ts` — whole-document history and save; `useDraft.ts` —
  edit a copy, commit on close.
- `DashboardBuilder.tsx` — the document, the selection and the dialogs.
- `DashboardEditor.tsx` — the Console's host: the package file, the browser's
  draft, the catalog from the file's imports, the live surface, export.
- `../Dashboard/useDashboardControls.ts` — the control row, shared by the viewer
  and the builder's live surface; `../Dashboard/telemetry.ts` — the events.

**Invariants** the suite pins: every dashboard in the repository opens and
writes back byte-identical when nothing changed; a splice that does not read
back as the document asked for is refused and the edit stays on screen; a
refusal names what it could not do; the writer never touches imports, model
files, or a `#` tag the document does not model.

**Tests**, by layer: unit specs on the reader, writer (including its refusals),
controls, catalog, preview, layout, literal encoding, the filter form and the
two gestures (geometry faked); integration specs on the builder (Testing
Library) and on `DashboardEditor` against a mocked client and real browser
storage; one Playwright spec on the Console's edit page. Lines covered:
builder 97%, writer 92%, reader 96%, editor host 93%.

**Telemetry**: the surfaces emit `DashboardEvent`s (`opened`, `open_refused`,
`saved`, `save_refused`, `exported`, `rows_shown`, `explored`), context-free;
the Console adds environment, package and dashboard and logs one structured
line per event under `[publisher.dashboard]`, `warn` on a refusal or a failed
query. A deployment that wants metrics forwards from there; nothing server-side
was added, by the no-API-change rule.

## 7. The notebook builder

A dashboard is a grid; a notebook is a line. The second surface the builder
should cover is the `.malloynb` notebook: a linear document of cells — markdown
prose, Malloy that defines and runs, results rendered as tables or charts — read
top to bottom, with the same governed parameters a dashboard has. Publisher
already renders it (`Notebook`, `NotebookCell`); what does not exist, on either
side, is a way to write one without a text editor.

**What a notebook is, for the builder.** The file is plain text: cells separated
by `>>>markdown` and `>>>malloy` lines, in order. A markdown cell is prose. A
Malloy cell is either definitions (imports, `source:`, `view:`, the `##` lines
that title the notebook and set `autorun`) or a `run:` whose result the reader
sees; a chart is a `run:` with a render tag (`# bar_chart`) on it, so "a chart
cell" is a query cell with a tag, not a third kind. The notebook surfaces the
givens its cells import, as a dashboard does, and every `run:` re-runs when a
control changes. Cells are independent: a later cell sees an earlier cell's
definitions through the file, not through a runtime state, which is what makes
the file the product here too.

**The same principles, and most of the same code.** Everything §1 established
carries over unchanged: the file is the product; the builder holds a document
only as editor state; write by splicing; refuse rather than guess; every notebook
in the repository opens and writes back unchanged. The reader is simpler than the
dashboard's — a cell split is a text operation, and the parser is needed only to
compile-check a Malloy cell — and so is the writer: a cell is a contiguous block,
so adding, removing, reordering or editing one replaces whole blocks and the
round-trip gate is the same read-back comparison. The shared core is what the
dashboard builder already has, generalised where it is document-specific:

- `useDashboardEditor` → a `useDocumentEditor<T>` (history, dirty, save with a
  refusal reason) parameterised by the document type and its writer.
- The writer's `Edit` / `applyEdits` primitives, `DiffDialog`, `BuilderToolbar`,
  `useDraft`, `DocumentStorage` (a `notebook` locator type already exists), the
  editor host's open / draft / resume / export flow, and the event seam
  (`notebook.*` events, the same shape and the same Console sink).
- `useDashboardControls` for the parameter row, `useDrill` for clickable cells,
  `ModelExplorer` (which `NotebookCell` already opens on a cell) for authoring a
  query, and the render tags the catalog already reads for choosing a chart.

**What the builder does.**

| Capability      | Plan                                                                                                                                                                                                                                                                                                    |
| --------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Cells           | Add a markdown or Malloy cell above or below any cell; remove; drag to reorder (one history entry per gesture, as tiles). A cell's identity is its position, so reorder is a block move.                                                                                                                |
| Markdown        | Edited in place, preview beside it, through the same markdown path the page description uses.                                                                                                                                                                                                           |
| Query cells     | A code editor with the compiler's own diagnostics — `compile_model` with scope `file`, which exists — and a Run that shows the result under the cell the way the viewer does. "Build with the explorer" opens `ModelExplorer` on the cell's source and writes the query it produces back into the cell. |
| Charts          | Pick a chart type on a query cell; the builder writes the render tag above the `run:` — a tier-1 tag edit, the same kind the dashboard builder makes on a view.                                                                                                                                         |
| Parameters      | The notebook's own givens, declared and bound the way §1's convention does for dashboards, with the same filter window; imported givens bindable, not editable.                                                                                                                                         |
| Settings        | `## title=`, `## autorun`, the `##"` description, from the edit bar.                                                                                                                                                                                                                                    |
| Live view       | Every `run:` cell runs against the document as it stands, under the current parameter values — the viewer's cell runner, fed the edited text.                                                                                                                                                           |
| Save and export | The same storage seam and the same read-only-origin rule: a package notebook is copied into the provider, saved there, exported back; a server write path waits on the same API decision as dashboards.                                                                                                 |
| Where it lives  | The SDK's `builder` entry (the parser is already paid for); the Console's `…/<name>.malloynb/edit` page beside the viewer.                                                                                                                                                                              |

**Against the state of the art.** Measured against the leading notebook
products — the Jupyter lineage and the hosted analytics notebooks built on it —
the builder above reaches parity on cells, markdown, charts as an attribute of a
query, parameters, drill-through and code-visible files under version control;
it is ahead on governance, since givens, row-level access and `#(authorize)`
apply to every cell with no wiring. It does not attempt: cells in other
languages (the notebook is Malloy by design), a reactive dependency graph between
cells (the file is the dependency; a later cell reads an earlier definition
because it compiles after it), or scheduled runs, publishing and alerts, which
are the same Platform class as for dashboards. The one format question is
whether a notebook may declare its own givens the way a dashboard now does;
measured behaviour says the viewer surfaces imported givens, and a cell that
declares one is the next thing to probe before the parameter row is built.

**Decisions this revises.** `.malloynb` stays the interchange format with the
VS Code extension and is authored, not extended: the builder writes only what
the format already has. The earlier reading that narrative layouts would arrive
as text tiles in a one-column dashboard (§4, G2) stands for _grids with prose_;
a linear analysis with code beside its results is a notebook, and the two
surfaces stay distinct.

**Steps.** (1) Generalise the editor core and the host flow out of the dashboard
builder (no behaviour change; the dashboard specs are the guard). (2) The notebook
reader and splice writer, with the repository round-trip suite over every
`.malloynb`. (3) Cells: add, remove, reorder, markdown in place. (4) The query
cell's editor with compile diagnostics and Run, then the explorer hand-off and
chart choice. (5) Parameters and settings. (6) The Console page, storage, export
and events. Each step ships behind the same gate as the dashboard builder: unit
specs on the reader and writer, an integration spec on the host, one browser
spec on the page.

## 8. Plan for each gap that needs an extension

Each step names the extension, who has to agree, and the Publisher work that
follows. The venue for every grammar item is the shared grammar package with
Malloyyo; the proposals go there together, since they interact.

### G1 · Positional layout and tile heights

_Extension:_ per-tile placement in `tiles=[…]`, as element properties the tag
parser already accepts: `tiles=[revenue_trend { row=2 col=1 width=6 height=2 }]`,
or a `row`/`height` pair over the existing `colspan`. _Who agrees:_ Malloyyo
maintainers, through the shared grammar package. _Renderer:_ none — the composite
grid is Publisher's own. _Steps:_ (1) write the proposal with the two spellings
and the flow-grid fallback for a file that omits placement; (2) land it in the
shared grammar package; (3) Publisher's `DashboardGrid` reads placement when
present and flows otherwise, so old files render unchanged; (4) the builder gains
drop-anywhere and a bottom edge, and the document model gains `row`, `col`,
`height` on a tile; (5) the writer emits placement on the tile entry (see G3), so
a repositioned tile is one edit on the `## artifact` line.

### G2 · Tile kinds and tabs

_Extension:_ `kind=` on a tile entry, with `text` first: `tiles=[intro { kind=text }, kpis]`.
The one decision is where a markdown body lives, since it does not fit a one-line
`##` tag: a doc-string on a placeholder view, or a sidecar file the entry names.
Tabs are a grouping over tiles and fit the same element-property extension.
_Who agrees:_ Malloyyo, same venue. _Renderer:_ none for text; a `button` or
`image` kind is Publisher UI. _Steps:_ (1) decide the body's home; (2) propose
`kind=text` and `tab=`; (3) Publisher renders text tiles through the same
markdown path as the page description; (4) the builder gets an "Add text" action
and tab management; (5) revise [choosing-a-surface.md](choosing-a-surface.md)
from three surfaces to two, since a narrative layout is then a dashboard.

### G3 · Layout on the tile entry, not the view

_Extension:_ read `colspan`, `break`, `label`, `subtitle`, `borderless` (and G1's
placement) off the tile entry as well as the view, entry winning. Additive and
backward compatible. _Who agrees:_ Malloyyo. _Steps:_ (1) propose alongside G1;
(2) Publisher's manifest merges entry over view; (3) the builder writes layout on
the entry for tiles whose view it does not own, which makes inherited tiles
resizable and lets one view sit at two widths on two pages.

### G6 · One grammar

_Extension:_ the shared grammar package itself, carrying G1–G3 and the two
spellings of grid width. _Steps:_ (1) start the package with the current common
subset; (2) add Publisher's per-tile view tags and `# dashboard { columns }` as
proposals; (3) Publisher's enumeration lint keeps unread properties visible in
both directions until it lands.

### Control tags and settings that need the API

The manifest's `Given` schema carries a control's contract to the client, so
`required`, curated values, display modes, linked filters, auto-refresh and a
timezone setting each add a field to it. _Who agrees:_ Publisher's API owners;
Malloyyo for the tag names. _Steps:_ (1) propose the tags to Malloyyo; (2) add
them to the schema and the server's reader; (3) the filter window and the
settings popover gain the controls, which the writer already knows how to emit.

### The server write path

A `PUT` for a model file with compile-first, atomic write, in-place reload,
restore-on-failure and `frozenConfig` refusal, plus a package-file
`DocumentStorage` provider and origin tracking. _Who agrees:_ Publisher's API
owners. Prototyped and rolled back on 2026-09-15; the shape is in §5.2.

### Renderer asks (`@malloydata/render`)

_Extension:_ chart types (pie, donut, area, funnel, waterfall, boxplot, gauge),
table sort by header, conditional formatting, reference and trend lines, value
labels, axis ranges, KPI comparison with spark charts on `big_value`, and a
`shape_map` that reads its size from its box. _Who agrees:_ the renderer's
maintainers, as upstream PRs. _Publisher side:_ the builder chooses a chart type
by writing the view's render tag, which is a tier-1 tag edit already supported;
vis options are the same. The map sizing fix removes the 588px floor the example
works around today.

### Malloy asks (`@malloydata/malloy`)

Nothing blocks on the language. Two parser items would remove workarounds: a
symbol tree that survives every refinement spelling, and problems reported from
`Malloy.parse` so the reader can refuse a broken file with the compiler's own
message. A tag-and-view printer would make the writer simpler still, but the
splice approach no longer needs one.

## 9. Sequence

1. Land PR #1158; keep the round-trip suite green over every dashboard in the
   repository. _Done except the merge._
2. Open the grammar conversation with Malloyyo with G1, G2, G3 and G6 as one
   proposal, with the control-tag names alongside.
3. When API changes are back in scope: the server write path and package-file
   provider (§8), then the control tags and settings that ride on the `Given`
   schema.
4. The notebook builder (§7): the shared editor core first, then cells, then
   the query cell's editor. It needs no API, renderer or format change, so it
   can proceed in parallel with 2 and 3.
5. Renderer and parser issues upstream, when there is appetite to file them.
6. When the grammar lands: placement, text tiles, tabs, entry-level layout (§8).
