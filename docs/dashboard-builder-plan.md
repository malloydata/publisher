# Dashboard and notebook builders: research, gaps, and plan

_Design and planning doc. The dashboard builder is the SDK component that opens a
`dashboards/*.malloy` file, lets a person arrange and filter it visually, and
writes the file back; the notebook (§7, planned) is the same idea for a linear
document, on a Malloyyo-style format rather than `.malloynb`. This document
records what was learned building the first,
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
grid item with the default `min-width: auto` does not do. And every edit is
LOCATED by Malloy's own parser, through `malloyTree`: the reader and the writer
take spans from one parse tree, so they cannot disagree about where a
declaration or a `where:` clause begins and ends.

An earlier version of this section claimed the symbol tree misreads a refinement
spelled `+ { limit: 5, where: … }` "(it compiles)", and built a layer of text
scanners around that. The parenthetical was wrong: that spelling is a syntax
error — a comma is not legal after `limit:` — and the builder was emitting it.
With the valid spelling the tree reports every declaration. A file with any
syntax error is not edited through the tree at all, because ANTLR recovers from
one by inventing structure it does not report.

## 2. Where the builder stands

| Capability                 | Today                                                                                                                                                                                                                                                                                                                                        |
| -------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Open an existing dashboard | Any composite `## artifact { tiles=[…] }` file the reader can fully represent. Refused with a reason and a line otherwise.                                                                                                                                                                                                                   |
| Save                       | Splices the file; the round-trip gate refuses a write it cannot read back. Comments and unmodelled Malloy survive. A save that adds or removes a tile shows the diff first. Into the package when the server takes writes (compile-first, atomic, reloaded in place, refused if the file changed since opening); into the browser otherwise. |
| Create                     | "Add dashboard" on the package page: a model, a source, the first tile's view and a title; the file the builder would write, written into the package and opened in the builder.                                                                                                                                                             |
| Layout                     | Drag to reorder (whole tile, `@dnd-kit/react`, keyboard included), drag the right edge for width, drop into the empty end of a row to move up.                                                                                                                                                                                               |
| Row structure              | `# break` is treated as positional: a move keeps the rows' shape; a drop into a gap is the one move that changes it.                                                                                                                                                                                                                         |
| Sizing aids                | Column guides and a width badge while dragging; width presets (full, ½, ⅓, ¼) on the tile's menu, and the arrow keys to nudge the selected tile's width.                                                                                                                                                                                     |
| Tile presentation          | Title and subtitle from the tile's own menu. Inherited tiles (declared on the model) are movable but not restyled.                                                                                                                                                                                                                           |
| Add or remove a tile       | Added from the package catalog (source → view, correct by construction); removed from the tile's menu. Both preview the file's diff before saving.                                                                                                                                                                                           |
| Filters                    | Declared in the dashboard and bound per tile from one place, the strip under the header, with a tiles-to-update mapping, per-tile comparison and a field picker.                                                                                                                                                                             |
| Filters from the model     | Bindable and removable from the dashboard; not editable, since the declaration is the model's.                                                                                                                                                                                                                                               |
| Clickable cells            | A `# drill` on any dimension the dashboard's own extension declares: destinations (this dashboard, the package's others) and the control a click sets, from the tile menu.                                                                                                                                                                   |
| Edit bar                   | One bar in both modes, at the same place and height: the state on the left (nothing while reading, an "Editing" chip while editing), the switch on the right (Edit becomes Done). Between them, grouped by what they do: add a tile and page settings; undo, redo and save; then the way out.                                                |
| Page settings              | Title, markdown description, grid width, run-as-controls-change (`autorun`) and starting values, from the edit bar.                                                                                                                                                                                                                          |
| Live view                  | Tiles run the document's bindings on the dashboard's own extension, so an edit is visible before it is saved; the control row follows the document.                                                                                                                                                                                          |
| Undo/redo                  | Whole-document history, one entry per gesture. Keyboard: ⌘Z / ⌘⇧Z, ⌘S, Esc, ←/→ to nudge width.                                                                                                                                                                                                                                              |
| Validation                 | A binding to a field the source does not have, or of a type the given cannot compare, is marked and blocks Apply when the catalog is known.                                                                                                                                                                                                  |
| Viewer                     | A grouped value with no `# drill` opens the rows behind it (`drill:` through the tile's view); a drill behaves as the tag says; each tile has "Explore from here" into the model explorer.                                                                                                                                                   |
| Telemetry                  | `onEvent` on the viewer, builder and editor: opened, saved, refused, rows shown, explored — each with outcome and duration. The Console logs them structured.                                                                                                                                                                                |
| Notebooks                  | `.malloynb` is deprecated: viewed read-only (`Notebook`), never written, and gone from the bundled examples. The authored notebook is a Malloyyo-style format that does not exist yet; see §7.                                                                                                                                               |
| Where it lives             | The SDK's lazy `builder` entry; the Console's `dashboards/<slug>/edit` page and package page (Add dashboard, Drafts); the write path `PUT …/models/dashboards/<slug>.malloy`.                                                                                                                                                                |

## 3. Gaps against the state of the art

Each gap is classed by where the fix lives. **Format** means the Malloyyo
grammar cannot express it; **Renderer** means `@malloydata/render`; **Runtime**
means the SDK or server, with no format change; **Platform** means storage,
access and delivery. "State of the art" is what the leading commercial builders
offer today.

Rows reaching parity leave the table: a gap list that carries what is no longer
missing stops being a list of what to do next. Verified and removed 2026-09-15,
each against the code and the test that holds it: **sizing aids** (width presets
on the tile's menu, the arrow-key nudge, column guides while dragging or
resizing), **add / remove a tile** (from the package catalog, both through the
diff), **filter declaration** (a control declared on the dashboard, written to
the file), **filter-to-tile binding** (per-tile field and comparison, including
untick), and **observability** (the `DashboardEvent` union and the Console's
structured sink). The one thing the state of the art still has here is a quick
layout that rewrites every tile at once, which was built and dropped by decision
(§5).

| Area                     | State of the art                                                                             | Builder today                                                                                                | Class                     |
| ------------------------ | -------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ | ------------------------- |
| Placement                | Dense column grid; row, col, width, height per tile; free drag and drop                      | Flow grid; order and `colspan`; a drop into a row's empty end. No row index, no height.                      | Format (G1)               |
| Tabs                     | Tabbed dashboards                                                                            | None                                                                                                         | Format (G2)               |
| Tile kinds               | Query, text, markdown, button, image, embedded content, filter tile, merged results          | Query tiles only; one markdown header per page                                                               | Format (G2)               |
| Query authoring          | Each tile owns an inline query edited in an explore UI                                       | A tile names an existing view, optionally refined by a filter                                                | Runtime, then Format (G3) |
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
| Governance               | Access filters and user attributes through embedding                                         | Givens, row-level access and `#(access_filter)` apply to every tile with no wiring                               | Ahead                     |
| Storage and access       | Database with folder ACLs                                                                    | A storage provider seam; browser storage today; the package-file provider needs a write API                  | Platform                  |
| Delivery                 | Schedules, alerts, PDF/CSV/PNG, signed embed                                                 | The file itself, saved into the package                                                                      | Platform                  |

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

Each is SDK or server work on the existing format. All ten were walked against
the code and its tests on 2026-09-15: eight shipped, two were built and then
dropped by decision (export, quick layout), and what stays deferred is marked
as such and says why. The evidence for each is named below rather than left to
the reader to find.

1. **Console wiring and lazy loading.** The SDK's `builder` entry (`React.lazy`
   boundary so the parser, 440 KB gzipped, loads only when the builder opens; the
   `globalThis.process` shim its dependencies need) and the Console's
   `dashboards/<slug>/edit` page. One static `import { Malloy }` anywhere
   reachable from the entry chunk defeats this, so the reader keeps its
   `await import`. Checked against the built bundle, not the intent: the chunk
   `index.html` loads carries no parser symbol, and `readDocument.spec` asserts
   the reader reaches the parser through `await import` and nothing else. The
   dashboard page now warms that chunk while idle, so the first Edit is a
   re-render rather than a download.
2. **Saving through the storage seam.** `DocumentStorage` stores Malloy text
   under `<env>/<package>/dashboards/<slug>.malloy`. A package dashboard is a
   read-only origin: edit copies it into the provider (the Console's default is
   this browser), save writes the copy, a draft that was there when the editor
   opened is offered on the next visit. **The server write path shipped 2026-09-15**, lifting the earlier
   deferral by decision: `PUT …/models/dashboards/<slug>.malloy` compiles the
   text as the file, writes atomically under the package lock, reloads the
   package _in place_, restores the previous text on a failed reload, refuses
   under `frozenConfig`, and refuses a file that changed since it was opened
   (`expectedHash`, SHA-256 of the opened text) rather than merging. The check
   the write, the reload and the restore happen under one hold of the package
   lock, so two saves racing
   on one file cannot both pass it; omitting `expectedHash` means create, and a
   file that is already there is refused the same way rather than overwritten.
   A create answers 201, a replacement 200. When the server takes writes the
   builder's Save goes there and a browser draft of the same file is
   superseded; otherwise the browser flow above stands.
3. **Add and remove tiles.** Both through the diff, each with its own test. The
   picker is built from the package catalog
   (`buildCatalog`), so every tile expression is correct by construction; the
   emitted view name is assigned once and never recomputed from position. A
   removed tile loses its declaration and `#` tags; a `//` comment above it
   stays, because the file cannot say whether it belonged to the tile, the row or
   the page — and the diff preview makes that the author's call.
4. **Dashboard settings.** Title, `##"` description, grid width and `autorun`,
   from a popover off the edit bar, committed on close as one history entry.
   A control's starting value is edited where the control is, in the filter
   window, rather than in this popover — the same place its binding and
   comparison are set, so one control is one dialog.
5. **Drill authoring.** "Clickable cells" on a tile's menu (`DrillDialog`): every dimension the
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
   controls apply); one with a drill does what its tag says, as before.
   Each tile's heading opens the model explorer on the tile's source with its
   view as the query.
8. **Export.** Dropped 2026-09-15: the file goes into the package, so handing
   a copy back had no audience left. CSV and PNG per tile are not started, and
   a file export can come back with them if it is asked for.
9. **Sizing aids.** Width presets, the arrow-key nudge and the column guides
   shipped; run-on-load is `autorun`. The one-click "set every tile to one
   width" was built and then **dropped 2026-09-15** (Kyle): with four presets a
   click away on the tile that needs them, a bar button that rewrote every tile
   at once earned neither its space nor its undo entry. **Deferred** with the
   format work in §7: auto-refresh and a timezone setting, which have no tag to
   write (§8).
10. **Upstream bug reports.** Skipped by decision; the reader works around the
    parser's symbol tree textually.

## 6. How it is built

The shape after the end-to-end quality pass (2026-09-15), recorded so the next
change lands in the right place.

**Modules** (`packages/sdk/src/components/DashboardBuilder`):

- `document.ts` — the editing projection: tiles, sources and their own
  dimensions, local givens with their control contract, drills, settings; one
  `tileKey` for the grid, the drag and the history.
- `malloyTree.ts` — the one reading of a file the reader and writer share, taken
  from Malloy's own parse tree and token stream: declarations under a source,
  `given:` in both spellings, the artifact line, a tile expression's steps, and
  the comment index every guard asks before it deletes a range. It refuses
  rather than guessing when the tree is not the shape it was written against.
- `readDocument.ts` — the tree for structure, spans for content; refuses with a
  reason and a line.
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
  draft, the catalog (built from the package's published models, limited to
  what the file imports), the live surface, export.
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
`saved`, `save_refused`, `rows_shown`, `explored`), context-free;
the Console adds environment, package and dashboard and logs one structured
line per event under `[publisher.dashboard]`, `warn` on a refusal or a failed
query. A deployment that wants metrics forwards from there; nothing server-side
was added, by the no-API-change rule.

## 7. The notebook: a Malloyyo format, not `.malloynb`

A dashboard is a grid; a notebook is a line — prose and queries in author order,
read top to bottom, results rendered as tables or charts, under the same
governed parameters. Publisher renders one today from `.malloynb`, the Malloy VS
Code extension's format (`>>>markdown` / `>>>malloy` cells in plain text). The
decision of 2026-09-13, reaffirmed 2026-09-15, is **not** to build on it:

- **`.malloynb` is deprecated (2026-09-15).** On ice since 2026-09-13, it is now
  on the way out: the `Notebook` viewer keeps working for packages that have
  one, nothing new is built on it, and no surface writes one. The first visible
  step is that the bundled examples no longer ship a notebook — `storefront` and
  `governed-analytics` each had one, and what they demonstrated (a narrative
  over a model, controls from givens) a dashboard demonstrates in the format
  that is not going away. The viewer leaves when the authored format below can
  carry those readers, and not before; removing it earlier would strand every
  package that has a `.malloynb` in it today. Its authoring side upstream has
  had maintenance commits only since April 2026, Malloyyo has no notebook format
  at all, and the Workbook editor's private JSON was retired (PR #1146) rather
  than become a third one.
- **The authored notebook is a Malloyyo-style format**: a Malloy file, in the
  family the dashboard already belongs to, whose cells are the file's own
  statements and annotations in order. It is git-native and agent-authorable,
  compiles as one model so every cell shares the file's definitions, is visible
  to the MCP surface like any model, and — because a cell is a contiguous block
  of Malloy — is read and written by the same splice discipline as a dashboard.

**The shape.** A `notebooks/<slug>.malloy` file tagged as a notebook artifact.
Its cells, in file order:

- **Markdown cells**: a markdown block written as an annotation. This is the one
  new grammar element and it is the same one a dashboard's text tile needs (G2),
  so it is proposed once and serves both surfaces. Two spellings to decide in
  the grammar venue: a standalone block (`##" …` lines already carry model-level
  markdown; a block form that can sit _between_ statements rather than only at
  the top), or prose attached to the statement below it as its doc string, with
  a standalone form only for prose that leads nothing.
- **Query cells**: a `run:` statement. A chart is a render tag on it
  (`# bar_chart`), not a third kind of cell; `# label` titles it.
- **Definition cells**: `import`, `source:`, `view:`, `given:` — shown as code
  or folded, at the author's choice, and read by every cell below them.
- **Parameters**: givens declared in the file and bound with `where: … $GIVEN`,
  exactly the dashboard convention of §1, so the reader's parameter row and a
  builder's filter window carry over unchanged.
- **Settings**: `title`, `autorun`, starting values on the artifact tag, as the
  dashboard has them.

What this shares with a dashboard is deliberate: one grammar extension (the
markdown block), one convention for parameters, one reader/writer discipline,
one storage seam, one event seam. What differs is the layout engine — a
notebook has none; file order is the layout, so reordering a cell moves a
block — and the reading mode, which is why the two remain distinct surfaces
rather than a notebook being a one-column dashboard.

**Against the state of the art.** Measured against the leading notebook
products — the Jupyter lineage and the hosted analytics notebooks built on it —
this reaches parity on cells, markdown, charts as an attribute of a query,
parameters, drill-through and code-visible files under version control, and is
ahead on governance (givens, row-level access and `#(access_filter)` apply to every
cell with no wiring) and on the file being the notebook rather than an export
of one. It does not attempt cells in other languages, a reactive dependency
graph (the file is the dependency: a later cell reads an earlier definition
because it compiles after it), or scheduling and publishing, which are the same
Platform class as for dashboards.

**The builder that follows.** Once the format is agreed, the notebook builder
is the dashboard builder's core with a linear surface: `useDashboardEditor`
generalised to a `useDocumentEditor<T>` (history, dirty, save with a refusal
reason); the writer's `Edit`/`applyEdits` primitives, `DiffDialog`,
`BuilderToolbar`, `useDraft`, the host's open / draft / resume / export flow,
`DocumentStorage` (a `notebook` locator type already exists) and the event seam
(`notebook.*`, the same Console sink); `useDashboardControls` for parameters,
`useDrill` for clickable cells, `ModelExplorer` for authoring a query cell and
writing it back, `compile_model` with scope `file` for the cell editor's
diagnostics, and the render tags the catalog already reads for choosing a
chart. Cells are added above or below any cell, removed, reordered by drag
(one history entry per gesture), markdown edited in place with a preview, and
every `run:` runs against the document as it stands under the current
parameter values. Every notebook in the repository opens and writes back
unchanged, as the dashboard suite already proves for dashboards.

**Decisions this records.** `.malloynb` read-only, never extended (2026-09-13).
The narrative surface is delivered by the Malloyyo family — the markdown block
of G2 — and not by a `.malloynb` editor; a grid with prose is a dashboard with
text tiles, a linear document is a notebook, and both stand on the same block.
[choosing-a-surface.md](choosing-a-surface.md) is revised when the format
lands, so that "notebook" there means this one.

**Steps.** (1) Propose the markdown block and the notebook artifact kind with
G1–G3 and G6 as one grammar package proposal (§8). (2) Generalise the editor
core and the host flow out of the dashboard builder (no behaviour change; the
dashboard specs are the guard) — the one step that needs no agreement and can
start now. (3) When the grammar lands: the notebook reader and splice writer,
with the repository round-trip suite over every notebook; the Console's
notebook page rendering the new format beside the `.malloynb` viewer. (4)
Cells: add, remove, reorder, markdown in place. (5) The query cell's editor
with diagnostics and Run, the explorer hand-off, chart choice. (6) Parameters,
settings, storage, export, events. Each step ships behind the dashboard
builder's gate: unit specs on the reader and writer, an integration spec on the
host, one browser spec on the page.

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

### G2 · Markdown blocks, tile kinds, tabs — and the notebook artifact

_Extension:_ a markdown block as an annotation that can sit between statements
(the one element §7's notebook and a dashboard's text tile both need), `kind=`
on a tile entry with `text` first (`tiles=[intro { kind=text }, kpis]`), a
notebook artifact kind whose cells are the file's statements in order, and tabs
as a grouping over tiles. The one decision is the markdown block's spelling: a
standalone block form of the existing `##"` doc string, or prose attached to the
statement below it with a standalone form for prose that leads nothing. _Who
agrees:_ Malloyyo, same venue. _Renderer:_ none for text; a `button` or `image`
kind is Publisher UI. _Steps:_ (1) decide the block's spelling; (2) propose the
block, `kind=text`, the notebook artifact and `tab=` together; (3) Publisher
renders markdown blocks through the same path as the page description, in a
grid as a text tile and in a notebook as a cell; (4) the dashboard builder gets
an "Add text" action and tab management, and the notebook builder of §7 follows;
(5) revise [choosing-a-surface.md](choosing-a-surface.md) so "notebook" means
the Malloyyo-style one and `.malloynb` is the import.

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

Shipped 2026-09-15 (§5.2), scoped to dashboard files. What remains here is
its generalisation, if ever wanted: a `DocumentStorage` provider over the
endpoint for hosts other than the Console, and writes to other kinds of file,
which would each need their own compile-first rule and a look at the security
posture.

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
2. Open the grammar conversation with Malloyyo with G1, G2 (markdown blocks,
   text tiles, the notebook artifact), G3 and G6 as one proposal, with the
   control-tag names alongside.
3. The control tags and settings that ride on the `Given` schema (§8), now
   that API changes are in scope again — the write path (§5.2) has landed.
4. The notebook (§7): the shared editor core can be generalised now with no
   agreement needed; the format goes into the grammar proposal of step 2; the
   builder follows the grammar.
5. Renderer and parser issues upstream, when there is appetite to file them.
6. When the grammar lands: placement, text tiles, tabs, entry-level layout (§8).
