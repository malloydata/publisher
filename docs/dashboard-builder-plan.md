# Dashboard builder: research, gaps, and plan

_Design and planning doc. The dashboard builder is the SDK component that opens a
`dashboards/*.malloy` file, lets a person arrange and filter it visually, and
writes the file back. This document records what was learned building the first
version, measures it against Looker's builder, records the decision to defer any
change to the Malloy renderer or the Malloyyo format, and lays out the steps for
each remaining gap. It is the reference for scoping the next releases; the
grammar and runtime of dashboards themselves are in
[malloyyo-dashboards-design.md](malloyyo-dashboards-design.md) and
[dashboards.md](dashboards.md)._

Status as of September 2026: the builder edits real package dashboards on branch
`sdk/dashboard-document` (PR #1158), with the follow-up refactor in PR #1165. It
is not yet wired into the Console.

## 1. What the research established

The research phase (September 2026) compared Looker's dashboard builder with the
Malloyyo dashboard format Publisher implements, then probed a running Publisher
to settle every question that could be settled by measurement rather than
reading. The conclusions that shape everything below:

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
the repository is opened by the test suite, so a real dashboard that stops
opening fails CI rather than a user.

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
dropping the next view declaration, so the reader finds views textually under
their `source:` line rather than trusting the tree.

## 2. Where the builder stands

| Capability                 | Today                                                                                                                                             |
| -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| Open an existing dashboard | Any composite `## artifact { tiles=[…] }` file the reader can fully represent. Refused with a reason and a line otherwise.                        |
| Save                       | Splices the file; the round-trip gate refuses a write it cannot read back. Comments and unmodelled Malloy survive.                                |
| Layout                     | Drag to reorder (whole tile, `@dnd-kit/react`, keyboard included), drag the right edge for width, drop into the empty end of a row to move up.    |
| Row structure              | `# break` is treated as positional: a move keeps the rows' shape; a drop into a gap is the one move that changes it.                              |
| Tile presentation          | Title, subtitle, row break and card from the tile's own menu. Inherited tiles (declared on the model) are movable but not restyled.               |
| Filters                    | Declared in the dashboard and bound per tile from one place, the strip under the header, with Looker's tiles-to-update mapping and a field picker |
| Filters from the model     | Bindable and removable from the dashboard; not editable, since the declaration is the model's.                                                    |
| Live view                  | Tiles run the document's bindings on the model source, so an edit is visible before it is saved; the control row follows the document.            |
| Undo/redo                  | Whole-document history, one entry per gesture. Keyboard: ⌘Z / ⌘⇧Z, ⌘S, Esc, ←/→ to nudge width.                                                   |
| Validation                 | A binding to a field the source does not have, or of a type the given cannot compare, is marked and blocks Apply when the catalog is known.       |
| Add or remove a tile       | **Not yet.** The writer refuses it; see §5.                                                                                                       |
| Where it lives             | A local harness. Console wiring, the storage provider and the lazy-loaded entry point are the next release; see §5.                               |

## 3. Gaps against Looker

Each gap is classed by where the fix lives. **Format** means the Malloyyo
grammar cannot express it; **Renderer** means `@malloydata/render`; **Runtime**
means the SDK or server, with no format change; **Platform** means storage,
access and delivery.

| Area                     | Looker                                                                                       | Builder today                                                                            | Class                     |
| ------------------------ | -------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- | ------------------------- |
| Placement                | 24-column grid; row, col, width, height per tile; free drag and drop                         | Flow grid; order and `colspan`; a drop into a row's empty end. No row index, no height.  | Format (G1)               |
| Sizing aids              | Width presets, gridlines while dragging, quick layout                                        | Gridlines while dragging; width badge; one grid width per page                           | Runtime                   |
| Tabs                     | Tabbed dashboards                                                                            | None                                                                                     | Format (G2)               |
| Tile kinds               | Query, text, markdown, button, image, extension, filter tile, merged results                 | Query tiles only; one markdown header per page                                           | Format (G2)               |
| Query authoring          | Each tile owns an inline query edited in the explore UI                                      | A tile names an existing view, optionally refined by a filter                            | Runtime, then Format (G3) |
| Add / remove a tile      | Yes                                                                                          | Refused by the writer: a view declaration's comment block has no recorded owner          | Runtime (splice tier 3)   |
| Filter declaration       | On the dashboard, from any field                                                             | On the dashboard, from a field of the tiles' source                                      | Parity                    |
| Filter to tile binding   | Per-tile field mapping, including "do not filter"                                            | Per-tile field mapping, per-tile comparison, including untick                            | Parity                    |
| Control types            | Twelve                                                                                       | Six: search, select, multiselect, range slider, time range, date picker                  | Runtime plus tags         |
| Required, curated values | Present                                                                                      | None                                                                                     | Runtime plus tags         |
| Linked filters           | A parent narrows a child's options                                                           | None                                                                                     | Runtime plus tags         |
| Cross-filtering          | Click any mark to filter every other tile                                                    | `# drill { to=self }` on a dimension; not authorable in the builder yet                  | Runtime, then Format      |
| Drill                    | Overlay of the rows behind a value, further drill, explore from here                         | Dimension drill to a dashboard or self; no overlay, no measure drill                     | Runtime                   |
| Chart types              | About twenty, plus Highcharts config                                                         | Twelve, from the view's own tag; the builder does not choose one                         | Renderer                  |
| Vis options              | Series colours, reference and trend lines, value labels, axis ranges, conditional formatting | None                                                                                     | Renderer                  |
| Dashboard settings       | Timezone, run on load, auto-refresh, download defaults, themes, mobile layout                | Title, description, starting values, `autorun`, grid width; none editable in the builder | Runtime                   |
| Editing model            | Explicit edit mode, explicit save, no undo                                                   | Explicit save, undo/redo                                                                 | Ahead                     |
| What editing does        | User dashboards in a database; LookML converted both ways                                    | Splices the authored file; comments survive                                              | Ahead                     |
| Governance               | Access filters and user attributes through embed                                             | Givens, row-level access and `#(authorize)` apply to every tile with no wiring           | Ahead                     |
| Storage and access       | Database with folder ACLs                                                                    | A storage provider seam; the Publisher package-file provider is not written              | Platform                  |
| Delivery                 | Schedules, alerts, PDF/CSV/PNG, signed embed                                                 | None                                                                                     | Platform                  |

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
  authored file, changes it, and gives it back with the comments intact. Looker
  has no equivalent.

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

## 5. What can be done now, with no Malloy, renderer or Malloyyo change

Ordered by what unblocks what. Each is SDK or server work on the existing format.

1. **Console wiring and lazy loading.** A `React.lazy` boundary so the parser
   (440 KB gzipped) loads only when the builder opens; an "Edit" entry from the
   dashboard page; the `globalThis.process` shim the parser's dependencies need in
   the browser. One static `import { Malloy }` anywhere reachable from the entry
   chunk defeats this, so the reader keeps its `await import`.
2. **The storage provider and the write path.** The `DocumentStorage` seam
   already stores Malloy text with a locator of `<env>/<package>/dashboards/<slug>.malloy`.
   A package dashboard is a read-only origin: edit copies it into the provider,
   save writes the copy, export gives it back. A Publisher package-file provider
   is where the server write path, the `publisher_data` copy and the gateway
   posture belong, so they land there rather than in the builder. Origin tracking
   (`contentHash` at copy time, re-fetch and compare on open, never auto-merge)
   is part of this step.
3. **Add and remove tiles (splice tier 3).** The writer refuses these today
   because a view declaration's comment block has no recorded owner. The plan is
   the picker plus a diff preview before save, so a possible comment move is one
   the author approves rather than a silent loss. The picker is built from the
   package catalog (`buildCatalog`), which makes every tile expression correct by
   construction; the emitted view name is assigned once and never recomputed from
   position. Removing a tile deletes its declaration and its `#` tags and leaves
   `//` comments in place.
4. **Dashboard settings in the builder.** Title, `##"` description, grid width,
   `autorun` and starting givens are all tier-1 edits on the `## artifact` line
   and the doc-comment block; the reader models them already.
5. **Drill authoring.** `# drill { to=[…] given=… }` on a dimension declared in the
   dashboard's own extension works and is linted today; the builder can author it
   from a tile's menu. That is also the format's only cross-filtering primitive
   (`to=self`), so click-to-filter on a dashboard's own dimensions arrives here,
   without a grammar change.
6. **Filter control tags Publisher already owns.** `required`, curated option
   lists, display modes and a "linked to" parent are additions to the control
   contract Publisher reads off a `given:` declaration. Unread tags are ignored by
   both projects, so shipping them in Publisher is safe; they should still be
   proposed to Malloyyo so the vocabulary stays one. Only the operator UI for
   free-text filters needs no new tag at all.
7. **Drill overlay and explore from here.** The rows behind a value are one
   query away, and `ModelExplorer` exists in the SDK. Runtime only.
8. **Export and download.** Copy or download the Malloy; later CSV and PNG per
   tile. The Workbook editor was cut because its export never shipped, so export
   ships with the first Console release, not after it.
9. **Sizing aids and settings that are pure UI.** Width presets, a quick-layout
   that sets every tile to one width, run-on-load, auto-refresh, a timezone
   setting on the control row.
10. **Upstream bug reports that cost nothing to file.** The parser's symbol tree
    misreading `+ { limit: 5, where: … }`; `Malloy.parse` exposing no problems for
    a file that does not parse; the map's fixed size.

## 6. Plan for each gap that needs an extension

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

## 7. Sequence

1. Land PR #1158 and #1165; keep the round-trip suite green over every dashboard
   in the repository.
2. Console wiring, lazy entry, storage provider, export (§5 items 1, 2, 8).
3. Tier-3 splice with diff preview: add and remove tiles from the catalog (§5.3),
   then dashboard settings and drill authoring (§5.4, §5.5).
4. Open the grammar conversation with Malloyyo with G1, G2, G3 and G6 as one
   proposal; file the renderer and parser issues.
5. Filter controls and runtime interaction (§5.6, §5.7, §5.9) in parallel with 4.
6. When the grammar lands: placement, text tiles, tabs, entry-level layout (§6).
