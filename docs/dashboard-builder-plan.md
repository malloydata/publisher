# Reports: what is built, and what is next

_Plan, updated 2026-10-17. What Publisher's reports and their builder do today, how the builder is
put together, how they compare with the state of the art in report and dashboard builders, and the
improvements still to make, in order. [reports.md](reports.md) is the reference for the format and
every behaviour named here._

A **report** is a `.malloy` file with an `## artifact` tag, anywhere in a package: a page of query and
text tiles behind one row of filter controls, laid out as a `grid` (a dashboard) or `linear` (a story read
top to bottom). The same file is read by people in the Console, by apps through the SDK, and by agents
over MCP and REST, and the Console's builder edits it by splicing the file rather than keeping a layout
document of its own.

## 1. What is built

### The format

- **One document type, two layouts.** `layout=grid` places tiles across `dashboard { columns=N }`;
  `layout=linear` stacks them in one column. The tag decides, never the folder.
- **Query tiles** name a view (`"overview -> kpis"`) or a model-level query; their label, subtitle, chart
  and card come from the view's tags.
- **Text tiles** are named `##|(markdown) name` blocks listed as `name { kind=text }`.
- **Layout on each tile's entry.** An entry can carry `row`, `col`, `colspan`, `rowspan` and `break`:
  `"overview -> kpis" { row=1 col=1 colspan=12 rowspan=3 }`. Entry values win over the view's
  `# colspan` and `# break`, so one view can sit at different widths on different reports.
- **Heights.** `rowspan=N` is N rows of 80px plus the 16px gaps between them. A chart fills it, a table
  scrolls in it, and any taller result scrolls instead of being cut off.
- **Free placement.** A report where any tile has a `row` is placed: rows are fixed tracks, each tile sits
  where it says, and empty cells stay empty. A placed tile with no `rowspan` is 5 rows (text 2); one with
  no `row` or `col` fills the first free cell. On a phone tiles stack top to bottom, then left to right.
- **Filters** are `given:` declarations with their control contract in tags (`label`, `control=select` or
  `multiselect`, `suggest { … }`, ranges); other inputs follow the given's type. A report declares its own
  filters, binds the model's by name, and sets starting values in `## artifact { givens { … } }`.
- **Drill.** `# drill { to=… given=… }` on a dimension makes its cells open another report, or this one,
  with a filter set. A grouped value with no drill opens the rows behind it.
- **One query as the page.** A report can instead name a single query laid out by `# dashboard`.

### Reading a report

- The Console and `<DashboardView>` draw both layouts on Publisher's own grid, with the filter row on
  top, Apply, and filter values in the URL.
- Each tile runs on its own and fails on its own; tiles far below the fold wait until they are near.
- A tile marks the filters it ignores, and waits with a message while a required filter has no value.
- Rows behind a value, drill to another report, and "Explore from here" for hosts that wire it.
- Governance applies with no wiring: givens, row-level access, `#(authorize)` and the package's curated
  surface. A tile the viewer may not read says so instead of running.

### Building a report

The Console's builder (`<ReportEditor>` in the SDK) edits the page as it reads:

- **Edit in place.** Edit opens the builder on the report itself; the header holds the save status, undo
  and redo, **Add** (a tile, text or a filter) and **Done**. Edits are kept as a draft in the browser
  as they are made; Done writes the file (Cmd/Ctrl+S writes without leaving), and a refused write keeps
  the builder open with its reason.
- **New report** from the package page: a model, source, view, title and layout, written at the package
  root or a path you choose.
- **Placement on a grid.** Drag a tile to any cell, empty space included; the first move places every
  tile where it is drawn (each edge snaps to the nearest row line, so tiles that touched still touch), and anything a tile lands on is pushed
  down. Row and column guides show while dragging, and faintly while a tile is selected. Option/Alt with
  an arrow moves the selected tile one cell. **Arrange tiles** packs every tile upward or lets the
  tiles flow again. On a linear report tiles reorder, and **+** inserts between two.
- **Size.** Every edge and corner resizes, with no handles drawn: the cursor shows a two-headed or
  diagonal arrow. The right edge sets width in columns and the bottom edge height in rows, by drag or
  keyboard; the left and top edges move that edge; a corner moves two at once.
  The bottom edge also snaps to auto height, so a tile can be made level with an auto neighbour. A resize
  pushes covered tiles down. The fewest rows offered are 3 for a chart, 2 for value cards or a table, and 1 for text.
- **Tiles.** Add from every view the package publishes (the builder adds the import), edit titles,
  subtitles and text in place, pick a chart from the tile menu (choices the view cannot draw are listed
  with the reason), duplicate, set clickable cells, remove.
- **Filters.** Add a filter, bind it per tile with its field and comparison, set its starting value, and
  bind or unbind the model's filters. The filter row reads in the same order the reader shows.
- **Safety.** Whole-document undo and redo, one step per gesture. A save that would not read back as the
  edit asked is refused, and comments and anything the builder does not model survive.

### Server and API

- `GET …/reports` and `…/reports/{path}` serve each report's manifest: its layout, tiles with their
  layout, the filter row in reading order, and starting values.
- `PUT …/models/{path}` creates or replaces a report: compiled first, written atomically, reloaded in
  place, restored on failure, refused if the file changed since it was read.
- The load-time lint names what a report says but does not do: unread properties, tiles that do not
  resolve, widths past the grid, placed tiles that overlap or run off the edge, layout on a linear report,
  text tiles with no block, and suggest lists over hidden sources.
- `POST …/models/{path}/compile` reads report text a host keeps in its own store, so the same viewer and
  builder work on documents held as text, with restricted tiles withheld.

### In other apps

- `@malloy-publisher/sdk` exports `DashboardView`; `@malloy-publisher/sdk/builder` exports
  `ReportEditor`, lazy-loaded so the Malloy parser loads only when editing.
- A host can draw the builder's controls in its own chrome (`onControlsChange`, `BuilderBar`), guard its
  own exit (`onDirtyChange`), pin queries to a version (`queryVersionId`), and listen to `report.*`
  events for telemetry.

## 2. How the builder is built

In `packages/sdk/src/components/ReportBuilder`:

- **Reading.** `malloyTree.ts` reads a file through Malloy's own parse tree and token stream;
  `readDocument.ts` turns it into the editing model in `document.ts` (tiles, sources, filters, drills,
  settings), refusing with a reason and a line when the file is not a shape it can represent.
- **Writing.** `spliceDocument.ts` compares the opened document with the edited one and edits only what
  changed: the tile list, settings, filters, drills, added and removed tiles, and each tile's presentation.
  A tile's layout is written on its entry, and the view's `# colspan` and `# break` come off when its
  layout changes. The result is read back and refused if it does not match.
- **Layout.** `layout.ts` (row structure for flowing reports), `placement.ts` (cells, push-down, placing a
  flowing report where it is drawn, pack and flow), `useTileReorder.ts` (moving a tile) and
  `useEdgeResize.ts` (resizing from any edge or corner), each gesture committed once on release, on the
  reader's own `DashboardGrid`.
- **The editor.** `useReportEditor.ts` (history and save), `useReportDraft.ts` (the browser draft),
  `ReportBuilder.tsx` (selection, menus and dialogs), `ReportEditor.tsx` (the host: the package file,
  the catalog, the live preview).

**Invariants the tests pin:** every report in the repository opens and writes back byte for byte when
nothing changed; a refusal names what it could not do; the writer never touches imports, model files or a
tag the document does not model; and the builder's filter row, tile order and layout match the reader's.

**Tests:** unit specs on the reader, writer, placement, layout and controls; integration specs on the
builder and editor with Testing Library; server specs that compile real reports; a Playwright spec on the
Console's edit page.

## 3. Against the state of the art

| Area            | State of the art                                                   | Publisher today                                                      | Next (§4) |
| --------------- | ------------------------------------------------------------------ | -------------------------------------------------------------------- | --------- |
| Placement       | Free grid placement with width and height per tile                 | Free placement, widths, heights, push-down, pack, keyboard moves     | 10        |
| Editing         | Explicit edit mode and save                                        | In-place edit, drafts, undo and redo, saves into a reviewable file   | Ahead     |
| Governance      | Access rules wired through embedding                               | Givens, row-level access and `#(authorize)` apply with no wiring     | Ahead     |
| Cross-filtering | Click any mark to filter every tile                                | Drill from table cells only                                          | 1         |
| Drill           | Rows behind any value, further drill, a way back                   | Rows behind a grouped value; drill to a report                       | 2         |
| Filters         | Date presets, comparisons, linked and curated lists, many controls | Select and multi-select with suggestions; inputs by the given's type | 3         |
| Tile actions    | Full screen, download, view query                                  | Explore from here, for hosts that wire it                            | 4         |
| Query authoring | Each tile's query edited in an explore panel                       | A tile names a view, optionally filtered                             | 5         |
| Tabs            | Tabbed pages                                                       | None                                                                 | 6         |
| Value tiles     | Change, trend line and target                                      | Big values                                                           | 7         |
| Dynamic text    | Titles and text that show values and filters                       | Static text                                                          | 8         |
| Tile kinds      | Image, button, divider, embedded content                           | Query and text                                                       | 9         |
| Charts          | Twenty or so types and rich chart options                          | The renderer's types, picked per tile                                | Renderer  |
| Speed           | Cached results shown at once                                       | Tiles run near the viewport                                          | 11        |
| History         | Versions and rollback                                              | The file's own history, outside the product                          | 12        |
| Delivery        | Share links, embed, schedules, alerts                              | The report URL and the SDK                                           | 13        |
| Collaboration   | Comments on tiles and data                                         | None                                                                 | 14        |

## 4. Improvements, in order

Sizes: **S** a few days, **M** about a week, **L** several weeks. "Where" follows the OSS-first rule: a
capability lives in open-source Publisher, and the reliability around it (schedules, guarantees, stored
state) lives in Credible, with Publisher keeping the hooks and the file format.

### Viewers: exploring from the page

| #   | Improvement                       | What it means                                                                                                                                           | Where                                                       | Size |
| --- | --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------- | ---- |
| 1   | Click any mark to filter the page | Clicking a bar, slice or state filters the other tiles, with a chip naming what is applied and one click to clear it.                                   | Publisher: renderer click events, the filter row, `# drill` | M    |
| 2   | Better drill                      | Drill from a total or any chart mark; the rows behind it in a side panel; a way back.                                                                   | Publisher                                                   | M    |
| 3   | Better filters                    | Date presets and period comparison, searchable multi-select, filters that narrow each other, curated value lists, required filters, more control types. | Publisher: the `Given` schema and control tags (API change) | M    |
| 4   | Tile actions                      | Full screen, download as CSV, PNG or PDF, show the query, explore from here, on every tile's menu.                                                      | Publisher                                                   | S    |

### Authors: staying in the builder

| #   | Improvement                  | What it means                                                                                                                           | Where                                    | Size |
| --- | ---------------------------- | --------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------- | ---- |
| 5   | Edit a tile's query in place | Fields, filters and sort chosen in a panel inside the tile, written as the tile's own view, not as an edit to the model.                | Publisher: builder and splice writer     | L    |
| 6   | Tabs                         | `tab=` on each entry, the open tab in the URL, and adding, renaming and reordering tabs in the builder.                                 | Publisher: entry layout, reader, builder | M    |
| 7   | Richer value tiles           | Change against the previous period, a trend line, and a target with conditional colour on a big value.                                  | Renderer, or Publisher's value cards     | M    |
| 8   | Text that reads the data     | Titles and text tiles that show current values and filters ("Revenue for {Region}: $2.1M").                                             | Publisher: text tiles and headings       | M    |
| 9   | More tile kinds              | Image, link button, divider or section header, and an embedded page from the package's `public/` folder.                                | Publisher: `kind=` on the entry          | M    |
| 10  | Finish placement             | Heights of their own on a phone, and maps that size from their box (with the renderer). Pack, flow and one-cell keyboard moves shipped. | Publisher: reader; renderer for maps     | S    |

### Reliability and delivery

| #   | Improvement          | What it means                                                                                                       | Where                                                              | Size |
| --- | -------------------- | ------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------ | ---- |
| 11  | Faster first view    | Show each tile's last result while the new one runs, load tiles progressively, and say why a tile is slow or empty. | Publisher; Credible for caching guarantees                         | M    |
| 12  | Version history      | What changed, who changed it, and rollback to an earlier version.                                                   | Publisher over the package's history; Credible for stored versions | M    |
| 13  | Sharing and delivery | Links that keep the current filters, embed with fixed filters, scheduled email or Slack snapshots, and alerts.      | Publisher for links and embed; Credible for schedules and alerts   | L    |
| 14  | Comments             | Comments pinned to a tile or a data point.                                                                          | Credible for stored threads; Publisher for the anchors             | M    |

### Also open

- **Renderer** (`@malloydata/render`): more chart types (pie, area, funnel, waterfall, gauge), table sort by
  header, conditional formatting, reference and trend lines, value labels, axis ranges, and maps that
  size from their box.
- **Settings:** timezone, auto-refresh and download defaults on the artifact tag, set from the builder;
  a builder control for grid width and run-on-load, which the file already carries.
- **Cleanup:** code kept only for compatibility with the Malloyyo dialect, now that it is no longer a
  constraint.

### Order

1. Click a mark to filter the page (1): it touches the renderer, the filter row and drill, and every viewer
   notices it.
2. Tabs (6) and richer value tiles (7): short and very visible.
3. Better filters (3) and tile actions (4).
4. Edit a tile's query in place (5): designed alongside 1–3, built after them.
5. The rest as their users ask for them, with delivery (13) and comments (14) planned with Credible.
