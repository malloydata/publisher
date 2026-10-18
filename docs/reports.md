<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Reports

> **What this is:** how to write a report, a `.malloy` file with an `## artifact` tag that Publisher
> serves as a filterable, clickable page of tiles and prose, declared entirely in Malloy with no code
> and no build step. One reference, with a section per layout.

A **report** is a `.malloy` file in a package whose model-level notes carry an `## artifact` tag. The
file _is_ the report: it imports the model parts it needs, names the tiles to show, declares its
filters, and says how to lay them out. Publisher discovers it at package load, lists it on the
package page in one **Reports** section, and serves it at `/<env>/<package>/<path>`, its package path
without `.malloy`.

A report has a **layout**, and that is the only thing that differs between reports:

- **`layout=grid`**: a two-dimensional grid of tiles behind one filter row, scanned at a glance. What
  other tools call a dashboard. See [Grid layout](#grid-layout).
- **`layout=linear`**: tiles and prose top to bottom, read in order. What other tools call a notebook
  or a data story. See [Linear layout](#linear-layout).

Everything else is one feature set that applies to both: filter controls from givens, URL-synced
state and Apply, text tiles, `# drill`, the Console builder, curation, the write path, and the
load-time lint. Changing a report's layout is a one-property edit, not a conversion between document
types.

**A report can live anywhere in the package.** Where a file sits carries no meaning: no directory is
required, none is a default, and none is a convention. Tags alone decide whether a file is a report
and which layout it has. Put a new report at the package root unless you have a reason to put it
somewhere else. The one place a report cannot live is `public/`, which Publisher serves as raw static
files.

```malloy
##! experimental.givens
## artifact { layout=grid title="Storefront overview" tiles=["overview -> kpis", "overview -> revenue_trend"] } dashboard { columns=12 }
import { order_items, products } from "storefront.malloy"

# label="Category" control=select suggest { source=products dimension=category }
given: CATEGORY :: filter<string> is f''

source: overview is order_items extend {
  # colspan=12
  # label="Key figures"
  # big_value
  view: kpis is key_figures + { where: category ~ $CATEGORY }

  # colspan=12
  # break
  # label="Revenue by month"
  view: revenue_trend is sales_by_month + { where: category ~ $CATEGORY }
}
```

The [`storefront`](../examples/storefront) example package ships reports of both layouts over the same
model.

The format began as [Malloyyo](https://github.com/malloydata/malloyyo)'s dashboards and is now
Publisher's own: it reads no Malloyyo-only spelling (`kind=`, a query-level `# artifact`,
`dashboard_columns=`), and Malloyyo reads none of Publisher's additions (reports outside `dashboards/`,
path drill targets, `layout=`, entry layout and placement).

## Where the pieces live

```
storefront/
  publisher.json           # package manifest
  index.malloy             # the package's surface: what reports and agents may read
  storefront.malloy        # sources, measures, reusable views, # drill tags
  givens.malloy            # given: declarations that model code reads, imported by name
  shared.malloy            # no artifact tag: an ordinary model that reports import
  overview.malloy          # a report, layout=grid
  category-review.malloy   # a report, layout=linear
  regions/
    west.malloy            # a report in a subdirectory, served at …/regions/west
```

What makes a file a report:

- **It is a `.malloy` file outside `public/`** (a legacy `.malloynb` is a linear report too, read-only).
- **It carries an artifact tag**, a model-level `## artifact { … }` that parses and does not set `kind=`.
  A `# artifact` on a `query:` or a `view:` is not read; see
  [One query as the page](#a-dashboard-from-one-query).
- **It is not the package's root `index.malloy`.** That file _is_ the surface. A tag on it is ignored
  and reported as an error (`index-is-not-a-report`), rather than the surface silently vanishing.

**An untagged `.malloy` file is a model like any other, wherever it lives.** It is how several reports
share a scoped source or a set of givens: put the shared definitions in an untagged file and import it.
There is no "shared include" state tied to a directory.

**The package path is a report's identity, and nothing else is.** `regions/west.malloy` is the report
`regions/west`: its Console route, its `GET …/reports/{path}` address, its `# drill` target, and the key
its builder and drafts use. Two reports may share a basename in different directories, and neither
shadows the other. The listing shows the title, or the basename when there is no title, as a display
label only.

**A report's imports are relative to its own location**, as Malloy imports always are. A report at the
root imports `"storefront.malloy"`; one in `regions/` imports `"../storefront.malloy"`. Moving a report
between directories means updating its import paths, and its prose links too, since a relative link in
a text tile resolves against the report's own location.

Each report compiles **as its own entry**, which is why it imports what it uses rather than inheriting
it: model-level annotations do not cross an import, so the artifact tag is only readable when the file
is the thing being compiled.

## The artifact tag

```malloy
## artifact { layout=grid title="…" tiles=[…] givens { CATEGORY=f'Outerwear' } autorun=false } dashboard { columns=12 }
```

| Construct                                                         | What it does                                                                                                                                                |
| ----------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `## artifact { … }`                                               | Makes the file a report. Model-level; the header above it holds only `##!` flags, `//` comments and the unnamed `"` notes that are the report's description |
| `layout=grid` / `layout=linear`                                   | The layout. **Always write it**: see [Which layout](#which-layout)                                                                                          |
| `kind=…`                                                          | No longer read. A tag that sets it is not a report, and the package says so (`report-kind-removed`, error) with the `layout=` to write                      |
| `title="…"`                                                       | The report's title. Without one, the title falls back to the description, then to an opening markdown heading, then to the basename                         |
| `tiles=[…]`                                                       | The tiles, in order. A query tile is a quoted `"source -> view"`; a text tile is `name { kind=text }`. See [Tiles](#tiles)                                  |
| `givens { NAME=value }`                                           | Starting control values, not a redeclaration. See [Apply, starting values, and the URL](#apply)                                                             |
| `autorun=false`                                                   | Batches control changes behind an Apply button                                                                                                              |
| `dashboard { columns=N }` beside the tag                          | The grid width, read on a grid report. `dashboard_columns=N` inside the tag is not read, and is reported with the fix                                       |
| `# colspan=K`, `# break`, `# label`, `# subtitle`, `# borderless` | Per-tile presentation, on the view a tile names. See [Laying out the grid](#laying-out-the-grid)                                                            |
| `# drill { to=[…] given=… }` on a source `dimension:`             | Makes cells that group by it clickable. See [Drill](#drill)                                                                                                 |

Anything else inside the artifact tag is a package warning naming it, because the reader looks
properties up by name and would otherwise serve the page as though the line were not written.

**The tag is one line, or a block.** A `## artifact { … }` line must stay on one line: wrapping it is a
compile error, and it fails the whole package rather than the one file. To spread a long tile list
over lines, write the same tag as a block: `##| artifact { …` on the opener line, the rest below it,
and `|##` on a line of its own at the opener's column. It reads, lints and edits exactly as the
one-line tag does, and the builder keeps whichever form the file has.

```malloy
##| artifact { layout=linear title="Category review"
  tiles=[
    intro { kind=text },
    "category_tiles -> revenue_trend",
    "category_tiles -> brand_ranking"
  ]
}
|##
```

<a id="which-layout"></a>

### Which layout

In order, first match wins:

1. `layout=grid` or `layout=linear` in the tag.
2. A tag that lists `tiles` (an empty list counts), or a model-level `dashboard { … }` grid tag on any `##`
   line of the file, is grid.
3. Anything else is linear.

A `.malloy` file whose tag comes out linear, by step 1 or step 3, is a report only when the tag has a
`tiles` key (an empty list counts). With none it is the retired cell format and makes no report (see
[Files written as `run:` cells](#files-written-as-run-cells)); a `.malloynb` is linear with no tag at all.
A `tiles` that is not a list (`tiles=foo`) makes the tag unreadable, so no report either.

So **a linear report that lists tiles must say `layout=linear`**, or step 2 reads it as grid. Write
`layout=` on every report, which is what the builder and the create dialog always do, and a file never
depends on steps 2 and 3. The rule reads the file's model-level notes, the same way whether the file is
compiled, failed to compile, or is text handed to the compile check or the save.

What layout changes is how tiles are placed, and nothing else. Linear renders them one column wide, top
to bottom, and ignores `colspan` and `break` (with a `notebook-tile-layout-ignored` warning); grid places
them by `columns`, `colspan` and `break`.

## Tiles

A report lists its tiles in `tiles=[…]`, in order: reading order on a linear report, placement order on
a grid. Tiles run as **separate queries**, which is the only way a page can span unrelated sources: a
nest's pipeline starts from its own query's source, and there is no way to combine two.

### Query tiles

A query tile is a `view:` on a source, named as a quoted string `"source -> view"`. Put the views on an
extension declared in the report itself, as thin wrappers over the modelled views:

```malloy
source: seasonal is order_items extend {
  # colspan=8
  # break
  # label="Revenue by month"
  view: revenue_trend is sales_by_month + { where: category ~ $CATEGORY }

  # colspan=4
  # label="By season"
  view: by_season is seasonality + { where: category ~ $CATEGORY }
}
```

The modelled view keeps its chart tag; the wrapper carries the tile's label and width and says which
controls the tile answers to (its `+ { where: … }` **binding**, see
[Filter controls](#filter-controls)). Tagging a thin re-declaration rather than the shared view itself
is what lets one modelled view sit at different widths, under different labels, on different reports.

- **Quote every query entry.** `"seasonal -> revenue_trend"` is a string; unquoted, the tag does not
  parse and no tile is read (`notebook-artifact-unparsed`).
- **A bare name** (`tiles=["overview"]`) names a model-level `query:` in the file.
- **A `run:` statement in a report that lists `tiles` is never shown**, and the lint says so
  (`notebook-layout-run`): define a view, list it, and delete the `run:`.
- **A tile expression is a string in an annotation, so the Malloy compiler never checks it.** Rename a
  view and the report still compiles; the tile fails at package load, where the lint names it. To see
  the lint before saving, compile at `scope: "package"`, which returns its findings with code
  `dashboard-lint`, each at the severity a load gives it. `scope: "file"` does not run it.

What tiles buy: a broken tile shows its error in place instead of blanking the page, a control change
re-runs only the tiles that reference it, and each tile gets its own row and byte budget. The control
row is the union across the tiles, with one exception worth knowing: if a tile cannot be resolved, the
row widens to every given the file surfaces rather than narrowing to the tiles that did resolve. The
unresolvable tile is a package warning of its own, but the control row is usually where it is noticed
first.

A tile that ignores some of the row's controls says so: a small amber chip under its heading reads
"Doesn't respond to Brand" (a count past three filters), and its tooltip explains that the tile's query
never reads that given. A tile that reads every control shows nothing, as does a tile that could not be
resolved, which runs with the whole row. "Reads" is what Malloy compiles the tile to, not just its
view's `where:`: a given read through the source's own `where:`, a joined source's `where:`, a dimension
or measure the tile uses, or the source's `#(authorize)`/`#(access_filter)` gate all count. The builder
shows the same chip, and counts a filter you have just added before it is saved.

A given that only the source's gate reads is in the tile's `givenNames`, so a value the host injects
(an `ORG` from its session, say) is sent with the tile rather than dropped, but it does not become a
control: a gate is a model concern, so the row stays the givens the tiles' queries read.

Three things tiles cost, none of them fixable by tagging differently:

- **The compiler does not check tile expressions** (above).
- **No per-parent-row grouping.** A `# dashboard` nest can repeat its whole grid once per row of a
  parent query; tiles have no parent query to repeat over.
- **Filtering lives in what the tiles name, not on the page.** There is no page-level `where:`.

<a id="text-tiles"></a>

### Text tiles

Prose between tiles is a tile of its own. List it in `tiles=[…]` as a bare name with `kind=text`, and
write its body as a `##|(markdown) name` block in the same file:

```malloy
## artifact { layout=grid title="Seasonality" tiles=[intro { kind=text colspan=6 break }, "seasonal -> revenue_trend"] } dashboard { columns=12 }
##|(markdown) intro
## How to read this page

Revenue first, then the seasonal split.
|##
```

- **The entry is `name { kind=text }`.** A bare name without it (`tiles=[intro, …]`) is the query-tile
  form for a `query:` called `intro`, so the block is not shown and the lint says to add
  `{ kind=text }`.
- **The block is `##|(markdown) name`.** The name is one bare word on the opener line, the body starts
  on the next line, and `|##` closes it at the opener's column. A heading goes inside the block,
  because a bare `## Heading` line is a model tag, not prose. Keep the parentheses: `##|markdown` draws
  a malformed-route warning and `##| markdown` reads the prose as tags. `##|(text) name` is also
  read as a text tile; write `(markdown)`.
- **The body is markdown**: headings, emphasis, lists, links and inline code.
- **On a grid the entry reads `colspan`, `break` and `rowspan`** and nothing else, as every tile's entry
  does (see [Placing a tile on its entry](#placing-a-tile-on-its-entry)). On a linear report all three
  are ignored.
- **The lint reports** an entry with no block, a block written twice, a `colspan` or `rowspan` that is not
  a positive integer, any other property on the entry, and a named `(markdown)` block no entry lists. Delete an
  unlisted block rather than leave it.

The report's own description is separate: the unnamed `"` notes above `## artifact`
(`##" One line saying what this report is for.`), which render as markdown under the title.

In the manifest a text tile is `{ kind: "text", name, markdown, colspan?, break?, rowspan? }` with no `query`, and a
query tile is `{ kind: "query", query, … }`. Anything that runs a report's tiles skips the text ones.

### File order

Write a report in one order: the header (`##!` flags, `//` comments, the `"` description, the artifact
tag), then imports and givens, then the prose blocks in tile order, then the extension that defines the
tile views. Tiles read in `tiles=[…]` order wherever their blocks sit; grouping the blocks before the
extension is what the builder writes, and it inserts a new block after the last one. Keep statements and
`#` tags below `## artifact`: nothing refuses one above it, but the header is where readers and the builder
look for the tag and the description.

<a id="grid-layout"></a>

## Grid layout

A grid report lays its tiles out across `columns` by each tile's `colspan` and `break`:

```malloy
##! experimental.givens
##" Revenue and margin at a glance, and where they come from.
##| artifact { layout=grid title="Storefront overview"
  tiles=[
    "overview -> kpis",
    "overview -> revenue_trend",
    "overview -> revenue_by_state"
  ]
} dashboard { columns=12 }
|##
import { order_items, products } from "storefront.malloy"

# label="Category" control=select suggest { source=products dimension=category }
given: CATEGORY :: filter<string> is f''
# label="Ordered since"
given: SINCE :: date is @2023-01-01

source: overview is order_items extend {
  # colspan=12
  # label="Key figures"
  # big_value
  view: kpis is key_figures + { where: category ~ $CATEGORY, where: created_at >= $SINCE }

  # colspan=8
  # break
  # label="Revenue by month"
  view: revenue_trend is sales_by_month + { where: category ~ $CATEGORY, where: created_at >= $SINCE }

  # colspan=4
  # label="Revenue by state"
  view: revenue_by_state is sales_by_state + { where: category ~ $CATEGORY, where: created_at >= $SINCE }
}
```

Presentation is per tile and lives on the **view**: `# colspan`, `# break`, `# label`, `# subtitle` and
`# borderless`, which is exactly the set `@malloydata/render` resolves for a `# dashboard` nest child.
One view therefore presents identically whether it is named as a tile here or nested under a
`# dashboard` query, and there is no second grammar to learn.

<a id="placing-a-tile-on-its-entry"></a>

### Placing a tile on its entry

A report can also place a tile with its own entry in `tiles=[…]`, and the entry wins over the view's
tags, property by property:

```malloy
## artifact { layout=grid tiles=["overview -> kpis" { colspan=12 rowspan=2 }, "overview -> revenue_trend" { colspan=8 break rowspan=5 }, "overview -> revenue_by_state" { colspan=4 }] } dashboard { columns=12 }
```

- **`colspan=N` and `break`** mean what the view's tags mean. `break=false` turns off a view's `# break`.
  Because the entry belongs to the report, one view can sit at a different width on each report that
  names it, and a tile on a view the report imports can be laid out at all.
- **`rowspan=N`** is the tile's height in grid rows, and only the entry sets it. A row is 80px, and a tile
  N rows tall also spans the N-1 gaps between them (16px each), so `rowspan=5` is 464px. A chart fills the
  height, a table scrolls inside it, and a text tile scrolls its prose. Without it a tile is as tall as
  its result, up to 400px, and a row is as tall as its tallest tile; a tile with a height keeps it beside
  a taller neighbour. A map draws at its own size, so give it the height it needs.
  On a placed report (below) it is the rows the tile spans.
- **`row=N` and `col=N`** place the tile: its top left corner sits in that grid row and column, counted
  from 1. A report where any tile has a `row` is **placed**. Its rows are then fixed 80px tracks, each
  tile sits where it says instead of flowing, and empty cells stay empty, which is how a tile can sit
  under a short neighbour beside a tall one. A placed tile with no `rowspan` is 5 rows (a text tile 2);
  one with no `row` or `col` fills the first free cell; `break` means nothing. On a narrow screen the
  tiles stack in one column, top to bottom and then left to right. The lint names a placed tile that
  runs past the grid's right edge, and two that cover each other.
- **An entry reads only these five.** A label, subtitle, chart or card still goes on the view, and the
  lint names any other property on an entry.
- **The Console builder writes here.** Resizing or moving a tile puts its layout on its entry and takes
  the view's `# colspan` and `# break` off, so the layout has one home. A view's tags are left alone
  until its tile's layout changes. On a grid a move is free placement: the first move places every tile
  where it is drawn, each edge snapped to the nearest row line so tiles that touched still touch, and the file lists
  placed tiles in reading order. **Arrange tiles** in the builder's bar then packs every tile upward to
  close the gaps, or lets the tiles flow again (their places cleared, their sizes kept).

```malloy
## artifact { layout=grid tiles=["overview -> kpis" { row=1 col=1 colspan=12 rowspan=3 }, "overview -> revenue_trend" { row=4 col=1 colspan=6 rowspan=3 }, "overview -> revenue_by_state" { row=4 col=7 colspan=6 rowspan=6 }, "overview -> revenue_by_category" { row=7 col=1 colspan=6 rowspan=3 }] } dashboard { columns=12 }
```

Here the trend and the category chart stack in the left half beside one tall map.

<a id="laying-out-the-grid"></a>

### Laying out the grid

Cards and tiles share one grid, so a page that lines up is a matter of four tags. Using one recipe
across a package's grid reports is what makes them read as one product rather than as several pages:

- **`columns=12`** on the `dashboard { … }` tag. Twelve divides by 2, 3, 4 and 6, so a row comes out
  even whether it holds three cards or four. The Console builder has no grid-width setting: it keeps the
  file's own `columns=` and lays tiles out on it, so set it in the file. A `columns` above 24 still
  renders, with ever-thinner tracks, and the package warnings flag it.
- **A `# colspan` on every tile, summing to `columns` per row.** Four cards at 3, three at 4, two tiles at
  6, a full-width table at 12. Leave them off and each item takes one column, which is too narrow for a
  line chart to draw in at all. A colspan wider than `columns` is clamped, and said so in the package
  warnings.
- **Do not forget `columns=` itself.** Without it the page falls back to a narrow default and every
  colspan is clamped to it, which is the usual reason a laid-out grid comes out one item per row; the
  package warnings name it.
- **`# break` on the first tile after the cards.** Without it the first tile flows into whatever columns
  are left beside the cards and the next one wraps. A break interrupts a row that would otherwise be
  shared; once a row's colspans sum to `columns`, the next item wraps on its own.
- **`# label="…"` on every tile view and aggregate**, including the aggregates inside a table view, whose
  column headers are field names too. The heading is otherwise derived from the view's name:
  `sales_by_month` over a chart, which reads like a database column. A view referenced by name is the
  exception: you can label the tile, but its own field names still reach the axes and column headers, so
  label the fields in the view if you want those too.

**A KPI row is a view of only measures.** A tile is one whole result, so a tile whose result is one row of
measures renders as big-value cards on its own, the way Malloyyo draws the same tile; `# big_value` on the
view says so explicitly, and `# table` opts out.

The traps:

- **No `# size=fill` on a grid tile.** Inside a grid it measures against the container the whole grid was
  handed rather than the tile, which produces a chart thousands of pixels tall. Tiles already size to
  their colspan.
- **A table tile sitting in the left part of a wide card with the rest blank** is not filled by the grid.
  `# table { size=fill }` on the view forces it. That is the table's own tag, a different thing from the
  chart `# size=fill` above.
- **A KPI card's label is one line that ellipses** rather than wrapping, so a narrow card truncates it
  silently: "Orders / customer" reads as "ORDERS / CUSTOMEI" at 1 column of 6. Widen the card or shorten
  the label.
- **A ratio needs a number format.** `order_count / customer_count` renders as `10.695` on a card;
  `# number="#,##0.0"` is the precision it actually carries.

### Labels the renderer takes from somewhere else

Two places read a field's **name** rather than its `# label`, so a labelled model still shows
`total_sales` on the page:

- **A `# shape_map`'s color legend** is titled with the measure's field name. Rename the measure in the
  view (`aggregate: revenue is total_sales`) rather than only labelling it. Renaming drops the measure's
  own format tags, so re-tag `# currency` at the rename.
- **A chart's series legend** reserves its width from the longer of the series label and its widest
  value, then truncates both to fit. A 4-character label over 4-digit years clips to `20…`; naming the
  series `# label="Order year"` instead of `"Year"` buys the column enough room.

Both are upstream renderer behavior, not Publisher's, and both are cheap to work around in the model.

Height is the one thing the surface decides rather than the tags. A grid caps each tile at 400px, so one
long table cannot set its row's height, and a result taller than that scrolls inside the tile. A linear
report lets a tile grow to fit what it holds, so a row of KPI cards shows every card, and a chart draws
at its default height. A table in a linear report stops at 700px and scrolls inside its tile, with its
header row pinned, so one long table cannot push the rest of the report off the page. No tag changes
that cap for a table tile; the `table.max_height` option belongs to a `# dashboard` query, where it caps
the tables inside it.

<a id="linear-layout"></a>

## Linear layout

A linear report renders its tiles one column wide, top to bottom, in `tiles=[…]` order. It is the layout
for a narrative: a question framed in prose, a query that answers it, the next question.

```malloy
##! experimental.givens
##" One category at a time: how it sells across the year and which brands carry it.
##| artifact { layout=linear title="Category review"
  tiles=[
    intro { kind=text },
    "category_tiles -> revenue_trend",
    trend_note { kind=text },
    "category_tiles -> brand_ranking"
  ]
}
|##
import { order_items, products } from "storefront.malloy"

#(description="Narrow to one product category")
# label="Category" control=select suggest { source=products dimension=category }
given: CATEGORY :: filter<string> is f''

##|(markdown) intro
## Category review

Pick a **Category** in the controls above and every chart below re-runs for it.
|##

##|(markdown) trend_note
Which brands are behind those numbers?
|##

source: category_tiles is order_items extend {
  # line_chart
  # label="Revenue by month"
  view: revenue_trend is sales_by_month + { where: category ~ $CATEGORY }

  # bar_chart
  # label="Top brands"
  view: brand_ranking is top_brands + { where: category ~ $CATEGORY }
}
```

- **`layout=linear` is required here**, because the tag lists tiles (see [Which layout](#which-layout)).
- `dashboard { columns }` has no effect on a linear report, and a `colspan`, `break` or `rowspan` on a tile
  entry is ignored with a warning (`notebook-tile-layout-ignored`).
- **Write prose that frames questions, not findings about today's data.** "Revenue spiked 23% in March"
  is stale the next time the data refreshes; "How is revenue trending?" is not.
- **A linear report is the natural home for a cross-model analysis.** The file compiles with all its
  imports in scope, so one report can import several models and a tile's source can join sources from
  different files.

### Files written as `run:` cells

An older linear form was a file of `run:` statements with `(markdown)` notes between them, under an
artifact tag with no `tiles`, either `## artifact { layout=linear }` or one naming no layout at all.
Publisher no longer reads it. Such a file is not a
report: it is not listed, its route opens nothing, and the builder refuses it. The package page names it
with `report-cells-removed` and the fix, and the compile check answers the same finding. A tag whose
`layout=` is misspelled (`layout=Grid`) and that lists no tiles is named `report-layout-unknown` instead.
The file still compiles as a model, and `PUT …/models/{path}` with the `expectedHash` of its current text
replaces it in place, so it can be rewritten as tiles where it is.

To bring one back, rewrite it as tiles, the form above:

1. Make each floating `(markdown)` note a named `##|(markdown) name` block, listed as `name { kind=text }`.
2. Make each `run: <source> -> <query>` a `view:` on an extension of that source the report declares, and
   list it as `"<extension> -> <view>"`. The render tags and `#"` caption that sat above the `run:` go on
   the view, the caption as `# label="…"`. A `run:` of a named `query:` can be listed by the query's name.
3. List the entries in `tiles=[…]` in reading order, keep `layout=linear`, and delete the `run:` lines.

`examples/storefront/category-review.malloy` is a report that was rewritten this way.

### Legacy `.malloynb` files

A `.malloynb` (cells delimited by `>>>markdown` and `>>>malloy`) is a deprecated format. Publisher keeps
read-only support, so existing ones still open and run as linear reports, wherever they are. Do not
create one. A `.malloynb`'s compile errors show only when its cells run, and it takes its options as
file-level `## autorun=false` and `## givens { … }` lines rather than inside an artifact tag.

## Filter controls

Controls are the `given:` declarations the tiles reference. **Declare them in the report file**: it is the
one file the builder edits, so a filter the builder adds, changes or removes is a declaration there. A
given the model already reads stays in the model, imported by name and never declared a second time (see
[Givens that stay in the model](#givens-that-stay-in-the-model)). Either way the tags on the declaration
are its control contract:

```malloy
##! experimental.givens

# label="Category" control=select suggest { source=products dimension=category }
given: CATEGORY :: filter<string> is f''

# label="Brand" control=multiselect suggest { query=brand_suggest dimension=brand }
given: BRAND :: filter<string> is f''

# label="Minimum line total" range_min=0 range_max=250
given: MIN_SALE :: filter<number> is f''

# label="Ordered since"
given: SINCE :: date is @2023-01-01
```

| Tag                                              | Renders as                                                                                        |
| ------------------------------------------------ | ------------------------------------------------------------------------------------------------- |
| `control=select` + `suggest { … }`               | A dropdown whose options are queried from the data                                                |
| `control=multiselect` + `suggest { … }`          | The same, taking several values                                                                   |
| `range_min=` / `range_max=` on `filter<number>`  | A two-handled range slider (`[lo to hi]`, or `>= lo` with the upper handle at the ceiling)        |
| none, on a `filter<date>` or `filter<timestamp>` | A time-range control: Today, last 7/30/90 days, last 12 months, or a custom range of days         |
| none, on a `date` or `timestamp`                 | A date picker                                                                                     |
| none, on a `filter<boolean>`                     | A dropdown of `true` and `false`; blank uses the model's default, so no filter when that is `f''` |
| none, on a `filter<string>`                      | A text box taking Malloy filter syntax                                                            |

`control=select` and `suggest` apply to string givens only; any other type gets the control in this table
for its type. A value a given's type cannot read, such as `asdf` for a `filter<boolean>`, is refused with a
`400` that names the given and quotes the parser's reason.

A sixth tag, `description`, has two spellings that do different things: `# description="…"` publishes to
the API but Publisher's own UI does not render it, while `#(description="…")` renders as helper text under
the control and draws a cosmetic compile diagnostic, that the prefix "is not a well-formed route", for any
multi-word value.

**Which controls appear is decided per report**, by which givens its tiles reference. Declaring ten and
referencing two shows two, so a declaration nothing binds shows nothing, silently.

### Binding a control to a tile

A tile's binding is a `+ { where: field ~ $GIVEN }` refinement on its view, one clause per control it
answers to. A view without one does not move when the control does, which is how a page keeps one tile
fixed while the rest filter. A `filter<…>` given binds with `~`; a plain `date` or `number` given is a value
and binds with `>=`, `<=` or `=`.

A tile declared as a reference, `view: x is base_view`, gets the refinement after the reference; a tile
whose body is written inline, `view: x is { aggregate: … }`, gets the binding as a depth-1 `where:`
statement inside the body's own first stage. Either way it is exactly what the builder reads and writes. A
`where:` anywhere else in an inline body (inside a `nest:`, part of a compound predicate, or in a second
stage) is left alone. A tile whose body has no one place for a binding, such as a `->` pipeline from a
named view, keeps its label, subtitle, colspan and position editable; only its filter control is off in the
builder, and the tile menu names the shape. A tile over a view declared outside the report
(`orders -> by_brand` against an imported source) is shown but not changed, because the builder does not
write model files.

**Binding is per declaration, not per name.** A report that declares its own `CATEGORY` over a source whose
model-level `where:` reads the model's `CATEGORY` compiles, shows the control, and filters nothing, because
the two declarations only share a name. Model-level scoping is the other design and still works: put a
scoped source in an untagged model that imports the givens it reads, import that model and those givens
into the report, and the controls render for the givens the tiles reach. The two do not mix on one given.

```malloy
// shared.malloy: no artifact tag, so an ordinary model.
##! experimental.givens
import { order_items } from "storefront.malloy"
import "givens.malloy"

source: scoped_sales is order_items extend {
  where: category ~ $CATEGORY and created_at >= $SINCE
}
```

A report that tiles over `scoped_sales` imports both `shared.malloy` and `givens.malloy`: nothing in the
report mentions `CATEGORY`, but the given namespace is per file, so without the import the control row
would be empty and the tiles would silently run at their defaults. Save the shared model before you
compile the report that imports it.

**A given has to be in the report file's own scope to be bindable**: declared there, or imported. A given
the file cannot see gets no control, and sending it at run time fails with "unknown given", even when a
`where:` that references it lives up an import chain. Everything about givens themselves (declaration,
types, defaults, access control) is in [givens.md](givens.md).

<a id="givens-that-stay-in-the-model"></a>

### Givens that stay in the model

A given stays in the model, declared once there, when any of these reads it: a model source, view or
measure; an `#(authorize)` or `#(access_filter)` gate; or an HTML data app. The report imports it by name
(`import { CATEGORY } from "givens.malloy"`) and never re-declares it. Getting this wrong fails in two
different ways:

- **Import a name and also declare it: a compile error** naming the clash. Drop the local declaration.
- **Declare a name the model already reads, without importing it: no error at all.** The report has a second
  given that shares only the name. Its control renders and moves, and the model's own `where:` or gate
  never sees the value.

Declare locally only what no model code reads. A `# drill` into a report seeds a given by name, so the
destination declares that same name as `filter<T>`.

### Suggest lists

A `suggest` reads either a `source=` and `dimension=` pair, or a named `query=` when the option list needs
its own ordering or filtering. **The source or query has to resolve in the report file**, so import it
there; a report whose suggest names something it cannot see is a package warning at load, and the control
still looks like a picker but says "Could not load the options for this control".

- `dimension=` can name a field of a directly joined source (one join level). Quote it, because an
  unquoted dotted value does not parse as a tag: `suggest { source=sales dimension="products.department" }`.
- On the `query=` form, `dimension` names a column of that query's output and only its last segment is
  read. Match it to the query's `group_by`; when it matches no column the picker falls back to the first
  column of the result.
- **A `query=` needs that query's own source imported as well.** An import is not transitive: the query
  resolves by name, so the file compiles and the package loads, but the picker answers
  `400 Undefined source '…'` the moment a reader opens it. The package warnings name it and say which
  source to import.

### Curation

In a package that [curates its surface](discovery-and-access.md), resolving is not enough. A package
curates when its root holds an `index.malloy` (every scaffolded package does), or when its
`publisher.json` has a legacy `explores` list. Every report is listed and served whatever the surface is,
but its tiles and each `suggest` may read only sources on the surface. A report reads
the surface; it never adds to it, so its own `export { … }` and named queries publish nothing. Export what
the controls and tiles read from `index.malloy`:

```malloy
// index.malloy
import "orders.malloy"

export { orders, customers } // every source a tile or a suggest reads
```

A source the report declares on top of a surface source (`source: big is orders extend { … }`) may be read;
one declared on top of a hidden source may not. A tile or suggest over a hidden source answers `404`, and
the package load warns about each one (see [What Publisher checks at load](#what-publisher-checks-at-load)).
A package with no `explores` and no root `index.malloy` has curation off.

**Tagging a model that the surface depends on takes it off the surface.** A tagged file is served as a
report and contributes nothing to the surface, so tagging a model that `index.malloy` imports or that
`explores` lists removes its sources from the surface; the package reports it (`report-on-surface`), naming
the importer. To hide a report, remove its artifact tag (and take it out of `explores`, if that lists it: a
file `explores` lists is published as an ordinary model).

<a id="apply"></a>

### Apply, starting values, and the URL

```malloy
## artifact { layout=grid title="Regional sales" autorun=false givens { REGION=f'West' } tiles=["sales -> by_region"] } dashboard { columns=12 }
```

- **`autorun=false`** puts an Apply button on the control row and batches changes behind it, so moving a
  date and picking a region re-runs the page once instead of twice. Worth it as soon as a page is slow
  enough that a reader notices.
- **`givens { … }`** sets this report's _starting_ values. It is an opening position, not a redeclaration.
- **Control state lives in the URL**, so a filtered report is a shareable link. A URL parameter beats the
  report's own starting values.

Both options go inside the artifact tag on either layout; a file-level `## autorun=false` does nothing on a
`.malloy` report (only a legacy `.malloynb` reads options at the file level).

<a id="drill"></a>

## Drill: making cells clickable

`# drill` is declared on a model **dimension**, not on a report:

```malloy
source: order_items is duckdb.table('data/order_items.parquet') extend {
  # drill { to=["category", "self"] given=CATEGORY }
  dimension: category is products.category

  # drill { to=self given=BRAND }
  dimension: brand is products.brand

  # drill { to="regions/west" given=REGION }
  dimension: region is regions.region
}
```

- **`to=<path>` navigates to a report**, of either layout, with the clicked value written into the named
  given. The target is the report's package path without `.malloy`, read from the package root, so a drill
  tag means the same thing whichever report renders it: `to=category` is `category.malloy` at the root,
  `to="regions/west"` is `regions/west.malloy`. Quote a path that contains a `/`. It is a plain path from the
  root, never relative: a target with a `.` or `..` segment, an empty segment, or a leading `/`
  (`to="../x"`, `to="./x"`, `to="/x"`) resolves to no report, is reported as an unresolved drill, and the
  click goes nowhere.
- **`to=self`** filters wherever the click came from, without leaving the page. Under `autorun=false` it
  stages the value as a draft: neither the page nor the URL changes until Apply is pressed, while the same
  menu's `to=<path>` entry navigates and arrives already applied.
- **A click always opens a small menu that says what it will do**, even when there is only one thing to do.
  It has no header: each entry names the value it acts on, in this order: `Filter this report to EU`
  for `to=self`, `Open <report> for EU` for each `to=<path>` (the report's file name, humanized), and
  `Show rows for EU, 2025-12` (every value of the clicked row) when the rows behind the value can be
  shown (below). A `# drill` tag adds entries to the menu; it never replaces
  the rows, and a click never acts on a tag the reader cannot see. A cell is marked clickable exactly when
  its menu would list something.
- **The value is written as filter syntax**, because a click cannot know what it is aiming at, so the
  destination's given should be a `filter<…>`. Seeding a plain `string` given across reports delivers the
  escaped spelling (`Ben\ &\ Jerry` for a cell reading `Ben & Jerry`), which matches nothing. `to=self` is
  exempt: there the surface knows the declared type and re-encodes for it.
- **Always write `given=`.** Without it the given is the dimension name exactly as the model spells it, so a
  `dimension: category` seeds a given called `category` rather than a declared `CATEGORY`. A `to=self`
  survives that, because a surface folds case when it looks up its own given. A `to=<path>` does not: the
  name goes into the destination's URL exactly as the tag spells it, the destination binds only the
  parameters it declares, and the click lands on an unfiltered page with nothing erroring.

**A target is always a path from the package root.** A bare name is a root file: `to=overview` is
`overview.malloy`, never a report called `overview` elsewhere. Write `to="sales/overview"` for
`sales/overview.malloy`.

**The rows behind a value.** On a tile layout a grouped value in a table offers **Show rows** in its menu,
whether or not its dimension carries a `# drill`. Picking it opens the rows behind the value: the tile's
source filtered by everything the value was filtered by, read from the result itself
(`run: <source> -> { where: <the tile's filters>, <each dimension of the row> = <its value>; select: *; limit: 200 }`).
The tile's own `where:`, any refinement's, an enclosing nest's and the applied controls all hold, and every
dimension of the clicked row goes in, enclosing rows included. A null dimension filters as `is null`, and a
view with its own `timezone:` is filtered in that zone.

That works for any tile shape that runs one stage: `"<source> -> <view>"`, a refined view
(`"<source> -> <view> + { where: … }"`), a tile naming a `query:`, an inline query, a view grouped by a
joined field (`customers.name`). Two shapes offer no rows, and their values are not marked: a view of more
than one stage (`view: v is { … } -> { … }`), whose later dimensions are an earlier stage's outputs that the
source cannot be filtered by, and a source with parameters.

Declaring it on the dimension is what makes it work everywhere: any result that groups by that dimension
is clickable, in a grid tile and in a linear report's tile or cell alike, with no per-report wiring. So when
a view is meant to be drilled, group by the tagged dimension: declaring
`dimension: category is products.category` and grouping by `category` gives the identical output field name
and numbers, and carries the tag.

**What a reader sees.** Cells in a drillable **table** column take a pointer cursor, and turn blue and
underlined under the pointer. They carry a button role and are reachable from the keyboard: one tab stop
per drillable column, ArrowDown and ArrowUp to step a row, Home and End to jump to the ends, and Enter or
Space to open the menu. Focus moves into the menu, and Escape closes it without acting. A measure cell
and a column header open no menu. A chart's bars, lines and points do not respond to a click at all, because the renderer
reports clicks on table cells only, so a report meant to be drilled wants at least one untagged (table)
tile.

A destination the surface cannot honor is not marked and not offered, deliberately, since a dead link is
worse than none; a `to=self` in a report that declares no control for its given is the usual case. Two
layouts lose the marking:

- **Another tile rendering a column with the same header.** Marking matches columns by their rendered
  header text, so a "revenue by category" chart beside a drillable `category` table unmarks that column.
  Only a non-drillable column suppresses; give the two different headings with `# label=`.
- **`# transpose`.** No cell in a transposed table is marked, but the renderer still routes the click, so
  the drill fires on a cell that looked like plain text. Keep a `# drill` dimension out of a transposed
  tile.

A drill only lands somewhere useful if the destination declares a control for the given being seeded.
**Publisher does not check that half.** The load-time lint checks that the target is a report in the
package, and for `to=self` that some model in the package declares the given; nothing reads the
destination's own givens. Click it and look.

<a id="a-dashboard-from-one-query"></a>

## One query as the page, and `# dashboard` layout

Everything the grid reads off a tile's view comes from `@malloydata/render`'s `# dashboard` grammar, which
lays out ONE Malloy result: top-level `aggregate:` measures become KPI cards and each `nest:` becomes a
tile, placed by the same `columns`, `colspan` and `break`. A `# dashboard` view renders that way anywhere,
in a tile of either layout or in a cell, and the same view looks the same in each.

One query whose single result is the whole page is a report with that query as its one tile: a model-level
`## artifact` naming the query, and the query's own `# dashboard` laying out its result:

```malloy
##! experimental.givens
import { order_items, products } from "storefront.malloy"
import "givens.malloy"

##" Revenue and margin at a glance.
## artifact { layout=grid title="Business Overview" tiles=["overview"] }
# dashboard { columns=12 }
query: overview is order_items -> {
  where: category ~ $CATEGORY
  aggregate:
    # label="Revenue"
    # colspan=6
    total_sales
    # label="Orders"
    # colspan=6
    order_count
  nest:
    # break
    # colspan=12
    # label="Revenue by month"
    sales_by_month
}
```

A `# artifact` on the `query:` itself (the older single-query form) is not read: the file is an
ordinary model. Move the tag to model level as above, the same properties plus `layout=grid` and
`tiles=["<query>"]`.

- **A `# dashboard` query cannot span sources**, where separate tiles can.
- **A KPI row is authored differently.** On a `# dashboard` query a top-level `aggregate:` measure _is_ the
  card; a nested `# big_value` view renders embedded, each measure a full-width bar inside one tile.
- **Without `columns=` there is no grid inside it**: cards flow side by side at their natural widths and
  every colspan is ignored. And a `f'…'` literal in a `givens { … }` block that shares a line with
  `# dashboard` drops every `# colspan` and `# break`, rendering one plain nested table; put `# dashboard`
  on its own line. To tell the two apart, run the tile and read `renderLogs`: `Unknown render tag 'colspan'`
  means the renderer never saw a `# dashboard` tag; `Ignored # colspan … only applies in columns mode`
  means it saw the tag but no count. Neither reaches the package warnings.
- `# dashboard { gap=N table { max_height=N | none } }` are options only a `# dashboard` query reads.

<a id="what-publisher-checks-at-load"></a>

## What Publisher checks at load

Every package load lints each report, one pass per report plus the checks its layout adds, and reports the
findings as package warnings, visible on the package page and in the server log. They catch the failures
that are otherwise silent: a control that never appears, a click that goes nowhere.

- **Drill targets.** A `# drill { to=… }` that names no report in the package; a `# drill` with no
  destination; a target that is not a plain path from the package root; a `to=self` whose given no model
  in the package declares. The drill checks read every model in the package, not just reports.
- **Controls.** A given whose `suggest` names a source, query or dimension the file cannot see, or a
  `suggest` in a form that cannot fetch options at all.
- **Tiles and layout.** A tile that does not resolve to a real view; on a grid, a non-positive `columns`, a
  `# colspan` or entry `colspan` that is not a positive integer or is wider than the grid, an entry
  `rowspan` that is not a positive integer, a property on an entry other than its layout, a
  `dashboard_columns=` inside the tag (not read; the width goes in `dashboard { columns }`); on a linear report, a `colspan`,
  `break` or `rowspan`
  (`notebook-tile-layout-ignored`) and a `run:` beside `tiles` (`notebook-layout-run`); any property inside
  the artifact tag that Publisher does not read.
- **Text tiles.** An entry with no block, a block written twice, a non-integer `colspan` or `rowspan`, an
  unknown property on the entry, and a named block no entry lists.
- **The tag and the prose.** An artifact tag that does not parse (`notebook-artifact-unparsed`, error);
  a `#(markdown)` note with no statement to take it (`notebook-markdown-attached-nowhere`, error); a
  floating or unnamed `(markdown)` note that no text tile shows (`notebook-markdown-block-unnamed`); each
  naming the line and the fix.
- **The layout.** A `layout=` other than `grid` or `linear` (`report-layout-unknown`, error); a tag that
  sets `kind=`, which is no longer read and makes no report (`report-kind-removed`, error, with the
  `layout=` to write); a linear tag with no `tiles`, the retired cell format, which makes no report
  (`report-cells-removed`, error, with the fix).
- **The surface.** A tagged root `index.malloy` (`index-is-not-a-report`, error); a report on the surface,
  one that `index.malloy` imports, directly or through other files, or that `explores` lists
  (`report-on-surface`); and a tile or `suggest` that
  reads a source the surface does not publish, for example:

  ```
  Tile orders_staging -> by_flag on report overview reads orders_staging, which index.malloy
  doesn't export, so it won't load. Fix: add orders_staging to the export { ... } in index.malloy.
  ```

  The warning leaves the source unnamed when the package gates anything with `#(authorize)`.

- **A shared grid basename.** Two or more grid reports with the same file name in different directories
  (`report-name-ambiguous`, warn, one per name, naming the paths): the deprecated `…/dashboards/{name}`
  route answers 409 for that name. The report routes address by path and are unaffected. Fix: rename one of
  the files.
- **Tags that did not parse**, on the report or on a `given:` declaration, and renderer tags the validator
  rejects.

That is the shape of the list rather than the whole of it: the findings on the package page are the
authoritative set. Findings carry a `severity`, but `warn` is the ordinary default, so read the text, not
the count. One message changes what the rest of the list means: **"Dashboard lint stopped early, so this
list is incomplete"**. A tag that does not parse is reported for **syntax** only, and no finding carries a
position, so an empty result is not proof that a tag is well formed.

**A file that does not compile fails the package, rather than appearing as a broken report.** The reload
answers `424` with the compile error and the previously compiled package keeps serving, so the listing
still answers 200 and looks healthy while your edit has not taken effect. `GET /api/v0/status` still knows:
a package serving an older model than its files appears in `loadErrors` with `stale: true` and the compile
message, and the entry clears on the next reload that compiles.

## Serving, URLs, and the API

| Path                                                    | What it is                                                                                                                             |
| ------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| `/<env>/<pkg>/<path>`                                   | The Console page, `<path>` being the report's package path without `.malloy`                                                           |
| `/<env>/<pkg>/<path>?CATEGORY=Outerwear`                | The same page, filtered: control state is URL state                                                                                    |
| `/<env>/<pkg>/<path>?edit`                              | The same report in the builder. `<path>/edit` is a legacy alias, redirected here unless a report is at `<path>/edit.malloy`            |
| `GET /api/v0/environments/<env>/packages/<pkg>/reports` | Every report: `path`, `name` (label), `layout`, `title`, `description`, `editable`, and the load `error` for one that does not compile |
| `GET …/reports/{path}`                                  | One report's manifest: title, `layout`, autorun, columns, controls, tiles; for a `.malloynb`, its cells                                |
| `PUT …/models/{path}`                                   | Write a report into the package and reload; the builder's save ([The write path](#the-write-path))                                     |

`{path}` is the `path` the listing returns. A report's tiles run through the ordinary query endpoint,
`POST …/models/{path}/query`, against the report's own path, with `givens` in the request body. There is no
report-specific execution path, so authorization, row limits and query caps behave exactly as they do
everywhere else. A tile's `query` is an expression, so send it as `{"query": "run: <it>"}`. Skip `kind`
`text` tiles, which have nothing to run.
[ai-agents.md](ai-agents.md) has the REST playbook.

**Deprecated aliases.** `GET …/dashboards` and `GET …/dashboards/{name}` answer with the grid reports, the
item resolving a unique basename and answering `409` naming every match when it is ambiguous (the package
warns about each such name at load, `report-name-ambiguous`);
`GET …/notebooks`, `…/notebooks/{path}` and `…/notebooks/{path}/cells/{i}` answer for the linear reports. An
old Console link `/<env>/<pkg>/dashboards/<name>` or `/notebooks/<name>` still opens: a file that is there
opens at its path, and otherwise a unique basename redirects to the report's path. All of these are removed
in a later release; use `…/reports`.

After editing a report file, `GET …/packages/<pkg>?reload=true` recompiles the package in place, and a
reload that fails to compile leaves the previously compiled model serving. [AGENTS.md](../AGENTS.md) §6
covers the edit loop and watch mode.

<a id="the-write-path"></a>

### The write path

`PUT …/models/{path}`, body `{"source": "…", "expectedHash": "<sha256 of the text you read>"}`, writes a
report into the package and reloads it. One rule for both layouts:

- **Create** (omit `expectedHash`) at any `.malloy` path in the package that does not exist yet, outside
  `public/` and never at the root `index.malloy`. Missing directories are created inside the package. The
  text must carry an artifact tag. A file already at that path answers `409`.
- **Replace** (with `expectedHash`) only a file with an artifact tag: a report the package serves, or a
  tagged file it refuses as one (a retired cell-format file, a `kind=` tag), so that file can be rewritten
  as tiles in place. The endpoint cannot overwrite a model, an untagged file, a `.malloynb` or `index.malloy`, wherever they live.
  A file that changed since you read it answers `409`, and nothing is merged.
- The text is compiled first (at `scope: "file"`) and refused with its problems when it does not compile.
  The write is atomic under the package lock, the package reloads in place, and the previous text is
  restored if the reload does not take it or the file is not then a served report. A text that declares a
  real `#(authorize)` or `#(access_filter)` gate outside prose is refused with `400`: gates live in the model.
- `403` under `frozenConfig`.

Two reports may share a basename in different directories; there is no name-collision `409`.

<a id="editing-in-the-console"></a>

## Editing in the Console

Every report page has an **Edit** button in the header, beside the breadcrumbs, and the package page has a
**+ Report** button: pick a model, a source and its view, a title and a layout (linear by default), and
the file is written as `<slug>.malloy` at the **package root**, or at another location you type, and opened
in the builder. No directory is ever created for you.

From there it is the classic loop. The builder's controls sit in the header, where Edit was: the save
status, undo and redo, an **Add** menu (a tile, text or a filter) and **Done**. The page
under them stays the report as it reads. On a grid, **drag a tile by its heading or padding to place it in any cell, empty space included,
drag any edge or corner to resize it, pick its chart and clickable cells from its ⋯
menu, and add filters from Add or with + Filter on the filter row.** A press inside a tile's result (its
table or chart) is the result's, never a move. A tile draws no handles: over an edge the cursor becomes a
two-headed arrow, and over a corner a diagonal one. The right and bottom edges set the width and height;
the left and top edges move that edge and keep the opposite one where it is (on a report that flows, the
first such drag places every tile where it is drawn, as a move does); a corner moves its two edges at
once. The width snaps to whole columns, with the columns drawn
behind the tiles and a label saying the size during the drag; the right edge is also a keyboard control (Tab to
it, Left and Right step one column, Home and End go to one column and full width). The height snaps to whole
rows the same way; on the bottom edge Up and Down step one row, Home and End go to the shortest and tallest, and
Delete, or a double-click on the edge, puts the tile back to auto height. The drag also snaps to auto
when that is nearer than a whole row: a row of auto tiles is as tall as its tallest result, which is
rarely a whole number of rows, so this is how a tile is dragged level with an auto neighbour. Both are written on the
tile's entry in `tiles=[…]`, so a tile on a view the report imports resizes like any other. A move
snaps to rows and columns, with both drawn behind the tiles, and pushes any tile it lands on straight
down; so does a resize. Nothing is pulled up to fill a gap until you ask: **Arrange tiles** in the bar
holds **Pack tiles upward**, which slides every tile up into the space above it, and **Let tiles flow**,
which clears every tile's place and keeps its size. While a tile of a placed report is selected, the
rows and columns show faintly behind the tiles, so empty space reads as somewhere to put one; with
Option/Alt held, the arrow keys move the selected tile one cell; a step into the tile beside it swaps the two, and anything else it lands on is pushed down. The
fewest rows offered are 3 for a chart (it needs room to draw its plot), 2 for value cards or a table, and 1
for a text tile; which a query tile is comes from what it draws. A row of
value cards in a tile set shorter than it needs is drawn compact (smaller figures, tighter cards, more
to a row) so it fits instead of scrolling; a map draws at its own size, so give it the height it needs.
A visually hidden handle keeps keyboard moves (Space picks the tile up, the arrows move it, Escape cancels).
On a linear report tiles are dragged into a new order, and the **+** on a tile inserts one between two.
The ⋯ menu holds **Chart** and **Color**, two submenus, **Clickable cells…** and **Duplicate**, which puts a copy right after the tile as a view of its own; a tile's size is set by dragging its
edges and corners (or focusing its right or bottom edge and pressing the arrow keys), and a tile is removed from the × on its corner. Titles, subtitles, descriptions and text tiles are click-to-edit where
they stand, with a faint wash under the pointer: in a one-line field Enter keeps the edit and Escape puts
the old text back; a text tile opens in place in the report's own type, where leaving it or Cmd/Ctrl+Enter
keeps it, Escape drops it, and Cmd/Ctrl+S saves from inside the field.

**The layout is the file's.** The builder draws the report in the layout its tag declares and never
rewrites `layout=`; to turn a linear report into a grid (or back), edit the tag in the file. A file's own
`dashboard { columns=N }` and `autorun=false` are kept and still apply, but the builder does not set them. Adding a tile offers every source the package publishes, and when the file
cannot already see the chosen source the builder adds a named import for it, at the right relative path for
the report's location.

The **Chart** choices are Default (as set in the model), Table, Line, Bar, Big value, Scatter, Shape map,
Segment map and Bar and line. A choice the view cannot render stays in the list, greyed, with its reason under
it: Big value needs results that are only totals, a map needs a map chart set in the model, and Bar and line
(`# combo_chart`) needs a view with a group by and exactly two totals, drawing the first as bars on the left
axis and the second as a line on the right. To pick which totals go where, write
`# combo_chart { y=orders y2=revenue }` on the view by hand. A chart line the builder
does not model, such as `# bar_chart { size=spark }`, is kept byte for byte, and Chart is disabled for that
tile with the reason shown; so is a tile inherited from a declaration on the source.

**Color** sets a bar, line, scatter or bar-and-line chart's color: the theme's own (the default), one of
the theme's series colors, or any other through the platform's color picker. It is written above the tile
as the per-chart theme line `# theme.palette.series=["#1f6aa8"]`, the same annotation a model can carry
(see [Theming](theming.md)), so it reads back when the file is reopened. A line listing a whole
palette is the author's, and the builder leaves it as written.

A chart's axes are titled from each field's `# label`; a field with none is titled from its name written as
a sentence (`total_revenue` reads "Total revenue"), on charts only, so a table keeps the column names its
author chose. A plotted chart is drawn no narrower than 320px, and a bar chart no narrower than it takes to
draw every bar, up to 4000px; in a narrower tile it scrolls sideways rather than dropping bars. A chart with
more bars than fit in 4000px still has the rest dropped under "Showing limited results".

A grid report with `tiles=[]` (only possible by hand-editing) opens in the builder but is not served until it
has a tile, and the builder will not remove the last tile of a report saved with tiles. A filter on a
dimension reached through a join keeps its full dotted path (`products.category`), written quoted. On a
screen narrower than 600px the Console hides **Edit** and **+ Report**, and an editor opened by URL asks
for **Edit anyway** first.

What makes it different from a classic BI tool is what the editing produces. There is no proprietary layout
document: the builder reads and writes the report file described above, splicing your changes into it rather
than regenerating it, so comments and anything it does not model survive the round trip. The result is a
source file you can review in a pull request, and one an agent can write by hand just as well.

Edits are kept as a draft in this browser while they are made (the status reads **Draft saved**), so
leaving the builder loses nothing; opening the report for editing again restores them. **Done** writes
them at once through `PUT …/models/{path}`, with no review step, and returns to the read-only page; a
refused write keeps the builder open with its reason. Cmd/Ctrl+S writes without leaving, and **Discard
edits** on the status goes back to the saved report. On a server that does not take writes (`frozenConfig`), Save keeps the edit in this
browser instead, keyed by the report's path, and the package page lists those drafts.

<a id="rendering-one-in-your-own-react-app"></a>

## Rendering one in your own React app

`@malloy-publisher/sdk` exports a `Report` component that renders a `.malloy` report of either layout from
`GET …/reports/{path}`, running every tile and `run:` cell through the query endpoint. `<Dashboard>` and
`<Notebook>` stay exported as deprecated wrappers over the same renderers, so an existing integration keeps
working, and `<Notebook>` is the one that renders a legacy `.malloynb`: its cells run only through the
deprecated notebook routes, which `Report` never calls. The Console is one consumer of these components rather than their
home: they take props instead of reading a router, so the host decides what a drill and a filter change
mean.

The component reads its server through context, so it has to sit inside a `<ServerProvider>`, mounted once
near the root of your app. Add `versionId` to the URI and every request it makes carries it: the manifest,
each tile's query, and each control's suggest query. Publisher itself answers `501 Not Implemented` to a
`versionId` on any route, so this is for a host that resolves versions at its own routing layer.

The example below uses the deprecated `<Dashboard>` wrapper, whose props stay stable until it is removed:

```tsx
import {
  Dashboard,
  encodeResourceUri,
  ServerProvider,
} from "@malloy-publisher/sdk";

<ServerProvider baseURL="https://publisher.example.com/api/v0">
  <Dashboard
    resourceUri={encodeResourceUri({
      environmentName: "examples",
      packageName: "storefront",
    })}
    dashboard="overview"
    givens={Object.fromEntries(searchParams)}
    // MERGE, do not replace. `managed` is every given this report declares,
    // including the ones holding no value right now, so a control the reader
    // CLEARED can be told from a parameter that was never yours.
    onGivensChange={(next, managed) =>
      setSearchParams(
        (current) => {
          for (const name of managed) {
            if (!Object.prototype.hasOwnProperty.call(next, name)) {
              current.delete(name);
            }
          }
          for (const [name, value] of Object.entries(next)) {
            current.set(name, value);
          }
          return current;
        },
        // Filtering is not a navigation step.
        { replace: true },
      )
    }
    onNavigate={(target) =>
      navigate(
        `/reports/${encodeURIComponent(target.path)}` +
          `?${new URLSearchParams(target.givens)}`,
      )
    }
  />
</ServerProvider>;
```

`target.path` is the drill's `to=` as written (`target.dashboard`, the same value, is deprecated).

Without `onNavigate`, drilling to another report is inert, and those cells do not read as clickable either,
so a host that has not wired navigation shows no affordance rather than a dead one. `to=self` still works,
since it never leaves the component. The Console's own report page is the worked example of both handlers.
Embedding into a **non-React** host page is a follow-up
([#931](https://github.com/malloydata/publisher/issues/931)); an [HTML data app](html-data-apps.md) remains
the surface with the complete embedding story.

## Editing one in your own React app

`<ReportEditor>` is the builder, from `@malloy-publisher/sdk/builder` (the main entry stays free of the
Malloy parser): the same editor the Console's `?edit` route mounts, keyed by the report's `path` (the
editor reads a file's layout by [Which layout](#which-layout), never by its folder, and never changes
it), plus `onEvent` (the `report.*` events; see the SDK README's
[Telemetry](../packages/sdk/README.md#telemetry) section) and `onDirtyChange`. It needs the same `<ServerProvider>`, and a
`<DocumentStorageProvider>` besides if the host wants a browser draft offered back when the package cannot
be written (see the SDK README's [Document Storage](../packages/sdk/README.md#document-storage) section).

The editor saves in place and, by default, draws no way out of itself: leaving is the host's. Draw your own
exit and guard it by tracking `onDirtyChange`, which reports whether anything is unsaved (an open text edit
counts), and blocking navigation while it is `true`. A host that would rather the editor drew its own way out
passes `onExit`: the toolbar then shows **Close** after Save, and Close on unsaved work asks first.

The editor's page-level controls can live in the host's own chrome instead of on the page. Pass
`onControlsChange`, and the editor draws no toolbar: it hands over a `BuilderControls` whenever they change
(the save status and `save`, `undo` and `redo`, the `layout` and its switch, the `add` actions for a tile,
text or a filter, and `exit` when `onExit` is set), with actions whose identity never changes, and
`undefined` when it goes away. Draw them with `BuilderBar` (`controls`, and `onDone` for a Done button), as
the Console does in its header, or place each one where your chrome has room: Save in your header, Add and
Layout in your own ⋯ menu. A host that switches between reading and editing from its own menu needs no Close:
switching to reading is the exit, through the same `onDirtyChange` guard.
`versionId` on the URI pins every read the editor makes; it never reaches the write, and Save turns itself
off against a pinned version rather than opening onto a compare-and-swap it can never win.

`queryVersionId` pins the editor's queries without pinning the document. The tiles, the manifest, filter
options, the report listing and the field catalog all run at that version. The document is still the one
`resourceUri` and `path` name, and it alone decides the kept draft, where Save writes, and whether Save is a
pinned one. Use it for a package that is republished on every change: open the editor on an unversioned
`resourceUri` with the version the reader ran at as `queryVersionId`. Edit then reuses the reader's cached
results instead of running every tile again, and kept edits survive the next republish. A text-held
document's compile takes no version, so only its tiles and options follow `queryVersionId`.

The other exports of `@malloy-publisher/sdk/builder`: `NotebookEditor` (a deprecated wrapper over
`ReportEditor`), `DashboardEditor` and `DashboardBuilder` (deprecated aliases of `ReportEditor` and
`ReportBuilder`, with their `*Props` types), `createRoute` (whether a host can create reports: `"package"`, `"storage"`, or
`undefined`), `useDocumentChoices` (the models and views the New dialog offers), `createDocument` (writes
the new file at the package root unless given a `directory` or `path`, always with `layout=` in the tag, and
returns its package path as `CreatedDocument.path`; a host store that files documents somewhere else
implements `DocumentStorage.resolveDocumentPath` so the new file's imports are written for where it really
lands), `reportPathFor` / `reportPathForTitle` (a new report's file at the package root or in a directory), the
template
builders that produce a new report's starting text, and `locatorFor` (a created
report's address in a host's own store, which is its path).

<a id="documents-held-as-text"></a>

## Documents held as text

A host that keeps reports in its own store, rather than as files in a package, still gets one format (a
`.malloy` file with an `## artifact` tag), one parser (Publisher's) and one viewer and editor (the SDK
builder). Each person who opens the report runs it as themselves.

`POST …/models/{path}/compile` at scope `append` reads the submitted text as a report when it carries a
model-level `## artifact` tag, and answers with a `document` beside `status` and `problems`: its `layout`
(by [Which layout](#which-layout)), the `manifest` the same text would serve once saved, and the file's own
`cells`. It is read from the submitted text alone, on top of the model in the URL, so the model's own `run:`
statements and `##` notes never join the document. Nothing runs, including a control's `suggest` query.
Problem positions are lines of the submitted text. The text carries no `import`, `##!` or `given:`, which
`append` refuses; the model supplies them.

**A tile or cell the caller may not read is not compiled.** Where `#(authorize)` denies the caller a tile's
source, that tile comes back `restricted: true` and the rest of the document compiles. A restricted tile
carries its expression and nothing derived from the source (no `givenNames`, no layout tags), and a
restricted cell is still listed. No diagnostic is returned for either, because a gated source's columns would
otherwise leak through the error text. A source defined from a gated one in the same document is restricted
along with it. A tile over a source off the package's query surface is an error problem
(`query-not-queryable`) and no `document`, judged on the text, on the source the compiled query reads, and on
the sources the document's own definitions join, so a document that compiles also runs on a curated package.
In a model with an `#(authorize)` gate, a definition whose base is hidden, hidden and locked, or absent answers
the same generic 404 whether or not a tile uses it. A tile expression is checked for the constructs `append`
refuses (`duckdb.table(…)` and the like) before anything compiles it, and refused with a 400; a tile that does
not parse, or that names a view or a source that does not exist, is a `tile-does-not-compile` problem carrying
the reason and a fix, and the document still comes back. A cell that names a source both gated and hidden from
the caller answers 404 for the whole request.

A given declared `#(secure)` carries `secure: true` on `Given`: its value is the host's to set. The builder
withholds its control only when it edits a document held as text; a saved file's givens show as before.

Compile at scope `append` refuses document text that writes `# image`, `# link` or markup in a `# label`, reads
`@env.` in an annotation or a `##` note (the document's own tag included), writes an annotation the tag parser
rejects or that exceeds 8,192 characters, frees a column with `except:` or `rename:` and then declares one of
the same name in the same extend chain, or carries more than 1,000 annotations or 64 KB of annotation text.
Plain `append` text without that tag is the caller's own compile and keeps those tags, and so does a model file.
`# label` is refused only when it holds markup that a `>` closes (`<b>x</b>`, `<!-- -->`); `a<b` is text. It
decides what is a document from the lexer's notes, so a tag after code on its line counts, and reads the parsed
annotations, so quoting, backticks and `#|` blocks do not get around it. In text-source mode the SDK also
removes `# image`, `# link` and markup-bearing `# label` properties from the result annotations it draws. Neither
is a complete defense: the renderer has other sinks that draw a data value as HTML or a URL with no tag at all,
and the except-and-redeclare refusal is partial, so a host should also run the viewer under a
Content-Security-Policy.

The SDK reads this with `<ReportEditor textSource={{ modelPath, hiddenGivens }} />` (the text comes from the
host's authoritative `DocumentStorage`) and `<DashboardView preamble runModelPath hiddenGivens />`. Every tile,
cell and control option runs as the document's definitions followed by one `run:`, sent to `modelPath`. Use the
package's `index.malloy` as `modelPath`: a base file the package hides is refused when the document compiles and
answers 404 when it runs. A restricted tile, or a tile the server answers 403, shows "You don't have access to
this data". The compile refuses a `given:` in held text, so "Add filter" is off and only the model's givens can be
bound, unless the host passes `textSource.compileSource`. That function returns the text to compile in place of
the document's own: the host removes the document's `given:` declarations and `##!` lines and writes each
`$NAME` as a literal value (`malloyLiteral` and `localGivenSpec` are exported for this). With it, "Add filter" is
on, the editor still saves the document's own text, and the builder runs each tile with the document's filter
values written in as literals, never sent to the server as givens. The saved text stays a report the package can
serve as it is. Pass `textSource.takenGivenNames` (every given the model declares) so a filter the builder adds
never takes a model given's name, and give the reader `<DashboardView literalReads />`, the document filters each
tile reads by its `tile.query`, so a tile filtered that way is not marked as ignoring them. `hiddenGivens` names givens the host sets itself, which get no control, and `givens` carries their
values. A gate whose given the request did not send denies, so a host that never sends them sees every tile on a
gated source as restricted.

## Where reports stop

Whatever renderer tags can express is the ceiling. There is deliberately no code escape hatch (no custom
component, no authored JavaScript) because a page that needs to go past the tags is an
[HTML data app](html-data-apps.md), which already does that job at whole-page scope with a real runtime and a
real embedding story. The reasoning, including why the sandboxed-component approach was built and then cut, is
in [malloyyo-dashboards-design.md](malloyyo-dashboards-design.md#custom-jsx-components-cut).

## See also

- [choosing-a-surface.md](choosing-a-surface.md): a report, and which layout, against an HTML data app
- [givens.md](givens.md): the parameter mechanism the controls are built on
- [discovery-and-access.md](discovery-and-access.md): the package surface a report reads
- [reports-design.md](reports-design.md): the design that made dashboards and notebooks one document type
- [malloyyo-dashboards-design.md](malloyyo-dashboards-design.md): the original design, and the grammar's
  provenance
- [console.md](console.md): the rest of the Publisher Console
- [`examples/storefront`](../examples/storefront), the ecommerce model this page's examples build on
