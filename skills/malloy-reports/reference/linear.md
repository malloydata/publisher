<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Linear layout

A report with `layout=linear` renders its tiles one column wide, top to bottom, in `tiles=[…]` order: a question framed in prose, a query that answers it, the next question. Everything not about placement (tiles, text tiles, givens, drill, lint) is in the skill itself.

## A linear report as tiles (the form to author)

```malloy
##! experimental.givens
##" One category at a time: how it sells across the year and which brands carry it.
##| artifact { layout=linear title="Category review"
  tiles=[
    intro { kind=text },
    "revenue_trend",
    "brand_ranking"
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

# viz=line
# label="Revenue by month"
query: revenue_trend is order_items -> sales_by_month + { where: category ~ $CATEGORY }

# viz=bar
# label="Top brands"
query: brand_ranking is order_items -> top_brands + { where: category ~ $CATEGORY }
```

A tile that needs nothing of its own (no title, chart or filter) is just its entry, `"order_items -> sales_by_brand"`, with no query. The report declares no source: its tiles are served views and queries over them.

- **`layout=linear` is required** when the tag lists tiles; without it the file is read as a grid.
- `dashboard { columns }` has no effect, and a `colspan` or `break` on a tile entry is ignored with a warning (`notebook-tile-layout-ignored`).
- **Tiles run through the model query endpoint**, against the report's own path, like a grid's.
- **A tile grows to fit what it holds, except a table.** KPI cards and lists show in full and a chart draws at its default height. A table stops at 700px and scrolls inside its tile with its header row pinned. No tag changes that for a table tile.

## Files written as `run:` cells (retired)

An older linear form was a file of `run:` statements with `(markdown)` notes between them, under a tag with no `tiles`. Publisher no longer reads it: such a file is not a report, and the package warnings name it (`report-cells-removed`). Never write one. To bring one back, rewrite it as tiles:

1. Each floating `##(markdown)` note or `##|(markdown)` block becomes a named `##|(markdown) name` block, listed as `name { kind=text }`.
2. Each `run: <source> -> <view>` becomes a tile. With no render tags, list it as `"<source> -> <view>"`. With tags, make it `query: <name> is <source> -> <view>` with the tags above it, and list it by name; a `#"` caption becomes `# label="…"`, and a chart tag becomes `# viz=…` (`skill:malloy-charts`). A `run:` of a named `query:` is listed by the query's name.
3. List the entries in `tiles=[…]` in reading order, keep `layout=linear`, and delete every `run:`.

## Multiple models and cross-model joins

A report compiles with **all** its `import`s in scope, so it can import several `.malloy` models, and a tile's source can reference, and **join**, sources from different imported files. Import each model you need at the top; no re-export wrapper is required.

**When the package root holds an `index.malloy`, tiles read only what it exports.** A tile over a source `index.malloy` does not export answers 404, even when the report imports that source's file. A source a report derives from an exported one (`source: mine is customers extend { ... }`) still works, though a report should not need one: refine the tile's query instead. A tile's own source over a raw table (`duckdb.table(...)`) is refused. To use a source, add it to the `export { ... }` in `index.malloy`.

## Style

**Build a story.** Each query should reveal something that motivates the next one. Start broad, then drill into what is interesting. Frame questions in markdown; let queries answer them.

**No data-specific insights in markdown.** Data refreshes will make them stale.

```malloy
##|(markdown) spike_note
WRONG: will become stale when data refreshes. Revenue spiked 23% in March.
|##

##|(markdown) trend_q
RIGHT: frames the question and lets the query answer it. How is revenue trending?
|##
```

Each is a text tile, listed in `tiles=[…]` by name, for example `trend_q { kind=text }`.

To add `limit`, `where`, or other options to an existing view, refine it with `+` in the tile's query: `query: top is orders -> my_view + { limit: 15 }`.

One query per tile, and remember a chart renders only the **first** aggregate in its result.

## Existing `.malloynb` files

A `.malloynb` (cells delimited by `>>>markdown` and `>>>malloy`) is a deprecated format, served read-only as a linear report. **Do not create one.** When maintaining one: its compile errors show only when cells run, so test queries in the model first; imports belong in a setup cell at the top, never inside a `run:` cell; and it reads `## autorun=false` and `## givens { … }` as file-level lines, where a `.malloy` report takes them inside its artifact tag.

### Converting a `.malloynb` to a `.malloy` report

The builder cannot edit a `.malloynb` (`?edit` on one says so), so convert it to edit it. Write a new `.malloy` file beside it, check it loads, then delete the `.malloynb`.

1. **The setup cell** (its `import`s, and any `given:` it declares) goes to the top of the new file, under `##! experimental.givens`. File-level `## autorun=false` and `## givens { … }` move inside the artifact tag: `##| artifact { layout=linear autorun=false givens { … } tiles=[…] }`.
2. **Each `>>>markdown` cell** becomes a named text tile, `##|(markdown) <name>` … `|##`, listed as `<name> { kind=text }`. Keep the text as written.
3. **Each `>>>malloy` cell with `run: <source> -> <view>`** becomes the entry `"<source> -> <view>"`. If the cell is `run: <source> -> <view> + { … }`, it becomes `query: <name> is <source> -> <view> + { … }`, listed as `"<name>"`.
4. **Each `run: <source> -> { … }` cell** (an inline stage, the common case) becomes `query: <name> is <source> -> { … }`, listed as `"<name>"`. Copy the stage exactly as written. The builder edits this form fully: chart, colour, title and filters go above or after the stage, which it never rewrites. A filter the cell applied with a given becomes `+ { where: field ~ $GIVEN }` after the stage.
5. **A cell's `#` tags** (a chart, `# label`) go on the `query:` line above it. A `# chart` tag becomes the builder's `# viz=bar|line`.
6. **A `run:` that pipes to a second stage** (`-> { … } -> { … }`) becomes a `query:` too, but the builder only shows it, and a click on its values opens no rows. To make it editable, keep only the first stage, or move the pipeline into the model as a view.
7. **List the tiles in the cells' order** in `tiles=[…]`, then reload and read the lint. Any finding is a conversion mistake.

```malloy
##! experimental.givens
##| artifact { layout=linear title="Q3 review"
  tiles=[intro { kind=text }, "by_carrier"]
}
|##
import { flights } from "flights.malloy"

##|(markdown) intro
How each carrier did this quarter.
|##

# viz=bar
# label="Flights by carrier"
query: by_carrier is flights -> {
  group_by: carrier
  aggregate: flight_count
}
```

## Common mistakes

| Mistake                                                                    | Fix                                                                                             |
| -------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| A tile list without `layout=linear`                                        | Read as a grid. Write `layout=linear`.                                                          |
| A linear tag with no `tiles=[…]`                                           | The retired cell format: no report (`report-cells-removed`). List the tiles.                    |
| `view_name { limit: 10 }`                                                  | Use `+`: `view_name + { limit: 10 }`                                                            |
| Floating `##(markdown)` prose in a tiled report                            | Never shown (`notebook-markdown-block-unnamed`). Make it a named text tile and list it.         |
| `## Heading` for a section title                                           | That line is model tags, not prose. Write the heading inside a `##\|(markdown) name` text tile. |
| `##\| markdown` or `##\|markdown` (no parentheses)                         | Write `##\|(markdown) name`.                                                                    |
| Text on the opener line after `##\|(markdown)`                             | Only the tile's name may follow it. Put the text on the next line.                              |
| `run:` in a report that lists `tiles=[…]`                                  | Never shown. List `"source -> view"` in `tiles` (or a `query:` by name), delete the `run:`.     |
| `tiles=[intro { kind=text }, orders -> headline]` (entry not quoted)       | Every `source -> view` entry is a quoted string. Unquoted, the tag does not parse.              |
| A bare `intro` in `tiles` for a `##\|(markdown) intro` block               | A bare name is the query-tile form for a `query:`. Write `intro { kind=text }`.                 |
| `colspan` or `break` on a linear report's tile entry                       | Ignored and reported (`notebook-tile-layout-ignored`). Remove them, or switch to `layout=grid`. |
| `#(filter)` in any form                                                    | Deprecated. Declare a `given:` on the model's source instead.                                   |
| Data-specific insights in a text tile                                      | Frame questions instead.                                                                        |
