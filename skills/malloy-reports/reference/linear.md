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
    "category_tiles -> revenue_trend",
    "category_tiles -> brand_ranking"
  ]
}
|##
import { order_items } from "storefront.malloy"

#(description="Narrow to one product category")
# label="Category" control=select suggest { source=order_items dimension=category }
given: CATEGORY :: filter<string> is f''

##|(markdown) intro
## Category review

Pick a **Category** in the controls above and every chart below re-runs for it.
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

- **`layout=linear` is required** when the tag lists tiles; without it the file is read as a grid.
- `dashboard { columns }` has no effect, and a `colspan` or `break` on a tile entry is ignored with a warning (`notebook-tile-layout-ignored`).
- **Tiles run through the model query endpoint**, and the report also carries cells made from its tiles, so cell runs and `get_context` work on it.

## Reports written as `run:` cells (the older form)

A linear report can also be a file of `run:` statements with `(markdown)` notes between them. Publisher still reads and serves it, and a person saving it in the Console converts it to tiles. Write new reports as tiles; maintain a cell report in its own form until someone converts it. `skill:malloy-analysis-report` writes this form for a quick write-up.

### The five authoring rules (cell reports)

Rules 1 and 4 hold for both forms; rules 2, 3 and 5 are about `run:` cells and prose notes.

1. **The header is `##!` flags, `//` comments and unnamed `"` notes (the description), then `## artifact { layout=linear … }`.** Nothing else goes above the tag. A statement or a tag above `## artifact` is an error, and the report is served with that error instead of opening.
2. **Prose is a `(markdown)` annotation, and the number of `#` says what it belongs to.** Floating markdown is a cell of its own: `##(markdown) text` (contiguous lines with no blank line between them merge into one cell) or a `##|(markdown)` block, with the body starting on the next line and the `|##` closer at the opener's column. Attached markdown, `#(markdown) text` or a `#|(markdown)` … `|#` block, belongs to the statement below it and renders with that cell. Keep the parentheses: `##| markdown` reads its prose as ordinary tags, and `##|markdown` draws a malformed-route warning. No body line starts with `|##`, and a blank line follows the closer. A `## Heading` line is model tags, not prose: write a heading inside a `##|(markdown)` block.
3. **Render tags sit directly above `run:`, with nothing between.** A `#"` directly above the `run:` is its caption; `# bar_chart`, `# label="..."` and the other render tags go in the same block. Attached `#(markdown)` goes above the `#"` caption when both are present, and renders as a header above the result.
4. **`given:` uses `NAME :: filter<T> is f''`, bound with `~`, declared before first use.** Put `##! experimental.givens` at the top of the file.
5. **Trailing prose is `##(markdown)`, never `#(markdown)` or `#"`.** Both belong to the statement below them, and at the end of a file there is none. An `import` or an `export` takes no annotation either, so prose above one is `##|(markdown)`.

### How the file becomes cells

Cells come from the file's own notes and statements, in file order (imported files add none):

| In the file                                                              | Cell                                                                   |
| ------------------------------------------------------------------------ | ---------------------------------------------------------------------- | --------------------------------------------------------- |
| `##!` lines, `## artifact`, and every `"` note above the tag             | the header: no cell                                                    |
| `##(markdown)` lines (contiguous) or a `##                               | (markdown)` block, after the tag                                       | one markdown cell                                         |
| `#(markdown)` or a `#                                                    | (markdown)`block above a`run:`, `source:`, `query:`, `given:`or`type:` | no cell of its own: it renders with that statement's cell |
| `run:` with its contiguous `#` tag block, including `run: <named query>` | one query cell                                                         |
| `import`, `source:`, `query:`, `given:`, `export { … }`, `type:`         | one definition cell **per statement**                                  |
| a non-`"` `##` note after the tag (`## title=…`)                         | not a cell                                                             |

Definition cells render folded; only query cells run. Earlier spellings are still read: `##"`, `##|"`, `##(text)` and `##|(text)` notes below the tag are markdown cells, like `(markdown)`. Write `(markdown)`. A query cell runs as a _restricted query_ against the already-compiled file, so an `import` inside a `run:` is rejected (`file imports are not permitted in a restricted query`).

### A complete cell report

```malloy
##! experimental.givens
##" One category at a time: how it sells across the year and which brands carry it.
## artifact { layout=linear title="Category review" }
import { order_items } from "storefront.malloy"

##|(markdown)
## Category review

Pick a **Category** in the controls above and every chart below re-runs for it.
Leave the control empty to read the whole catalog.
|##

#(description="Narrow to one product category")
# label="Category" control=select suggest { source=order_items dimension=category }
given: CATEGORY :: filter<string> is f''

##(markdown) How is revenue trending for the selected category?

#(markdown) ### Revenue by month
#" Revenue by month for the selected category
# line_chart
# label="Revenue by month"
run: order_items -> sales_by_month + { where: category ~ $CATEGORY }

##(markdown) Which brands are behind those numbers?

#" The eight brands with the most revenue in the selected category
# bar_chart
# label="Top brands"
run: order_items -> top_brands + { where: category ~ $CATEGORY }

##(markdown) Last, the best-selling products, defined once as a named query and then run.

query: top_products_in_category is order_items -> top_products + { where: category ~ $CATEGORY }

#(markdown) ### Best sellers
run: top_products_in_category
```

The `##|(markdown)` block and each `##(markdown)` line are floating markdown cells. The two `#(markdown)` lines are attached: each renders as a header with the `run:` below it. The `given:` and the `query:` are definition cells, and each `run:` is a query cell.

### When the Console converts it

Saving a cell report in the builder turns each `run:` into a `view:` on a `<source>_tiles` extension appended to the file (a `query:` used by exactly one run is folded into its view), each prose note into a `##|(markdown) text_N` block, and leaves definitions, imports, givens and comments where they were. The first Save asks before it rewrites the file ("Convert this notebook?"); Cancel writes nothing, and the file's history in its repository is the way back. Write a cell report so it converts:

- Every `run:` is `<source> -> <view or query body>` with a named source. An inline `extend` before the arrow, a source that is not a name, or a refinement (`q + { … }`) of a multi-stage query is refused, and so is a run that resolves through more than ten named queries.
- A chart line the editor does not model (for example `# bar_chart { size=spark }`) is kept, and the Viz type picker is disabled for that tile with the reason shown.
- The editor opens a report **read-only**, saying why, when it cannot place cells one by one: two statements or notes on one line, text after a block closer, a comment straddling two cells, a lone carriage return (use LF or CRLF), a statement above the `## artifact` tag, a statement no cell can hold, or a tag value Malloy cannot read, such as a malformed date literal (`@2024-13-01`).

## Multiple models and cross-model joins

A report compiles with **all** its `import`s in scope, so it can import several `.malloy` models, and a tile's source or a cell can reference, and **join**, sources from different imported files. Import each model you need at the top; no re-export wrapper is required.

**When the package root holds an `index.malloy`, tiles and cells read only what it exports.** A tile or cell over a source `index.malloy` does not export answers 404, even when the report imports that source's file. A source the report derives from an exported one (`source: mine is customers extend { ... }`) still works. A cell's own source over a raw table (`duckdb.table(...)`) is refused. To use a source, add it to the `export { ... }` in `index.malloy`.

## Style

**Build a story.** Each query should reveal something that motivates the next one. Start broad, then drill into what is interesting. Frame questions in markdown; let queries answer them.

**No data-specific insights in markdown.** Data refreshes will make them stale.

```malloy
##(markdown) WRONG: will become stale when data refreshes. Revenue spiked 23% in March.

##(markdown) RIGHT: frames the question and lets the query answer it. How is revenue trending?
```

To add `limit`, `where`, or other options to an existing view, refine it with `+`: `my_view + { limit: 15 }`.

One query per tile or cell, and remember a chart renders only the **first** aggregate in its result.

## Existing `.malloynb` files

A `.malloynb` (cells delimited by `>>>markdown` and `>>>malloy`) is a deprecated format, served read-only as a linear report. **Do not create one.** When maintaining one: its compile errors show only when cells run, so test queries in the model first; imports belong in a setup cell at the top, never inside a `run:` cell; and it reads `## autorun=false` and `## givens { … }` as file-level lines, where a `.malloy` report takes them inside its artifact tag.

## Common mistakes

| Mistake                                                                    | Fix                                                                                                                                                                  |
| -------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- | ------------ | ------------ |
| A tile list without `layout=linear`                                        | Read as a grid. Write `layout=linear`.                                                                                                                               |
| `view_name { limit: 10 }`                                                  | Use `+`: `view_name + { limit: 10 }`                                                                                                                                 |
| A statement or a `#` tag above `## artifact`                               | Only `##!` flags, `//` comments and unnamed `"` notes go in the header. Move the statement below the tag.                                                            |
| `## Heading` for a section title                                           | That line is model tags, not prose. Write the title inside a `##                                                                                                     | (markdown)` block.                                                                                   |
| `##                                                                        | markdown`or`##                                                                                                                                                       | markdown` (no parentheses)                                                                           | Write `##    | (markdown)`. |
| `##(markdown)text` (no space after the route)                              | Malloy drops the note. Write `##(markdown) text`.                                                                                                                    |
| Text on the opener line after `##                                          | (markdown)`                                                                                                                                                          | Only a name may follow it, and only a text tile needs one. Put the text on the next line.            |
| `#(markdown)` above an `import` or `export`                                | Those take no annotation. Write `##                                                                                                                                  | (markdown)`.                                                                                         |
| A `#` tag separated from its `run:` by another statement                   | Render tags sit directly above the `run:`. Move the tag.                                                                                                             |
| `#(markdown)` or `#"` as the last thing in the file                        | Trailing prose is `##(markdown)`; both need a statement below them.                                                                                                  |
| `given:` used before it is declared                                        | A cell reads only the givens declared above it. Move the `given:` up.                                                                                                |
| `##                                                                        | (markdown) name` block in a cell report                                                                                                                              | A name means something only where a `tiles=[name { kind=text }]` entry lists the block. Drop it: `## | (markdown)`. |
| `run:` in a report that lists `tiles=[…]`                                  | Never shown. Define a `view:`, list `"source -> view"` in `tiles`, delete the `run:`.                                                                                |
| `tiles=[intro { kind=text }, orders_tiles -> headline]` (entry not quoted) | Every `source -> view` entry is a quoted string. Unquoted, the tag does not parse.                                                                                   |
| A bare `intro` in `tiles` for a `##                                        | (markdown) intro` block                                                                                                                                              | A bare name is the query-tile form for a `query:`. Write `intro { kind=text }`.                      |
| `colspan` or `break` on a linear report's tile entry                       | Ignored and reported (`notebook-tile-layout-ignored`). Remove them, or switch to `layout=grid`.                                                                      |
| `#(filter)` in any form                                                    | Deprecated. Declare a `given:` on the model's source instead.                                                                                                        |
| `import` inside a `run:` cell                                              | Imports are file-wide; put them at the top.                                                                                                                          |
| Data-specific insights in markdown                                         | Frame questions instead.                                                                                                                                             |
| A cross-model join in a quick `skill:malloy-analysis-report` cell          | That cell renders against a single model. Write it as a report tile over a source the report declares with the join, since the report compiles with all its imports. |
