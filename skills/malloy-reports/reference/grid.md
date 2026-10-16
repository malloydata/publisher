<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Grid layout

A report with `layout=grid` places its tiles across `dashboard { columns=N }` by each tile's `# colspan` and `# break`. Everything not about placement (tiles, text tiles, givens, drill, lint) is in the skill itself.

## The four tags that make a page line up

Cards and tiles share one grid. Publisher reads these tags off what each tile in `tiles=[…]` names: the report's own `query:`, or a served view the tile names directly. Copy this recipe and use the same count on every grid report in the package so they read as one product:

1. **`columns=12`** on the `dashboard { … }` tag after the artifact tag. Twelve divides by 2, 3, 4 and 6, so a row is even with three cards or four. The builder has no grid-width setting; set it in the file.
2. **A `# colspan` on every tile, summing to 12 per row.** Four cards at 3, three at 4, two tiles at 6, a full-width table at 12. Omit them and every item falls to a single column, a twelfth of the width, too narrow for a line chart to draw in at all. A colspan wider than `columns` is clamped, and the package warnings say so.
3. **`# break` on the first tile after the cards.** Otherwise it flows into the columns left beside the cards and the next tile wraps. Not needed per row: once a row sums to 12 the next item wraps on its own.
4. **`# label="..."` on every tile and aggregate**, including the aggregates inside a table view, whose column headers are field names too. A view referenced **by name** is the exception: you can label the tile, but its own field names still reach the axes and column headers, so label the fields in the view if you want those too.

All four go on a `query:` of the report's own over the view, which is the place to put them: the modelled view stays reusable and each report decides its own widths.

```malloy
# colspan=8
# break
# label="Revenue by month"
query: revenue_trend is order_items -> sales_by_month
```

The entry in `tiles=[…]` then names `"revenue_trend"`. `# subtitle="..."` and `# borderless` go there too; a tile reads all five. A `# colspan` on a report tile's query is read against the report's own `dashboard { columns=N }`: the rule that a `# colspan` needs a `# dashboard { columns=N }` above it is about the nests of one `# dashboard` query (below), not about a report's tiles. Declare no `source: … extend { … }` to hold the tiles. A text tile's entry takes `colspan` and `break` directly: `intro { kind=text colspan=6 break }`.

**Do not forget `columns=` itself.** Without it the grid falls back to a narrow default and every colspan is clamped to it, which is the usual reason a page comes out one item per row; the package warnings name it. `dashboard_columns=N` inside the artifact tag is a deprecated alias: read when `columns` is absent, an error when the two disagree. Any other property inside the artifact tag that Publisher does not read is a package warning naming it.

## The traps

- **A KPI row is a view of only measures.** A tile is one whole result, so a tile that comes back as one row of measures renders as big-value cards on its own (`# big_value` says so explicitly). A report's tile cannot turn a view the model tags `# big_value` into a table: `big_value` is drawn whatever `viz` says, so change it in the model.
- **No `# size=fill` on a grid tile.** It measures against the container the whole grid was handed, not the tile, so it yields a chart thousands of pixels tall. Tiles already size to their colspan.
- **A table tile sitting in the left part of a wide card with the rest blank** is not filled by the grid. `# table { size=fill }` on the tile's query forces it. That is the table's own tag, not the chart `# size=fill` above.
- **A KPI card's label is one line that ellipses** rather than wrapping. Widen the card or shorten the label.
- **A ratio needs a number format.** `order_count / customer_count` renders as `10.695` on a card; `# number="#,##0.0"` is the precision it carries.
- **A `# shape_map` legend is titled with the measure's field _name_, not its `# label`.** Rename it in the view: `aggregate: revenue is total_sales`. Renaming drops the measure's format tags, so re-tag `# currency` at the rename.
- **A series legend sizes itself from the longer of the series label and its widest value**, then truncates both. A 4-character label over 4-digit years clips to `20…`; `# label="Order year"` instead of `"Year"` buys the room.

The last two are upstream renderer behavior, cheap to work around in the model. Height is the one thing the surface decides: a grid caps each tile.

## Lint findings specific to a grid

- A non-positive `dashboard { columns }`, or one above 24.
- A tile's `# colspan` that is not a positive integer, or is wider than the grid (clamped).
- A filter most tiles over a source apply, missing from one: the warning names the tile and the given.
- A `dashboard_columns=` alias, an error when it disagrees with `columns`.
- A text tile entry whose `colspan` is not a positive integer, or that carries a property other than `colspan` and `break`.

## One query as the whole page

Not the usual report: here the page is one query's result, and its own `# dashboard { columns=N }` places its nests. A `# dashboard` query lays out its one result itself: top-level `aggregate:` measures are KPI cards, each `nest:` a tile. List it as the report's one tile, a model-level named query:

```malloy
## artifact { layout=grid title="Overview" givens { REGION=f'US' } tiles=["overview"] }
# dashboard { columns=12 }
query: overview is orders -> { … }
```

A `# artifact` on the `query:` itself (the older single-query form) is not read: the file is an ordinary model. Move the tag to model level as above.

- It cannot span sources, and a guessed field inside it fails the whole package load, where a bad tile fails alone.
- A top-level `aggregate:` there is the KPI card; do not nest a `# big_value` view for it (that renders each measure as a full-width bar in one tile). Nest views for tiles; give each a `# colspan` and the first tile after the cards a `# break`. `# dashboard { gap=N table { max_height=N | none } }` are options only a `# dashboard` query reads.
- **One plain nested table, every `# colspan` and `# break` dropped**: a `f'…'` filter literal in a `givens { … }` block shares a line with `# dashboard`. Put `# dashboard` on its own line.
- **Nothing lines up**: `# colspan` without `columns=N`, so items flow at their natural widths.

To tell those two apart, run the tile, `{"query": "run: overview"}`, and read `renderLogs` on the response: `Unknown render tag 'colspan'` means the renderer never saw a `# dashboard` tag (check both causes); `Ignored # colspan … only applies in columns mode` means it saw the tag but no count. Neither reaches the package warnings.
