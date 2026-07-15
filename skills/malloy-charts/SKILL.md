---
name: malloy-charts
description: Chart selection guidance and renderer reference for Malloy views. Use when choosing visualization types, adding chart annotations, user asks "what chart should I use", "how should I visualize this", or when deciding between bar_chart, line_chart, scatter_chart, etc.
---

# Chart Selection for Malloy

> Malloy uses Vega-Lite under the hood. `#` tags control visualization. Call `malloy_searchDocs` with topic "rendering" for the full tag reference (or see https://docs.malloydata.dev/documentation/visualizations/overview).

## Decision Tree: Which Chart?

| Data Shape | Default Choice |
|-----------|---------------|
| Aggregates only (no group_by) | `# big_value` |
| 1 time column + 1 measure | `# line_chart` |
| 1 category + 1 measure | `# bar_chart` |
| 2 numeric columns | `# scatter_chart` |
| Geographic (US states) + 1 measure | `# shape_map` |
| Route data (lat/lon pairs) | `# segment_map` |
| Multiple perspectives | `# dashboard` with `nest:` |
| Nested query to pivot | `# pivot` |
| Filtered aggregates side-by-side | `# flatten` |
| Detailed rows | Default table (no annotation) |

| Goal | Renderer |
|------|---------|
| Compare categories | `# bar_chart` (sort by value, limit ~15) |
| Show composition | `# bar_chart.stack` |
| Trend over time | `# line_chart` |
| Highlight KPIs | `# big_value` with `# label` |
| Correlation | `# scatter_chart` |
| Compare dimensions | `# dashboard` (nest chart views) |
| Before/after | `# transpose` or `# pivot` |
| Multiple metrics per category | Default table, `# flatten`, or `y=['a','b']` |

**Constraints:**
- ONE aggregate per chart view (charts render only the first; use `y=['a','b']` for multi-measure)
- No fixed scale on measure definitions: use `# currency` not `# currency=usd0m`
- One tag per line
- Alias joined fields in `group_by` before `order_by`
- Define measures in source, not in views


## Chart Types

### `# bar_chart`

**Data shape:** `group_by` = x-axis, `aggregate` = y-axis, optional 2nd `group_by` = series.

```malloy
# bar_chart
view: by_carrier is { group_by: carrier, aggregate: flight_count, order_by: flight_count desc, limit: 10 }

# bar_chart.stack
view: by_region is { group_by: category, region, aggregate: revenue }

# bar_chart { y=['revenue','cost'] }
view: rev_vs_cost is { group_by: category, aggregate: revenue, cost }
```

**Key properties:** `.stack`, `.size` (spark/xs/sm/md/lg/xl/2xl), `.x`, `.x.limit`, `.y` (supports `y=['a','b']`), `.series`, `.series.limit` (default 20), `.title`, `.subtitle`, `.x.independent`, `.y.independent`

**Field role tags:** `# x`, `# y`, `# series` on individual fields to assign roles explicitly.

### `# line_chart`

**Data shape:** `group_by` (temporal/numeric) = x-axis, `aggregate` = y-axis, optional 2nd `group_by` = series.

```malloy
# line_chart
view: trend is { group_by: order_month, aggregate: revenue, order_by: order_month }

# line_chart { size=spark }
view: mini_trend is { group_by: order_month, aggregate: revenue, order_by: order_month }
```

**Key properties:** `.zero_baseline`, `.interpolate` (e.g., `step`), `.size`, `.y` (supports `y=['a','b']`), `.series.limit` (default 12), `.title`, `.subtitle`

### `# scatter_chart`

**Data shape:** Fields by position: x, y, color, size (bubble), shape.

```malloy
# scatter_chart
view: correlation is { group_by: customer_id, aggregate: avg_price, total_quantity }
```

### `# shape_map`

Choropleth. US states only. Fields: state name, value.

```malloy
# shape_map
view: by_state is { group_by: state, aggregate: revenue }
```

### `# segment_map`

Route map. US only. Fields: start_lat, start_lon, end_lat, end_lon, color.


## Layout Types

### `# big_value`

KPI cards. Aggregates only, no `group_by`.

```malloy
# big_value
view: summary is {
  aggregate:
    # label="Revenue"
    # currency
    revenue
    # label="Orders"
    # number=auto
    order_count
}
```

**Properties:** `.size`, `.sparkline=<nested_view_name>`, `.comparison_field`, `.comparison_label`, `.down_is_good`

### `# dashboard`

Card-based multi-tile layout. Apply to a view whose body is a nested query; the view's own fields lay out automatically:

- `group_by` dimensions -> a row header (repeats once per row; omit for a single block)
- `aggregate` measures -> KPI cards, one per measure
- each `nest:` -> a tile, rendered by the tag above it (`# table` default, or `# bar_chart` / `# line_chart` / `# big_value`)

**Two modes.** Flex (default): tiles flow and wrap; `# break` forces a new row. Columns: `# dashboard { columns=N }` lays tiles into N equal columns, `# colspan=n` widens a tile, `# break` starts a new row, overflow wraps.

```malloy
// Flex: measures become KPI cards, the nest becomes a tile
# dashboard
view: overview is {
  group_by: category
  # currency
  aggregate:
    avg_retail is retail_price.avg()
    sum_retail is retail_price.sum()
  nest:
    # bar_chart
    by_brand is { group_by: brand, aggregate: avg_retail is retail_price.avg(), limit: 10 }
}

// Columns: # colspan widens tiles, # break ends a row
# dashboard { columns=12 }
view: layout is {
  group_by: category
  # currency
  aggregate:
    # colspan=4
    avg_retail is retail_price.avg()
    # colspan=4
    sum_retail is retail_price.sum()
    # colspan=4
    max_retail is retail_price.max()
  nest:
    # break
    # colspan=6
    # bar_chart
    # subtitle="Top brands"
    by_brand_chart is { group_by: brand, aggregate: avg_retail is retail_price.avg(), limit: 8 }
    # colspan=6
    by_brand_table is { group_by: brand, aggregate: product_count is count(), limit: 8 }
}
```

**Tags:** `# dashboard { columns=N }` (columns mode), `{ gap=PX }` (tile spacing, default 16; never a mode), `{ table.max_height=PX|none }` (cap table tiles). On a measure or nest: `# colspan=N` (columns mode only), `# break` (both modes), `# subtitle="..."` (tile), `# borderless` (drop card chrome), `# label="..."` (card title).

For rich KPI cards (sparklines, comparison deltas, several metrics on one card) nest a `# big_value` view instead of relying on the dashboard's own measures:

```malloy
# dashboard
view: kpis is {
  group_by: category
  nest:
    # big_value
    revenue_card is {
      aggregate:
        # label="Revenue"
        # currency
        # big_value { sparkline=trend }
        total_revenue is retail_price.sum()
      # line_chart { size=spark y.independent=true }
      # hidden
      nest: trend is { group_by: bucket is floor(id / 100)::number, aggregate: total_revenue is retail_price.sum(), order_by: bucket, limit: 20 }
    }
}
```

**Rules:** `# dashboard` needs a nested-query view (no effect on a scalar). `# colspan` works only in columns mode and is ignored (warns) in flex. `columns` is any positive integer; a `# colspan` over the column count clamps to a full row. Style tiles via the instance theme, or theme the views inside with `# theme.*` (see Theming below).

### `# pivot`

Pivot nested results into columns. Max 30 pivot columns.

```malloy
view: sales is {
  group_by: product, aggregate: total
  nest: # pivot
    by_quarter is { group_by: quarter, aggregate: revenue }
}
```

### `# transpose`

Swap rows/columns. Good for period comparisons.

```malloy
# transpose
view: comparison is {
  aggregate:
    # label="This Month"
    current_revenue
    # label="Last Month"
    prior_revenue
}
```

### `# list` / `# list_detail`

List renders as comma-separated values. List_detail shows `value (detail)` pairs.

### `# flatten`

Collapse nested record into parent table as columns. Use for side-by-side filtered aggregates:

```malloy
view: segments is {
  group_by: product, aggregate: total_revenue
  nest: # flatten
    enterprise is { where: segment = 'Enterprise', aggregate: # label="Enterprise" revenue }
  nest: # flatten
    smb is { where: segment = 'SMB', aggregate: # label="SMB" revenue }
}
```

### `# table`

Default (implicit). Use explicitly for `.size=fill` property.


## Field Formatting Tags

| Tag | Use For | Shorthand |
|-----|---------|-----------|
| `# number` | Numeric formatting | `=auto` (K/M/B), `=id` (no commas), `=1k`, `=1m` |
| `# percent` | Percentages | (none needed) |
| `# currency` | Money | `=usd2m` (USD, 2 decimals, millions); scale only in views |
| `# duration` | Time durations | `=seconds`, `=minutes`, `=hours`, `=days` |
| `# data_volume` | Storage sizes | `=bytes`, `=kb`, `=mb`, `=gb` |
| `# link` | Hyperlinks | `.url_template="https://example.com/$$"` |
| `# image` | Inline images | `.height=40px`, `.width=100px` |

**Currency codes:** `usd` ($), `eur`, `gbp`. **Scale:** K/M/B/T/Q or `auto`.
**Number suffix styles:** `word` ("42.5 million"), `letter` ("42.5M"), `scientific`.

## Utility Tags

| Tag | Purpose |
|-----|---------|
| `# hidden` | Hide from output (still usable for sorting/references) |
| `# label="..."` | Override display name |
| `# description="..."` | Tooltip text |
| `# tooltip` | Include nested view in chart tooltip |
| `# break` | Force new dashboard row |
| `# column { width=sm }` | Table column width |

## Model-Level Defaults

```malloy
## viz.line_chart.defaults.y.independent=true
## viz.bar_chart.defaults.stack
```

## Theming

Publisher styles charts and tables from one structured theme. The instance sets it (in `publisher.config.json`'s `theme` block or the **Settings, then Theme** editor); a model overrides it per result with `# theme.*` annotations, or model-wide with `## theme.*`. Per-chart annotations use the same nested `palette.*` / `font.*` vocabulary as the config, not flat key names, and they win over the instance theme for the keys they set. The forms:

| Annotation | Controls | Modes |
|-----------|----------|-------|
| `# theme.palette.series` | Categorical series colors (array) | shared |
| `# theme.palette.background.{light,dark}` | Chart canvas + table background | per-mode |
| `# theme.palette.tableHeader.{light,dark}` | Table header text color | per-mode |
| `# theme.palette.tableHeaderBackground.{light,dark}` | Table header row background | per-mode |
| `# theme.palette.tableBody.{light,dark}` | Table body text color | per-mode |
| `# theme.palette.tile.{light,dark}` | Dashboard tile background | per-mode |
| `# theme.palette.tileTitle.{light,dark}` | Dashboard tile title color | per-mode |
| `# theme.palette.mapColor.{light,dark}` | Choropleth gradient (`# shape_map` / `# segment_map`) | per-mode |
| `# theme.font.family` | Font for all rendered text | shared |
| `# theme.font.size` | Table font size (px) | shared |

The seven `palette.*` color keys each take a `.light` and/or `.dark` variant so dark mode gets its own value. `palette.series`, `font.family`, and `font.size` are single values shared across modes.

```malloy
// Model-wide defaults (## applies to every view in the model):
## theme.palette.series = ["#14b3cb", "#e47404", "#1474a4"]
## theme.font.family = "Inter, sans-serif"

// Per-view override (# applies to this result only; beats the instance theme):
# theme.palette.background.light = "#fafafa"
# theme.palette.background.dark = "#111111"
# theme.palette.tableHeader.dark = "#94a3b8"
view: revenue_by_month is {
  group_by: month
  aggregate: revenue
}
```

**Precedence**, highest to lowest, per key: `# theme.*` on the view, then `## theme.*` model default, then the instance theme, then Publisher's built-in defaults. A per-chart annotation overrides the instance for the keys it sets; unset keys fall through to the instance. (This is the reverse of a bare `@malloydata/render` embed, where the embedder wins: Publisher reads the annotation itself and layers it on top.)

Quote values that contain spaces or a leading `#`. The light/dark default (`defaultMode`) and the toggle lock (`allowUserToggle`) are instance-only: set them in the config `theme` block or the editor, not as annotations. The gotchas-rendering skill lists the annotation forms that look valid but do nothing.


## Advanced Patterns

### Sparklines in KPI Cards

```malloy
# big_value { sparkline=trend }
view: revenue_kpi is {
  aggregate: # label="Revenue" # currency revenue
  nest: # line_chart { size=spark } # hidden
    trend is { group_by: order_date, aggregate: revenue, order_by: order_date }
}
```

### KPIs with Comparison Deltas

```malloy
# big_value { comparison_field=prior_month comparison_label="vs Last Month" }
view: rev_delta is {
  aggregate: # label="Revenue" # currency revenue, # hidden prior_month
}
```

Use `down_is_good=true` for metrics where decrease is positive (churn, defects).

### Inline Mini-Charts in Table Rows

```malloy
view: carriers is {
  group_by: carrier, aggregate: flight_count
  nest: # line_chart { size=spark }
    trend is { group_by: month, aggregate: flight_count, order_by: month }
}
```

### Multi-Measure Series

```malloy
# bar_chart { y=['revenue','cost'] }
view: rev_vs_cost is { group_by: quarter, aggregate: revenue, cost }
```

### Hierarchical Drill-Down

```malloy
# list_detail
view: explorer is {
  group_by: region, aggregate: revenue
  nest: # bar_chart
    by_category is { group_by: category, aggregate: revenue, order_by: revenue desc, limit: 10 }
}
```

### Distribution (Histogram)

Call `malloy_searchDocs("autobin")` for syntax:
```malloy
# bar_chart
view: price_dist is { group_by: bucket is autobin(price, 20), aggregate: order_count }
```


## Patterns for Missing Chart Types

| Desired | Malloy Approximation |
|---------|---------------------|
| Pie/donut | `# bar_chart` sorted by value |
| Treemap | Nested table with `order_by: desc` |
| Heatmap | `# pivot` with color values |
| Stacked area | `# line_chart` with series (overlaid lines) |
| Funnel | `# bar_chart` with ordered stages |
| Gauge/bullet | `# big_value` with `.comparison_field` |


## Chart Annotations on Queries with `nest:`

A top-level chart tag (e.g., `# bar_chart`) renders only the outer query; any `nest:` views are silently hidden from the rendering (still in raw data). To show nests, use `# dashboard` on the outer query with chart tags on each nest. Otherwise, drop the `nest:`.


## Common Mistakes

| Mistake | Fix |
|---------|-----|
| Two aggregates in chart | ONE aggregate, or use `y=['a','b']` |
| `# currency=usd0m` on measure | `# currency` (no scale) on defs; scale only in views |
| Chart annotation on `nest:` line | Put on the **view definition** |
| Tags on same line | One tag per line |
| Sparkline not showing | Add `# hidden` to nested view AND reference in `.sparkline=` |
| Pivot > 30 columns | Filter/limit the nested group_by |

NOTE: The term 'constructor' is a reserved term in Vega-Lite. If the word 'constructor' appears in the query, it will cause the rendering to fail. Never use it in a query and avoid using it as a dimension in a model.

For more patterns, call `malloy_searchDocs` with topics like "bar charts", "line charts", "dashboards", "autobin", "percent of total", "comparing timeframes", or "pivots".

## Further Reading

- [Visualizations Overview](https://docs.malloydata.dev/documentation/visualizations/overview) - Official docs
- [Bar Charts](https://docs.malloydata.dev/documentation/visualizations/bar_charts) - Stacked, grouped, series
- [Bump Charts Blog](https://docs.malloydata.dev/blog/2023-10-26-malloy-bump-chart/) - Ranking over time
- [Dataviz is Hierarchical](https://docs.malloydata.dev/blog/2024-02-29-hierarchical-viz/) - Nested data visualization philosophy
