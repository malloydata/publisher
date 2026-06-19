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

Multi-tile layout. Use `# break` to force new row.

```malloy
# dashboard
view: overview is {
  nest: # big_value
    kpis is { ... }
  nest: # line_chart
    trend is { ... }
  # break
  nest: # bar_chart
    breakdown is { ... }
}
```

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
## theme.fontFamily="Inter, sans-serif"
```


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
