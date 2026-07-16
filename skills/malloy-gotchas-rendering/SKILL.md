---
name: malloy-gotchas-rendering
description: Common Malloy renderer annotation mistakes. Read BEFORE adding chart annotations, formatting tags, or building dashboards. Covers tag syntax, scale rules, sparkline setup, and big_value patterns.
---

# Rendering Gotchas

> **Read this before adding renderer annotations.** These patterns cause most rendering issues.

## One Tag Per Line

Each `#` annotation must be on its own line directly above the field. Never combine tags on one line.

```malloy
// WRONG, will not work
# label="Revenue" # currency
revenue

// RIGHT
# label="Revenue"
# currency
revenue
```

## No Fixed Scale on Measures

Use `# currency` (no scale) on measure definitions. The same measure renders at many granularities: `usd0m` turns $500 into `$0.0M`.

```malloy
// WRONG on a measure definition
# currency=usd0m
measure: revenue is sum(total)

// RIGHT, no scale on measure
# currency
measure: revenue is sum(total)
```

Add scale (e.g., `# currency=usd0m`) only in views after confirming value ranges with queries.

## `# big_value` Needs `# label` on Each Measure

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

Without `# label`, big_value cards show raw field names which are often unclear.

## Sparkline Setup

Sparklines in `# big_value` require TWO things: a `# hidden` nested view AND a `.sparkline=` reference.

```malloy
# big_value { sparkline=trend }
view: revenue_kpi is {
  aggregate:
    # label="Revenue"
    # currency
    revenue
  nest:
    # line_chart { size=spark }
    # hidden
    trend is { group_by: order_date, aggregate: revenue, order_by: order_date }
}
```

If the sparkline doesn't show: check that `# hidden` is on the nested view AND the view name matches `.sparkline=`.

## Comparison Deltas

```malloy
# big_value { comparison_field=prior_month comparison_label="vs Last Month" }
view: rev_delta is {
  aggregate:
    # label="Revenue"
    # currency
    revenue
    # hidden
    prior_month
}
```

Use `down_is_good=true` for metrics where decrease is positive (churn, defects).

## `# dashboard` Layout

In a `# dashboard` view, fields render by role: `group_by` -> a repeating row header, `aggregate` measures -> KPI cards, each `nest:` -> a tile. Put each tile tag on its own line above the nested view. A lone renderer tag also works on the `nest:` line, but a tile usually carries several tags (`# break`, `# colspan`, `# subtitle`, then `# bar_chart`), and only the own-line form keeps each tag on its own line.

```malloy
// RIGHT: tag above the nested view; the measure auto-renders as a card
# dashboard
view: overview is {
  group_by: category
  # currency
  aggregate: avg_retail is retail_price.avg()
  nest:
    # bar_chart
    by_brand is { group_by: brand, aggregate: avg_retail is retail_price.avg(), limit: 10 }
}
```

- **`# colspan` only works in columns mode.** Set `# dashboard { columns=N }` first; in flex mode `# colspan` is ignored. `# break` (new row) works in both modes.
- **`gap` is spacing, not a mode.** `columns=N` enters columns mode; `# dashboard { gap=24 }` only changes tile spacing.
- **`# dashboard` needs a row-producing view (no effect on a scalar).** A flat view of top-level `aggregate:` measures still renders KPI cards; add `nest:` for chart and table tiles.
- **Use `# colspan`, not the old `# span`.** Tile styling comes from the instance theme.

## Theming

- **Per-chart theme keys are nested, not flat.** In Publisher, `# theme.palette.tableHeader.dark = "#94a3b8"` works; a flat `# theme.tableHeaderColor = "#94a3b8"` is silently dropped. Publisher's annotation reader only understands the structured `palette.*` / `font.*` form (the same vocabulary as the config), not the renderer's flat `MalloyExplicitTheme` key names.
- **A per-chart annotation OVERRIDES the instance theme.** Precedence, highest to lowest, per key: `# theme.*` (view), then `## theme.*` (model), then the instance theme (config / Settings, then Theme editor), then built-in defaults. A `# theme.*` view tag beats a `## theme.*` model default, and both beat the instance theme for the keys they set. This is the opposite of a bare `@malloydata/render` embed, where the embedder wins.
- **Only seven palette keys take light/dark.** `background`, `tableHeader`, `tableHeaderBackground`, `tableBody`, `tile`, `tileTitle`, and `mapColor` each accept a `.light` and/or `.dark` variant. `palette.series`, `font.family`, and `font.size` are single values shared across modes; a `.light`/`.dark` on them does nothing.
- **`# theme.palette.mapColor.{light,dark}` recolors choropleths only.** It sets the saturated end of the `# shape_map` / `# segment_map` gradient (per mode). Rect-mark heatmaps keep their built-in scheme.
- **`defaultMode` and `allowUserToggle` are instance-only.** No per-chart annotation controls the light/dark default or the toggle lock; set them in the config `theme` block or the editor.
- **Environment-level theming is not applied yet.** Only the instance theme and the `# theme.*` / `## theme.*` per-chart annotations take effect today.
