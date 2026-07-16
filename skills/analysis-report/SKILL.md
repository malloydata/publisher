---
name: analysis-report
description: Combine validated Malloy queries into a notebook report or dashboard. Use when the user asks to "create a report", "build a dashboard", "combine these into a report", or wants a persistent multi-query artifact.
---

# Creating Reports

An ad-hoc report is a `.malloynb` notebook that combines markdown narrative with live Malloy query cells. There is no dedicated report tool: you author the notebook directly. Load `skill:malloy-notebooks` for the full `.malloynb` cell format and authoring rules; this skill covers when to build one and how to design good report content (cells, chart annotations, narrative structure).

> **Tool names** are written bare here — `get_context`, `execute_query`, `search_malloy_docs`. The exact prefixed name depends on the host surface; match each against the tools you actually have.

## Before building a report

1. **Run each query first** via `execute_query` to verify it works and returns expected results.
2. **Explain the results** to the user as you go: walk through the analysis step by step.
3. **Then assemble the notebook** once the analysis is validated.

Do NOT build the notebook in the same turn as `execute_query`. Explain first, then build.

## Filters are inherited from the model, don't declare them in the report

Reports do not (and cannot) define their own filters. If the source has `#(filter)` annotations, Publisher renders the filter widgets, parses caller parameters, and injects `where:` clauses server-side automatically: the report inherits and displays those filters with no extra work. If the analysis needs a knob the source doesn't expose, the right move is to add a `#(filter)` to the source itself (see your modeling workflow's parameterizable-filter guidance for `#(filter)`), not to wedge a filter widget into the report. For curated notebooks with their own per-notebook filter UI on top of the model, see `skill:malloy-notebooks` instead.

## What goes in the report

Do NOT add an H1 heading in any cell (use H2 and below for sections); the notebook name serves as the title. To redo the structure rather than tweak one cell, rewrite the notebook file end-to-end.

Markdown cells own narrative; query cells own a single Malloy query whose chart annotation tells the renderer how to display the result. Markdown supports H2 headings, lists, bold, and inline code. Keep narrative cells short, one idea per cell, so the rendered output reads as a story instead of a wall of text.

In a `.malloynb` file each cell is delimited by a `>>>markdown` or `>>>malloy` marker. A markdown cell looks like:

```
>>>markdown
## Section heading
Narrative text here.
```

A query cell looks like:

```
>>>malloy
# bar_chart
run: source -> { group_by: dim; aggregate: measure }
```

Each Malloy cell must be a standalone query (for example `run: source -> { ... }`). The notebook's leading `>>>malloy` cell holds the `import` statement for the model file; individual query cells do not repeat it. If a query fails validation when executed, fix it and rerun.

A well-structured report typically follows this pattern:

```
[Markdown]  ## Overview: what question are we answering, what data is in scope (date range, entity count)
[Malloy]    KPI cell: headline numbers (e.g., # big_value, or # dashboard with nested # big_value cells)
[Markdown]  ## Trend: describe what we should look for over time
[Malloy]    Time-series cell (e.g., # line_chart on a date dimension)
[Markdown]  ## Breakdown: where the signal is
[Malloy]    Categorical cell (e.g., # bar_chart on a categorical dimension)
[Markdown]  ## Key takeaways: what the user should walk away with
```

Use this as a default; deviate when the analysis warrants. A grounded report names the time range and entity count up front so every number that follows has context.

## Choosing chart types and annotations

Read `skill:malloy-charts` before picking visualizations: it owns chart-type selection, properties, and the placement rules for chart annotations. `skill:malloy-queries` covers Malloy query patterns and the critical placement rules for chart-annotation tags.

When in doubt:
- KPIs / single numbers -> `# big_value`, often nested inside `# dashboard`.
- Trend over time -> `# line_chart`, usually on the primary date dimension.
- Category comparisons -> `# bar_chart`, ordered by the metric.
- Tabular data with many columns -> a plain table cell with `# table.size=fill`.
- Multiple coordinated charts -> `# dashboard` with `nest:` blocks.

Annotations go **before** `run:`, never inside curly braces:

```malloy
# bar_chart
run: source -> {
  group_by: category
  aggregate: revenue
  order_by: revenue desc
  limit: 10
}
```

A `# dashboard` cell composes nested views, useful for KPIs alongside a trend in a single cell. Each `nest:` is a tile; any top-level `aggregate:` measures render as KPI cards. For a fixed grid, use `# dashboard { columns=N }` with `# colspan` on each tile (see `skill:malloy-charts`):

```malloy
# dashboard
run: source -> {
  nest:
    # big_value
    kpis is {
      aggregate:
        # label="Revenue"
        # currency
        total_revenue

        # label="Orders"
        # number=auto
        order_count
    }
  nest:
    # line_chart
    trend is {
      group_by: order_date.month
      aggregate: total_revenue
      order_by: 1
    }
}
```

Key rendering rules to keep in mind when shaping a cell:
- FIRST `group_by` = x-axis, FIRST `aggregate` = y-axis.
- Override field roles with `# x`, `# y`, `# series` on individual fields.
- For multiple measure series, place `# y` above the `aggregate:` keyword.
- One aggregate per chart view: use `# dashboard` with nested views for multiple charts.
- Use `# table.size=fill` for standalone table queries.

## Editing an existing report

For small targeted changes (fix one cell, insert one new cell), edit that cell in the `.malloynb` file rather than recreating the whole notebook. For structural rewrites (reordering many cells, changing the narrative arc), rewrite the notebook file.

## IMPORTANT

You CANNOT see the rendered output of notebook cells. Do not claim to see charts, values, or patterns from report cells you haven't explicitly executed via `execute_query`. If you need to analyze results, run the query via `execute_query` first.
