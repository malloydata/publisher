---
name: malloy-analysis-report
description: Combine validated Malloy queries into a report (a linear report, read top to bottom). Use when the user asks to "create a report", "combine these into a report", or wants a persistent multi-query artifact.
---

<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Creating Reports

An ad-hoc report is a Publisher **report** with `layout=linear`: a `.malloy` file at the package root, `<slug>.malloy` (unless the user asks for another location), that combines markdown narrative with live Malloy queries, as text tiles and query tiles read top to bottom. Load `skill:malloy-reports` for the full format and authoring rules; this skill covers when to build one and how to design good report content (tiles, chart annotations, narrative structure). Never write a new `.malloynb`, never write a report as bare `run:` statements (that older form is no longer read), and do not create a `notebooks/` or `dashboards/` directory for it.

## Before building a report

1. **Run each query first** via `execute_query` to verify it works and returns expected results.
2. **Explain the results** to the user as you go: walk through the analysis step by step.
3. **Then assemble the report** once the analysis is validated.

Do NOT build the report in the same turn as `execute_query`. Explain first, then build.

## Filters are inherited from the model, don't declare them in the report

Do not declare filters in an ad-hoc report. If the source declares `given:` parameters (or legacy `#(filter)` annotations), Publisher renders the controls, parses caller parameters, and applies them server-side automatically: the report inherits and displays them with no extra work. If the analysis needs a knob the source doesn't expose, the right move is to add a `given:` to the source itself, not to wedge a filter widget into the report. `#(filter)` is deprecated in favour of native Malloy `given:` parameters. Never add a `#(filter)` annotation: every use, including `required`, `implicit`, and date/number ranges, has a `given:` form. The `malloy-model` skill covers this under § Legacy: Parameterizable Filters. For a curated report with its own filter controls on top of the model, see `skill:malloy-reports` instead.

## What goes in the report

Do NOT add an H1 heading in any markdown (use H2 and below for sections); the `title` in the `## artifact` tag serves as the title. To redo the structure rather than tweak one tile, rewrite the report file end-to-end.

Text tiles own narrative; query tiles own a single Malloy query whose chart annotation tells the renderer how to display the result. Markdown supports H2 headings, lists, bold, and inline code. Keep text tiles short, one idea per tile, so the rendered output reads as a story instead of a wall of text.

The file has four parts, in this order:

1. **The tag**, `##| artifact { layout=linear title="..." tiles=[…] }`, listing every tile in reading order. A text tile is `name { kind=text }`; a query tile is the quoted string `"<extension> -> <view>"`.
2. **The `import`** of the model file, relative to the report's own location (`"order_analysis.malloy"` from the package root).
3. **The text tiles**, each a `##|(markdown) name` block: the name on the opener line, the body on the lines below, `|##` closing it.
4. **One extension of the source**, `source: <source>_tiles is <source> extend { … }`, holding a `view:` per query tile. The view's chart tag and `# label="…"` (the tile's heading) go directly above it.

Compile the file with `/compile` (`"scope": "file"`, at the path `<slug>.malloy`) before saving; a report compiles as a model, so its errors come back there. After saving and reloading, read the package warnings: a tile that names a missing view is reported there, not by the compile. A complete report:

```malloy
##| artifact { layout=linear title="Sales report"
  tiles=[
    overview { kind=text },
    "order_analysis_tiles -> headline",
    trend { kind=text },
    "order_analysis_tiles -> revenue_by_month",
    breakdown { kind=text },
    "order_analysis_tiles -> top_categories",
    takeaways { kind=text }
  ]
}
|##
import "order_analysis.malloy"

##|(markdown) overview
## Overview
What is driving sales, and which categories carry it? The tiles below cover every order in the model.
|##

##|(markdown) trend
## Trend
How does revenue move over time?
|##

##|(markdown) breakdown
## Breakdown
Which categories account for the most revenue?
|##

##|(markdown) takeaways
## Key takeaways
What to look at next.
|##

source: order_analysis_tiles is order_analysis extend {
  # big_value
  # label="Headline"
  view: headline is {
    aggregate:
      # label="Revenue"
      # currency
      total_revenue

      # label="Orders"
      # number=auto
      order_count
  }

  # line_chart
  # label="Revenue by month"
  view: revenue_by_month is {
    group_by: order_date.month
    aggregate: total_revenue
    order_by: 1
  }

  # bar_chart
  # label="Top ten categories by revenue"
  view: top_categories is {
    group_by: category
    aggregate: total_revenue
    order_by: total_revenue desc
    limit: 10
  }
}
```

When the model already has the view you want, the tile view refines it rather than repeating it: `view: top_categories is by_category + { limit: 10 }`.

A well-structured report typically follows this pattern:

```
[Text]   ## Overview: what question are we answering, what data is in scope (date range, entity count)
[Query]  KPI tile: headline numbers (e.g., # big_value, or # dashboard with nested # big_value views)
[Text]   ## Trend: describe what we should look for over time
[Query]  Time-series tile (e.g., # line_chart on a date dimension)
[Text]   ## Breakdown: where the signal is
[Query]  Categorical tile (e.g., # bar_chart on a categorical dimension)
[Text]   ## Key takeaways: what the user should walk away with
```

Use this as a default; deviate when the analysis warrants. A grounded report names the time range and entity count up front so every number that follows has context.

## Choosing chart types and annotations

Read `skill:malloy-charts` before picking visualizations: it owns chart-type selection, properties, and the placement rules for chart annotations. `skill:malloy-queries` covers Malloy query patterns and the critical placement rules for chart-annotation tags.

When in doubt:

- KPIs / single numbers -> `# big_value`, often nested inside `# dashboard`.
- Trend over time -> `# line_chart`, usually on the primary date dimension.
- Category comparisons -> `# bar_chart`, ordered by the metric.
- Tabular data with many columns -> a plain table tile with `# table.size=fill`.
- Multiple coordinated charts -> `# dashboard` with `nest:` blocks.

Annotations go **directly above** the tile's `view:`, never inside its curly braces:

```malloy
# bar_chart
view: top_categories is {
  group_by: category
  aggregate: revenue
  order_by: revenue desc
  limit: 10
}
```

A `# dashboard` tile composes nested views, useful for KPIs alongside a trend in a single tile. Each `nest:` is a tile; any top-level `aggregate:` measures render as KPI cards. For a fixed grid, use `# dashboard { columns=N }` with `# colspan` on each tile (see `skill:malloy-charts`):

```malloy
# dashboard { columns=2 }
view: summary is {
  nest:
    # colspan=2
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

Key rendering rules to keep in mind when shaping a tile:

- FIRST `group_by` = x-axis, FIRST `aggregate` = y-axis.
- Override field roles with `# x`, `# y`, `# series` on individual fields.
- For multiple measure series, place `# y` above the `aggregate:` keyword.
- One aggregate per chart view: use `# dashboard` with nested views for multiple charts.
- Use `# table.size=fill` for standalone table queries.

## Editing an existing report

For small targeted changes (fix one view, insert one tile), edit that view or text block in place and add its entry to `tiles=[…]`, rather than recreating the whole report. For structural rewrites (reordering many tiles, changing the narrative arc), rewrite the report file. An existing `.malloynb` is read, not edited: to change its story, write a `.malloy` report. A tag that sets `kind=notebook` is no longer read, so the file is not a report (`report-kind-removed`): replace it with `layout=linear` and list the tiles. A linear file with no `tiles=[…]`, written as bare `run:` statements, is no longer a report: rewrite it as tiles (`skill:malloy-reports`, `reference/linear.md`).

## IMPORTANT

You CANNOT see the rendered output of report tiles. Do not claim to see charts, values, or patterns from report tiles you haven't explicitly executed via `execute_query`. If you need to analyze results, run the query via `execute_query` first.
