---
name: malloy-analyze
description: Explore data for insights and build views/dashboards/notebooks. Use when user asks to "analyze this data", "find insights", "explore for patterns", "what's interesting", "what's driving X", "build a dashboard", "create views", or any analysis task. For EDA exploration, start at Step 1. For building views on an existing model, jump to View Patterns.
---

# Analysis with Malloy

This skill covers two workflows:
- **EDA exploration** (Steps 1-6): iteratively query data, build hypotheses, validate findings
- **View/dashboard building**: create views, dashboards, notebooks from an existing model

> **Tool names** are written bare here — `get_context`, `execute_query`, `search_malloy_docs`. The exact prefixed name depends on the host surface; match each against the tools you actually have.

To formalize analysis into a polished semantic model, hand off to the modeling skill's "Starting from Analysis" workflow (`skill:malloy-model`).

## Prerequisites

- The Publisher MCP tools must be configured (`get_context`, `execute_query`, `search_malloy_docs`). If they are not available, **STOP** and ensure the Publisher MCP server is connected.
- Call `search_malloy_docs` liberally: it has powerful analysis patterns (window functions, cohorts, percent-of-total, nested drill-downs).

# EDA WORKFLOW

```
ORIENT → PROFILE → HYPOTHESIZE → INVESTIGATE → VALIDATE → SYNTHESIZE
                     (user)                      (user)     (user)
```

## Adaptive Checkpoints

The 6-step structure is a framework, not a rigid script.

| Situation | Adaptation |
|-----------|------------|
| **User has a clear hypothesis** ("what's driving churn?") | Skip HYPOTHESIZE, jump to INVESTIGATE on their question |
| **Open-ended** ("what's interesting?") | Follow all steps. PROFILE and HYPOTHESIZE are essential |
| **User wants you to just go** ("explore and show me") | Compress checkpoints, present findings at SYNTHESIZE |

## Step 1: ORIENT: Understand the Data

1. Ground yourself with `get_context`. It returns the package's sources, views, and fields (with their docs), so this is where you learn what data exists.
2. Note the source names, the connection they sit on, and the key tables/fields they expose.
3. Inspect the existing dimensions, measures, and views the model already defines, then query the data to confirm shape and values.
4. Create a working analysis file (this grows throughout the session):
   ```malloy
   source: main_table is conn.table('schema.table') extend { primary_key: pk }
   ```

**Output to user:** Brief summary of available data. Ask: *"What questions are you most interested in? Or should I look for what's interesting?"*

## Step 2: PROFILE: Statistical Profiling

**Directed analysis** (user has a question): Profile only columns relevant to their question.
**Open-ended** (no question yet): Profile broadly, looking for surprises.

### Key Profiling Queries

**Column overview:** `run: source -> { index: * limit: 100 }`

**Numeric distributions:**
```malloy
run: source -> {
  aggregate: min_val is min(col), max_val is max(col), avg_val is avg(col), null_count is count() { where: col is null }
}
```

**Categorical breakdown:** `run: source -> { group_by: col, aggregate: n is count(), order_by: n desc, limit: 20 }`

**Time range:** Check earliest/latest dates, gaps, seasonality.

**Duplicates:** `run: source -> { group_by: pk, aggregate: n is count(), having: n > 1, limit: 10 }`

**Add useful profiling dimensions/measures to your analysis file as you go.** Build incrementally.

## Step 3: HYPOTHESIZE: Form Questions

**Skip presentation if user already has a clear question.** Use profiling to refine it and jump to INVESTIGATE.

| Signal from profiling | Hypothesis type |
|----------------------|-----------------|
| Skewed distribution | Outlier analysis |
| Time patterns | Trend/seasonality |
| Category imbalance | Segment comparison |
| Correlated columns | Driver analysis |
| Unexpected NULLs | Data quality |

**CHECKPOINT (open-ended only):** Present 3-5 hypotheses ranked by potential impact. Ask which to pursue.

## Step 4: INVESTIGATE: Deep-Dive

### Outlier Detection
Search `search_malloy_docs("window functions")` for ranking and percentile patterns.

### Trend Analysis
```malloy
# line_chart
view: trend is { group_by: period is date_col.month, aggregate: key_metric, order_by: period }
```

### Segment Comparison
```malloy
view: segment_comparison is {
  group_by: segment_dim
  aggregate: row_count, key_metric
  nest:
    # line_chart
    trend is { group_by: period is date_col.month, aggregate: key_metric, order_by: period }
}
```

### Driver Analysis
```malloy
run: source -> {
  group_by: candidate_driver
  aggregate: row_count, avg_metric is avg(metric_col),
    high_rate is count() { where: metric_col > threshold } / nullif(count(), 0)
  order_by: high_rate desc
}
```

### Multi-Source Comparison (Source vs Group)

Compare each source to its group average using query-as-source:

```malloy
query: team_stats is source -> { group_by: team, season, aggregate: team_avg is avg(points) }
query: driver_stats is source -> { group_by: driver, team, season, aggregate: driver_points is sum(points) }

source: driver_vs_team is from(driver_stats) extend {
  join_one: ts is from(team_stats) on team = ts.team and season = ts.season
  dimension: advantage is driver_points - ts.team_avg
}
```

### Nested Analysis (Malloy's Superpower)

Use `nest:` for multi-level drill-downs in a single query:
```malloy
# dashboard
view: deep_dive is {
  nest: # big_value
    kpis is { aggregate: # label="Total" total_metric, # label="Count" row_count }
  nest: # bar_chart
    by_dim is { group_by: dim, aggregate: metric, order_by: metric desc, limit: 10 }
  nest: # line_chart
    over_time is { group_by: period is date.month, aggregate: metric, order_by: period }
}
```

### Build As You Go

Every useful query should leave an artifact in your `.malloy` file. New dimension? Add it. New measure? Add it. Interesting view? Save it. This file becomes the input for formalizing into a model if the user wants one.

## Step 5: VALIDATE: Triangulate

For each finding, validate with at least ONE of:
- Cross-check with another metric (revenue spiking? do order counts also?)
- Check the denominator (high rate from tiny sample?)
- Examine time consistency (pattern or one-time event?)
- Look at raw data (`select: * where: condition limit: 20`)
- Check for data artifacts (NULLs, duplicates, encoding)

**CHECKPOINT:** Present each finding with: the insight, the evidence, confidence level, and assumptions made.

## Step 6: SYNTHESIZE: Compelling Summary

Build a dashboard view that tells the story:
```malloy
# dashboard
view: analysis_summary is {
  nest: # big_value
    headlines is { aggregate: ... }
  nest: # line_chart
    trend is { ... }
  nest: # bar_chart
    breakdown is { ... }
}
```

Document insights as view descriptions: `#(doc) Top 10% of customers drive 62% of revenue.`

Present to user: top 3-5 insights, supporting views, open questions, and recommended next steps.

**Ready to formalize?** Hand off to the modeling skill's "Starting from Analysis" workflow (`skill:malloy-model`).

# VIEW PATTERNS

For building views on an existing model (base + joined source files already exist).

## Starter Views (2-3 max initially)

1. **`summary`**: KPI cards (`# big_value`)
2. **`by_time`**: Time trend (`# line_chart`)
3. **`by_category`**: Category breakdown (`# bar_chart`)
4. **`dashboard`**: Nested view combining the above (`# dashboard`)

**DRY rule:** Do NOT define measures/dimensions inline in views. Reference existing ones from base source files.

## View Annotations

| Annotation | Use For | Notes |
|-----------|---------|-------|
| `# big_value` | KPI summary | 2-5 metrics with `# label` on each |
| `# transpose` | Summary with group_by | Swaps rows/columns |
| `# dashboard` | Multi-visualization | Tiles nested views |
| `# line_chart` | Time trend | ONE aggregate only |
| `# bar_chart` | Category breakdown | ONE aggregate only |
| (none) | Detailed table | Supports multiple aggregates |

**Rules:**
- One tag per line, never combine annotations on one line
- One aggregate per chart view, charts render only the first
- No fixed scale on measures: use `# currency` (no scale); fixed scale only in views after confirming ranges
- Place chart annotation on the nested view definition, not on `nest:` itself

For complete chart reference including scatter_chart, shape_map, sparklines, and all configuration options, see `skill:malloy-charts` or call `search_malloy_docs("rendering")`.

## Field-Level Formatting

| Tag | Use For |
|-----|---------|
| `# currency` | Monetary values |
| `# percent` | Rates/percentages |
| `# number=auto` | Large counts (K/M/B) |
| `# number=id` | Non-quantity numbers (years, IDs) |
| `# label="Name"` | Custom display name |
| `# hidden` | Internal/helper fields |
| `# duration=seconds` | Time durations |

# NOTEBOOKS (.malloynb)

Cells delimited by `>>>markdown` or `>>>malloy`. **Never use `>>>malloysql`.**

```
>>>markdown
# Sales Analysis

>>>malloy
import "order_analysis.malloy"

>>>malloy
run: order_analysis -> summary
```

**Compile errors in `.malloynb` are NOT shown in the linter**: only visible on cell execution.

A notebook is also the home for a polished, narrated report: alternate `>>>markdown` cells (the story) with `>>>malloy` cells (the views), and let the malloy cells carry the chart tags. For the full cell-shape and report-authoring conventions, see `skill:malloy-notebooks`.

### Interactive Filters

**Notebooks do NOT define filters themselves.** When you import a model, the model's `#(filter)` annotations on the source are **inherited and displayed automatically**: the publisher renders the filter widgets, parses caller parameters, and injects `where:` clauses server-side. You don't redeclare them in the consumer. If the analysis needs a knob the model doesn't expose, the right move is to add a `#(filter)` to the source itself (see `skill:malloy-model` § Parameterizable Filters with `#(filter)`), not to wedge filtering into the consumer.

The notebook-level `##(filters)` annotation and the dimension-level `#(filter) {"type": "..."}` JSON-blob form are **unsupported legacy syntax**, don't use them. The only supported form is `#(filter) name=... dimension=... type=...` declared above the source.

### View Refinement

Use `+` to modify existing views: `run: source -> my_view + { limit: 15, where: status = 'active' }`

## Done

Step complete. Output: analysis `.malloy` file with views, insights, and reusable building blocks. For chart/renderer details, see `skill:gotchas-rendering` or call `search_malloy_docs`. To formalize into a model, hand off to the modeling skill (`skill:malloy-model`).

Publishing is out of scope for now: open-source Publisher serves the model from disk, and self-hosters publish via git plus their host's publish path.
