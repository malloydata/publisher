---
name: gotchas-rendering
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
