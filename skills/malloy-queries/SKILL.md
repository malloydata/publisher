---
name: malloy-queries
description: Malloy query patterns, syntax rules, and chart annotation reference. Consult before writing or debugging any query. Covers dates, aggregates vs dimensions, join paths, filters, string matching, and common error patterns.
---

# Malloy Query Reference

Only use field names defined in the model. Ground yourself first with `get_context`; never invent entities or guess field names.

> **Tool names** are written bare here — `get_context`, `execute_query`, `search_malloy_docs`. The exact prefixed name depends on the host surface; match each against the tools you actually have.

## Query Patterns

**Simple aggregation:**
```malloy
run: source -> {
  aggregate: total_revenue, order_count
}
```

**Group by dimension:**
```malloy
run: source -> {
  group_by: category
  aggregate: revenue
  order_by: revenue desc
  limit: 10
}
```

**Time trend:**
```malloy
# line_chart
run: source -> {
  group_by: order_date.month
  aggregate: revenue
  order_by: 1
}
```

**Filtered query:**
```malloy
run: source -> {
  where: status = 'active'
  group_by: region
  aggregate: count_orders, total_revenue
}
```

**Run a pre-built view:**
```malloy
run: source -> view_name
```

**Refine a view with additional options:**
```malloy
run: source -> view_name + { limit: 10, where: region = 'US' }
```

**Percent of total:** use `all()`, not `parent()`.
```malloy
run: source -> {
  group_by: category
  aggregate:
    revenue
    pct_of_total is revenue / all(revenue)
}
```

**Conditional dimensions with `pick`:** `pick` is a keyword, not a function.
```malloy
run: source -> {
  group_by:
    tier is pick 'Premium' when price > 100
            pick 'Standard' when price > 50
            else 'Budget'
  aggregate: count()
}
```
Wrong: `pick('Premium') { ... }` (that's not Malloy syntax).

**Window functions with `calculate:`:** running totals, `lag()`, `lead()`, and other window operations belong in `calculate:`, not `aggregate:`.
```malloy
run: source -> {
  group_by: month is order_date.month
  aggregate: revenue
  calculate: prev_month_revenue is lag(revenue)
  order_by: month
}
```

## Field Paths and Joins

**Joins are defined in the model.** Never write `join_one` / `join_many` inside a query. Every query is rooted on one source, and you reach joined sources via dot notation within the query body.

The `->` operator separates a **source** from a **view** (query transformation). It does NOT navigate between joined sources.

Wrong:
```malloy
run: candidate -> hiring_manager -> { aggregate: employee_count }
```
Right:
```malloy
run: candidate -> { aggregate: hiring_manager.employee_count }
```

Use the field paths defined in the model **verbatim**. If the model defines `hiring_manager.employee_count`, do not strip the `hiring_manager.` prefix; that prefix is the join namespace, not a separate source to navigate to.

## Dates and Time

### Comparisons need `@` literals

**Use `@` date literals for comparisons.** Never compare a timestamp to a bare number.

Wrong (compares a timestamp to the integer `2020`):
```malloy
where: order_date.year >= 2020
```
Right:
```malloy
where: order_date >= @2020-01-01
```

### Truncation accessors return timestamps, not integers

`order_date.month` returns a month-truncated timestamp (e.g., `2025-06-01 00:00:00`), useful for `group_by:`. It is NOT a 1-12 integer, and it cannot be chained: writing `order_date.month.day` fails. Stop at the first truncation.

Available truncations: `.year`, `.quarter`, `.month`, `.week`, `.day`, `.hour`, `.minute`, `.second`. Stop at the first.

### `month`, `year`, `day`, `quarter` are reserved words

Don't try to extract a numeric month with a function call: `month(order_date)`, `year(order_date)`, etc. fail to parse in `where:` because the names are reserved. The same names can also break aliases:

Wrong: `group_by: month is order_date.month`  →  parse error
Right: `group_by: order_month is order_date.month`
Right (when a column is literally named `month`): `` group_by: `month` ``

### Filtering by date range

For a single contiguous range, prefer the `?` apply operator with a partial-date literal: it's the idiomatic Malloy form and works with date or timestamp fields:

```malloy
where: order_date ? @2025                    -- anywhere in 2025
where: order_date ? @2025-Q3                 -- Q3 2025 (Jul-Sep)
where: order_date ? @2025-06 to @2025-09     -- June through August (upper bound excluded)
where: order_date ? @2025-06-01 for 3 months -- same range, duration form
where: order_date ? now - 1 year for 1 year  -- the last full year
```

Bounded `>=`/`<` with two literals also works and is sometimes clearer:

```malloy
where: order_date >= @2025-06-01 and order_date < @2025-09-01
```

## Aggregates vs Dimensions

**`where:` filters rows before aggregation. `having:` filters aggregate results.** Picking the wrong one is the single most common query error.

- `where:` only sees dimensions / raw columns.
- `having:` only sees aggregates / measures.

Wrong: `where: total_revenue > 1000`  (total_revenue is an aggregate)
Right: `having: total_revenue > 1000`

Wrong: `having: region = 'US'`  (region is a dimension)
Right: `where: region = 'US'`

Prefer inline expressions in `having:` rather than defining an extra named aggregate just to filter on:
```malloy
having: count() > 20
```

**Don't put aggregates in `group_by:`, or dimensions in `aggregate:`.**

Wrong: `group_by: total_sales` (where `total_sales` is `sum(price)`)
Right: `group_by: category; aggregate: total_sales`

**Scalar functions are not aggregates.** `concat()`, `substring()`, arithmetic on raw fields, etc. belong in `group_by:` or `select:`, never `aggregate:`.

Wrong: `aggregate: full_name is concat(first_name, ' ', last_name)`
Right: `group_by: full_name is concat(first_name, ' ', last_name)`

**Counting:**
- `count()`: row count.
- `count(field)`: **distinct** count of that field.
- There is **no** `count(distinct field)` syntax, and `count(*)` is wrong.

Wrong: `count(distinct customer_id)`, `count(*)`
Right: `count(customer_id)`, `count()`

**Aliases from `group_by:` aren't visible in `where:`.** `where:` is evaluated before `group_by:`, so it can't see aliases defined there. Reference the source field directly.

Wrong:
```malloy
group_by: region_alias is customer.region
where: region_alias = 'US'
```
Right:
```malloy
where: customer.region = 'US'
group_by: region_alias is customer.region
```

## String Matching

Malloy's `~` operator is **regex**, not SQL LIKE. Use the raw-string `r'...'` form; no `%` wildcards.

Wrong: `where: name ~ '%Alonso%'`
Right: `where: name ~ r'Alonso'`

Both sides must be strings. For multi-value equality, use the `?` partial-match operator:
```malloy
where: region ? 'US' | 'CA' | 'MX'
```

## Order By

`order_by:` can reference a `group_by` alias or a column position. It **cannot** reference a dotted join path; alias the field in `group_by:` first.

Wrong: `order_by: customer.region`
Right:
```malloy
group_by: region is customer.region
aggregate: revenue
order_by: region
```

## Field Selection Tips

When the model exposes both a human-readable name and an internal code/ID (e.g., `aircraft_model_name` vs `aircraft_model_code`, `customer_name` vs `customer_id`), **prefer the human-readable one** for anything the user will see (group-bys in charts, labels, breakdowns). Check the `#(doc)` field descriptions in the model to disambiguate.

## Chart Annotations

Chart annotations (e.g., `# bar_chart`, `# line_chart`, `# big_value`) go **before** `run:`, `view:`, or `nest:`, never inside curly braces. Field-level tags (`# label`, `# currency`, `# x`, `# y`) go above individual fields inside the query block:

```malloy
# bar_chart
run: source -> {
  group_by: category
  aggregate:
    # label="Revenue"
    # currency
    revenue
  order_by: revenue desc
  limit: 10
}
```

Charts render only the **first** aggregate. For multiple measures on one chart, place `# y` above the `aggregate:` keyword or use the `y=['a','b']` shorthand. A misplaced chart annotation (e.g., inside `{ }`) typically produces the error *"field is a bar chart, but is not a repeated record"*.

Read the `malloy-charts` skill for chart types, properties, data shape requirements, and selection guidance.

## When a Query Fails

Read the error against the tables above and below. Most failures match a known pattern and can be fixed directly. If the cause is not obvious after that, remove pieces (filters, joins, nested views) until the query compiles; whatever you removed last is the bug.

| Error message | Likely cause / fix |
|---|---|
| `Cannot compare a timestamp to a number` | Comparing `date.year` to an integer. Use `date >= @2020-01-01` instead. |
| `no viable alternative at input '<word>'` | Two common causes: a reserved keyword used as an alias or as a function call (e.g., `month is ...`, `month(date_field)`) — rename, backtick, or use `date_field.month` / a `?`-apply filter instead; or a semicolon used to separate fields within a clause — fields under one `aggregate:`/`group_by:` are comma- or newline-separated, never `;` (the error points at the field right after the `;`). |
| `'<field_name>' is not defined` | Field doesn't exist in the source. Re-check against the model definition; you may have stripped a join prefix. |
| `missing {DAY, HOUR, MINUTE, MONTH, QUARTER, SECOND, WEEK, YEAR}` | Chained date property too deep (e.g., `.month.something`). Stop at the first truncation. |
| `field is a bar chart, but is not a repeated record` | Chart annotation placed inside `{ }`. Move `# bar_chart` above `run:` / `view:` / `nest:`. |
| `Parser encountered unexpected statement` | Unsupported feature, or syntax placed where Malloy doesn't allow it (e.g., `pick` inside a nested view). |
| Query silently returns zero rows | Filter value mismatch (case, spelling, format). Run a distinct-values query on the dimension to confirm the literal. |

## Syntax Help

For anything not covered here, call `search_malloy_docs` with the topic (for example "string functions", "nested queries").
