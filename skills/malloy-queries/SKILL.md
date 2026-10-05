---
name: malloy-queries
description: Read before writing or debugging a Malloy query. Dates, aggregates vs dimensions, joins, filters, strings, window functions, chart annotations, and the compile errors a SQL habit produces.
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Malloy Query Reference

Only use field names defined in the model. Ground yourself first with `get_context`; never invent entities or guess field names.

> **Tool names** are written bare here - `get_context`, `execute_query`, `search_malloy_docs`. The exact prefixed name depends on the host surface; match each against the tools you actually have.

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
  group_by: order_month is order_date.month
  aggregate: revenue
  calculate: prev_month_revenue is lag(revenue)
  order_by: order_month
}
```

## Field Paths and Joins

**Use the joins the model declares.** Every query is rooted on one source, and you reach a joined source's fields by a dotted path within the query body (for example `stores.region`). Never write `join_one` or `join_many` in a query, and never invent a join key. If the field you need is on a source the model does not join, say so and ask the user to add the join to the model.

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

`order_date.month` returns a month-truncated timestamp (e.g., `2025-06-01 00:00:00`), useful for `group_by:`. It is NOT a 1-12 integer.

Available truncations: `.year`, `.quarter`, `.month`, `.week`, `.day`, `.hour`, `.minute`, `.second`.

Anything else is an extraction function, not an accessor: `day_of_year()`, `day_of_week()` (1 = Sunday), `day()`, `week()`, `month()`, `quarter()`, `year()`, `hour()`, `minute()`, `second()`.

Wrong: `group_by: created_at.day_of_year`  →  `'created_at' cannot contain a 'day_of_year'`
Right: `group_by: doy is day_of_year(created_at)`

### `month`, `year`, `day`, `date`, `count` and friends are reserved as names

They cannot name an output field. The set is wider than it looks: the timeframes and their plurals (`day`/`days`, `month`/`months`, `year`/`years`, ...), type names (`date`, `timestamp`, `number`, `string`), aggregate names (`count`, `sum`, `avg`, `min`, `max`, `all`), and `now`, `source`, `table`, `by`, `on`, `is`, `asc`, `desc`. The functions are fine: `month(order_date)` extracts 1-12 anywhere, including `where:`.

Wrong: `group_by: month is order_date.month`  →  `'month' is a reserved word, so to use it as a name you must quote it`
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

`~` is for strings, not dates: `where: order_date ~ @2025` does not compile (Malloy reports `mysterious error in range computation`). Use `?`, or `=` for a whole year, month or day.

Bounded `>=`/`<` with two literals also works and is sometimes clearer:

```malloy
where: order_date >= @2025-06-01 and order_date < @2025-09-01
```

Every bound names the field, and a range only goes with `?`:

| Wrong | Error | Right |
|---|---|---|
| `d > @2021 and < @2022` | `unexpected '<'` | `d ? @2021`, or `d >= @2021-01-01 and d < @2022-01-01` |
| `d > @2021 and @2022` | `'logical operator' Can't use type date` | same |
| `d = (@2021 to @2022)` | `A Range is not a value` | `d ? @2021 to @2022` |
| `ts > @2021 to @2022` | **none** - it compiles, and dropped a 2021-03-04 row | `ts ? @2021 to @2022` |

The last one is the dangerous one: a comparison operator in front of a range is accepted, so nothing warns you and the count is simply wrong. `Cannot compare a timestamp to a boolean` means the right-hand side of a comparison is itself a condition; split it into one comparison per bound.

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

Wrong: `group_by: total_sales` (where `total_sales` is `sum(price)`)  →  `Cannot use an aggregate field in a group_by operation`
Right: `group_by: category; aggregate: total_sales`

A measure is not a `calculate:` field either. `calculate:` takes a window function over an aggregate, not the aggregate itself.

Wrong: `calculate: t is total_sales`  →  `Cannot use an aggregate field in a calculate operation`
Right: `aggregate: total_sales` (or `calculate: prev is lag(total_sales)` for a window)

**Scalar functions are not aggregates.** `concat()`, `substr()`, arithmetic on raw fields, etc. belong in `group_by:` or `select:`, never `aggregate:`.

Wrong: `aggregate: full_name is concat(first_name, ' ', last_name)`
Right: `group_by: full_name is concat(first_name, ' ', last_name)`

**Counting:**
- `count()`: row count.
- `count(field)`: **distinct** count of that field.
- There is **no** `count(distinct field)` syntax, and `count(*)` is wrong.

Wrong: `count(distinct customer_id)`, `count(*)`
Right: `count(customer_id)`, `count()`

There is no method form of `count` on a joined field either: `shipments.shipment_id.count()` fails with `'shipments.shipment_id' is not a source or join`. Write `count(shipments.shipment_id)`.

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

Charts render only the **first** aggregate. For multiple measures on one chart, place `# y` above the `aggregate:` keyword or use the `y=['a','b']` shorthand. A chart annotation left as the last line inside `{ }` fails with *"Parser enountered unexpected statement type 'unimplemented'"* (the compiler's spelling); one after the closing `}` fails with *"Object annotation not connected to any object"*.

Read the `malloy-charts` skill for chart types, properties, data shape requirements, and selection guidance.

## More Compile Mistakes

**Aggregating a joined field takes method syntax.** `sum`, `avg`, `min` and `max` over a dotted path through a `join_many` fail with `Join path is required for this calculation; use 'inventory_items.item_cost.sum()'`. The message gives the fix. Over a `join_one` path the call form compiles and is correct.

Wrong: `measure: cogs is sum(inventory_items.item_cost)`
Right: `measure: cogs is inventory_items.item_cost.sum()`

`count(joined.field)` is the exception: it is the correct distinct count through a join, so keep it as written (see Counting above).

**Scalar functions never take method form, and nothing chains onto a function call.** `round`, `floor` and `ceil` are always `round(x, 2)`. The errors (`something is missing before 'round'`, `Cannot call function round(number, number) with source`) name `round` without saying so, and read like a typo somewhere else.

Wrong: `avg(price).round(2)`, `price.avg().round(2)`, `avg_price.round(2)`
Right: `round(avg(price), 2)`, `round(price, 2)`

**`sum` and `avg` need a numeric field.** `avg(status)` fails with `Can't use type string`. A name that reads numeric (`order_number`, `zip`, `account_id`) is often typed string, so check the type in the `get_context` result. Count a string field instead of averaging it.

**A measure is already an aggregate.** `aggregate: busiest is max(flight_count)` fails with `Aggregate expression cannot be aggregate` and does not name the field. Aggregate per group, then take the maximum in a second stage:
```malloy
run: flights -> { group_by: carrier, aggregate: n is flight_count } -> { aggregate: busiest is max(n) }
```
For only the top row, use `order_by: n desc` with `limit: 1`.

**A window's braces bind to the function.** Put `{ partition_by: ..., order_by: ... }` directly after the window call, not after the division: `sum_cumulative(n) { partition_by: region, order_by: n desc } / region_total`. For a share within a group, compute the denominator in `aggregate:` (`region_total is all(count(), region)`); a window's `partition_by` does not apply to `all()`.

**A dotted path must name a join the source declares.** If the source declares the join as `carrier`, `carriers.name` fails with `'carriers.name' is not a source or join`. Confirm the join name and the field under it in a `get_context` result; do not infer either from a table name or a plural/singular guess.

**`order_by:` can only name an output column.** `order_by: total` fails with `Unknown field total in output space` when the query never emits `total`. `group_by` or `aggregate` it first, and alias it if it comes through a join.

**Charts: one aggregate per view.** A `# bar_chart` or `# line_chart` view renders only its first aggregate. For several metrics, nest separate chart views in a `# dashboard`, or use `y=['revenue','cost']`.

**Define measures and dimensions in the source, and reference them in the view.** `aggregate: revenue` in a view, not a fresh `sum(total)` there.

**Truncate for charts, extract for comparisons.** `ts.month` is the first day of the month (right for a time-series chart, which then orders correctly); `month(ts)` is the number 1 to 12 (right for comparing across years). `year(ts)` renders as `2,018`; tag it `# number=id`, as for zip codes and IDs.

**Combine an alternation with other filters using a comma.** `where: is_us = true, party ? 'Democrat' | 'Republican'`. The `?` alternation means "any of these values". With `and` it works only when the alternation comes second.

**Strings.** An apostrophe inside single quotes ends the literal (`no viable alternative at input 's'`), so use double quotes: `"Mac's Diner"`. There is no concatenation operator (`unexpected '+'`, `no viable alternative at input '||'`): write `concat(origin, '-', destination)`.

**A `;` inside a clause ends it.** Between clauses a newline, comma or `;` all work. Within one clause, separate fields with commas or newlines, or the next field is orphaned (`no viable alternative at input 'charters'`). The same message appears for an unnamed aggregate after the first: `aggregate: n, max(x)` fails at `max`, so name every entry.

**`Query execution failed: ...` means Malloy compiled and the warehouse refused the SQL**, often over a type. The position it quotes is in the generated SQL, not in your query. BigQuery, for example, will not partition a window function on a FLOAT64 column; `partition_by:` takes only a field name, so cast in `group_by:` (`season_key is season::string`) and partition on `season_key`, or fix the type in the model.

## When a Query Fails

Read the error against the tables above and below. Most failures match a known pattern and can be fixed directly. If the cause is not obvious after that, remove pieces (filters, joins, nested views) until the query compiles; whatever you removed last is the bug.

| Error message | Likely cause / fix |
|---|---|
| `Cannot compare a timestamp to a number` | Comparing `date.year` to an integer. Use `date >= @2020-01-01` instead. |
| `no viable alternative at input '<word>'` | Often a `;` between fields within one clause - fields under one `aggregate:`/`group_by:` are comma- or newline-separated (the error points at the field right after the `;`). |
| `unexpected '<field>', expected 'not' or 'null'` | A **reserved word used as a name**, in a multi-line `aggregate:` / `group_by:` list. `second`, `minute`, `hour`, `day`, `week`, `month`, `quarter`, `year` are reserved. On its own line the compiler says so plainly (`'second' is a reserved word, so to use it as a name you must quote it`), but as a later entry in a multi-line list the parser has already committed, and the error points at the NEXT field instead - so you read it as a problem with the line below. Rename the field or backtick it. Verified against the compiler both ways. |
| `'logical operator' Can't use type <string\|date\|...>` | **A `?` apply that comes before `and` needs parentheses**, not just the alternation form. `where: t ? 'a' | 'b' and flag` swallows `and flag` into the alternation list, and `where: d ? @2015 and x = 'y'` fails the same way with `Can't use type date`. Wrap the apply: `where: (d ? @2015) and x = 'y'`. An apply that comes after `and` (`where: flag and t ? 'a' | 'b'`) compiles as written. The error names the *type on the left of the apply*, so it points at a clause that is perfectly fine and tells you nothing about the missing parentheses; on a date column it can also surface a spurious second error about `!= null` on an unrelated line. |
| `Circular reference to '<name>' in definition` | A **measure aliased to its own name** (`aggregate: games is games`), which compiles fine until a `having:` references it. Alias to a different name, or drop the alias entirely. |
| `Unknown function '<name>'. Use '<name>!(...)' to call a SQL function directly.` | The function does not exist in Malloy (`substring` is `substr`; there is no `median`, and no `percentile`, either as `percentile(x, 0.5)` or as `x.percentile(50)`). If `get_context` lists a percentile or median measure, use that. **In an ad-hoc query the suggested fix is a dead end**: a query sent to a server is compiled in restricted mode, so `median!(...)` then fails with "direct SQL function calls are not permitted" and following the error message costs two round trips. A saved model file is not compiled that way, so the same call is allowed there and fails only on its own merits. Use the Malloy spelling, or express it another way (an ordered `limit` for a median-like value); reach for `!(...)` only in a model file, and only knowing it pins you to one dialect. |
| `Required filter "<name>" (dimension: <dim>) was not provided` | The source declares a required `#(filter)`. `get_context` lists it under `source_info.filter_params` with its `name`, `type` and `required`; pass a value through `execute_query`'s filter-parameters argument, keyed by the filter's `name` (not the dimension). A `where:` on the dimension does not satisfy it. |
| `'<field_name>' is not defined` | Field doesn't exist in the source. Re-check against the model definition; you may have stripped a join prefix. |
| `field is a bar chart, but is not a repeated record` | Chart annotation placed inside `{ }`. Move `# bar_chart` above `run:` / `view:` / `nest:`. |
| `Parser enountered unexpected statement` | Spelled that way by the compiler. Most often a chart annotation left as the last line inside `{ }` - move it above `run:`. Also syntax Malloy doesn't allow in that position (e.g., `pick` inside a nested view). |
| Query silently returns zero rows | Filter value mismatch (case, spelling, format). Run a distinct-values query on the dimension to confirm the literal. |

## Syntax Help

For anything not covered here, call `search_malloy_docs` with the topic (for example "string functions", "nested queries").
