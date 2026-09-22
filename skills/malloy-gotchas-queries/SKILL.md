---
name: malloy-gotchas-queries
description: Common Malloy query and view mistakes, for writing views, queries or notebooks - chart constraints, aggregate filters, joined field aliasing, method syntax, time truncation vs extraction.
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Query & View Gotchas

> **Read this before writing views or queries.** These patterns cause most query errors.

## Charts: ONE Aggregate Per View

Charts render only the **first** aggregate. Use exactly one aggregate per `# bar_chart` / `# line_chart` view.

```malloy
// WRONG: revenue is ignored
# bar_chart
view: x is { group_by: status, aggregate: order_count, revenue }
// RIGHT: single aggregate
# bar_chart
view: x is { group_by: status, aggregate: revenue }
```

For multiple metrics: nest separate chart views in a `# dashboard`, or use `y=['revenue','cost']` for multi-measure series.

## Joined Fields in `order_by`: Must Alias First

```malloy
// WRONG: compile error
view: x is { group_by: races.season_year, aggregate: pts, order_by: races.season_year }
// RIGHT: alias then reference
view: x is { group_by: yr is races.season_year, aggregate: pts, order_by: yr }
```

Any time you `group_by` a joined field, create an alias and use it in `order_by`.

## `having:` vs `where:`: Aggregate Filters

```malloy
// WRONG: "Aggregate expressions not allowed in where"
view: x is { group_by: cat, aggregate: n is count(), where: n > 10 }
// RIGHT
view: x is { group_by: cat, aggregate: n is count(), having: n > 10 }
```

- `where:` filters rows BEFORE aggregation (dimensions/raw columns)
- `having:` filters AFTER aggregation (measures)

## Aggregating Joined Fields: Method Syntax

```malloy
// WRONG: compile error: "Join path is required for this calculation; use 'inventory_items.item_cost.sum()'"
measure: cogs is sum(inventory_items.item_cost)
// RIGHT: method syntax
measure: cogs is inventory_items.item_cost.sum()
```

`sum`, `avg`, `min`, and `max` over a dotted joined path all produce that compile error; the diagnostic message even tells you the exact fix. Don't worry about catching this in code review; the compiler does it for you.

**Method syntax is for aggregates over a path. Scalar functions never take it.**

```malloy
// WRONG: "something is missing before 'round'"
aggregate: avg_price_r is avg(price).round(2)
aggregate: avg_price_r is price.avg().round(2)
// WRONG: "Cannot call function round(number, number) with source"
aggregate: avg_price_r is avg_price.round(2)
dimension: rounded is price.round(2)
// RIGHT: scalar functions are always call form
aggregate: avg_price_r is round(avg(price), 2)
dimension: rounded is round(price, 2)
```

Two separate rules produce those errors:

- **No method call chains onto the result of a function call.** `avg(price).round(2)` and `price.avg().round(2)` are both parse errors. The message names `round` without saying it is unsupported in that position, so it reads like a typo somewhere else. `.floor()` and `.ceil()` fail identically.
- **Scalar functions have no method form.** `round`, `floor`, and `ceil` are always `round(x, 2)`, never `x.round(2)`, whether `x` is a named measure or a plain column.

`price.avg()` and `inventory_items.item_cost.sum()` are correct because `avg` and `sum` are aggregate functions over a field path, which is exactly what method syntax is for.

**Exception: `count(joined.field)` is correct, not a bug.** `count(joined.field)` is the **canonical Malloy idiom** for distinct-count through a join. Keep it as-is even when nearby `sum`/`avg`/`min`/`max` calls have to use method syntax. The closest method-syntax form `joined.count()` counts *rows* in the joined source (different semantics, differs from the distinct count when the joined field has duplicates within the joined table). The Malloy docs example `joined.count(field)` does NOT compile against current Malloy (error: `Expression illegal inside path.count()`); it only works for double-nested paths like `aircraft.count(aircraft_models.code)`.

## `sum`/`avg` Need a Numeric Field

```malloy
// WRONG: "Can't use type string" - status is a string column
aggregate: avg_status is avg(status)
// RIGHT: aggregate a numeric field; count a string one
aggregate: avg_price is avg(price), statuses is count(status)
```

Check the field's type in the `get_context` result before aggregating it. A name that reads numeric (`order_number`, `zip`, `account_id`) is very often typed string.

## A Measure Is Already an Aggregate

```malloy
// WRONG: "Aggregate expression cannot be aggregate" - flight_count is a measure
run: flights -> { aggregate: busiest is max(flight_count) }
// RIGHT: aggregate per group, then take the max in a second stage
run: flights -> { group_by: carrier, aggregate: n is flight_count } -> { aggregate: busiest is max(n) }
```

The message does not name the field. When it appears, look for a model measure you wrapped in another aggregate. If you only need the top row, skip the second stage: `order_by: n desc` with `limit: 1`.

## `= null` Silently Matches Nothing

```malloy
// WRONG: may return zero rows with no error
aggregate: missing is count() { where: seats = null }
// RIGHT
aggregate: missing is count() { where: seats is null }
```

Depending on the server this is either rejected or run as a filter that matches nothing, and a count of zero looks like an answer. Use `is null` / `is not null`.

## Window Braces Bind to the Function

```malloy
// WRONG: "`partition_by` is not supported for this kind of expression"
calculate: share is sum_cumulative(n) / all(n) { partition_by: region }
// RIGHT: the brace goes directly after the window function call
calculate: share is sum_cumulative(n) { partition_by: region } / all(n)
```

## Dotted Paths Must Name a Declared Join

```malloy
// WRONG: the source declares the join as `carrier`, so this fails with
//   "'carriers.name' is not a source or join"
run: flights -> { group_by: carriers.name }
// RIGHT: use the join name the source actually declares
run: flights -> { group_by: carrier.nickname }
```

A dotted path resolves only against a join declared on the source you are running. Confirm both the join name and the field under it in a `get_context` result; do not infer either from a table name or a plural/singular guess.

## `order_by:` Can Only Name an Output Column

```malloy
// WRONG: "Unknown field total in output space" - total is never emitted
run: orders -> { group_by: state, aggregate: revenue is sum(total), order_by: total }
// RIGHT: order by a column the query actually outputs
run: orders -> { group_by: state, aggregate: revenue is sum(total), order_by: revenue }
```

`order_by:` resolves against the query's *output* columns, not the source's fields. To order by something, `group_by` or `aggregate` it first - and if it comes through a join, alias it (see above).

## Chart Annotation Placement

Place `# bar_chart` / `# line_chart` on the **nested view definition**, not on `nest:` itself. Putting it on `nest:` causes "not a repeated record" errors.

## DRY: Define in Source, Reference in View

```malloy
// WRONG: inline in view
view: summary is { aggregate: revenue is sum(total) }
// RIGHT: reference existing measure
view: summary is { aggregate: revenue }
```

## Time Truncation vs Extraction

| Syntax | What it does | Returns |
|--------|--------------|---------|
| `ts.month` | Truncates to start of month | Timestamp (`@2024-03-01`) |
| `month(ts)` | Extracts month number | Integer (1-12) |
| `ts.year` | Truncates to start of year | Timestamp (`@2024-01-01`) |
| `year(ts)` | Extracts year number | Integer (2024) |

Use `.month` for time series charts (proper date ordering). Use `month()` for cross-year comparison.

**Year integers render with commas.** `year(ts)` displays as `2,018`. Tag with `# number=id` to suppress commas. Same for zip codes, IDs.

## `?` Alternation: Use Commas to Combine Filters

The `?` operator is Malloy's **alternation operator**: a shorthand for "match any of these values." `party ? 'Democrat' | 'Republican'` means `party = 'Democrat' OR party = 'Republican'`. The `|` separates the alternatives.

When combining an alternation filter with other filters, **use a comma**:

```malloy
// CANONICAL: commas separate independent filter conditions
where: is_us = true, party ? 'Democrat' | 'Republican'
```

`and` works in some arrangements (when the alternation is the second operand) but produces a confusing `'logical operator' Can't use type string` compile error when the alternation comes first. The comma form is unambiguous in every position, so just use it.

## Strings: Apostrophes and Concatenation

```malloy
// WRONG: "no viable alternative at input 's'" - the apostrophe ends the literal
where: name = 'Mac's Diner'
// RIGHT
where: name = "Mac's Diner"

// WRONG: "unexpected '+'" / "no viable alternative at input '||'"
group_by: route is origin ++ '-' ++ destination
// RIGHT: there is no concatenation operator
group_by: route is concat(origin, '-', destination)
```

## A Semicolon Inside a Clause Ends It

Between clauses a newline, comma or `;` all work. Within one clause, separate fields with commas or newlines: a `;` closes the clause, and the next field is orphaned.

```malloy
// WRONG: "no viable alternative at input 'charters'"
run: schools -> { aggregate: total is count(); charters is count() { where: is_charter } }
// RIGHT
run: schools -> { aggregate: total is count(), charters is count() { where: is_charter } }
```

The same message appears for an unnamed aggregate after the first: `aggregate: n, max(x)` fails at `max`. Name every entry.

## Compiles, Then Refused by the Warehouse

`Query execution failed: ...` means the Malloy compiled and the warehouse rejected the SQL, often over a type. The position it quotes is in the generated SQL, not in your query. BigQuery, for example, will not partition a window function on a FLOAT64 column; cast it (`partition_by: season::string`) or fix the type in the model.
