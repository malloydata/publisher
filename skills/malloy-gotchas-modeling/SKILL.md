---
name: malloy-gotchas-modeling
description: Common Malloy modeling mistakes and how to avoid them. Read BEFORE writing source definitions, dimensions, measures, or joins. Covers reserved words, NULL checks, date functions, type casts, field management (extend except/accept/rename vs include public/internal/private), and query-based source gotchas.
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Modeling Gotchas

> **Read this before writing Malloy code.** These patterns cause most modeling errors.

> **Tool names** are written bare here - `get_context`, `execute_query`, `search_malloy_docs`. The exact prefixed name depends on the host surface; match each against the tools you actually have.

## Reserved Words: Backtick Them

**When in doubt, backtick it.** Unquoted reserved words cause cascading errors on unrelated lines.

```malloy
// WRONG                        // RIGHT
dimension: d is Date::date      dimension: d is `Date`::date
```

Words most likely to appear as column names:
```
date, time, day, month, year, quarter, week, hour, minute, second,
number, string, boolean, type, table, source, index, count, sum, avg, min, max,
true, false, null, is, on, with, all, from, by, in, to, for, select, order_by,
top, bottom, desc, asc, row, range, current, window, rank
```

- `number`: only the bare word needs backticking; `account_number` is fine
- `source`: reserved; use a different alias like `traffic_source`

## NULL Checks: `is not null`, NOT `!= null`

```malloy
// WRONG                             // RIGHT
dimension: is_sold is sold_at != null   dimension: is_sold is sold_at is not null
```

## Date Functions vs Properties

```malloy
// WRONG: day_of_week is a function        // RIGHT
dimension: dow is created_at.day_of_week   dimension: dow is day_of_week(created_at)
```

**Property access:** `.month`, `.year`, `.quarter`, `.day`, `::date`
**Function call required:** `day_of_week()`, `week()`, `hour()`, `minute()`, `second()`

## `.date` Is a Cast, Not a Truncation

Calendar truncations are `.day`, `.week`, `.month`, `.quarter`, `.year` (plus `.hour`, `.minute`, `.second` for timestamps). `.date` is **not** among them: it's a **cast** (`::date`), not a truncation, so `created_at.date` does not compile. This bites twice: once at compile time, and again as a latent bad `#(doc)` comment that only a review pass catches ("truncated to date" is a doc smell; it should say "to day").

```malloy
// WRONG                          // RIGHT
created_at.date                   created_at.day     // truncate to day
                                   created_at::date   // cast to a date
```

## Interval Functions: `unit(start to end)`, and the unit decides the operand type

An interval is `unit(start to end)`. Two rules, both enforced by the compiler:

- **Never subtract.** `days(a - b)` fails with `Can not offset time by 'date'`. The `to` form is the only one.
- **Mixing a date and a timestamp needs a cast, unless the date side is a literal.** A date *literal* widens to a timestamp on its own, so `days(@2020-01-01 to now)` compiles. A date *column* does not: `days(signup_date to now)` fails with `Cannot measure from date to timestamp`. Cast the odd one out (`::date`, `::timestamp`). `now` is a timestamp.

Which units accept what:

| Units | Kind | Operands |
|-------|------|----------|
| `seconds`, `minutes`, `hours`, `days` | clock | timestamps or dates |
| `weeks`, `months`, `quarters`, `years` | calendar | **dates only**: on timestamps they fail with `Cannot measure interval using 'month' for 'timestamp' values; calendar interval measurement requires dates` |

```malloy
// WRONG: subtraction, and a calendar unit applied to timestamp columns
dimension: gap is days(closed_at - opened_at)
dimension: months_open is months(opened_at to closed_at)

// WRONG: approximating a calendar unit that exists
dimension: months_open is days(opened_at to closed_at) / 30.44

// RIGHT
dimension: days_open   is days(opened_at to closed_at)
dimension: months_open is months(opened_at::date to closed_at::date)
```

The calendar units are real and exact. If one fails, read the message: it is telling you to cast the operands, not to divide by 30.44.

## Safe Division: Always `nullif`

```malloy
// WRONG              // RIGHT
a / b                 a / nullif(b, 0)
```

## String Columns Need Casts for Aggregates

```malloy
// WRONG: "Can't use type string"     // RIGHT
measure: avg_score is avg(score)      measure: avg_score is avg(score::number)
```

**Dirty columns: null the sentinel before casting.** `::number` is a strict cast, so a column that carries non-numeric sentinels (`'NA'`, `'N/A'`, `''`, `'-'`, `'null'`) compiles fine but fails at query time with `Could not convert string 'NA' to DOUBLE`. Strip the sentinel with `nullif` first, then cast (aggregates skip nulls):

```malloy
// WRONG: throws on 'NA' at query time   // RIGHT: nulls 'NA', then casts
measure: s is avg(score::number)         measure: s is avg(nullif(score, 'NA')::number)
```

Chain `nullif` for multiple sentinels: `nullif(nullif(score, 'NA'), '')::number`. Sample the column's values first (`run: source -> { group_by: score; limit: 20 }`) to see which sentinels it uses.

## Boolean Columns: No Quotes

```malloy
// WRONG                                     // RIGHT
count() { where: complaint = 'true' }        count() { where: complaint = true }
```

Check schema: if `BOOL`, use `true`/`false`. If `STRING`, use `'true'`/`'false'`.

## `greatest()` / `least()` Are Null-Poisoning

Malloy's `greatest()` / `least()` return **NULL if *any* argument is null**, unlike Postgres `GREATEST`/`LEAST`, which ignore nulls. Porting a LookML/SQL expression verbatim is a silent parity bug: the number just goes null for any row with a missing input. Coalesce the result back to a non-null argument:

```malloy
// WRONG: one null input nulls the whole thing
dimension: last_touch is greatest(email_at, call_at)

// RIGHT: fall back so a null arg can't poison the result
dimension: last_touch is greatest(email_at, call_at) ?? email_at ?? call_at
```

## No Scalar Median; Raw-SQL Aggregates Don't Compile

**There is no scalar `median`, and `PERCENTILE_CONT` cannot be expressed as a measure in this build.** Every documented form for a custom SQL aggregate - `percentile_cont!(x, 0.5)`, `sql_number(...)`, `sql_number(...) { is_aggregate: true }`, and the `# is_aggregate` annotation - resolves as a **scalar** and fails with *"Cannot use a scalar field in a measure declaration."* The docs' own `avg_dist` example fails the same way. This is a deployed-runtime limitation, not a syntax error you can fix: **do not** burn cycles trying `!`, `sql_number`, or `is_aggregate` variations to get a median.

```malloy
// DOES NOT COMPILE in this build (all forms resolve as scalar):
measure: median_x is percentile_cont!(x, 0.5)
measure: median_x is sql_number("PERCENTILE_CONT(...) ...") { is_aggregate: true }
```

**Ship `avg` instead, or defer median with a documented gap** ("median deferred: no scalar median / runtime rejects raw-SQL aggregates"). Tell the user; don't silently substitute `avg` for a metric that was specified as median.

**`stddev` does work**, so reach for it when the question is about spread. It is a native Malloy aggregate rather than a raw-SQL escape, so unlike everything above it compiles both inline and as a `measure:`, and it is the sample standard deviation. `variance`, `stddev_samp`, and `stddev_pop` are not Malloy functions, and pushing them through `!` fails as a scalar exactly like `percentile_cont!`.

```malloy
// WORKS: inline, or as a measure on a source
run: order_items -> { aggregate: sd is stddev(sale_price) }
source: items is order_items extend { measure: price_stddev is stddev(sale_price) }
```

## Field Management: `extend {}` and `include {}`, in that order

Malloy has two field-management mechanisms for base sources. **`include {}` is the curated default; `extend { except / accept / rename }` handles the renames.** They do compose, but only in one order: the `extend {}` that renames must come **before** the `include {}`, and `include {}` must name the field as it is *after* the rename.

| Mechanism | Where it lives | Keywords | Experimental flag? |
|---|---|---|---|
| Access modifiers (default) | `include {}` | `public:` / `internal:` / `private:` | Yes (`##! experimental.access_modifiers`) |
| Field management | `extend {}` | `accept:` / `except:` / `rename:` | No |

### Default: `include {}` for documented, curated base sources

Use `include {}` whenever the source doesn't need a `rename:`. It's the only way to attach `#(doc)` tags to raw columns, and it's the canonical way to hide empty/garbage/duplicate columns (`internal:`) and sensitive ones (`private:`). See `skill:malloy-model` § Access Modifiers.

```malloy
##! experimental.access_modifiers
source: orders is conn.table('orders') include {
  public:
    #(doc) Order identifier
    order_id

    #(doc) Customer who placed the order
    user_id

  internal:
    raw_payload_json    // empty after JSON extraction
    legacy_status_code  // superseded by status_code
}
```

### When a `rename:` is needed: rename first, then `include {}`

The usual reason is a collision inside `include {}`: a measure cannot share a name with a raw column, even one tagged `internal:`, and the compiler says so (`Cannot redefine 'revenue' 'revenue' is internal`). The fix is to rename the raw column out of the way, which frees the name for the measure. Order is what makes it work:

```malloy
##! experimental.access_modifiers
// RIGHT: rename frees `revenue`, include curates what is left, measure takes the name
source: orders is conn.table('orders')
  extend { rename: raw_revenue is revenue }
  include {
    #(doc) Revenue as loaded, before adjustments
    internal: raw_revenue
    public: order_id, user_id
  }
  extend { measure: revenue is raw_revenue.sum() }
```

Two ways to get the order wrong, with the errors they produce:

- **`include {}` before the renaming `extend {}`** fails with `Can't find field 'X' to set access modifier`, currently surfaced as an internal compiler error. `include` runs against names that no longer exist by the time the rename is applied.
- **Naming the pre-rename column inside `include {}`** fails with `` `revenue` not found ``. After a rename only the new name exists; use it.

You do not have to give up `include {}` to get a rename: the curated surface, `#(doc)` on raw columns, and the `public/internal/private` tiers all survive. Renaming the *measure* instead is still worth considering when the raw column name is the one people know, but it is a modeling preference, not a workaround for a limitation.

### `extend {}` clauses (reference)

- **`accept:`**: allow-list, keep only the named columns
- **`except:`**: deny-list, drop the named columns; keep everything else (mutually exclusive with `accept:`)
- **`rename:`**: alias a raw column to free up its original name for a measure or dimension

### Migrating `conn.sql()` to `conn.table()` + Malloy clauses

The biggest reason teams reach for `conn.sql()` is column gating, aliasing, and per-row derivation in one place. All three have native equivalents:

1. **Verify the schema**: `run: <source> -> { select: *; limit: 1 }` to discover all columns. Anything in the table but not in the SQL's `SELECT` was being intentionally hidden, so preserve that gating.
2. Switch to `conn.table('…')`.
3. Hidden columns: `include { internal: ... }` (lets you also `#(doc)` the public columns). A `rename:` in the same source does not force you off `include {}` - see item 4 for the order.
4. SQL aliases: an `extend { rename: ... }` before `include {}`, naming the field by its new name in `include {}` (they compose, but only in that order). If the alias was to free up a name for a measure, use `rename: raw_X is X`, then `measure: X is raw_X.sum()`.
5. SQL derivations: `dimension:` definitions in `extend {}`.
6. SQL `WHERE`: source-level `where:`.

## Cannot Redefine Query-Based Source Columns

Columns from `table -> { group_by, aggregate }` or `conn.sql()` already exist. You cannot re-declare them.

```malloy
// WRONG: "Cannot redefine 'user_id'"
source: facts is conn.table('t') -> { group_by: user_id, aggregate: total is sum(amt) }
  extend { dimension: user_id is user_id }
// RIGHT: add only NEW derived dimensions
source: facts is conn.table('t') -> { group_by: user_id, aggregate: total is sum(amt) }
  extend { dimension: is_high_value is total > 1000 }
```

To add `#(doc)` tags to existing query columns, use `include {}` between the query and extend.

## Extending a Source Cannot Reuse a Name It Already Defines

```malloy
// WRONG: "Cannot redefine 'overview'" when sales already declares view: overview
source: wines is sales extend { view: overview is { aggregate: record_count } }
// RIGHT: give the extension its own name
source: wines is sales extend { view: summary is { aggregate: record_count } }
```

An extension adds to the parent's namespace, it does not override it. This bites when you extend a source to "replace" one of its views: rename the new definition, or edit the view on the parent source instead of extending it. Malloy reports the same `Cannot redefine 'X'` for dimensions and measures that collide with an inherited name, per the sections above and below.

## Never Use `conn.sql()` When Malloy Has a Native Pattern

```malloy
// WRONG: raw SQL for pre-aggregation
source: facts is conn.sql("""SELECT user_id, SUM(amount) AS total FROM orders GROUP BY user_id""")
// RIGHT: Malloy query-based source
source: facts is conn.table('orders') -> { group_by: user_id, aggregate: total is sum(amount) }
```

**Mandatory: call `search_malloy_docs` before reaching for `conn.sql()`.** Don't argue from intuition. Most patterns that look SQL-only have a Malloy equivalent, including the ones reviewers historically said couldn't be expressed.

| Looks like it needs SQL | Malloy equivalent |
|---|---|
| Multi-CTE pipeline | Stacked query-based sources: `source: a is t -> {...}`; `source: b is a -> {...}`; `source: c is b -> {...}` |
| UNNEST / array column access | `array_column.each.field`: arrays auto-join as nested tables ([data types docs](https://docs.malloydata.dev/documentation/language/datatypes#array-access)) |
| PIVOT (conditional aggregation) | Filtered aggregates: `aggregate: a is x.sum() { where: cat = 'a' }, b is x.sum() { where: cat = 'b' }` |
| Window functions (any frame, including custom) | `calculate:` with `sum_cumulative`, `lag`, `lead`, `rank`, `row_number`, `avg_moving`, `first_value`, `last_value`: supports `partition_by:` and `order_by:` ([window functions docs](https://docs.malloydata.dev/documentation/language/functions#window-functions)) |
| `ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING` | `sum_cumulative(x) - x` (cumulative-including-current minus current = cumulative-excluding-current) |
| `WHERE date = (SELECT max(date) FROM …)` (latest snapshot) | `join_cross` to a one-row aggregate source, then filter on the joined `max_date` field |
| Multi-key joins | `join_one: x is target on a = x.a and b = x.b and c = x.c` |
| `greatest()` / `least()` / `CASE` chains | All native: `greatest(a, b, c)`, `least(a, b)`, `pick 'x' when cond else 'y'` |
| Dialect-specific scalar functions | `function_name!return_type(args)`: Malloy's raw-SQL function escape (no `conn.sql()` block needed) |

**Genuinely valid `conn.sql()` candidates (rare):**

- SQL features Malloy explicitly doesn't model (e.g., DML/DDL, specific `MERGE` patterns)
- Multi-stage transformations where every CTE has 3+ joins to different tables AND the result is consumed by multiple downstream sources, but in this case an intermediate table in the data warehouse is usually still better than `conn.sql()`

**Never use `conn.sql()` for:** simple column selection or renaming, `WHERE` filters, two-table joins, column type casts, latest-snapshot patterns, conditional aggregation, or window functions of any kind.

If a project's standards file specifies a stricter policy (e.g., a `search_malloy_docs` rationale comment requirement above every `conn.sql()` block), defer to that.

## JSON Files: Read Them In Place Like CSV

```malloy
// RIGHT: .json works like .csv/.parquet
source: reviews is duckdb.table('data/reviews.json')
// RIGHT: newline-delimited JSON is read the same way
source: events is duckdb.table('data/events.ndjson')
// RIGHT: read options need read_json_auto in a SQL source
source: nested is duckdb.sql("""SELECT * FROM read_json_auto('data/reviews.json')""")
// WRONG: shelling out to python, or converting to CSV first
```

DuckDB reads JSON directly, so never preprocess a `.json` file before modeling it and never reach for a scripting language to inspect one. Both a top-level array of objects and newline-delimited JSON work through `duckdb.table()`.

Quirk: JSON carries no schema, so a value written as `"90"` arrives as a string where the same data in CSV would be inferred as a number. Cast it in the source, under a new name (reusing the column's own name is a redefinition error):

```malloy
source: reviews is duckdb.table('data/reviews.json') extend {
  dimension: points_num is points::number
}
```

## Excel Files: Read `.xlsx` In Place, Never Convert

```malloy
// RIGHT when the sheet is a plain table (header in row 1, data under it, no blank row inside
// it): read it where it sits, like .csv/.parquet (in a Publisher package the sandbox
// connection is `duckdb`)
source: budget is duckdb.table('data/budget.xlsx')
// RIGHT for anything messier. Profile the top rows first to find the real header row and the
// last real column, because nothing else will tell you where they are. Put the probe in the
// model file as its own source: Publisher refuses raw SQL in an ad-hoc query.
//   SELECT * FROM read_xlsx('data/sales.xlsx', sheet = 'Sales Data',
//                           range = 'A1:Z15', header = false, all_varchar = true)
source: sales is duckdb.sql("""
  SELECT * FROM read_xlsx('data/sales.xlsx',
    sheet = 'Sales Data',      -- EDIT: only the first sheet is read by default
    header = true,
    range = 'A5:J100000'       -- EDIT: A5 is the real header row. Keep the column bound at the
  )                            -- last real column; the row bound just has to clear the end.
  WHERE "Order ID" LIKE 'SO-%' -- EDIT, REQUIRED: a data-row predicate. This is what ends the
""")                           -- read; drop it and every empty row in the range comes back.
// WRONG: converting the spreadsheet to Parquet or CSV first (an unnecessary extra step)
```

Do not convert spreadsheets before modeling. DuckDB's excel extension reads `.xlsx` directly and loads automatically on first use, so a sheet that is a plain table needs nothing more than `duckdb.table()`. Converting does not avoid any of the problems below, it just moves them into a copy that goes stale the next time someone updates the workbook.

**Plenty of real exports are not plain tables, and nothing tells you.** A report title, a "generated on" banner, a merged group header, a blank line above the header, or a blank spacer row inside the data are all ordinary, and none of them is visible from Malloy. There is no error either: the package loads, the server reports serving, the query returns 200, and the number is just wrong. So make two checks before building on the read: compare `aggregate: record_count is count()` against what you know is in the file, and `select: *; limit: 1` to see what the columns really are. If either disagrees with the file, the read is wrong and so is every measure over it.

`table()` takes a plain file path only, so anything needing `read_xlsx` options (`sheet`, `range`, `header`, `ignore_errors`, `normalize_names`, `all_varchar`, `empty_as_varchar`, `stop_at_empty`) goes through the SQL-source form.

Quirks:

- Only the FIRST sheet is read by default. Select another with `sheet = 'Name'`. There is no function that lists a workbook's sheet names, but passing one that does not exist reports a suggestion (`Sheet "x" not found ... Did you mean: "Notes"`), which is one way to find a name you were not given.
- A title or banner row above the header collapses the read. DuckDB takes the first row it finds as the column names, so a lone title cell in A1 becomes the only column. How many rows you then get is the next quirk's business: whatever sits between the title and the first blank row, often none or one, otherwise a plausible-looking partial count. Pass a `range` that starts at the real header row.
- With no `range`, `stop_at_empty` defaults to true and the read stops at the first blank row, which on a real sheet is usually a spacer between blocks rather than the end of the data: a 30-row sheet with one spacer after row 10 reads as 10 rows. `stop_at_empty = false` lifts that, but it only helps when the header really is in row 1; with a title above the header you need the `range` anyway, and a `range` flips the default for you. It also hands the blank rows back as all-null rows, so the count comes out one high per spacer until you filter them.
- A `range` reads every cell inside it, so an overshot bound manufactures padding: past the last real column you get all-null fields (`A5:Z100000` on a ten-column sheet yields 26, the extras named `C10` and `_1` through `_15`), and past the last real row all-null rows (`A5:J100000` on a 1,500-row sheet reads 99,995). Spacers, subtotals, and footnotes come through as rows too. So the row filter is not tidying-up, it is the thing that ends the read: filter to what a data row looks like (`WHERE "Order ID" LIKE 'SO-%'`) rather than to `IS NOT NULL`, which keeps any footnote carrying text in the first column. A bound that falls SHORT of the data is the dangerous direction: the rows and columns past it are dropped with no error at all, so overshoot the row bound and let the filter end the read.
- Every number in an xlsx is stored as a double, so there are no integer columns. Typing is per column and decided by the FIRST data row, and `$1,234`, `12%` and `N/A` are all text: a text cell in that first row makes the whole column a string (on one real export, all ten of them), while a text cell further down leaves the column numeric and makes the read throw instead (`Could not convert string ... to DOUBLE`). `ignore_errors = true` fixes that second case, nulling the bad cells and keeping the column a number. It does nothing for the first.
- Sample the column's SHAPES before writing any conversion, not its values: `run: source -> { group_by: shape is replace(raw_col, r'[0-9]', '9'); aggregate: n is count(); order_by: n desc }` collapses every value to its format and counts it, so on one real price column the 16 euro-denominated rows surface beside the 1,484 in dollars. A plain `group_by raw_col; limit: 20` sorts lexicographically, which hides exactly the shapes that matter.
- Convert in the SQL source, not in Malloy, where `::number` throws on the first bad cell. `try_cast(regexp_replace("Total Revenue", '[^0-9.-]', '', 'g') AS double)` nulls what it cannot read instead of failing and is right for a plain `$1,234.56`, but it is not a general parser. It concatenates every digit in the cell, so `1,234 (see tab 2)` becomes 12342. It understands only a leading ASCII `-`, so an accounting `(1,234)`, a Unicode minus and a `CR` suffix all come back positive, while a trailing `-` (`1,234-`) comes back null and drops the row from the sum. And it assumes `.` is the decimal point, so a European `1.234,56` comes back a thousandfold small. Handle the shapes your sample actually found, and divide a percent by 100. Failure is quiet either way: a cast that fails on every row sums to 0 rather than erroring, and a text date strips to a number rather than a null (`'01/02/2023'` becomes 1022023).
- Check the answer against the sheet's own total row, read as raw text. Lift the data-row filter and select the footer by its label, which usually sits in a different column from the one your data-row predicate uses: on one export `WHERE "Customer Name" = 'TOTAL'` finds it and `WHERE "Order ID" = 'TOTAL'` returns nothing, and an empty result reads as a pass. Do not run the total through the same expression, because a wrong sign survives a row count, survives `select: *`, and cancels out when both sides are parsed the same broken way.
- A sheet with no header row whose first row is all text silently loses that row to header detection. Pass `header = false`.
- Headers with spaces are kept verbatim: backtick them in Malloy, or pass `normalize_names = true` for snake_case names.
- `all_varchar = true` hands back each cell's stored value as text, so a date arrives as its raw Excel serial number rather than a date: `'44929'` from a sheet Excel wrote, `'44927.0'` from one DuckDB's own xlsx writer wrote, and `'44929.5'` where the cell carries a time of day. Which form you get depends on the tool that wrote the file, so do not detect serials by matching for an integer; `try_cast(... AS double)` accepts all three and returns null for a cell that was stored as text (`'01/02/2023'`), which is the test you want. Convert with `date '1899-12-30' + floor(try_cast(d AS double))::int`, not from 1900-01-01. Both wrappers earn their place: adding a double to a date does not compile, and a bare `::int` rounds, so an afternoon timestamp would land on the next day.
- A date column that mixes both, which is what an export edited by hand gives you, needs both branches or you silently lose every row of one kind: `CASE WHEN try_cast(d AS double) IS NOT NULL THEN date '1899-12-30' + floor(try_cast(d AS double))::int ELSE try_strptime(d, '%m/%d/%Y')::date END`. Without `all_varchar`, a uniformly date-formatted column arrives as real `date` and `timestamp` values, and a stray text cell behaves exactly as the typing rule above says. Note what `ignore_errors = true` does here: it nulls that cell rather than parsing it, so the hand-typed date is lost silently.

## Duplicate Rows: Check Before Building Measures

```malloy
run: source -> { group_by: pk_field, aggregate: n is count(), having: n > 1, limit: 10 }
```

Symptoms: `sum()` returns astronomical values. Causes: event tables, batch retries, merged sources.

## Mixed-Grain Joins: A Pre-Aggregated Source Ignores Your Filters

Joining an aggregate-grain source (a decade/month/region summary table) into a detail-grain source produces values that do **not** respond to the query's filters. Malloy's symmetric aggregates prevent fan-out; they cannot prevent this, because the joined value is unfiltered *by construction*: it was computed over the whole population before the query ran.

```
run: track_analysis -> {
  where: genre = 'Rock'
  group_by: decade
  aggregate: track_count                    // filtered: Rock only     -> 701
  group_by: decade_trends.decade_track_count // unfiltered population  -> 1,088
}
```

Two count-shaped numbers side by side, one filtered and one not; read as "701 of 1,088 Rock tracks" it is simply wrong: 1,088 is every genre. Two legitimate resolutions:

- **Keep the join as a population baseline** when comparing a row to the whole population is the intent (e.g. `energy_vs_decade`). Then every joined field's `#(doc)` must say it is a fixed population value that does not respond to filters, and count-shaped fields with no comparison purpose (like `decade_track_count`) should be `internal:`; they only invite the misreading.
- **Compute the aggregate as a query-based source from the detail table** so it derives from one source of truth and the derivation is visible.

This is the modeling-time consequence of ignoring `skill:malloy-scope`'s advice to skip pre-aggregated snapshot tables and compute fresh in Malloy instead.

## Thresholds Are Decisions, Not Syntax

Before writing a `pick` expression or filtered measure with a numeric cutoff, see `skill:malloy-model` § Key Rules: every boundary must be user-supplied, distribution-derived (query the percentiles first), or explicitly flagged as an assumption in its `#(doc)`. Never invent one silently.

## `except:` Removes Fields From Namespace Entirely

`except:` in `include {}` completely removes fields: dimensions and measures cannot reference excluded fields. Use `internal:` instead when derived dimensions need the raw column.

```malloy
// WRONG: dimension references excluded field
source: x is conn.table('t')
include { except: raw_date }
extend { dimension: order_date is raw_date::date }  // ERROR! raw_date is gone

// RIGHT: internal fields are still available in extend
source: x is conn.table('t')
include { internal: raw_date }
extend { dimension: order_date is raw_date::date }  // Works
```

## Source Order: Define Joined Tables First

Malloy compiles top-to-bottom. Define lookup/dimension tables before the source that joins them, or use `import` statements in multi-file projects.

## MUST Search Docs Before Using Unfamiliar Patterns

Call `search_malloy_docs` BEFORE first use of any of these. Don't guess the syntax:
- `pick` expressions
- Window functions (`calculate`)
- `percentile` or statistical functions: but see the hard limit above, raw-SQL aggregates (`sql_number` / `is_aggregate` / `percentile_cont!`) do **not** compile as measures in this build; there is no scalar median (`stddev` is the exception and does work as a measure)
- Time interval functions (`days()`, `months()`): always `unit(start to end)`, and calendar units need date operands (see above)
- Query-based sources (`source: x is (q -> {...}) extend {...}`; `from()` was removed and no longer parses)
- `!` operator / `sql_number()`
