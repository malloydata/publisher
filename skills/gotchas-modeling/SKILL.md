---
name: gotchas-modeling
description: Common Malloy modeling mistakes and how to avoid them. Read BEFORE writing source definitions, dimensions, measures, or joins. Covers reserved words, NULL checks, date functions, type casts, field management (extend except/accept/rename vs include public/internal/private), and query-based source gotchas.
---

# Modeling Gotchas

> **Read this before writing Malloy code.** These patterns cause most modeling errors.

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

## Boolean Columns: No Quotes

```malloy
// WRONG                                     // RIGHT
count() { where: complaint = 'true' }        count() { where: complaint = true }
```

Check schema: if `BOOL`, use `true`/`false`. If `STRING`, use `'true'`/`'false'`.

## Field Management: `extend {}` vs `include {}` Don't Compose

Malloy has two field-management mechanisms for base sources. **`include {}` is the curated default; `extend { except / accept / rename }` is the fallback when a `rename:` is unavoidable.** They have different capabilities and **do not combine**.

| Mechanism | Where it lives | Keywords | Compatible with `rename:`? | Experimental flag? |
|---|---|---|---|---|
| Access modifiers (default) | `include {}` | `public:` / `internal:` / `private:` | **No** | Yes (`##! experimental.access_modifiers`) |
| Field management (fallback) | `extend {}` | `accept:` / `except:` / `rename:` | Yes (same block) | No |

### Default: `include {}` for documented, curated base sources

Use `include {}` whenever the source doesn't need a `rename:`. It's the only way to attach `#(doc)` tags to raw columns, and it's the canonical way to hide empty/garbage/duplicate columns (`internal:`) and sensitive ones (`private:`). See `skill:malloy-model` (reference/access-modifiers.md).

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

### When `rename:` is unavoidable: fall back to `extend {}`

`include {}` does not compose with `rename:`. The combination errors with `Can't find field 'X' to set access modifier` because `rename:` runs first and leaves no `X` for `include` to attach a modifier to. There's also a collision inside `include {}` itself: a measure cannot share a name with a raw column, even one tagged `internal:` (`Cannot redefine 'X'`), and the natural fix for that is `rename:`, which then triggers the first error.

When a rename is genuinely required (most often during `conn.sql()` to `conn.table()` migration where a SQL alias matches a measure name that's already in heavy use downstream), drop `include {}` and curate the source with `extend { except: ... }` + `rename:` instead. You forfeit `#(doc)` on raw columns and the `public/internal/private` tiers, but keep column gating and the rename.

```malloy
// RIGHT: rename is required to free `revenue` for the measure
extend {
  except: legacy_status_code   // hide garbage column without include {}
  rename: raw_revenue is revenue
  measure: revenue is raw_revenue.sum()
}
```

If you can rename the measure or split the source instead, prefer that: it preserves `include {}` and the curated surface.

### `extend {}` clauses (reference)

- **`accept:`**: allow-list, keep only the named columns
- **`except:`**: deny-list, drop the named columns; keep everything else (mutually exclusive with `accept:`)
- **`rename:`**: alias a raw column to free up its original name for a measure or dimension

### Migrating `conn.sql()` to `conn.table()` + Malloy clauses

The biggest reason teams reach for `conn.sql()` is column gating, aliasing, and per-row derivation in one place. All three have native equivalents:

1. **Verify the schema**: `run: <source> -> { select: *; limit: 1 }` to discover all columns. Anything in the table but not in the SQL's `SELECT` was being intentionally hidden, so preserve that gating.
2. Switch to `conn.table('…')`.
3. Hidden columns: preferably `include { internal: ... }` (lets you also `#(doc)` the public columns). If a `rename:` is also needed in the same source, fall back to `extend { except: ... }`.
4. SQL aliases: `extend { rename: ... }` (forces the fallback path, since `rename:` and `include {}` don't compose). If the alias was to free up a name for a measure, use `rename: raw_X is X`, then `measure: X is raw_X.sum()`.
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

## Never Use `conn.sql()` When Malloy Has a Native Pattern

```malloy
// WRONG: raw SQL for pre-aggregation
source: facts is conn.sql("""SELECT user_id, SUM(amount) AS total FROM orders GROUP BY user_id""")
// RIGHT: Malloy query-based source
source: facts is conn.table('orders') -> { group_by: user_id, aggregate: total is sum(amount) }
```

**Mandatory: call `malloy_searchDocs` before reaching for `conn.sql()`.** Don't argue from intuition. Most patterns that look SQL-only have a Malloy equivalent, including the ones reviewers historically said couldn't be expressed.

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

If a project's standards file specifies a stricter policy (e.g., a `malloy_searchDocs` rationale comment requirement above every `conn.sql()` block), defer to that.

## Duplicate Rows: Check Before Building Measures

```malloy
run: source -> { group_by: pk_field, aggregate: n is count(), having: n > 1, limit: 10 }
```

Symptoms: `sum()` returns astronomical values. Causes: event tables, batch retries, merged sources.

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

Call `malloy_searchDocs` BEFORE first use of any of these. Don't guess the syntax:
- `pick` expressions
- Window functions (`calculate`)
- `percentile` or statistical functions
- Time interval functions (`days()`, `seconds()`)
- Query-based sources (`from()`)
- `!` operator / `sql_number()`
