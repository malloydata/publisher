---
name: malloy-debug
description: Fix Malloy compile errors and understand error messages. Use when encountering errors in .malloy files, user says "fix this error", "malloy error", "compile error", "syntax error", or sees 20+ cascading errors.
---

# Debugging Malloy Errors

> **Tool names** are written bare here - `get_context`, `execute_query`, `search_malloy_docs`. The exact prefixed name depends on the host surface; match each against the tools you actually have.

## Get Diagnostics

**Claude Code (in VS Code terminal):** Call `mcp__ide__getDiagnostics` with the file URI.

**VS Code Copilot / Cursor:** Use the `ReadLints` tool on the file path, or open the file and check lints in the editor.

**Claude Code (standalone terminal):** No IDE diagnostics available. Ask the user to open the file in VS Code with the Malloy extension and report the errors.

## Strategy

**Errors cascade.** Later errors may be caused by or hidden behind earlier ones. Fix the FIRST error only, re-check diagnostics, repeat. Do not attempt to fix multiple errors at once.

1. Look at FIRST error, ignore all others
2. Call `search_malloy_docs` with the error message if unsure
3. Fix that one issue, re-check diagnostics
4. Repeat until clean. New errors may appear as earlier ones are resolved

## Quick Fixes

| Error | Fix |
|-------|-----|
| "Unknown field" | Check typo, source order, wrong source, or missing `import` |
| "Can't use type string" | Cast: `field::number` |
| "Aggregate not allowed in where" | Use `having:` instead |
| 20+ random errors | Backtick reserved word (`` `Date` ``, `` `Hour` ``, `` `number` ``) |
| "Can't find field" with `rename:` | Never use `rename:`. It's incompatible with `include {}`. Use `internal:` + `dimension:` instead |
| Import path errors | Check paths: `import "orders.malloy"`. All files should be in the same directory (flat layout) |
| `from()` errors | Verify the source query returns the expected columns, check that imported sources are defined |
| "Cannot redefine 'X'" | Field already exists from query-based source (`-> { group_by, aggregate }`). Remove the dimension, add only NEW derived fields in `extend {}`. Use `include {}` to add `#(doc)` tags to existing fields. |

## Gotchas Checklist

### Backtick Reserved Words
```malloy
// WRONG                        // RIGHT
dimension: d is Date::date      dimension: d is `Date`::date
```
Common reserved words: `Date`, `Timestamp`, `Type`, `Hour`, `source`, `year`, `month`, `day`, `week`, `quarter`, `count`, `sum`, `avg`, `min`, `max`, `number`, `string`, `boolean`, `true`, `false`, `order`, `select`, `from`, `where`, `index`, `table`, `time`, `now`, `today`, `range`, `window`, `row`, `current`

**When in doubt, backtick it.** See the Malloy docs (https://docs.malloydata.dev) for the full categorized list of reserved words.

**Note on `number`:** Only the bare word needs backticking. Compound names like `account_number` are fine.

### Use `is not null` for NULL Checks
```malloy
// WRONG                        // RIGHT
is_sold is sold_at != null      is_sold is sold_at is not null
```

### Call Date Functions, Don't Access as Properties
```malloy
// WRONG                        // RIGHT
dow is created_at.day_of_week   dow is day_of_week(created_at)
```
Properties: `.month`, `.year`, `.day` | Functions: `day_of_week()`, `week()`, `hour()`

### Use `having:` for Aggregate Filters
```malloy
// WRONG                        // RIGHT
where: order_count > 10         having: order_count > 10
```

### Cast Strings Before Aggregating
```malloy
// WRONG                        // RIGHT
avg(score)                      avg(score::number)
```

### Use Boolean Literals Without Quotes
```malloy
// WRONG                        // RIGHT
where: active = 'true'          where: active = true
```

### Alias Joined Fields Before Using in order_by
```malloy
// WRONG                        // RIGHT
group_by: races.year            group_by: yr is races.year
order_by: races.year            order_by: yr
```

### Use Method Syntax for Joined Aggregates
```malloy
// WRONG                        // RIGHT
sum(items.cost)                 items.cost.sum()
```

### Define Lookup Tables First (or Use Imports)
```malloy
// Single file: define lookup tables before referencing them
// Multi-file: use import statements
import "customers.malloy"

source: orders is conn.table('orders') extend {
  join_one: customers with customer_id  // Works because customers imported
}
```

### Use `nullif` for Division
```malloy
// WRONG                        // RIGHT
a / b                           a / nullif(b, 0)
```

## Cross-File Errors

| Error | Fix |
|-------|-----|
| "Can't find source X" | Add `import "X.malloy"` at top of file (all files in same directory) |
| Wrong import path | All `.malloy` files should be in the package root (flat layout). Use `import "orders.malloy"`, not `import "../sources/orders.malloy"` |
| Circular imports | Source A imports Source B which imports Source A. Restructure to break the cycle |
| `from()` "Can't find field" | Verify the source query's GROUP BY and aggregate fields match what you reference in `extend {}` |
