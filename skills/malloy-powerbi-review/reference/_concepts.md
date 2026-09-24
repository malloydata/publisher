<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Power BI → Malloy Concept Mapping

Reference table for translating Power BI (TMDL / DAX) constructs to Malloy. Referenced by multiple reference files.

## Model Objects

| Power BI (TMDL) | Malloy | Notes |
|--------|--------|-------|
| `table X` | `source:` | One source per table |
| `column Y` | column reference, or `dimension:` when renamed or derived | Plain columns need no declaration |
| `column Y` with `type: calculated` | `dimension:` with the translated expression | DAX evaluated at refresh; see `translate-measures.md` |
| `measure M = <DAX>` | `measure:` | The translation is the hard part, not the syntax |
| `partition P = m` with `mode: import` | `conn.table('schema.table')` | The M code names the real source table |
| `partition P` with `mode: directQuery` | `conn.table('schema.table')` | Same target, no imported copy |
| `table X` whose partition is a DAX expression | Computed source, or push upstream | A calculated table |
| `/// description text` | `#(doc)` tag | Triple-slash doc comment, directly above the object |
| `isHidden: true` (or bare `isHidden`) | **not a direct mapping** | Classify the reason first; see `rls-roles.md`. Do NOT map it to `internal:` mechanically: Power BI hides surrogate keys by default and `internal` fields are unreachable through join paths. |
| `role R` + `metadataPermission` / `columnPermission` | `private:` / `internal:` | Object-level security, a real permission; see `rls-roles.md` |
| `displayFolder:` | nothing | Authoring convenience, no Malloy equivalent |
| `hierarchy` with `level` | nothing structural | Note level order as drill intent |
| `role R` + `tablePermission` | `#(access_filter)` / `#(authorize)` | Narrow grammar; see `rls-roles.md` |
| `perspective` | access modifiers, or a separate source | A curated subset of the model |
| `LocalDateTable_<guid>`, `DateTableTemplate_<guid>` | **skip** | Auto-generated, one per date column |

## Relationships

A Power BI relationship is declared once at model level, not on either table. `fromColumn` is the **many** side, `toColumn` is the **one** side.

**TMDL writes only non-default properties.** A typical relationship is three lines - `relationship <guid>`, `fromColumn:`, `toColumn:` - and the absence of the properties below is meaningful, not missing data. Read absent as **many-to-one, single-direction, active**. The relationship name is a GUID and carries no information. The same shorthand applies to booleans generally: `isHidden` alone implies `true`, so a grep for `isHidden: true` misses every one written that way.

| Power BI | Malloy | Notes |
|--------|--------|-------|
| `fromCardinality: many` / `toCardinality: one` | `join_one:` on the fact source | The common case |
| `fromCardinality: one` / `toCardinality: one` | `join_one:` | Direct mapping |
| `fromCardinality: many` / `toCardinality: many` | **flag** | No direct equivalent; changes results |
| `crossFilteringBehavior: oneDirection` (default) | ordinary `join_one:` | Filter flows from the one side to the many side |
| `crossFilteringBehavior: bothDirections` | **flag** | No Malloy equivalent; ask what it was for |
| `isActive: false` | **flag** | Exists for `USERELATIONSHIP`; find the measures that use it |
| `relyOnReferentialIntegrity: true` | nothing | A query-plan hint |

## Column Types

| Power BI `dataType` | Malloy | Notes |
|--------|--------|-------|
| `string` | `string` | Direct mapping |
| `int64` | `number` | Direct mapping |
| `double` | `number` | Direct mapping |
| `decimal` | `number` | Fixed-point in Power BI; check precision if money |
| `boolean` | `boolean` | Direct mapping |
| `dateTime` | `date` or `timestamp` | Pick by whether a time component is used |
| `binary` | **skip** | Not analyzable |

## Aggregations

| DAX | Malloy | Notes |
|--------|--------|-------|
| `SUM(T[c])` | `sum(c)` | Direct mapping |
| `AVERAGE(T[c])` | `avg(c)` | Direct mapping |
| `MIN` / `MAX` | `min()` / `max()` | Direct mapping |
| `SUMX(T, <row expression>)` | `sum(<expression>)` | **The common case, and it is a direct mapping.** `SUMX(Sales, Sales[Qty] * Sales[Price])` is `sum(qty * price)`. No context transition is involved. |
| `AVERAGEX` / `MINX` / `MAXX` / `COUNTX` over a row expression | `avg()` / `min()` / `max()` / `count()` | Same: an iterator over a row-level expression is an ordinary aggregate |
| `SUMX(T, [Some Measure])` | **flag** | A measure reference inside the iterator *is* context transition; see `translate-measures.md` |
| `RELATED(Other[c])` | `other.c` | Follows a relationship; becomes a join path |
| `RELATEDTABLE(Other)` | the joined source | Usually an aggregate over a `join_many:` |
| `SELECTEDVALUE(T[c])` | **flag** | Depends on the query's grouping; no measure-level equivalent |
| `VAR x = ... RETURN ...` | inline, or a `dimension:` | A local binding; translate the body |
| `COUNTROWS(T)` | `count()` | Direct mapping |
| `DISTINCTCOUNT(T[c])` | `count(c)` | **`count(field)` is already the distinct count in Malloy.** `count(distinct c)` is a parse error, not a deprecation. |
| `COUNT(T[c])` | `count() { where: c is not null }` | **Not `count(c)`.** DAX `COUNT` counts non-blank rows; Malloy's `count(c)` counts *distinct* values. They differ on any column with repeats, and the wrong one compiles. |
| `DIVIDE(a, b)` | `a / nullif(b, 0)` | `DIVIDE` returns blank on divide-by-zero |
| `DIVIDE(a, b, alt)` | `(a / nullif(b, 0)) ?? alt` | The third argument is the divide-by-zero result |
| `COALESCE(a, b)` | `a ?? b` | Direct mapping |
| `IF(cond, a, b)` | `pick a when cond else b` | Direct syntax translation |
| `SWITCH(TRUE(), c1, v1, ...)` | chained `pick ... when` | Direct syntax translation |
| `BLANK()` | `null` | Not identical in arithmetic; see below |

**`BLANK()` is not `null`.** In DAX, `BLANK() + 1` is `1`; in SQL and Malloy, `null + 1` is `null`. A measure that leans on blank-as-zero arithmetic changes behavior on translation. Check any measure that adds or subtracts other measures.

## Filter Context

**This section is the one that produces wrong numbers.** Read `translate-measures.md` before translating anything that uses `CALCULATE`.

| DAX | Malloy | Notes |
|--------|--------|-------|
| `CALCULATE(expr, T[c] = "x")` | `measure { where: c = 'x' }` **only when safe** | DAX **overwrites** the filter on `T[c]`; Malloy **intersects**. Different answers. |
| `CALCULATE(expr, KEEPFILTERS(T[c] = "x"))` | `measure { where: c = 'x' }` | `KEEPFILTERS` makes DAX intersect, which is what Malloy already does. Safe. |
| `CALCULATE(expr, ALL(T))` | `all(expr)` **only when grouping is the sole filter** | **Not equivalent in general.** DAX `ALL` removes *filters*; Malloy `all()` removes *grouping* and still obeys the query's `where:`. See below. |
| `CALCULATE(expr, ALL(T[c]))` | `exclude(expr, c)` **with the same caveat** | Same divergence, one dimension |
| `CALCULATE(expr, REMOVEFILTERS(T[c]))` | `exclude(expr, c)` **with the same caveat** | `REMOVEFILTERS` is the clearer spelling of `ALL` as a modifier |
| `CALCULATE(expr, ALLEXCEPT(T, T[keep]))` | `all(expr, keep)` | **Keeps** the listed columns and removes the rest, so it is `all(..., keep)`, not `exclude(..., keep)` |
| `CALCULATE(expr, ALLSELECTED(...))` | **flag** | Depends on the visual's own filter scope; no equivalent |
| `expr / CALCULATE(expr, ALL(T))` | percent-of-total with `all()` | Matches only under the caveat below; search the Malloy docs for the pattern |
| `USERELATIONSHIP(...)` | **flag** | Switches to an inactive relationship for one measure |
| `FILTER(T, cond)` as a `CALCULATE` argument | depends | A table filter, not a column filter; see `translate-measures.md` |
| `EARLIER` / `EARLIEST` | **flag** | Row-context construct with no equivalent |
| `RANKX`, `TOPN` | window functions, or a query | Not a measure in Malloy |
| `SUMX(FILTER(T, cond), expr)` | `sum(expr) { where: cond }`, with the `FILTER` caveat | Only a context transition if a measure reference appears inside; see `translate-measures.md` |

**`ALL` removes filters; `all()` removes grouping.** This is the same overwrite-versus-intersect divergence as the `CALCULATE` row above, wearing different clothes, and it is on the *safe*-looking side of the table.

In a report grouped by category with a filter `Year = 2023`:

| | DAX `CALCULATE([Sales], ALL(Sales))` | Malloy `all(sales)` |
|---|---|---|
| Category filter (from the grouping) | removed | removed |
| Year filter | **also removed** | **still applied** |
| Result | total across all years | total for 2023 |

They agree when the grouping is the only filter in play, which is exactly the unfiltered grand total people check first. Treat every `ALL`/`REMOVEFILTERS` translation as Class 2 and validate it with a filter active on a column other than the grouping.

**Second constraint:** a dimension named in `all()` or `exclude()` must be an output field of the query, so `exclude(expr, c)` written as a source-level `measure:` only works in queries that `group_by: c`.

## Time Intelligence

Every one of these is a **rewrite, not a transcription**. They depend on a table marked as the model's date table with a contiguous date column, a concept Malloy does not have. Translate the intent.

| DAX | Malloy intent | Notes |
|--------|--------|-------|
| `TOTALYTD(expr, Date[Date])` | Aggregate filtered to the year to date | Express the date range explicitly |
| `SAMEPERIODLASTYEAR(Date[Date])` | Compare against the prior year's range | Usually a join or a second query |
| `DATEADD(Date[Date], -1, YEAR)` | Shift the date range | Same |
| `DATESYTD`, `DATESMTD`, `DATESQTD` | Range on the date dimension | Same |
| `PREVIOUSMONTH`, `PARALLELPERIOD` | Shifted range | Same |

## Formatting

| Power BI | Malloy | Notes |
|--------|--------|-------|
| `formatString: "\\$#,0.00"` | `# currency` | Map to Malloy render tags |
| `formatString: "0.00%"` | `# percent` | Map to Malloy render tags |
| `formatString: "#,0"` | `# number="#,0"` | Map to Malloy render tags |
| `dataCategory: WebUrl` | `# url` | Map to Malloy render tags |
| `dataCategory: Place` / `City` / `Country` | nothing automatic | Geo hint for chart selection |
