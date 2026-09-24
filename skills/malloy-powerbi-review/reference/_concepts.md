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
| `isHidden: true` | `internal:` or `# hidden` | Classify the reason first; see `rls-roles.md` |
| `displayFolder:` | nothing | Authoring convenience, no Malloy equivalent |
| `hierarchy` with `level` | nothing structural | Note level order as drill intent |
| `role R` + `tablePermission` | `#(access_filter)` / `#(authorize)` | Narrow grammar; see `rls-roles.md` |
| `perspective` | access modifiers, or a separate source | A curated subset of the model |
| `LocalDateTable_<guid>`, `DateTableTemplate_<guid>` | **skip** | Auto-generated, one per date column |

## Relationships

A Power BI relationship is declared once at model level, not on either table. `fromColumn` is the **many** side, `toColumn` is the **one** side.

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
| `COUNTROWS(T)` | `count()` | Direct mapping |
| `COUNT(T[c])` | `count(c)` | DAX `COUNT` skips blanks |
| `DISTINCTCOUNT(T[c])` | `count(distinct c)` | Direct mapping |
| `DIVIDE(a, b)` | `a / nullif(b, 0)` | `DIVIDE` returns blank on divide-by-zero |
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
| `CALCULATE(expr, ALL(T))` | `all(expr)` | Ungrouped aggregate |
| `CALCULATE(expr, ALL(T[c]))` | `exclude(expr, c)` | Ungrouped, one dimension removed |
| `CALCULATE(expr, REMOVEFILTERS(T[c]))` | `exclude(expr, c)` | `REMOVEFILTERS` is the clearer spelling of the same thing |
| `CALCULATE(expr, ALLSELECTED(...))` | **flag** | Depends on the visual's own filter scope; no equivalent |
| `expr / CALCULATE(expr, ALL(T))` | percent-of-total with `all()` | A documented Malloy pattern; search the docs for it |
| `USERELATIONSHIP(...)` | **flag** | Switches to an inactive relationship for one measure |
| `FILTER(T, cond)` as a `CALCULATE` argument | depends | A table filter, not a column filter; see `translate-measures.md` |
| `EARLIER` / `EARLIEST` | **flag** | Row-context construct with no equivalent |
| `RANKX`, `TOPN` | window functions, or a query | Not a measure in Malloy |
| `SUMX(FILTER(T, cond), expr)` | depends | Context transition; see `translate-measures.md` |

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
