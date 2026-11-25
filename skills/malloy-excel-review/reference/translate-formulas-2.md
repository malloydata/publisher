<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

## 5. Function families

| Excel | Go to |
|---|---|
| `SUMIF(S)`, `COUNTIF(S)`, `AVERAGEIF(S)`, `MAXIFS`, `MINIFS`, `SUMPRODUCT`, `COUNT`, `COUNTA`, `AVERAGE`, `SUBTOTAL` | `cookbook-lookup-aggregate.md` |
| `VLOOKUP`, `HLOOKUP`, `XLOOKUP`, `INDEX`/`MATCH`, `LOOKUP`, `IFERROR` | `cookbook-lookup-aggregate.md#la8` to `#la10` |
| pivot tables, `GETPIVOTDATA` | `cookbook-pivot.md` |
| roll-forwards, compounding, depreciation, circular references, `NPV`/`PMT`, Monte Carlo, Data Tables, Goal Seek | `cookbook-scenario.md` |
| `xl/model/` (Power Pivot), `CUBEVALUE` | `power-pivot.md`, then `skill:malloy-powerbi-review` |
| `RAND*`, `WEBSERVICE`, vendor feeds, VBA UDFs | X or NR: name the seam, do not invent Malloy |
| `RANK`, `MEDIAN`, `PERCENTILE`, `STDEV` | `cookbook-lookup-aggregate.md#la13` |
| `DSUM`, `DCOUNT`, `DAVERAGE`, `DMAX`, `DMIN`, `DGET` | C: `cookbook-lookup-aggregate.md#la15`; the criteria range is cells, so it is a given or a source |
| a share, maximum or average taken over the groups of another aggregate | `cookbook-lookup-aggregate.md#la16` |
| `CHOOSE`, `SWITCH` returning a value; `OFFSET`/`INDEX` as a case selector | the section after next |
| `LET`, `FILTER`, `UNIQUE`, `SORT`, `SEQUENCE` | next section |
| `PROPER`, `UPPER`, `LOWER`, `TRIM`, `CLEAN`, `SUBSTITUTE`, `TEXTJOIN`, `CONCAT` | T. Malloy has `upper()`, `lower()`, `trim()` and `replace()`; `CLEAN`, `TEXTJOIN` and `CONCAT` need a stanza-side expression over the same columns. `PROPER` has no direct built-in; Excel capitalises after any non-letter, so use a per-character expression, *(semantics-cited, run on DuckDB 1.4.5)*: `list_aggr(list_transform(range(1, length(x) + 1), i -> case when i = 1 or not regexp_matches(substr(x, i - 1, 1), '\pL') then upper(substr(x, i, 1)) else lower(substr(x, i, 1)) end), 'string_agg', '')` gives `O'Neil Smith` and `3Rd Street` (an empty string returns null, so wrap it in `coalesce`). Splitting on spaces gives `O'neil` and is wrong for this. Flag `proper_semantics` marks the formula |
| `FORECAST.ETS`, `.CONFINT`, `.SEASONALITY`, `.STAT` | NR: Excel's exponential smoothing (AAA ETS) is internal, with no Malloy or DuckDB equivalent. The user chooses a model (statsmodels or similar, outside Malloy) and the cached values are the only oracle. `FORECAST`, `FORECAST.LINEAR`, `SLOPE`, `INTERCEPT` (T) and `TREND`, `LINEST` (C) are closed-form and keep their routes. The constant row before a forecast run is usually the forecast's anchor or input, not a plug: confirm before reporting `plug_at_end` as a defect |
| `NORM.S.INV`, `NORM.S.DIST`, `NORM.DIST`, `NORM.INV` | C, with a recipe: DuckDB 1.4.5 core has no normal quantile or CDF (`erf`, `erfinv`, `probit` and `norm_*` do not exist; checked in `duckdb_functions()`), so write one stanza-side. Use a closed form accurate to 1e-12 or better, such as an AS241-class rational approximation for the quantile (about 1e-16), or an `erf` Taylor series with Newton iteration in a recursive `duckdb.sql` source for the CDF. State the precision, and check it against the cached `NORM.*` constants before relying on it: such forms matched Excel's cached `NORM.INV`, `NORM.S.DIST` and `NORM.DIST` at 1e-9 to 1e-17 on real workbooks. An approximation that is looser than 1e-12 or unchecked against the cache is not acceptable. *(semantics-cited)*. Box-Muller is for `RAND`-driven simulation only (`cookbook-scenario.md#sc7`), never for a `NORM.*` of a non-random argument |

### `LET` and the dynamic-array functions

Run on a hand-built workbook (the fixture has none of these), the classifier reported:

```
| R1 | S | D1 | formula | 1 | single_cell | T | LET | hardcoded_constant let |
| R2 | S | D2 | spill | 1 | range_aggregate | C | FILTER |  |
| R3 | S | D3 | spill | 1 | range_aggregate | C | UNIQUE |  |
| R5 | S | D4 | spill | 1 | range_aggregate | C | SORT |  |
| R6 | S | D5 | spill | 1 | constant | C | SEQUENCE |  |
- R2 S!D2: dynamic-array spill: the output size is dynamic, so it is never regioned silently
```

- **`LET`** is T, with the `let` flag. It works like DAX `VAR`: each name is a
  `dimension:` (row-local) or a named intermediate aggregate, and the body is the final
  expression. `_xlpm.` is the parameter prefix and is stripped.
- **`FILTER`, `UNIQUE`, `SORT`, `SORTBY`, `SEQUENCE`** are C, in a spill and inside a scalar aggregate such as `COUNTA(UNIQUE(..))` alike (the strictest function in the formula sets the route). A spilled result has a dynamic size,
  so it is never lifted as data and never regioned. Port the intent, not the spill:
  `FILTER` is a `where:`, `UNIQUE` is a `group_by:`, `SORT` is an `order_by:`, and
  `SEQUENCE` is a date or number spine (`cookbook-scenario.md`). A cell that reads a
  spill through `A1#` is NR (`ANCHORARRAY`: ask which spill it follows). The cost is
  that a view, not a cell range, now owns the order and the cut-off.
- **`LAMBDA`, `MAP`, `SCAN`** and the other array helpers are NR (section 3).
- **A defined name whose body is a `LAMBDA` or another formula.** The classifier lists the name (`defined_names`) but not the body: the body is workbook text. Ask the user to share it from Name Manager. If they cannot, infer the function from the cached results of the cells that call it and verify the inference across hundreds of cells, label it `semantics-cited (hand-derived)`, and never claim it is the exact body.

### Recipes: spills, structured references and text-bucketed formulas

Tiny synthetic ports, all `semantics-cited`; compile-check each fragment. `orders`
is a lifted table source with `region`, `amount` and `order_date`.

| Excel | Malloy | Cost |
|---|---|---|
| `=Orders[Region]` spilled (a bare table column) | `run: orders -> { select: region }` | the spill's extent is the view's row count; a view owns the order |
| `Orders[Amount]`, `Orders[@Amount]`, `Orders[#Totals]` | the lifted table source's columns: `amount` per row for `[@Col]`, `amount.sum()` for a whole column; `[#Totals]` is the totals row the lift excluded, so it is a parity oracle, not a field | the totals row is checked against the measure, never lifted |
| `=SUMPRODUCT((TEXT(Orders[Date],"mmm yyyy")=A2)*Orders[Amount])` | `run: orders -> { group_by: order_date.month; aggregate: amount.sum() }`, then read the row for the month | the text label is gone; the group key is a date |
| `TEXT(x, "0.0%")`, `TEXT(d, "mmm yyyy")` as a label | keep the number or date in the model and format it in the presentation layer (a render tag or the app), not in a model field | a model field that returns the formatted string cannot be summed or sorted |
| a month after the last data month | no row exists to group; join the data to a calendar spine (`cookbook-scenario.md#sc4`) and `?? 0` the measure | the spine's bounds are a decision (a `given:`) |
| `UNIQUE`/`SORT` over a range that runs past the data | the trailing blank or `0` row the spill returns is dropped by a `group_by` port; keep it with a calendar spine (`cookbook-scenario.md#sc4`) when the workbook shows it | otherwise the port has one row fewer than the cache |
| `SORT`/`SORTBY` over a range with a blank cell (a blank sorts last and spills as `0`) | `group_by: v is coalesce(x, 0)` shows the blank as 0 the way Excel renders it; a bare `group_by: x` keeps the null group too (rows 1, 3, null), so it does not drop the blank. Ran on a live Publisher | the zero is Excel's rendering of a blank: say so in the report |

### Week numbers and `TEXT(date, "fmt")`

Do the date formatting in the stanza wrapper, or use Malloy date functions (`order_date.month`). A raw `strftime!` pass-through in a Publisher query is refused with a clear HTTP 400 (`direct SQL function calls (!type(...)) are not permitted` in a restricted query): do the date formatting in the stanza wrapper. The DuckDB forms below ran on DuckDB 1.4.5 (English names; Excel's follow the workbook locale):

| Excel | DuckDB, in the stanza | Note |
|---|---|---|
| `WEEKNUM(d)`, `WEEKNUM(d, 1)` (system 1: the week containing Jan 1 is week 1, weeks start Sunday) | `(floor((dayofyear(d) + dayofweek(date_trunc('year', d)) - 1) / 7) + 1)::int` | no DuckDB built-in matches. `strftime(d, '%U')::int + 1` is wrong when Jan 1 is a Sunday (it gives 2). The formula agreed with a hand derivation on nine dates across year ends |
| `ISOWEEKNUM(d)`, `WEEKNUM(d, 21)` | `weekofyear(d)` | ISO 8601: `2021-01-03` is 53, `2024-12-30` is 1 |
| `TEXT(d, "yyyy-mm")` | `strftime(d, '%Y-%m')` | text, so compare as text |
| `TEXT(d, "mmm")`, `"mmmm"` | `strftime(d, '%b')`, `'%B'` | `Mar`, `March`; Excel writes the workbook locale's names (a four-letter abbreviation in some), so compare against the cached strings, never assume English |
| `TEXT(d, "ddd")`, `"dddd"` | `strftime(d, '%a')`, `'%A'` | `Tue`, `Tuesday`; the same locale caveat |
| `WEEKDAY(d)`, `WEEKDAY(d, 1)` (Sunday = 1 ... Saturday = 7) | `dayofweek(d) + 1` | DuckDB `dayofweek` is 0 = Sunday |
| `WEEKDAY(d, 2)` (Monday = 1 ... Sunday = 7) | `isodow(d)`, or `((dayofweek(d) + 6) % 7) + 1` | the two agree |
| `WEEKDAY(d, 3)` (Monday = 0 ... Sunday = 6) | `(dayofweek(d) + 6) % 7` | |
| `DATEDIF(a, b, "Y")`, `"M"`, `"D"` | `date_sub('year', a, b)`, `date_sub('month', a, b)`, `date_sub('day', a, b)` | complete units, as `DATEDIF` counts. `date_diff('year', a, b)` counts boundaries instead and gives 1 for 31 Dec to 1 Jan |
| `DATEDIF(a, b, "YM")` | `date_sub('month', a, b) % 12` | months after the last complete year |
| `DATEDIF(a, b, "YD")` | `date_part('day', b - (a + to_years(date_sub('year', a, b))))` | days after the last complete year; a 29 February start clamps to 28 February in DuckDB. The bare subtraction is an `INTERVAL` (`364 days`), not a number, so take its day part |
| `DATEDIF(a, b, "MD")` | `date_part('day', b - (a + to_months(date_sub('month', a, b))))` | days after the last complete month (an `INTERVAL` until `date_part` takes its day part). Excel's own `"MD"` is known to misbehave (negative or inconsistent results when the end day is before the start day): flag every `"MD"` cell, port the sane form and compare it to the cache, never claim parity. `a` after `b` is `#NUM!` in Excel |

The `WEEKDAY` and `DATEDIF` DuckDB forms ran on DuckDB 1.4.5 (Sunday 2024-01-07 gives 1, 7, 6 for types 1, 2, 3; Monday 2024-01-08 gives 2, 1, 0; `2020-03-15` to `2021-03-14` gives 0 complete years, 11 months, 364 days, 27 days after the last month; the `YD` and `MD` forms with `date_part` were re-run on DuckDB 1.5.5 and gave 364 and 27 as integers). Label the Excel side `semantics-cited`; other week systems (`WEEKNUM(d, 2)` and the rest) are not covered, so write them in the stanza and check them against the cache.

### `CHOOSE`, `SWITCH` and case selectors

`CHOOSE` and `SWITCH` that return a value are `pick ... when ... else`; `CHOOSE` used as a
*reference* stays NR (section 3).

```malloy
dimension: tier_label is
  pick 'Low' when tier = 1
  pick 'Mid' when tier = 2
  pick 'High' when tier = 3
  else null
```

- **`CHOOSE(tier, "Low", "Mid", "High")`** truncates a decimal index (`CHOOSE(2.7, ...)` returns
  the 2nd) and gives `#VALUE!` outside `1..3`. A Malloy `pick ... when tier = 2` returns null
  for 2.7, so wrap the index: `floor(tier)`. The `else null` is a silent change. Say so, or find where the workbook
  wraps it in `IFERROR`.
- **`SWITCH(region, "east", ..., "west", ..., default)`** compares text case-insensitively,
  so write `lower(region) = 'east'`. With no default and no match it gives `#N/A`, which is
  again `null`.
- Label `semantics-cited`; compile-check the snippet.

**`OFFSET` or `INDEX` as a case selector.** `=INDEX(Cases!B2:D2, Assumptions!B1)` or
`=OFFSET(Cases!A2, 0, Assumptions!B1)` picks one scenario column by a case number in an
input cell. The case number is a `given:` and the choice is a `pick`:

```malloy
given: CASE_NO :: number is 1

dimension: growth is
  pick 0.05 when $CASE_NO = 1
  pick 0.08 when $CASE_NO = 2
  pick 0.02 when $CASE_NO = 3
  else null
```

Prefer a scenario table and a `filter<string>` given when the cases have names or more than a
handful of values (`cookbook-scenario.md#sc10`). `OFFSET` itself stays **NR** (it is volatile
and returns a reference): port the intent only after the user confirms the selector is all it
does. `OFFSET` is zero-based and `INDEX` is one-based; an out-of-range `INDEX` is `#REF!`, and
index `0` returns the whole row or column, neither of which the `else null` reproduces.
`semantics-cited`.

### Inconsistent neighbours

A formula in a copied-down row or column that differs by one term from its neighbours (Excel's green-triangle case, for example a sum that stops one row short) is reproduced faithfully, not harmonised, and goes in the findings list with the cell, both numbers and the question for the owner (`parity.md` section 9).

### Solver report sheets

The Answer, Sensitivity and Limits sheets a Solver run leaves behind hold constants copied from that run. They are typed results, not sources: compare the model to their values and do not model them as data. Their cell addresses can be stale against the saved Solver names (a report may describe an older copy of the model), so match by position and say so. A changing cell that no formula reads is an inert model; report it (`cookbook-scenario.md#sc9`).

## 6. Constants and plugs are findings

A formula region with a `hardcoded_constant` flag has a literal the author typed in;
a region with `plug` has a cell where someone overwrote one formula of the copied-down
set. **Never absorb either into the translation.** Translate the region as the formula
says, show where the workbook disagrees with it, and ask the owner which is right
(`cookbook-lookup-aggregate.md#la12` does this for `Report!I6` = 999 against a
formula that gives 100).

## 7. Reporting

One row per region, with the value Excel cached and the value Malloy returned at the
same context, measured, not asserted:

| Region | Cell | Recipe | Route | Excel cached | Malloy | Match | Label |
|---|---|---|---|---|---|---|---|

A workbook with hundreds of regions registers one combined oracle source, not hundreds (`cookbook-lookup-aggregate.md#la17`).

Give at least two filter contexts per reporting measure, including one that hits the
case-insensitive and blank cases on purpose. A report with no mismatches and no `C`
rows is a red flag when the data exercises the flagged quirks: any real workbook has
cells that disagree with a faithful translation. For a quirk the data cannot hit,
"no mismatches" proves nothing; record it `not_exercised` or `semantics-cited
(hand-derived)` (`parity.md` section 9). A cell whose cached value comes from code
outside the file (an add-in, a feed) is `matches as of save` at best.
