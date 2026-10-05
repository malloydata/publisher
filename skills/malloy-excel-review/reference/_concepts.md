<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Excel → Malloy Concept Mapping

Reference table for translating Excel constructs to Malloy. Referenced by `recover-sources.md` and `translate-formulas.md` for type mapping and syntax translation; the cookbooks carry a worked recipe for every row marked with one, each labelled `executed` or `semantics-cited` in place.

A row is a **direct mapping** only where it says so. Everywhere else the construct compiles but means something different, and the cell that tells you is the cached value, so check it (`parity.md`).

## Workbook Objects

| Excel | Malloy | Notes |
|--------|--------|-------|
| Excel Table (`ListObject`) | `source:` over `read_xlsx(sheet=, range=, header=true)` | The table's `ref` minus its totals row. Always an explicit `sheet=` and `range=` |
| A range of constants with a header row | the same, with the range from the classifier | `dimension ref` is not a table; use the connected region |
| Several blocks on one sheet | one source per region | Never one source per sheet by default |
| Calculated column (`calculatedColumnFormula`) | `dimension:` | Never lift the cached column as data; see `recover-sources.md` |
| Formula copied down a region, row-local | `dimension:` | The router's `row_local` split |
| Formula over a range (`SUMIFS`, `SUM`, `AVERAGE`) | `measure:`, or an aggregate in a `run:` | The `range_aggregate` split |
| Running total, opening plus flow | `calculate: sum_cumulative(...)` in a query or view | Query-level, not a reusable `measure:`; the cost is that every consumer repeats it |
| Absolute reference to an input cell (`$B$4`) | `given:` | One per assumption; the widget comes free in a dashboard |
| Data validation list | the allowed values of a `given:` | A free enum |
| Form control with `fmlaLink` | `given:` | The linked cell is an input |
| Defined name (a constant) | `given:` or a literal | A named range of cells is a source or a join, not a name |
| Defined name (a `LAMBDA`) | a dimension or measure, or inline at each call site | NR when it cannot be inlined |
| Pivot table | a view | `cookbook-pivot.md`; the snapshot is a parity oracle only |
| Hidden or `veryHidden` sheet | a question for the user | Values are withheld from the report on purpose |
| Hidden row, autofilter, slicer, timeline | nothing | UI state; never baked into a source |
| Sheet comment, cell format, chart | nothing | Presentation; secrets in comments are scanned, never emitted |
| `xl/model/` (Power Pivot) | `skill:malloy-powerbi-review` | `power-pivot.md` |

## References

| Excel | Malloy | Notes |
|--------|--------|-------|
| `A1`, `$A$1`, `A1:B9` | a column of a source, a `given:`, or a region | Resolved by the classifier's R1C1 split, not by the address |
| `Sheet2!B3` | a field of the source lifted from that region | The cross-sheet dependency graph names which |
| `Table1[Amount]` | `amount` | The column name, snake-cased in the source |
| `Table1[@Amount]`, `[[#This Row],[Amount]]` | the same field, as a dimension | Row-local |
| `Table1[#Totals]`, `Table1[#Headers]`, `Table1[#All]` | nothing; the lift excludes totals and header rows | The totals row is a parity oracle (`recover-sources.md`, "Check the lift") |
| `Sheet1:Sheet3!A1` (a 3D reference) | a `UNION ALL` of the sheets in the lifting SQL | Flag it: the sheets must share a layout, and the sheet name becomes a column |
| `A:A`, `1:1` | the whole column of the source | A full-column reference includes any totals row under the data; the source does not |
| `INDIRECT`, `OFFSET`, `CHOOSE` used as a reference | **no recipe** (NR) | The dependency graph is incomplete; ask what the number means |
| Spill reference `A1#` | **no recipe** (NR) | Ask which spill it follows |
| External reference `[Book.xlsx]Sheet!A1` | flag, never resolved | The value is a snapshot |
| `_xlfn.`, `_xlws.`, `_xlpm.` prefixes | stripped | The function is the name after the prefix |

## Column Types

| Excel | Malloy | Notes |
|--------|--------|-------|
| Number, currency, percentage, accounting | `number` | The format is presentation; compare values, not what `numFmt` shows |
| Text | `string` | Case-sensitive in Malloy, case-insensitive in `SUMIFS`, `COUNTIF`, `VLOOKUP`, `MATCH`; see Matching |
| Boolean (`t="b"`) | `boolean` | Cached `TRUE`/`FALSE`; `TRUE + 1` is 2 in Excel and a type error in Malloy |
| Date or date-time (a serial in a date-styled cell) | `date` or `timestamp` | A serial under the 1900 or 1904 system; see Dates |
| Error (`t="e"`) | `null` | `#N/A` is a legitimate no-match oracle; other errors map to null only with the `IFERROR` routing stated |
| Blank | `null` | `read_xlsx` returns empty cells as DOUBLE when the first data row is blank, and an empty string cell as `''`, not null |
| Text that looks like a number | `string`, converted on purpose | A text `"1"` in a numeric column is silently coerced by a typed read. Read with `all_varchar = true` and `try_cast` |
| Mixed column | one source column per meaning | The classifier counts numbers and text per column |
| Rich data (linked data type, `IMAGE`) | **skip** (NR) | The base value is a placeholder |

## Dates

| Excel | Malloy | Notes |
|--------|--------|-------|
| 1900 system serial `n` > 60 | `date '1899-12-30' + n` (in the lifting SQL) | The serial is a day count |
| serial 1 to 59 | `date '1899-12-31' + n` | One day later than the rule above gives |
| serial 60 | `null` | The phantom 1900-02-29; not a date, and no translation maps it back |
| a source with a date-styled serial below 61 | the printed stanza reads it `all_varchar` and converts every date column with `CASE WHEN x < 60 ... WHEN x < 61 THEN NULL ELSE ... END` | 1 to 59 a day later, 60 NULL, and a stanza comment says to check those cells by hand. Use `floor(n)` before `::int` when a serial has a time part |
| `date1904="1"` | `date '1904-01-01' + n` on an `all_varchar` read | A typed read applies the 1900 base and is four years off, with no error |
| Text date (`"2024-07-15"`) | `try_strptime` in the lifting SQL | Excel's `">="&B2` compares only the serial ones; a decision for the user |
| Unix epoch seconds in a column (numbers near 1.7e9, not serials) | `to_timestamp(x)` in the lifting SQL (`timestamp_s`); an Excel serial is days: `date '1899-12-30' + x` | Convert in the stanza wrapper: setting a timestamp pragma (`sql_timestamp`) may be rejected by an ad-hoc query |
| `TODAY()`, `NOW()` | a `given:` pinned to `dcterms:modified` | The cache is as of the last calculation; `now` changes every run |
| `YEAR(d)`, `MONTH(d)`, `DAY(d)` | `year(d)`, `month(d)`, `day(d)` | Extraction. `d.year` is truncation |
| `DATEDIF`, `d2 - d1` | `days(d1 to d2)`, `months(d1 to d2)` | Not subtraction of dates |
| `EOMONTH`, `EDATE`, `WORKDAY` | **rewrite** | Port the intent in the lifting SQL or a date dimension; `cookbook-scenario.md#sc4` has the spine |

## Aggregations

| Excel | Malloy | Notes |
|--------|--------|-------|
| `SUM(r)` | `sum(c)` or `c.sum()` | Direct mapping |
| `AVERAGE(r)` | `avg(c)` | **Not direct.** Excel skips blanks and text; `avg()` skips null only. A text number read as a number, or a blank read as 0, shifts the mean. `cookbook-lookup-aggregate.md#la6` |
| `MIN`, `MAX` | `min()`, `max()` | Direct mapping |
| `COUNT(r)` | `count() { where: c is not null }` on a numeric-only column | Counts numbers only. **Not `count(c)`**, which is a distinct count in Malloy. `#la5` |
| `COUNTA(r)` | `count() { where: c is not null }` | Counts any non-blank cell |
| `COUNTBLANK(r)` | `count() { where: (c ?? '') = '' }` on a string column; `count() { where: c is null }` on a number or date column | Excel's blank includes the empty string. `?? ''` on a numeric column does not compile (`Mismatched types for coalesce`). `#la4` |
| `SUMIF(S)`, `COUNTIF(S)`, `AVERAGEIF(S)`, `MAXIFS`, `MINIFS` | `agg() { where: ... }` | Criteria are `and`ed in one `where:`. Case, text numbers, blanks and wildcards differ; `#la1` to `#la5` |
| `SUMPRODUCT(mask * r)` | `sum(c) { where: mask }` | The boolean-mask shape. `#la7` |
| `SUMPRODUCT(a, b)` | `sum(a * b)` | The plain-product shape. `#la7` |
| `SUBTOTAL(9, r)` | `sum(c)` | Every `SUBTOTAL` code excludes filter-hidden rows; 1-11 vs 101-111 differ only for manually hidden rows. `AGGREGATE` over a filtered or hidden range reflects UI state too: the port needs the filter as a `where:`, reported |
| `LARGE`, `SMALL`, `PERCENTILE`, `MEDIAN` | no portable measure | `rank()` in a query for top-N; a percentile through the lifting SQL |
| `STDEV.S`, `STDEV.P` | `stddev()` where the dialect has it | Check sample against population |
| `GETPIVOTDATA(...)` | the view cell it reads | `cookbook-pivot.md#p8` |

## Matching and Lookups

| Excel | Malloy | Notes |
|--------|--------|-------|
| `"east"` as a criterion or key | `lower(c) = 'east'` | **Excel ignores case; Malloy does not.** Define one `lower()` key dimension and use it everywhere |
| Criterion `">=2"` or `">="&B2` | `c >= 2` on a numeric-only column | Compares numbers to numbers; a text `"1"` never matches |
| Criterion `COUNTIF(r, 1)` | numeric and text `"1"` both match | The text `"1"` also counts. `#la5` |
| Wildcards `*`, `?` | anchored regular expression, or `~` with `%` | `LIKE` makes `_` a wildcard, so a criterion containing `_` needs the regular expression. `#la3` |
| `""`, `"<>"`, `"<>x"` | `(c ?? '') = ''`, `c is not null`, `c != 'x'` | Malloy `!=` is null-inclusive; raw `a != b` is TRUE for null vs null and null vs `''`, so use `coalesce(x, '') != coalesce(y, '')`. `#la4` |
| `VLOOKUP(k, t, n, FALSE)`, `INDEX`/`MATCH(..., 0)`, `XLOOKUP` | `join_one:` on the key | An exact lookup is a join. `#la8` |
| `VLOOKUP(k, t, n, TRUE)`, `LOOKUP`, `MATCH` with the third argument omitted | a range join against a sorted bracket table | **Approximate by default.** Over unsorted data Excel returns whatever its binary search lands on. `#la9`, `#la10` |
| `XLOOKUP` `match_mode` -1 or 1 | a range join, no sort needed | Excel's search is linear, so correct on unsorted data |
| `IFERROR(x, y)` | `x ?? y` | Only where the error is a no-match; a divide-by-zero needs `nullif`. State which cells the workbook wraps |
| `ISNA`, `ISBLANK` | `is null` | Check the blank-versus-empty-string case. A formula that returns `""` is read back from the cache as NULL, so the oracle cannot tell `""` from blank on a formula cell: compare with `coalesce(x, '')` |

## Logic and Operators

| Excel | Malloy | Notes |
|--------|--------|-------|
| `IF(c, a, b)` | `pick a when c else b` | Direct syntax translation |
| `IFS`, nested `IF` | chained `pick ... when` | Direct syntax translation |
| `SWITCH(x, v1, r1, ..., d)` | `pick r1 when x = v1 ... else d` | Direct syntax translation |
| `AND`, `OR`, `NOT` | `and`, `or`, `not` | Direct mapping |
| `a / b` | `a / nullif(b, 0)` | Excel gives `#DIV/0!`. A bare `/` in DuckDB gives NaN for `0/0` and inf for `x/0`, not null, so a port that compares to the cache sees a number where Excel has an error. An error cell in the cache matches null, never a number; label it `semantics-cited` (`parity.md` section 1) |
| `a & b` | `concat(a, b)` | A number is formatted by Excel's rules, not the database's |
| `a ^ b` | `pow(a, b)` | Direct mapping |
| `=` on text | `=` | Case-sensitive in Malloy; see Matching |
| `LET(x, v, body)` | a dimension per name, then the body | Works like DAX `VAR`; `_xlpm.` is stripped |
| `FILTER`, `UNIQUE`, `SORT`, `SEQUENCE` | `where:`, `group_by:`, `order_by:`, a generated spine | Port the intent, not the spill |
| `LAMBDA`, `MAP`, `SCAN`, `REDUCE` | **no recipe** (NR) | Inline a `LAMBDA` at each call site when it is row-local |

## Text and Math

| Excel | Malloy | Notes |
|--------|--------|-------|
| `LEFT`, `RIGHT`, `MID` | `substr(c, start, len)` | Excel counts from 1; check the offset |
| `LEN`, `UPPER`, `LOWER`, `SUBSTITUTE` | `length`, `upper`, `lower`, `replace` | Direct mapping |
| `TRIM` | `trim(replace(c, r' +', ' '))` | Excel `TRIM` also collapses inner runs of spaces, which a bare `trim` leaves (`'  a   b  '` gives `a   b`, Excel gives `a b`) |
| `FIND`, `SEARCH` | `nullif(strpos(c, x), 0)`; `SEARCH` as `nullif(strpos(lower(c), lower(x)), 0)` | `FIND` is case-sensitive and `SEARCH` is not. A miss is `#VALUE!` in Excel and `0` from `strpos`, so a bare `strpos` turns an error into a number; `nullif(..., 0)` gives null, which matches an error cell. `SEARCH` also reads `*` and `?` in its text as wildcards |
| `VALUE`, `TEXT` | `try_cast` in the lifting SQL | `TEXT` is a format string, which is presentation; `# percent` and `# currency` cover the common ones |
| `ROUND` | `round(x, n)` | Excel rounds half away from zero; check the database's rule on a `.5` boundary |
| `INT` | `floor()` | Rounds toward negative infinity, as `floor` does |
| `ABS`, `SQRT`, `LN`, `EXP`, `POWER`, `MOD` | `abs`, `sqrt`, `ln`, `exp`, `pow`, `%` | Direct mapping, except `MOD` takes the sign of the divisor |
| Compounding `(1+g)^n` over changing rates | `exp(sum_cumulative(ln(1 + g)))` | `cookbook-scenario.md#sc3`; `pow()` when the rate is constant |

## Financial and Statistical Functions

DuckDB has no `PMT`, `NPV`, `IRR` or `NORM.INV`. Each routes **C** and is written out from its definition in the recipe that needs it.

| Excel | Malloy | Notes |
|--------|--------|-------|
| `PMT(rate, n, pv)` | the annuity formula in a `dimension:` | `cookbook-scenario.md#sc5` |
| `NPV(rate, flows)` | `sum(flow / pow(1 + rate, period))` | Excel discounts the first flow one period |
| `IRR` | no closed form | A recursion or iteration in the lifting SQL, or it stays in Excel |
| `NORM.INV(RAND(), m, sd)` | a seeded Box-Muller draw in DuckDB SQL | Same distribution, not the same draws; Box-Muller is for `RAND`-driven simulation only. `#sc7` |
| `NORM.INV(p, ...)`, `NORM.S.DIST`, `NORM.DIST` on a non-random argument | a stanza-side closed form accurate to 1e-12 or better | Check it against the cached values. `translate-formulas.md` |
| `RAND`, `RANDBETWEEN` | **X** | Never compared cell for cell |

## What-If Tools

| Excel | Malloy | Notes |
|--------|--------|-------|
| Data Table (What-If) | a precomputed scenario grid, with a `given:` choosing the row | `cookbook-scenario.md#sc8` |
| Goal Seek, Solver over a discrete range | the same grid | The cache holds the last optimum |
| Goal Seek, Solver over a continuous range | **X**, stays in Excel | `#sc9` names the seam |
| Scenario Manager | a scenario table plus a `given:` | `#sc10` |
| `iterate="1"` with a cycle | a recursive source unrolled to a fixed N with a tolerance | State N and the tolerance. `#sc6` |

## Functions With No Mapping

These have no recipe by design. Route them through `translate-formulas.md` rather than inventing Malloy.

| Excel | What to do |
|--------|-------|
| `WEBSERVICE`, `FILTERXML` | **X**, security flag. Never fetch |
| `RTD`, `BDP`, `BDH`, `FDS`, `CIQ`, `HsGetValue`, `DBRW`, `SAPGetData` and other vendor feeds | **X**. The cache is a dated snapshot; ask whether the organisation has the vendor's warehouse feed |
| `CUBEVALUE`, `CUBEMEMBER` | **NR**; `power-pivot.md` |
| `_xludf.` or an unknown bare function name, cached `#NAME?` | **NR**: which add-in or VBA function? |
| `STOCKHISTORY`, `IMAGE`, `FIELDVALUE` | **NR**; the cache is a snapshot or a placeholder |
| `PY(` (Python in Excel) | **C**; the code is pandas, translated by hand. A Python-object result has no scalar oracle |

## Formatting

| Excel `numFmt` | Malloy | Notes |
|--------|--------|-------|
| `$#,##0.00` | `# currency` | Map to Malloy render tags |
| `0.00%` | `# percent` | Map to Malloy render tags |
| `#,##0` | `# number="#,0"` | Map to Malloy render tags |
| A date format | nothing | A date column renders as a date |
