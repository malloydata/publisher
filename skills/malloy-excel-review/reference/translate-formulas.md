<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Formula Translation (Step 3)

> Route every formula **region** to a recipe before translating it. Emitting Malloy is
> the easy part; knowing which formulas change meaning on the way across is the work.

Read `_concepts.md` for the syntax mapping and `recover-sources.md` for what was
lifted. This file is about where each formula goes. The recipes are in
`cookbook-lookup-aggregate.md` (aggregation and lookups), `cookbook-pivot.md` and
`cookbook-scenario.md`.

A workbook rarely has thousands of different formulas. It has a few dozen, each copied
down or across a region. The classifier reads Excel's own shared-formula storage,
normalizes every formula to R1C1, and groups identical ones: **one region is one
candidate field, not N**. Route regions, not cells.

## 0. Can the cached values be trusted?

Parity compares Malloy to the number saved next to each formula. That only works if
Excel (or something equivalent) computed the number. Read the classifier's **Oracle**
section first.

| Oracle line | Meaning | Do |
|---|---|---|
| `Cached values: untrusted` (`fullCalcOnLoad=1`, a formula with no `<v>`, a missing `calcId`) | A library wrote the file and expects Excel to recompute | Ask for a recalculated save before comparing anything. The fixture is in this state on purpose. |
| `calcMode="manual"` or `calcOnSave="0"` | Values may be stale | Same. |
| `volatile` (`TODAY`, `NOW`, `RAND*`) | The cache is as of the last save | Pin `TODAY`/`NOW` to the saved date (`workbook_props.modified`, `docProps/core.xml` `dcterms:modified`) as a `given:` (`AS_OF`). A label such as `TEXT(TODAY(), "mmm yyyy")` is checked by pinning `AS_OF` to that date and comparing, then changing `AS_OF` and confirming the label changes (non-vacuity). `RAND` routes to X. |
| `volatile_dep` / `random_dep` (region flags) | the region reads, directly or transitively, a `RAND*` cell (`random_dep`, routes X) or a `TODAY`/`NOW` cell (`volatile_dep` only, route unchanged) | Its cache is one sample of the draws: compare it statistically, or pin the draws as data (feed the cached draws across as a data stanza and compare the dependents exactly; `cookbook-scenario.md#sc7`). A `TODAY`/`NOW` dependent is checked with `AS_OF` pinned. |
| `cached_errors` | `t="e"` cells | `#N/A` is a legitimate no-match oracle (Malloy null); other errors (`#DIV/0!`, `#VALUE!`) map to null only (a division is `a / nullif(b, 0)`: a bare `/` gives NaN or inf, not null) with the `IFERROR` routing stated: say which cells the workbook wraps and which it does not. `--json` lists the first 20 refs per error type per visible sheet in `oracle.cached_error_cells` (totals in `cached_error_cell_totals`; a hidden sheet gets counts only). Excel keeps the error and Malloy has only null, so record each cached error as `error == null` with those refs; it is unfixable by design. |
| `subtotal_ui_state` | any `SUBTOTAL` code (the fixture's `SUBTOTAL(9)` and `SUBTOTAL(109)` are both flagged) or `AGGREGATE` over a filtered or hidden range | The cache reflects UI state. Every `SUBTOTAL` code excludes filter-hidden rows; codes 1-11 and 101-111 differ only for manually hidden rows. A filter is UI state: the port needs it as an explicit `where:` to match, so report it rather than baking it in silently. |
| `date_serial_60` | The phantom 1900-02-29 | Not a date. See `recover-sources.md#dates`. |
| a pivot | Numbers are as of its `refreshedDate`, from `pivotCacheRecords` | Name which one parity compared against. |

A cache from a non-Excel producer needs the extra care in `parity.md` section 3: booleans standing in for numbers, ratios kept to about 10 significant digits, and hand-derivation at scale.

Compare values, not what `numFmt` displays, and state a numeric tolerance. A cell that
reads through an external link is a snapshot: flag it, do not compare it.

## 1. Run the classifier

```
python3 scripts/classify_workbook.py book.xlsx            # Markdown report
python3 scripts/classify_workbook.py classify book.xlsx --json
```

`--json` carries customer structure (names, formulas, SQL). Keep it local; never
paste it into a PR or a shared doc. Read the report in this order:

| Section | What to take from it |
|---|---|
| **Security flags** | Printed first (for example macros, XLM, DDE, ActiveX, `veryHidden` sheets, secret-labelled cells). Resolve before anything else. |
| **Sheets** | Class per sheet (`data`, `lookup`, `report`, `input`, `calc`, `config`), formula cells and constants counted separately, hidden rows, merges, autofilter. |
| **Oracle** | Section 0 above. |
| **Sources** | One lift stanza per region (`recover-sources.md`). |
| **Formula regions** | One row per region: `Split`, `Route`, `Functions`, `Flags`. The routing table. |
| **Routes / Functions** | Counts per route and per function; use them to size the job. |
| **Code attached** | VBA, add-ins, vendor feeds. Each is a seam (X or NR). |
| **Dependency graph** | Region-level, with cycles. `iterate=1 and a cycle was found` is a real circularity; `iterate=1` alone is only a setting. Regions in a cycle route C (`circular`). |
| **Not read** | What the script did not look at. A route is not evidence about these. |

On the bundled `fixture.xlsx`: 55 regions, **T 40 regions / 77 cells, C 13 / 23,
X 2 / 1001, NR 0 / 0**. The route is a priority order, not a verdict: a T region can
still be wrong, so parity-test every non-T region and spot-check the T ones.

## 2. The three-way split (the router's primary key)

The classifier's `Split` column says what a region **is**, before you look at its
functions. On the fixture's 55 regions it is `range_aggregate` 27, `single_cell` 15,
`row_local` 8, `absolute` 3, `constant` 2:

| Split | The formula refers to | Becomes | Fixture example |
|---|---|---|---|
| `row_local` | cells in its own row, copied down or across more than one cell | a `dimension:` | `Data!F2:F12` `=tbl_Sales[[#This Row],[Qty]]*tbl_Sales[[#This Row],[Price]]` is `dimension: revenue is coalesce(qty_coerced, 0) * price` |
| `single_cell` | a one-cell region that would otherwise be `row_local` (a relative reference or a lookup) | a measure expression or a scalar; read the sheet class | `Report!B13` `=B3*1.08`, every `Lookup!H2:H10` test, `Forecast!B8` |
| `range_aggregate` | a range | a `measure:` (or a window) | `Report!B3` `SUMIFS(...)` is `revenue.sum() { where: ... }` |
| `absolute` | single `$A$1`-style cells | a `given:` or a constant | `Forecast!B7:F7` `=(Assumptions!$B$4-Assumptions!$B$5)/Assumptions!$B$6` reads three assumptions |
| `constant` | nothing | a literal, flagged | `MonteCarlo!B2:B1001` `NORM.INV(RAND(),100,15)` (also the What-If table `Assumptions!E3:E6`) |

Four reading rules, because the split is mechanical and the sheet is not:

- **`single_cell` is a question, not an answer.** One cell cannot be a calculated
  column. On a `report` sheet `Report!B13` is a scalar built from another report cell
  (a measure expression, with its `1.08` lifted to a `given:`). On a `lookup` sheet a
  test cell such as `H4` is a join. Read the sheet class and the cell's neighbours.
- **A relative reference with an offset is a prior-period reference**, not a
  same-row field: `Forecast!C2:F2` `=B5` (opening = last period's closing). It is a
  roll-forward (`cookbook-scenario.md`), not a column.
- **A running total is a window.** `Forecast!B9:F9` `=SUM($B$3:B3)` is
  `range_aggregate`. In Malloy it is `calculate: sum_cumulative(flow)` in a query or
  view. A `measure:` cannot hold it (executed: `Cannot use an analytic field in a
  measure declaration`), so the cost is that every consumer repeats the `calculate:`.
  Executed on the unpivoted `Assumptions!A10:F11` flows (the `flows` source in
  `recover-sources.md`): 1000, 2200, 3700, 5500, 7500, the cached `Forecast!B9:F9`.
  Label: `executed`.

  ```malloy
  run: flows -> {
    group_by: fiscal_year
    aggregate: flow
    calculate: cumulative_flow is sum_cumulative(flow)
    order_by: fiscal_year
  }
  ```
- **A lookup is a join, whatever its split.** `VLOOKUP(750, ...)` in `Lookup!H4` is a
  `single_cell` region whose key is a hardcoded constant (`hardcoded_constant` flag);
  lift the key to a `given:` or a column. The same lookup copied down a data column is
  `row_local`.

### Where a `given:` can and cannot reach

`absolute` regions become givens, and a given is usable in a `where:`, a `dimension:`
or a `measure:` expression. Whether it reaches *inside* a `duckdb.sql(...)` source
matters for recursive scenario sources, so it was tried five ways (Publisher 0.9.0,
DuckDB 1.5.5):

| Shape | Result |
|---|---|
| `$g` written inside the SQL text | Does **not** substitute. DuckDB reads `$g` as a bind parameter: `Expected 1 parameters, but none were supplied`, at compile time. |
| `%{$g}` inside the SQL text | Parse error (`unexpected '$g'`). |
| a source parameter (`##! experimental.parameters`, `source: s(n::number is 4) is duckdb.sql(...)`) used as `%{n}` | Compile error: `Reference to undefined object 'n'`. |
| a wrapper that filters the lifted result: `duckdb.sql(...) extend { where: i <= $g }` | **Works**; the given overrides at query time. It can only filter or scale the SQL's output, so it cannot change what a recursion computes. |
| **query interpolation `%{ some_query }`**, where that query selects the given | **Works**, including inside `WITH RECURSIVE` and at run time with an override. |

The last shape is the one for assumption-driven recursions:

```malloy
##! experimental.givens

given: GROWTH_RATE :: number is 0.05
given: OPENING :: number is 10000

source: assumption_row is duckdb.sql("""SELECT 1 AS k""") extend {
  dimension: growth is $GROWTH_RATE
  dimension: opening is $OPENING
}
query: assumption_q is assumption_row -> { select: growth, opening }

source: compounding is duckdb.sql("""
  WITH RECURSIVE r(period, balance) AS (
    SELECT 1, a.opening * (1 + a.growth) FROM (%{ assumption_q }) a
    UNION ALL
    SELECT r.period + 1, r.balance * (1 + a.growth) FROM r, (%{ assumption_q }) a WHERE r.period < 5
  )
  SELECT * FROM r
""")
```

```malloy
run: compounding -> { select: *; order_by: period }
```

Executed: with the defaults the five balances are 10500, 11025, 11576.25, 12155.0625
and 12762.815625, the cached `Forecast!B6:F6`. With `givens: {"GROWTH_RATE": 0.02}` the
fifth is 11040.808032, and with `0.08` it is 14693.280768, the cached What-If data
table cells `Assumptions!E3` and `E6`. Label: `executed`. `#@ persist` on such a source
is refused at planning (`given_in_persisted_query`): a persisted build would bake in the
default givens. `cookbook-scenario.md#persistence` has what was tried.

## 3. Routes

| Route | Meaning | Typical triggers |
|---|---|---|
| **T** | Translate | `SUMIFS`, `COUNTIF`, `AVERAGE`, exact `VLOOKUP`/`INDEX-MATCH`, arithmetic, `IFERROR`, row-local columns |
| **C** | Translate at a stated cost | approximate-match `VLOOKUP`/`MATCH`/`LOOKUP`, `SUBTOTAL`/`AGGREGATE`, `TODAY`/`NOW`, financial and statistical functions with no DuckDB built-in (`PMT`, `NPV`, `IRR`, `NORM.INV`, ...), named `LAMBDA`s, Python in Excel |
| **X** | Stays in Excel | `RAND*`, `WEBSERVICE`/`FILTERXML`, live vendor feeds (`BDP`, `FDS`, `CIQ`, Smart View, TM1, ...) |
| **NR** | Ask the user | `OFFSET`/`INDIRECT`, `CHOOSE` used as a reference, spill references (`A1#`), `LAMBDA`/`MAP`/`SCAN`, CUBE functions, linked data types, a function that is not built in or defined (a possible add-in, `xll_udf`, or VBA UDF, `vba_udf`), XLM macros, `GETPIVOTDATA` (it reads a pivot's rendered cell), and any read through an external workbook link or through a defined name that points into another workbook |

Each region carries a plain-language reason. Say it in the report; do not paraphrase
it into something softer. An `NR` region has no recipe by design: ask what the number
means and rewrite the intent.

## 4. The routes that must not be misfiled

Each of these compiles as a plain translation and returns a different number. The
classifier raises the flag; the cookbook has the worked recipe.

| Flag | What Excel does | Malloy / DuckDB default | Recipe |
|---|---|---|---|
| `ci_match` | `SUMIF(S)`, `COUNTIF(S)`, `AVERAGEIF(S)`, `MAXIFS`, `MINIFS`, `VLOOKUP`, `HLOOKUP`, `MATCH`, `XLOOKUP` ignore case on text keys; the flag is not set when the lookup value is a numeric literal or numeric constant cell, or the key range is all numeric constants, and stays set when the key type is unknown; a criteria built from a number or date (`">="&DATE(...)`, `">"&A1` over a number cell, `">5"`) is numeric and does not set it. `sources[].case_variant_keys` counts, per lifted text column, the lowercase values that have more than one spelling, and the text report says "ci_match cannot bite: no case variants" when every text key column a flagged region reads has none. A plain `=` or `<>` comparison (`A2=B2`, `(A2:A9="x")` in `IF`, `IFS` or a `SUMPRODUCT` mask) is flagged too, with detail `=`/`<>`, unless a side is a number literal or both sides are all-numeric constant cells | equality is case-sensitive: `East`, `east` and `EAST` are three groups, and a text-key lookup finds nothing | `la1`, `la8` (text keys) |
| `approx_match` | `VLOOKUP`/`HLOOKUP` with `TRUE` or no 4th argument, `MATCH` with no 3rd, and `LOOKUP` binary-search **sorted** data for the last key at or below the target; unsorted data returns whatever the search lands on | an equality join returns null | `la9`, `la10` |
| `last_match_idiom` | `LOOKUP(n, 1/(condition), results)`: the errors from the false rows are skipped, so it returns the result on the **last** row meeting the condition; it is not a sorted search and `approx_match` is not raised | port it as the row with the greatest row number meeting the condition (`order_by` that row `desc` with `limit: 1`, or `max()` of it), never as a range join | `la9` |
| `approx_match` on `XLOOKUP`/`XMATCH` (`match_mode` -1 or 1) | next smaller or larger by value, found by a **linear** search, so it is correct on unsorted data | an equality join returns null; the range join in `la9` is right without a sorted table | `la9` |
| `XLOOKUP`/`XMATCH` `search_mode` 2 or -2 | a binary search that **assumes sorted** data | the same sorted-table check as `VLOOKUP TRUE` | `la9` |
| `criteria_wildcard` on `XLOOKUP` (`match_mode` 2) | a wildcard match, case-insensitive | a `LIKE`/regex, not a range join | `la3` |
| `criteria_comparison`, `criteria_cell` | `">="&A1` compares **numbers to numbers** only: a text `"1"` never matches. A criterion built with `&` from a cell or expression is text, so the number is cut to 15 significant digits first (the detail says `15sig`) *(semantics-cited)* | the lifted column is numeric and includes the text `"1"`; a strict `>` at full float precision disagrees on values that differ past digit 15 | `la2` (round both sides to 15 significant digits, or compare with a tolerance) |
| `criteria_wildcard` | `*` and `?` in a criterion | `=` is literal | `la3` |
| `criteria_cell` on a bare text criterion cell (`SUMIF(rng, A1, ...)`) | the cell's text is a criterion, not a value: it matches case-insensitively, `*`, `?` and `~` in it are wildcards, and a leading operator inside the text (`>=5`, `<>x`) is parsed as a comparison, so a cell holding `>=5` selects numbers, not the literal text *(semantics-cited)* | `lower(x) = lower($C)` matches the literal text only | `la1`, `la2`, `la3`; if the cell can hold an operator or a wildcard, the given must be parsed the same way, so ask what values it takes |
| (no flag) wildcards in an exact `VLOOKUP`/`HLOOKUP`/`MATCH` key | `VLOOKUP`/`HLOOKUP` (FALSE) and `MATCH` (0) honour `*` and `?` in a text lookup value; `XLOOKUP`/`XMATCH` only with `match_mode` 2 | `=` is literal | `la8` |
| `criteria_blank`, `criteria_nonblank` | `""` matches empty and empty-string cells; `"<>"` matches non-empty | null is not equal to anything | `la4` |
| `criteria_numeric` | `COUNTIF(range, 1)` also matches the **text** `"1"` | a numeric column cannot hold text | `la5` |
| `count_numbers_only`, `counta_nonblank` | `COUNT` counts numbers; `COUNTA` counts non-blank cells of any type | `count(field)` is a **distinct** count | `la5` |
| `avg_skips_blank_text` | `AVERAGE` skips blanks and text | `avg()` skips null only; a coerced text number or a blank read as 0 shifts the mean | `la6` |
| `sumproduct_mask`, `sumproduct_product` | a boolean mask times ranges, or a plain product of ranges; `SUMPRODUCT(1/COUNTIF(rng, rng))` is the distinct-count idiom, not a product | two different Malloy shapes; the idiom is a distinct count | `la7`; the idiom is `la14` |
| `full_column`, `full_column_total` | `A:A` includes every row, including a totals row under the data | the source excludes the totals row, so Malloy is *right* and the workbook double counts | `la11` |
| `hardcoded_constant`, `plug` | a literal inside a formula, or a literal cell overwriting a formula in a copied-down region | a dimension recomputes the plug away | `la12` |
| `date_as_text` | a column whose cells are all text that parses as a date (`2024-01-05`, `13/01/2024`, `Jan 2024`) is text to Excel's `TEXT`, `DATEVALUE`, `MONTH` and `YEAR` only through an implicit parse, and the flag sits on the source column and on every region reading it through one of those four | a lifted column of strings, not a `date`: `year()` and friends fail or read null until the port parses it explicitly. Never guess day/month order: the flag says `ambiguous` when both orders parse, and the owner decides | `recover-sources.md` |
| `index_row_zero` | `INDEX(range, row, col)` whose row or column argument is computed by subtraction (`ROW()-1`, `MATCH(..)-1`) or by counting or summing (`COUNTIF(..)`) can evaluate to 0, and `INDEX(range, 0)` returns the **whole column** (a spill, or an implicit intersection in one cell) instead of an error; the detail names `row` or `column` | a scalar lookup that returns one value, or nothing, where Excel returned the column; check the boundary row the argument reaches 0 on, and port the intended scalar | none needed |
| `typed_overwrite` | a constant of any type (text, boolean or error; a number is a `plug`) sits inside a column or row whose cells on both sides are the same formula, so one cell of a copied-down region was typed over; the flag is on both neighbouring regions and names the cell, and a hidden sheet gets none | the dimension recomputes the cell and disagrees with the sheet there: a workbook defect, not a rule to port | `la12` |
| `volatile` (`TODAY`) | recomputes at every open | pin to `dcterms:modified` as a `given:` | `la12` |
| `getpivotdata` | reads one cell of a pivot's rendered output, which is the pivot's snapshot as of `refreshedDate`; the region lists the pivot it reads | the same view, queried | `cookbook-pivot.md#p8` |
| `external_ref` | a reference like `'[book.xlsx]Sheet'!A1` or `[1]Sheet1!A1`, or a defined name (`external: true` in `defined_names`) that points into another workbook, reads that workbook's values **as of the last link refresh** | the file is not here and the link is never resolved | ask for the source; see below |
| `intersection` | a space between two references is the intersection operator: `(RowName ColName)` is one cell | the script resolves it to the crossing cell (empty intersection: no read), not to the two ranges | none needed |
| `dynamic_array_scalar` | a `cm` cell whose array ref is one cell is a scalar that Excel flags as a dynamic array; also a scalar wrapper over a dynamic-array function such as `COUNTA(UNIQUE(..))` (the detail names both) | a flagged single-cell formula, not a spill; its neighbours are still lifted | none needed |
| `proper_semantics` | `PROPER` capitalises every letter that follows a non-letter, not only after a space: `o'neil` is `O'Neil`, `3rd` is `3Rd`, and the rest is lowercased | a split-on-space port gives `O'neil` and `3rd`; the route stays T, so check the cases with an apostrophe, hyphen or digit | the function-families row below |
| `empty_argument` | a call with an empty argument (`AVERAGE(A1:A9,)`, `SUM(A1,,B1)`, `IF(A1,,3)`) reads it as 0 or empty, not as omitted: `AVERAGE` gains a zero in its denominator (n+1), `COUNT` and `COUNTA` gain one, and an empty `IF` value is 0. Raised for `SUM`, `SUMPRODUCT`, `AVERAGE`, `COUNT`, `COUNTA`, `MAX`, `MIN`, `PRODUCT`, `STDEV`, `VAR`, `MEDIAN` and the value arguments of `IF`/`IFS`; the detail is the function and argument position. Not raised for lookups, where an empty argument is a documented default. *(semantics-cited)* | the port drops the empty argument and the result differs | port the empty argument as a literal `0`, or ask whether it was a typo, and say which |
| (no flag) `MAX`/`MIN` over an empty range | Excel returns 0 | `max(x)` of no rows is null | port as `coalesce(max(x), 0)` (and `min` alike). *(semantics-cited)* |
| (no flag) a text dash (`-`) returned by `INDEX` or a lookup | the cell holds text, not a blank | read numerically it lifts as null, or fails the cast | read that column `all_varchar` and `nullif(x, '-')`, then `try_cast` to a number. *(semantics-cited)* |
| `hidden_dep` (conservative) | the flag propagates through labels and unevaluated `IF` branches, so a region can carry it without its value depending on a hidden cell | the oracle cannot be compared, so the region is unverified | when most of a model is `hidden_dep`, ask the user to share the hidden toggles, then compare; never read the hidden cells |
| `form_controls`, `solver` (report blocks) | list each control's linked cell and each Solver changing cell, by reference | they are inputs, not data | declare each as a `given:`; a Solver result is a typed value, so compare the typed values to the model and do not re-run the search (`cookbook-scenario.md#sc9`). *(semantics-cited)* |
| typed constants inside formula columns (`given_candidates` with `where: "formula_column"`, and possibly an `inputs` stanza) | an opening balance, a seed or a weights row that formulas read | not data, not a plug | declare each as a `given:` pinned from the cache and say so. *(semantics-cited)* |
| `number_to_text` | `&` joins a number (a literal, a numeric cell, or `ROUND`/`AVERAGE`/`SUM`/`COUNT`/`MAX`/`MIN`/`DAYS` and arithmetic) to text; the detail is the operand shape, e.g. `ROUND(...)&text`. Not raised when the number is wrapped in `TEXT`/`FIXED`/`DOLLAR` | Excel writes the number in General format: `55` not `55.0`, no trailing zeros, at most 15 significant digits; a naive cast prints `55.0` or 17 digits. *(semantics-cited)* | round, cast to `varchar`, then strip trailing zeros so the text is the number's shortest decimal form (`cast(round(x, 1) as varchar)` alone prints `55.0`; `regexp_replace(regexp_replace(s, '(\.[0-9]*?)0+$', '\1'), '\.$', '')` gives `55`, run on DuckDB 1.4.5), and compare the result as text against the cache |
| (no flag) `=`/`<>` where a side is blank | a blank cell equals `""` and `0`, so blank `=` blank is TRUE and `<>` is FALSE | raw DuckDB `a != b` is **null** whenever either side is null (`null != null` and `'a' != null` both return null, run on DuckDB 1.4.5); Malloy's raw `a != b` is TRUE for null against null and for null against `''` (run on a live Publisher), where Excel calls them equal | write the blank-aware form `coalesce(x, '') != coalesce(y, '')`, which reads the same in both |
| `opaque_dependency` on `INDIRECT` | an address built from a visible constant (a column letter in a cell, a sheet name chosen by a selector) is resolvable: the constant is the control | the classifier flags every `INDIRECT` as opaque and routes it NR | set the constant as a `given:` and port the intent (pick the column or range by the given, as in the `CHOOSE` selector section below). Ask the user when the address comes from hidden or external data. *(semantics-cited)* |
| `ref_error` | a cached `#REF!` flows through arithmetic as `#REF!` (error propagation), and `IFERROR`/`ISERROR` guards catch it | an error source is null, so `null + x` is null and the unguarded case matches | port each `IFERROR`/`ISERROR` guard as `coalesce`, and say which cells the workbook wraps. *(semantics-cited)* |
| `ROUNDUP`, `ROUNDDOWN` | `ROUNDUP` rounds away from zero and `ROUNDDOWN` toward zero, whatever the digit: neither is banker's rounding and neither is `ROUND` | `round` is half away from zero (`_concepts.md`), a different function | `ROUNDUP(x, 0)` is `sign(x) * ceil(abs(x))` (`-2.5` gives `-3`), `ROUNDDOWN(x, 0)` is `trunc(x)` (`-2.5` gives `-2`), both run on DuckDB 1.4.5; for n digits scale by `10^n` inside, which brings float error, so compare at a tolerance. *(semantics-cited)* |
| `row_range_aggregate` | a `SUM`/`AVERAGE` over a range that is a whole row spanning several blocks also includes the row's subtotal cells (a quarter total inside a run of months) | a column `sum` over the lifted data never sees those subtotal cells | list the row's data cells, or the blocks meant, explicitly; confirm with the owner whether the subtotal cells were meant to count. *(semantics-cited)* |

**Reads through an external link are snapshots.** A region flagged `external_ref` routes NR,
and so does every formula that reads a defined name that points into another workbook. Its
cached value is what the other workbook held at the last link refresh. It cannot be
recomputed here, and a match against it proves only that the translation reproduces the
snapshot. Ask whether the other workbook is the source of record (`discover.md`), and say
"as of the last refresh" in the report. The index form Excel writes (`[1]Sheet1!A1`) is
covered by unit tests only: no Excel-saved file tested had a formula that reads
through a link. A library-written file naming the other workbook literally does, and it routes NR.

Also in the same family, from the lift rather than the formula: a text number inside a
numeric column, a text date, serial 60 and the 1904 system
(`recover-sources.md`).

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
