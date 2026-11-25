<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Recover Sources (Step 2)

> A workbook hands you cells, not tables. This step decides which ranges become
> sources, lifts only the cells that are data, and checks the lift against the
> workbook's own totals before anything is built on it.

Read `translate-formulas.md` for what happens to the formula cells this step leaves
behind. Every `read_xlsx` behavior below was run on Publisher 0.9.0 with the DuckDB
it bundles, **v1.5.5**, against the bundled `fixture.xlsx` and a small scratch
workbook of layout traps. The fixture's cached values come from the `python` build
(see `fixtures/README.md`), so a number labelled `semantics-cited` matches that
build's model of Excel, not an Excel save.

## The rule: never lift a formula cell

`read_xlsx` returns each cell's **cached value**. A calculated column lifted as data
makes parity compare a number with itself, and it hides the formula that was supposed
to be translated. So a source lifts a cell only when it has no `<f>` and sits outside
every array, spill, What-If data table and pivot output range.

`classify_workbook.py` does most of this for you. Its **Sources** section prints one
`duckdb.sql("... read_xlsx(...)")` stanza per region, names the columns it did *not*
lift (`-- not lifted (formula columns ...)`), and lists the ranges that are never
lifted. Every formula cell it left out must come back as a `dimension:` or `measure:`
(see `translate-formulas.md`). A formula cell in the minority of a lifted column (a total
or a derived cell among constants) is listed in `formula_cells_in_lifted`, and the stanza sets it
to NULL by sheet row so it never enters as data:
`CASE WHEN __r IN (28) THEN NULL ELSE "E" END`, where `__r` is the sheet row number of each row the
read returns (`row_number() OVER ()` plus the first row; `read_xlsx` returns rows in sheet order).
Each such cell is parity-only: write it as a `dimension:` or `measure:`, never read it as data. A
column with more than 200 of them is not lifted at all (its reason says so). In a wide layout the
stanza NULLs the (row, period) pair instead (`WHERE NOT (__r IN (28) AND period = '2025')`). In the fixture,
`tbl_Sales[Revenue]` is a calculated column, so the stanza lifts `Region`, `Product`, `Qty`, `OrderDate`, `Price` and the
model recomputes revenue:

```malloy
dimension: revenue is coalesce(qty_coerced, 0) * price
```

`coalesce` is there because Excel multiplies a blank by 0 and SQL propagates null.
The recomputed column sums to **356.25**, the table's totals-row cached value
(`Data!F13`). That equality is the check that the lift and the formula are both right.

## Where a source starts and stops

| What the workbook has | What the classifier reports | What the source does |
|---|---|---|
| Excel Table (`xl/tables/*.xml`) | `Excel Tables: tbl_Sales A1:F13 (data A1:F12)` | Lift `ref` **minus the totals row**. `A1:F13` returns the `Total` row as data (verified: it arrives as `Region = 'Total'`). The totals row's cached value is the parity oracle for the column. |
| Named range | `Defined names: 2 ref` (the text report counts them; the targets, such as `SalesData` = `Data!$A$1:$F$12`, are in `--json` `defined_names`) | Same as a range. A name that points at a formula or constant is not a source. |
| A defined name whose body is a formula (`kind` `formula`, not `ref`) | listed in `--json` `defined_names` with its name, scope, kind and `hidden`; the body is **not** printed and the name is not translated | Treat the name as a formula region: ask the user for its body (or read it from the workbook's own Name Manager with them), then translate it from a recipe or route it NR. Formulas that read the name stay unresolved until then; report the gap, never guess the body. A `LAMBDA` body is workbook text too: ask for it from Name Manager, or infer it from the cached results of the cells that call it, verify across hundreds of cells, label it hand-derived and never claim it exact. |
| Bare range, no Table | a `range` stanza with a header guess | Check the header and the last row by hand; nothing else vouches for them. |
| A headerless table whose first row is data (a code, a name and a cost, say, with a derived column beside it) | `header = false`, columns named by sheet letter, the derived column under `-- not lifted` | The first row is a record, not a header, and a lone number in it (a cost of 2000) is not a period. A header row is wide only when it has two or more numeric period cells, none of the rows below put text under them, and the first row is not two text cells and one number matching the row beneath. Name the columns yourself. A first-row formula that repeats the formulas in the rows beneath it (the same relative formula down the column) also marks the first row as data. |
| A header row that holds numbers the formulas below read (`I$4` markups) | `inputs_in_header` in `--json` and a `-- inputs in the header row` line in the stanza; the range starts at the first data row with `header = false`, the names taken from the header row | Each numeric header cell a formula reads by an absolute row is an input: declare it as the suggested `given:` (`given: input_i4 :: number is 0.15`) or keep a 3-row source of name, value and note. The formula columns under them are not lifted. |
| A controls block: labels in one column, inputs (numbers, a text selector, a date, a percent) in the next, read by formulas as `$B$2` (or, for a selector in `IF`, `IFS`, `CHOOSE`, `SWITCH` or `INDEX`, by any ref read by two formulas or sitting in a labelled block) | `given_candidates` in `--json` (the text report's "Given candidates"): ref, kind, value; the stanza reads the input column as text plus a `_number` column, so a selector such as `Base` is never NULLed, even when formula outputs share the column | Declare each candidate as a `given:` with its listed value; a text selector is a text given. Pivot output cells and `GETPIVOTDATA` arguments are never listed. A typed constant that any formula reads, inside a formula-dominated block or in a column or row of formulas (an opening balance, a driver in a forecast column, a weights row), is listed too, with `where: "formula_column"` when its line is otherwise formulas; a complete rectangle of them also gets an `inputs` stanza. A row or column of 5 or more adjacent labels used as criteria headers (the months across a report, say) is a dimension list, not N givens: declare ONE given or none and iterate the dimension with a `group_by:` or a values source; label it `dimension_list`. A constant that a list validation targets (kind `choice`, with `choices` or `choices_source`), or that a formula reads as an `IF` condition, a lookup value or a criteria, is listed even when read once and even when the sheet is not formula-dominated. |
| A header with a leading or trailing space | the stanza reads positionally and aliases to `columnN` | DuckDB trims header names, so selecting `" ABC LTD"` by its written name is a Binder Error. Rename the column in the model. |
| Several blocks on one sheet | one `### Sheet!Ref` stanza per connected block | One source per block. `dimension ref` is the sheet's bounding box, not a table. |
| A formula-dominated block (labels and period headers by hand, formulas for the body) | the constant label column and header row each get their own stanza; the formula body gets none, or a `no stanza:` line giving the reason (`stanza_refused`) | A printed stanza is the only source range: never widen one by hand to take in the body, which is translated output. If a range you need is not printed, report it as a skill gap and ask the user. |
| A column whose cells are all text dates (`2024-01-05`, `13/01/2024`, `Jan 2024`) | `sources[].date_as_text` (column, range, format, order) and a `-- date_as_text:` line in the stanza; formulas reading it through `TEXT`, `DATEVALUE`, `MONTH` or `YEAR` carry the same flag | The column is text, not date serials: parse it explicitly in the model (`strptime`/`try_cast`), and never guess day/month order when the report says `ambiguous` or `conflicting`: ask the owner. |
| A printed column name that Malloy reads as a keyword or date part (`month`, `year`, `day`, `date`, `index`) | `sources[].reserved_columns` and a `-- Malloy-reserved column name(s):` line in the stanza; the output name stays equal to the header | Backtick the name in Malloy (`` `month` ``) or rename it with `SELECT ... AS` in the wrapper; the script never renames it for you. |
| A first row that may be a header the script did not take as one (a computed-text header cell over text, or text beside a stray number) | `sources[].first_row_text` (`first_row_text_kind`: `all` or `mostly`) and a `-- first row is ... text and may be a header` line in the stanza; a computed-text header over numeric columns is taken as a header | The read starts at that row with `header = false`, so a header lands as one data row (151 rows for 150 records): confirm in the sheet, then filter it out in the wrapper. |
| Merged two-row header | `2-row header (merged group labels above the column names)` | Start the range at the **first data row**, use `header = false`, and name the columns yourself (below). |
| Subtotal or total rows inside the data | `WHERE COALESCE("A", '') NOT ILIKE '%subtotal%'` and `subtotal rows (6) are excluded by their label; check no data row carries it`, or, when no label is shared, `WHERE __r NOT IN (81)` | A label predicate must not match a data row. The row-position form depends on the rows staying where they are: re-derive it if rows move. A row counts as a total only when an aggregate in the block's own columns sums rows of that block above it; aggregates elsewhere on the sheet never exclude a row. Every exclusion is listed by sheet row with its formula cell (`sources[].excluded_rows`, a `-- excluded rows: 24 (via B24)` stanza line, and the text report): open each cell and confirm the row is a total, not data. Keep the subtotal rows as parity checks. |
| Grouped (outline) rows | `--json` only: `sheets[].outline_rows`, the count of rows with `outlineLevel > 0` (the text report does not print it) | Detail rows sit under a subtotal row. Read the subtotal's formula. Codes 1-11 and 101-111 differ only for rows hidden by hand: a row hidden by a filter is excluded by every code. Never lift the subtotal row. |
| A merged sheet of prose only (charts, insights, notes) | merged cells holding text, no numbers and no formula | It holds text, not data: skip it unless a formula reads it, and say it was skipped. |
| Hidden rows | `hidden rows in range` | `read_xlsx` returns them (below). Decide per formula whether Excel counted them. |
| Hidden or `veryHidden` sheets | State column of the **Sheets** table | `read_xlsx` reads them by name. Lift one only when something visible depends on it. |
| `autoFilter`, slicers, timelines | Autofilter column | UI state. Never bake the filter into the model. |
| Periods across columns | `wide layout recognised` | Unpivot (below). |

The template `packages/create-malloy-package/templates/model.custom.xlsx.malloy`
walks the manual version of this (probe the top rows, then fix `sheet`, `range` and a
data-row `WHERE`) and `skill:malloy-gotchas-modeling` lists the `read_xlsx` options.
This step replaces the guessing with the ranges the classifier measured. Keep the
template's two checks: compare `record_count` with what you know is in the file, and
compare a sum with the sheet's own total.

A printed stanza reads the workbook by its bare file name, so copy the workbook into the package folder next to the model; keep scratch `.malloy` files outside that folder until they are registered (`parity.md` section 3).

## What `read_xlsx` does, verified

| Probe | Result |
|---|---|
| No `sheet =` | Reads the first sheet and says nothing about it. |
| No `range =`, a title row in A1 | The title becomes the only column name; the read stops at the first blank row. On the probe sheet it returned **0 rows**. |
| No `range =`, a blank spacer row inside the data | `stop_at_empty` defaults to true: **3 of 5** data rows came back. `stop_at_empty = false` returns all of them plus an all-null row for the spacer. |
| Explicit `range =` | Flips `stop_at_empty` off and reads every cell in the range. `A3:B100` on a 5-row block returns the 5 rows plus all-null padding to row 100. Filter on a column every real row has. |
| First data cell blank | The column is typed `DOUBLE`. A text cell further down then fails at query time: `Failed to parse cell 'B4': Could not convert string 'n/a' to DOUBLE`. `ignore_errors = true` nulls it. `empty_as_varchar = true` types the column `VARCHAR` instead. |
| First data cell text in a numeric column | The whole column is `VARCHAR`; the numbers arrive as `'2'`, `'3'`. |
| **Text `"1"` inside a numeric column** | Coerced to `1.0` **with no error**. After the lift it cannot be told from a number. |
| `all_varchar = true` | Every cell is its stored text. A text `"1"` and a numeric `1` are both `'1'`; a date-styled cell is its serial (`'45292'`), and a text date is `'2024-07-15'`. |
| Empty-string cell vs absent cell | In a `VARCHAR` column an empty-string cell is `''` and an absent cell is `NULL`. |
| Header row of numbers (years across the top) | Not recognised as a header: the row comes back as data in columns `A`, `B`, `C`. Pass `header = true`. |
| First row all text and every data row all text | The first row is consumed as the header. Pass `header = false` (columns are then named `A`, `B`, ...). |
| Text date in a date-styled column | Typed read fails: `Could not convert string '2024-07-15' to DOUBLE`. With `ignore_errors = true` that cell becomes null. |
| Serial 60 (the 1900 leap-year bug) | A typed read gives `1900-02-28`. The day Excel shows, 1900-02-29, does not exist. |
| `date1904="1"` workbook | A typed read applies the 1900 base: serial 43830 came back as **2019-12-31**; the sheet means **2024-01-01**. Four years off, no error. |
| Hidden row | Returned like any other row. |
| `veryHidden` sheet | Readable by name. |

Labels: all `executed` (`read_xlsx` on DuckDB 1.5.5). The serial-60 and 1904 rows
are `semantics-cited` for what *Excel* shows, because the fixture is not an Excel save.

Two consequences shape every lift:

1. **A range with text among numbers is read with `all_varchar = true` and converted in
   the SQL.** DuckDB types a column from its first row, so a text cell under a blank or a
   number fails at run time (`Could not convert string ... to DOUBLE`), and so does an error
   value (`#N/A`) or a formula string in any column of the range, lifted or not. The
   classifier prints that read for you: `all_varchar = true`, `TRY_CAST(... AS DOUBLE)` on a
   column that is mostly numbers, a date (or timestamp, when the number format shows a time) from the
   serial on a date column, and a text column left as text with a typed `_number` copy beside it when
   it holds some numbers. Each text cell, boolean or `t="d"` ISO date string in a numeric column is
   named in a comment and set to NULL by sheet row, because Excel's `SUM` ignores it and `TRY_CAST`
   would read `'1'`, `' 7 '`, `'1e3'`, `'nan'` and a boolean as numbers (delete a row from the
   `CASE WHEN __r IN (...)` list to coerce that cell instead). In a 1904 workbook every date column is
   converted from `1904-01-01` even when it holds no text, because a typed read is four years and a day
   off; a date-styled serial below 61 makes the source read with `all_varchar` and every date or datetime
   column in it convert as `CASE WHEN x < 60 THEN <1899-12-31 base> WHEN x < 61 THEN NULL ELSE <1899-12-30 base> END`
   (1 to 59 a day later than the plain conversion, 60 NULL), with a stanza comment to check those cells by
   hand; a source with no such serial keeps the plain conversion. The classifier no
   longer prints a per-sheet `mixed text/number columns` line; `--json` `sheets[].mixed_columns`
   counts the lifted data rows only, titles and headers excluded.
2. **A text number is invisible after the lift.** The classifier names the cells in
   the stanza (`-- text cells in numeric, date or blank-led columns: C5 (numeric), D8 (date)`),
   and `--json` carries them as `sources[].text_cells`. Handle each one explicitly
   (below). Silent coercion is how `COUNT` and `SUMIFS ">="` go wrong.

**Column names Malloy rejects.** A printed stanza keeps the sheet's header text, and a header such
as a month, year or date heading can be a word Malloy refuses unquoted (`years`, `month` and `year` were observed); the stanza prints a `-- Malloy-reserved column name(s):` comment and `sources[].reserved_columns` lists them. The same words are rejected as `given:` names: a given named `YEAR` fails, so call it `PICK_YEAR`. If Malloy rejects a
printed column name, quote it with backticks in Malloy or rename it in a wrapper
`SELECT ... AS`; never edit the printed range to dodge it. The same applies to an oracle read.

## The `sales` source, annotated

This is the `Data` table lifted for the cookbook. Every column is converted in the
SQL, where `try_cast` nulls what it cannot read instead of failing.

```malloy
source: sales is duckdb.sql("""
  SELECT
    row_number() OVER () + 1 AS sheet_row,
    "Region" AS region,
    "Product" AS product,
    "Qty" AS qty_raw,
    try_cast("Qty" AS DOUBLE) AS qty_coerced,
    CASE WHEN row_number() OVER () + 1 = 5 THEN NULL ELSE try_cast("Qty" AS DOUBLE) END AS qty_num,
    try_cast("Price" AS DOUBLE) AS price,
    "OrderDate" AS date_raw,
    CASE WHEN try_cast("OrderDate" AS DOUBLE) < 60 THEN date '1899-12-31' + floor(try_cast("OrderDate" AS DOUBLE))::int
         WHEN try_cast("OrderDate" AS DOUBLE) > 60 THEN date '1899-12-30' + floor(try_cast("OrderDate" AS DOUBLE))::int
    END AS order_date,
    try_strptime("OrderDate", '%Y-%m-%d')::date AS order_date_text
  FROM read_xlsx('data/fixture.xlsx', sheet = 'Data', range = 'A1:F12', header = true, all_varchar = true)
""") extend {
  dimension: revenue is coalesce(qty_coerced, 0) * price
  dimension: order_date_any is coalesce(order_date, order_date_text)
}
```

What each column is for:

- `sheet_row` is the worksheet row (`row_number() + 1` because the header is row 1).
  It lets a rule name a cell the classifier reported ("hidden row 5", "text number in
  C5"). It relies on `read_xlsx` returning rows in file order; verified here (row 5
  came back as the West / `'1'` row). In a source with a `WHERE`, compute it in an
  inner select first, because a window runs after `WHERE`.
- `qty_coerced` is what Excel's `*` sees: the text `"1"` counts as 1. A blank stays
  null here, and `revenue` turns it into 0 with `coalesce`. Use it for arithmetic and
  for `COUNTIF(range, 1)`, which also matches the text.
- `qty_num` is what `SUMIFS ">="`, `COUNT` and `AVERAGE` see: numbers only. The `= 5`
  nulls the one text cell, the `C5` the classifier reported. This is a data-specific
  line that the reported text cells tell you to write; there is no general way to
  tell the cells apart after the lift.
- `order_date` is the serial converted with the leap-year rule below (serial 60, the
  phantom 1900-02-29, becomes null); `order_date_text` is the hand-typed text date.
  Excel compares only the serial one in `">="&B2` (cached `Report!B15` is 289), so
  parity uses `order_date`. The business answer may want `order_date_any` (the text date is almost certainly a
  date). That is a decision for the user: it changes the number
  (`cookbook-lookup-aggregate.md#la2`).

## Dates

- **Serial or text.** A column that mixes both (an export edited by hand) needs both
  branches, as above. A typed read with `ignore_errors` silently drops the text one.
- **The 1900 leap-year bug.** Excel counts a 1900-02-29 that never existed. Serials
  1 to 59 are one day later than `date '1899-12-30' + serial` gives (that base is one
  day early for them); serial 60 is not
  a date; from 61 on, `date '1899-12-30' + serial` is right. Executed on literals:

  | serial | `date '1899-12-30' + s` | corrected |
  |--:|---|---|
  | 1 | 1899-12-31 | 1900-01-01 |
  | 59 | 1900-02-27 | 1900-02-28 |
  | 60 | 1900-02-28 | `NULL` |
  | 61 | 1900-03-01 | 1900-03-01 |
  | 45292 | 2024-01-01 | 2024-01-01 |

  ```sql
  CASE WHEN s < 60 THEN date '1899-12-31' + floor(s)::int
       WHEN s < 61 THEN NULL
       ELSE date '1899-12-30' + floor(s)::int END
  ```

  Use `floor(s)::int` in both date branches: DuckDB rounds a double on `::int`, so a serial
  with a time part (`45291.9`) would land on the next day (`2024-01-01` instead of `2023-12-31`).

  The `sales` source above uses this rule, so `Data!D10` (serial 60) lifts as null.

  `Data!D10` in the fixture is serial 60. Label: `semantics-cited` (what Excel
  displays is not re-checked against Excel).
- **The 1904 system.** `workbookPr date1904="1"`: read the cells with `all_varchar`
  and add the serial to `date '1904-01-01'`. Executed on `fixture_1904.xlsx`: serial
  43830 gives 2024-01-01, matching the same dates in `fixture.xlsx`; the typed read
  gave 2019-12-31. Label: `semantics-cited`.
- A pivot or SUMIFS that compares dates compares serials. Convert the cell the formula
  compares against (`">="&B2` with B2 = 45292) to a `given:` date.

## Layout traps

### Merged two-row header (`Ledger`)

`Ledger` has `Group | Entry | Amounts` over `Debit | Credit`, with `A1:A2` and `B1:B2`
merged and `C1:D1` merged. Both obvious reads fail at run time (`header = true` over
`A1:D10`: `Could not convert string 'Ops' to DOUBLE`; `header = false` over the same
range: `... string 'Credit' to DOUBLE`). The classifier reads from the first data row
with `header = false` and takes the column names from the merged and sub-header cells
(`Group`, `Entry`, `Debit`, `Credit`):

```
SELECT "A" AS "Group", "B" AS "Entry", "C" AS "Debit", "D" AS "Credit"
FROM read_xlsx('fixture.xlsx', sheet = 'Ledger', range = 'A3:D9', header = false)
WHERE COALESCE("A", '') NOT ILIKE '%subtotal%'
```

The source below is that stanza with lowercase aliases (so no quoting in Malloy),
`try_cast` on the amounts and a `sheet_row`:

```malloy
source: ledger is duckdb.sql("""
  SELECT * FROM (
    SELECT row_number() OVER () + 2 AS sheet_row,
           "A" AS grp, "B" AS entry,
           try_cast("C" AS DOUBLE) AS debit, try_cast("D" AS DOUBLE) AS credit
    FROM read_xlsx('data/fixture.xlsx', sheet = 'Ledger', range = 'A3:D9', header = false)
  ) WHERE COALESCE(grp, '') NOT ILIKE '%subtotal%'
""")
```

```malloy
run: ledger -> {
  group_by: grp
  aggregate:
    total_debit is debit.sum()
    total_credit is credit.sum()
    credit_excl_hidden is credit.sum() { where: sheet_row != 5 }
  order_by: grp
}
```

The range stops at row 9 on purpose: row 10 is a trailing subtotal (the classifier's
`trailing total rows left out of the range`), and row 6 is an in-range subtotal the `WHERE` removes by its label. Executed:

| group | debit | credit | cached `SUBTOTAL(9)` | match |
|---|--:|--:|---|---|
| Ops | 1250 | 100 | `Ledger!C6` 1250, `D6` 100 | yes |
| Sales | 0 | 1250 | `Ledger!C10` 0, `D10` 1250 | yes |

Row 5 (`Refund`, credit 100) is hidden by hand, not by a filter. `SUBTOTAL(9)` counts a
manually hidden row, and the cached `D6` = 100 confirms it; excluding `sheet_row = 5` gives
Ops credit **0**, which is what `SUBTOTAL(109)` would return. Had a filter hidden row 5,
both codes would exclude it. A filter is UI state, so the port needs it as an explicit
`where:` (the filtered column and its criterion, from the classifier's autofilter listing)
and the report must say the number is about a filtered screen. Label: `executed`.

### Wide to long

Periods across columns (`Assumptions!A10:F11`, `Period | 2025 | ... | 2029`) become
one row per label and period:

```malloy
source: flows is duckdb.sql("""
  SELECT * FROM (SELECT * FROM read_xlsx('data/fixture.xlsx', sheet = 'Assumptions', range = 'A10:F11', header = true))
  UNPIVOT (amount FOR period_ IN (COLUMNS(* EXCLUDE ("Period"))))
""") extend {
  dimension: fiscal_year is period_::number
  measure: flow is amount.sum()
}
```

Executed: 5 rows, 2025 to 2029, 1000, 1200, 1500, 1800, 2000, the constants the
`Forecast` sheet reads. The classifier prints a safer form of this: it reads from the first data
row with `header = false`, selects each column by its sheet letter and names it (`"B" AS "2025"`),
so a formula column inside the range (a forecast year written as `=C2*1.1`) is simply not selected
and a formula cell among the constants is removed by row and period. When a lifted period header
cannot be named that way (a header that is itself a formula) and a formula column sits in the block,
the stanza is refused with a reason instead of reading the formula column as data. The hand-written
form above passes `header = true` (years are numbers, so detection would not take the row as a header). **The new column's name must differ from the label
column**: DuckDB compares names without case, so `FOR period` next to a `"Period"`
column comes back as `period_1`. The classifier suffixes the name (`period_`) for this
reason. Label: `executed`.

A wide block whose cells are formulas (`Forecast!B1:F9`) is not lifted at all; it is
the translated result, not the input.

## Check the lift

Do these before building measures; each is one query.

1. **Row count** against the sheet: `sales` lifts 11 rows (`A2:A12`); the Table has 11
   data rows plus the totals row.
2. **Total against the sheet's own total**, read independently of the lift: the
   recomputed `revenue.sum()` is 356.25 and `Data!F13` caches 356.25.
3. **Mixed columns**: for each column the classifier named, count how many cells are
   text and make the measure's semantics explicit (`qty_num` vs `qty_coerced`).
4. **Trailing rows**: anything under the range that is not data (totals, footnotes)
   stays out of the range, not out of a filter that a later edit can drop.
5. **Hidden rows and sheets**: list them in the migration report; they changed what
   the workbook's own formulas counted.

## What this step cannot see

- Whether a cell the classifier reports as text was meant as a number (it reports the
  cell; the owner says what it meant).
- Whether a hidden row was hidden by a filter or by hand.
- Any source that is not on a sheet: `xl/connections.xml`, Power Query and
  `queryTables` mean the sheet is a cache of a database. See `discover.md` on the data
  question before lifting a cache.
