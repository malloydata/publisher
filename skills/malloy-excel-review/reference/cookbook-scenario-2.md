<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

<a id="sc7"></a>

## SC7 - Monte Carlo

**The Excel**

```
MonteCarlo!B2:B1001   =NORM.INV(RAND(),100,15)
MonteCarlo!C2         =AVERAGE(B2:B1001)
```

**What it means** One thousand draws from a normal distribution, recomputed at every
recalculation. `RAND()` is volatile, so the classifier routes the region **X** (the cells
stay in Excel): no engine reproduces Excel's draws, and the cached values are one
sample. What translates is the **model**: the distribution, the draw count, and the
statistics read from it. A region that reads the draws, directly or through other regions
(`C2`, or anything built on it), carries `volatile_dep` and `random_dep` and routes **X** with
reason `reads random draws`: compare dependents statistically, or pin the draws as data: feed the cached draws across as a
data stanza and compare the dependents exactly.

**The Malloy** Seeded draws in DuckDB. `NORM.INV(p, mean, sd)` is the inverse normal
CDF, which DuckDB lacks. For draws driven by `RAND()` the Box-Muller transform of two
uniforms is enough, because the distribution is the same and the draws cannot match anyway.
Box-Muller is for this simulation case only: a `NORM.INV`, `NORM.S.DIST` or `NORM.DIST` of a
non-random argument needs a closed form accurate to 1e-12 or better, checked against the
cached values (`translate-formulas.md`, function families):

```malloy
given: MC_SEED :: number is 20240630
given: MC_DRAWS :: number is 1000
given: MC_MEAN :: number is 100
given: MC_SD :: number is 15

source: mc_input_row is duckdb.sql("""SELECT 1 AS k""") extend {
  dimension:
    seed is $MC_SEED
    draws is $MC_DRAWS
    mean is $MC_MEAN
    sd is $MC_SD
}
query: mc_inputs_q is mc_input_row -> { select: seed, draws, mean, sd }

source: draws is duckdb.sql("""
  WITH p AS (SELECT * FROM (%{ mc_inputs_q }))
  SELECT i,
         p.mean + p.sd * sqrt(-2 * ln(1 - u1)) * cos(2 * pi() * u2) AS value
  FROM (
    SELECT i,
           (hash(i, p.seed, 1) % 1000000007) / 1000000007.0 AS u1,
           (hash(i, p.seed, 2) % 1000000007) / 1000000007.0 AS u2
    FROM generate_series(1, (SELECT draws FROM p)::BIGINT) t(i), p
  ), p
""")

// the workbook's cached sample, for the parity comparison only; it is not a model source
source: mc_sheet is duckdb.sql("""
  SELECT "Value" AS value FROM read_xlsx('data/fixture.xlsx', sheet = 'MonteCarlo', range = 'A1:C1001', header = true)
""")
```

`mc_sheet` reads the `Value` column, which is a **formula column** (`NORM.INV(RAND(), ...)`).
The classifier does not lift formula output as data, and this source is not a model input: it
is the parity key, the one cached sample the statistics are compared with.

```malloy
run: draws -> {
  aggregate:
    n is count()
    mean is value.avg()
    sd is value.stddev()
    below_85 is count() { where: value < 85 } / count()
    above_115 is count() { where: value > 115 } / count()
}
```

**State the seed and the draw count in the report.** Both are givens, so they are in
the model, not in a comment.

**Seeding choice.** The obvious `setseed()` then `random()` is **not reproducible** inside
one `duckdb.sql()` statement: executed twice with the same seed, the means were 99.285
and 100.893. The draws above use `hash(i, seed, k)` instead, which is a pure function of
its arguments: the same seed gave identical results on every run (means 99.82695062468339
twice), a different seed (7) gave 100.63. `hash` is not documented as stable across DuckDB
versions, so the draws can change on an upgrade while the statistics do not.

**What to compare: distribution statistics, never draws.** Draws cannot match. Compare
each statistic with the model's own parameters, within sampling error, and show the
workbook's cached sample next to it:

| Statistic | Theory N(100, 15) | Workbook cached sample (1000) | Malloy, seed 20240630 (1000) | Malloy, 100,000 draws |
|---|--:|--:|--:|--:|
| mean | 100 | 99.382 | 99.827 | 99.997 |
| standard deviation | 15 | 14.511 | 14.645 | 15.012 |
| share below 85 | 0.1587 | 0.153 | 0.153 | 0.15899 |
| share above 115 | 0.1587 | 0.132 | 0.148 | 0.15953 |

Tolerance for 1000 draws: three standard errors, so the mean within 1.42
(`3 * 15 / sqrt(1000)`), the standard deviation within 1.0
(`3 * 15 / sqrt(2 * 999)`), and a tail share within 0.035
(`3 * sqrt(0.1587 * 0.8413 / 1000)`). Both samples pass; at 100,000 draws all four sit
within 0.02 of theory. Label: the seeded recipe is `executed`; the workbook's own draws
are not reproducible, so there is no cell-for-cell comparison and none is claimed.

**What it costs** The cells stay X. If the workbook's Monte Carlo is the product (a
distribution of outcomes people look at), the Malloy source reproduces the distribution
on demand and with a fixed seed, which Excel cannot. If someone relies on the *specific*
numbers a particular recalculation showed, that is a snapshot, and no translation
recovers it.

<a id="sc8"></a>

## SC8 - Data Tables, and Goal Seek or Solver over a discrete range

**The Excel**

```
Assumptions!E2     =Forecast!F6                     the output the table varies
Assumptions!D3:D6  2% | 4% | 6% | 8%                the input column
Assumptions!E3:E6  {=TABLE(,B3)}                    <f t="dataTable" ref="E3:E6" r1="B3">
```

**What it means** A one-variable What-If Data Table: for each value in `D3:D6`, put it in
`B3` (the growth rate), recalculate, and record `Forecast!F6`. The classifier reports the
table in `regions[].data_table` and under "Data tables (what-if)" in the text report:
`r1` and `r2` as written (`r1` is the ROW input cell and `r2` the COLUMN input cell of a two-way table; in a one-way table `dtr` says whether `r1` is a row or a column input), `dt2D` and `dtr`, then the decoded `row_input_cell` and
`column_input_cell` (here `column_input_cell` = `B3`), the axis ranges (`column_axis` = `D3:D6`),
the `formula_cells` it records (`E2`; a two-way table has one `formula_cell`, a `row_axis` and a
`column_axis`), plus `input_cells` and `axis_refs` as lists, and `axis_oracle_stanzas`: one exact-range ORACLE read per axis, so the values across the top and down the side are checked too. When a lifted stanza's range holds the
input cell or the axis values, the stanza says so (`-- WARNING: the range holds data table ...`) and
the source carries `data_table_overlaps`: those cells are what-if levers, not data, so exclude them from the
source and make the input a `given:`. Only the top-left cell
carries the `<f t="dataTable">`; the other three are bare cached values, which is why the
classifier lists `Assumptions!E3:E6` under "Never lifted". `Goal Seek` and `Solver` over a
**discrete** set of candidates are the same idea with a pick at the end.

**The Malloy** A precomputed grid: one row per candidate input, the whole recursion run for
each. The candidate column is lifted from the sheet (`D3:D6` is constants), and a given
picks the row to show:

```malloy
given: SCENARIO_GROWTH :: number is 0.06

source: growth_grid is duckdb.sql("""
  WITH RECURSIVE g AS (
    SELECT row_number() OVER () AS scenario, "D"::DOUBLE AS growth
    FROM read_xlsx('data/fixture.xlsx', sheet = 'Assumptions', range = 'D3:D6', header = false)
  ),
  r(scenario, growth, period, balance) AS (
    SELECT g.scenario, g.growth, 1, (a.opening * (1 + g.growth))::DOUBLE
    FROM g, (%{ inputs_q }) a
    UNION ALL
    SELECT r.scenario, r.growth, r.period + 1, r.balance * (1 + r.growth)
    FROM r WHERE r.period < (SELECT count(*) FROM (%{ flows_q }))
  )
  SELECT * FROM r
""") extend {
  dimension: is_selected is abs(growth - $SCENARIO_GROWTH) < 1e-9
}
```

```malloy
run: growth_grid -> { where: period = 5; select: growth, balance; order_by: growth }
```

| `growth` | `balance` after period 5 | cached `Assumptions!E3:E6` |
|--:|--:|--:|
| 0.02 | 11040.808031999999 | 11040.808032 |
| 0.04 | 12166.529024000003 | 12166.529024000001 |
| 0.06 | 13382.255776 | 13382.255776000004 |
| 0.08 | 14693.280768000004 | 14693.280768000006 |

`executed`, all four within 1e-8. With `givens: {"SCENARIO_GROWTH": 0.02}` and
`where: is_selected and period = 5` the query returns the one row, 11040.808031999999.

**When there is no grid to precompute.** The same four numbers come from the SC3 and
SC1 sources with `givens: {"GROWTH_RATE": 0.02}`: 11040.808032, the cached `E3`. When the
model is already driven by givens, a Data Table is a loop over a given, and the live source
*is* the table. Build the grid only when every row has to show at once, or when the source
is persisted (a persisted source cannot read a given).

**Goal Seek over a grid.** "What growth rate reaches 14,000 in 2029?" has a closed form
here, `1.4^(1/5) - 1 = 0.0696`, so use it when there is one. When there is not, scan
candidates and take the closest:

```malloy
given: TARGET_BALANCE :: number is 14000

source: goal_grid is duckdb.sql("""
  WITH g AS (SELECT (i / 1000.0)::DOUBLE AS growth FROM generate_series(0, 200) t(i))
  SELECT g.growth, a.opening * power(1 + g.growth, (SELECT count(*) FROM (%{ flows_q }))) AS balance
  FROM g, (%{ inputs_q }) a
""") extend {
  dimension: gap is abs(balance - $TARGET_BALANCE)
}
```

```malloy
run: goal_grid -> { select: growth, balance, gap; order_by: gap; limit: 3 }
```

The three closest rows are growth 0.07 (balance 14025.5, gap 25.5), 0.069 (13960.1,
gap 39.9) and 0.071 (14091.2, gap 91.2). `semantics-cited (hand-derived)`: the exact answer
`1.4^(1/5) - 1 = 0.06961` lies between the two nearest grid points (0.069 and 0.070),
so the grid's resolution (here 0.001) is the answer's precision. **Goal Seek leaves no trace in the file** except the typed value
in the changing cell, so a hardcoded input with many digits is the tell. Ask the owner
what it was solving for.

**What it costs** The grid is stored in the model, so it is stale the moment an input it
did not vary changes. A grid answers "which row", not "what is the optimum"; the step is
the precision.

**A two-variable table needs a two-key grid.** `<f t="dataTable" dt2D="1" r1="..." r2="...">`
is the two-input form: a row of values for one input, a column for the other, and the output
at every crossing. The grid above has one key. A real planning workbook can carry several What-If data tables,
one of them two-variable and the rest one-variable, so the 2D shape is not a corner case. The recipe is the same
with a second candidate column, crossed. Here the second input is the opening balance (the
fixture has no 2D table, so its row of values is invented for the recipe; on a real file lift
both axes from the table's header row and column):

```malloy
source: growth_open_grid is duckdb.sql("""
  WITH RECURSIVE g AS (
    SELECT row_number() OVER () AS g_idx, "D"::DOUBLE AS growth
    FROM read_xlsx('data/fixture.xlsx', sheet = 'Assumptions', range = 'D3:D6', header = false)
  ),
  o AS (SELECT * FROM (VALUES (1, 8000.0), (2, 10000.0), (3, 12000.0)) t(o_idx, opening)),
  r(g_idx, o_idx, growth, opening, period, balance) AS (
    SELECT g.g_idx, o.o_idx, g.growth, o.opening, 1, (o.opening * (1 + g.growth))::DOUBLE FROM g, o
    UNION ALL
    SELECT r.g_idx, r.o_idx, r.growth, r.opening, r.period + 1, r.balance * (1 + r.growth)
    FROM r WHERE r.period < (SELECT count(*) FROM (%{ flows_q }))
  )
  SELECT * FROM r
""")
```

```malloy
run: growth_open_grid -> {
  where: period = 5
  group_by: growth
  aggregate:
    open_8000 is balance.sum() { where: opening = 8000 }
    open_10000 is balance.sum() { where: opening = 10000 }
    open_12000 is balance.sum() { where: opening = 12000 }
  order_by: growth
}
```

`executed`. The 10000 column is the one-variable table again (11040.808032, 12166.529024,
13382.255776, 14693.280768 against the cached `Assumptions!E3:E6`), which is the cross-check
that the crossing is right. The other two columns are `semantics-cited (hand-derived)`: 8000 and 12000
scale the same recursion linearly, so 2% on 8000 is `0.8 * 11040.808032 = 8832.6464256`, the query's first cell. Pivot
the two keys into a grid in the consumer, as the data table shows them. A real two-variable
table's cached cells are the comparison once there is an Excel save of one.

<a id="sc9"></a>

## SC9 - Continuous Goal Seek and Solver

**What it means** An optimisation over a continuous variable, with constraints, over a model
whose evaluation is the workbook. The cache holds the last optimum; the objective, the
changing cells and the constraints are in the `solver_*` defined names, which the
classifier counts (`solver`, route X). There is nothing here to translate: the search is
the work. The report's Solver block (`solver` in `--json`) lists the objective, the changing cells and the constraints by
reference for visible sheets, and each changing cell is a given candidate labelled `solver changing cell`.
The cached Solver result is a typed value: compare those typed values to the model and do not
re-run the search. *(semantics-cited)*

The Answer, Sensitivity and Limits report sheets are constants copied from a run: typed results, not sources. Compare the model to their values; do not model them as data. Their cell addresses can be stale against the saved Solver names (a report may describe an older copy of the model), so match by position and say so. A changing cell that no formula reads is an inert model: report it.

**Routes**

- **Stays in Excel (the default).** Name the seam: *the model translates; the solve does
  not.* What feeds across it is the set of **solved input values** (the changing cells), which the
  Malloy side takes as givens or as a table, and the objective's value for the
  report. Re-run the solve in Excel when the assumptions change and carry the new values
  across. The migration report lists the workbook as "reporting translated, optimisation
  retained".
- **An HTML data app running Pyodide.** A package's `public/` page can load Pyodide
  (CPython compiled to WebAssembly) and run `scipy.optimize` in the browser over rows it
  fetched with `Publisher.query(...)`; see `skill:malloy-html-data-apps` and its
  `reference/runtime.md`. Vendor Pyodide into `public/vendor/` rather than
  loading it from a CDN, for the reason the data-app skills give. **Untested here**: the
  Python runs in the viewer's browser with the viewer's data authority, outside
  Publisher, and nothing in this skill executed it. Offer it as an option, not a result,
  and keep the objective written in Malloy so a reviewer can read it.
- **Not an option: Python inside Publisher.** Notebooks and dashboards carry no author-written
  JavaScript file, only markdown, Malloy and renderer tags (`docs/security-posture.md` in the Publisher
  repository), so there is no place to run server-side Python from one.

<a id="sc10"></a>

## SC10 - Scenario Manager, form controls, data validation

All three are inputs with names. None of them has a cached output of its own, so
the recipes are `semantics-cited (hand-derived)`. The `form_control` and `scenario_manager`
tells were spec-derived and are now confirmed against real files.

**Scenario Manager** (`<scenarios>` in the sheet XML) stores named sets of values for
changing cells. Translate it to a table of scenarios and a given that picks one; the
assumptions then read the picked row:

```malloy
source: scenarios is duckdb.sql("""
  SELECT * FROM (VALUES ('Base', 0.05), ('Upside', 0.08), ('Downside', 0.02)) t(scenario, growth)
""")

# label="Scenario" control=select suggest { source=scenarios dimension=scenario }
given: SCENARIO :: string is 'Base'

query: scenario_q is scenarios -> { where: scenario = $SCENARIO; select: growth }

source: scenario_balance is duckdb.sql("""
  WITH RECURSIVE r(period, balance) AS (
    SELECT 1, (a.opening * (1 + s.growth))::DOUBLE FROM (%{ inputs_q }) a, (%{ scenario_q }) s
    UNION ALL
    SELECT r.period + 1, r.balance * (1 + s.growth) FROM r, (%{ scenario_q }) s WHERE r.period < 5
  )
  SELECT * FROM r
""")
```

```malloy
run: scenario_balance -> { where: period = 5; select: balance }
```

Base 12762.815625000001, with `givens: {"SCENARIO": "Upside"}` 14693.280768000004, and
`"Downside"` 11040.808031999999. The given is a `string`, not a `filter<string>`: a filter can
hold several items (`Base, Upside`), and `scenario_q` then returns several rows, which the
recursion cross-joins (ten balances at period 5 instead of one). A scenario is one choice. The scenario values here are invented for the recipe;
the upside and downside happen to equal the cached data-table cells `Assumptions!E6`
and `E3` because they use 8% and 2%. A real `<scenarios>` element holds each
scenario's changing cells (`inputCells`, with the cell in `r` and the value in `val`).
The classifier prints each scenario's name, input cells and values, and the JSON carries them
in the top-level `scenarios` (a scenario on a hidden sheet is withheld, and the comment and
user fields are dropped). On a hand-built workbook of that shape:

```
- scenario Base on R: B2 = 10
- scenario High on R: B2 = 20, B3 = 0.5
```

Carry over the scenario the owner names, not the one currently displayed (`current` in the
`<scenarios>` element is only the last one applied).

**Form controls** (`xl/ctrlProps/ctrlProp*.xml`) bind a control to a cell with `fmlaLink`;
a list or combo box also has `fmlaRange` for its items. The linked cell is an input, so it
becomes a `given:`; the range becomes the allowed values (the `suggest` source above, lifted
from that range). A list or combo box writes the **index** of the chosen item (1-based)
into the linked cell and the model usually does `INDEX(range, link)` (Excel's documented
behavior; not checked against a file here); keep the given as the item text and the
`INDEX` disappears. A check box writes TRUE or FALSE (a `boolean` given),
and a spinner or scroll bar writes a number between its `min` and `max` (a `number` given;
`# range_min=` and `range_max=` on a `filter<number>`).

**Data validation lists** (`dataValidations`, `type="list"`) are the same allowed-values
idea without a control: a free enum for a `string` or `filter<string>` given with
`control=select`. A validation formula that is a literal list (`"Base,Upside,Downside"`)
becomes a `VALUES` source like `scenarios` above; one that points at a range is lifted from
that range.

**What it costs** The control is tied to the given by name, not by cell. If the workbook's
formulas read the linked cell directly, every formula that did must be translated to read
the given, which is what the `absolute` regions already list.

<a id="persistence"></a>

## Persistence

`#@ persist` materializes a source so queries read a table. On a source that reads a given,
**Publisher refuses it at planning**. Tested by adding `##! experimental.persistence` and
`#@ persist name="compounding_tbl"` to
`source: persisted_compounding is compounding -> { select: * }`, where `compounding` is the
recursion in `translate-formulas.md` (it reads its givens through `%{ }`). The package's
build plan listed it under `refusedSources` with `reason: reads_given` and the
message that a given in a persisted query "is substituted at BUILD time, so the table
would hold the default's rows and serve them to every caller". The same shape of
recursion with its numbers written as literals was accepted into the build plan. Only
planning was checked; no build was run.

So a scenario source is either **live** (reads givens; computed on every query; fine at
forecast scale) or **persisted with fixed inputs** (a snapshot of one scenario, with the
other inputs at their cell values). A grid ([SC8](#sc8)) can be persisted with every
candidate row, and the row chosen at read time by an `extend { where: abs(growth -
$SCENARIO_GROWTH) < 1e-9 }` on the persisted query, which `docs/materialization.md`
describes as a read-time predicate; a literal-input grid with that `extend` was accepted
into the build plan (not built).

<a id="stays-in-excel"></a>

## Stays in Excel

Name each seam in the migration report. A seam is where the translated model ends and
the workbook keeps working, and the report must say what crosses it.

| What stays | Why | The seam | What crosses it |
|---|---|---|---|
| `RAND`-driven cells (`MonteCarlo!B2:B1001`) | volatile; no engine reproduces the draws | the distribution parameters | mean, sd and draw count go to Malloy as givens; the statistics come back for comparison |
| Continuous Solver and Goal Seek | the search is the work | the changing cells | solved input values go to Malloy as givens or a table; re-solve in Excel when assumptions change |
| VBA-driven logic: event macros, macro-written cells, button handlers | code outside the file's formulas; macro writes are not recomputed by anything | each macro, by name | the cells it writes are snapshots; a pure VBA UDF may translate once the user exports the source (`C`), a macro that writes cells does not (`X`) |
| VSTO or add-in code that writes cells | the values are static in the file | the assembly or add-in name | the cells it wrote, as of the save |
| Live feeds (`BDP`, `FDS`, `CIQ`, `HsGetValue`, `DBRW`, `XFGetCell`, ...) | the data lives in another system | the system | point Malloy at the vendor's warehouse feed if the organisation has one; the cache is a dated snapshot |
| `WEBSERVICE`, `FILTERXML` | a fetch at recalculation | the URL (never fetched here) | whatever the service returned, as of the save |
| ActiveX controls, embedded OLE objects, DDE links | executable or opaque content | the object | skipped entirely; ask the user |

A formula whose output comes from any of these has a cached value that is a **snapshot,
not an oracle**: parity for it is "matches as of save" at best (`parity.md`). A model
that mixes translated and retained parts is still a good outcome. The failure is
translating the retained part into something that looks right.

## What the recipes were checked against

| Recipe | Cached cells | Result | Label |
|---|---|---|---|
| SC1 givens against `Assumptions!B2:B7` | six values | six `drift = false` | `executed` |
| SC2 `cumulative_flow` | `Forecast!B9:F9` | 1000, 2200, 3700, 5500, 7500 match | `executed` |
| SC3 `compounded` | `Forecast!B6:F6` | match to 1e-12 | `executed` |
| SC4 spine | none | 2030 and 2031 carry 7500 | `semantics-cited (hand-derived)` |
| SC5 `depreciation` | `Forecast!B7:F8` | match | `executed` |
| SC5 `loan` | none | final balance 9.5e-12; payment 2373.96 | `semantics-cited (hand-derived)` |
| SC6 `interest_loop` | `Forecast!B4:F5` | within 6e-5 of the cache (tolerance 0.01) | `semantics-cited` |
| SC7 seeded draws | `MonteCarlo!B2:C1001` | statistics within sampling error; draws not compared | `executed` |
| SC8 `growth_grid` | `Assumptions!E3:E6` | all four match to 1e-8 | `executed` |
| SC8 `goal_grid` | none | nearest grid point 0.07 | `semantics-cited (hand-derived)` |
| SC10 `scenario_balance` | none (upside and downside equal `E6`, `E3`) | match | `semantics-cited (hand-derived)` |
