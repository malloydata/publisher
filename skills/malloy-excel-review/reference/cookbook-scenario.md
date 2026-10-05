<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Cookbook: Scenario and Financial Models (Step 3)

> A scenario model is half reporting and half machinery. The reporting half
> (actuals, budget against actual) translates like any other sheet. The machinery
> (input cells, roll-forwards, recurrences, circular references, Monte Carlo, Data
> Tables, Solver) splits three ways: it translates, it translates at a stated cost,
> or it stays in Excel. This file says which, and names the seam for the last group.

Read `translate-formulas.md` first: it routes the region and explains how a `given:`
reaches inside a `duckdb.sql()` source. `recover-sources.md` defines the `flows`
source this file builds on.

**The recipes below are worked, each labelled `executed` or `semantics-cited` in place.** Where
a recipe says it ran, it ran on a local Publisher (0.9.0, DuckDB v1.5.5, REST) against
`fixtures/fixture.xlsx` and the numbers shown are what came back. The labels are the ones in
`cookbook-lookup-aggregate.md`:

| Label | Meaning |
|---|---|
| `executed` | The Malloy ran and the cached value is not an Excel-quirk route: a plain sum, join, window or recursion. |
| `semantics-cited` | The route is an Excel quirk (here: iterative-calculation convergence). Excel's behavior is cited from the file format or from the fixture generator's model of it; the Malloy was executed separately and compared with that value. The committed fixture is the `python` build, so a cached value is the generator's model, not an Excel save. Re-label `executed (Excel)` only once `fixtures/README.md` records one. |
| `semantics-cited (hand-derived)` | The fixture has no cached cell for this formula. The Malloy ran; the expected value was worked out by hand from the data, shown with the working. |

## The split

| Construct | Route | Recipe |
|---|---|---|
| Actuals, GL rollups, budget against actual | T | the reporting cookbooks |
| Input cells feeding the model | T | [SC1](#sc1) a `given:` per assumption |
| Opening plus running flows | T | [SC2](#sc2) a cumulative window over a period dimension |
| Compounding growth | C | [SC3](#sc3) `exp(sum(ln(1+g)))`, or `pow()` when the rate is constant |
| Forecast periods with no rows | C | [SC4](#sc4) a generated period spine |
| Recurrences (depreciation, debt waterfalls) | C | [SC5](#sc5) a `WITH RECURSIVE` source |
| Circular references (`iterate="1"`) | C | [SC6](#sc6) the recursion unrolled to a fixed pass count with a tolerance |
| Monte Carlo (`RAND`) | C for the model, X for the cells | [SC7](#sc7) seeded draws in DuckDB |
| What-If Data Tables, discrete Goal Seek and Solver | C | [SC8](#sc8) a precomputed scenario grid |
| Continuous Goal Seek and Solver | X | [SC9](#sc9) stays in Excel, or Pyodide in a data app |
| VBA-driven logic | X | [Stays in Excel](#stays-in-excel) |
| Scenario Manager, form controls, data validation lists | T | [SC10](#sc10) a scenario table and a `given:` |

Route is a priority order, not a verdict. A C recipe is a working model that costs
something; each recipe states the cost.

## Setup

One given per input cell. The defaults are the cell values, so a query with no override
reproduces the workbook.

```malloy
##! experimental.givens

// Assumptions!B2:B7
given: OPENING :: number is 10000
given: GROWTH_RATE :: number is 0.05
given: ASSET_COST :: number is 12000
given: SALVAGE :: number is 2000
given: LIFE :: number is 5
given: INTEREST_RATE :: number is 0.06
// calcPr iterateCount and iterateDelta
given: MAX_ITER :: number is 100
given: TOLERANCE :: number is 0.001

// one row carrying every given, for %{ } interpolation into duckdb.sql() sources
source: input_row is duckdb.sql("""SELECT 1 AS k""") extend {
  dimension:
    opening is $OPENING
    growth is $GROWTH_RATE
    cost is $ASSET_COST
    salvage is $SALVAGE
    life is $LIFE
    rate is $INTEREST_RATE
    max_iter is $MAX_ITER
    tol is $TOLERANCE
}
query: inputs_q is input_row -> { select: opening, growth, cost, salvage, life, rate, max_iter, tol }

// the period columns of Assumptions!B11:F11, one row per period
query: flows_q is flows -> { group_by: fiscal_year; aggregate: flow; order_by: fiscal_year }
```

`$g` written inside SQL text does not substitute and a source parameter is not visible
there (`translate-formulas.md`, "Where a `given:` can and cannot reach"). The recursive
sources below therefore read the givens through `%{ inputs_q }`, which does work, at run
time, with a request override.

**Recursive CTE columns take their type from the anchor.** DuckDB fixes each column's
type from the first (non-recursive) term. A given arrives as an integer when it has no
decimal point, so `SELECT a.opening ...` as an anchor makes the column `INTEGER` and
every later row is silently rounded: executed, a loan's `opening_balance` came back
`8226` instead of `8226.036` until the anchor was cast. A `0.0` literal is
`DECIMAL(2,1)` and fails once a larger value arrives (`Could not cast value 630.000000
to DECIMAL(2,1)`). Cast every anchor column that carries money to `DOUBLE`, as the
sources below do.

<a id="sc1"></a>

## SC1 - Input cells become givens

**The Excel**

```
Assumptions!B2:B7   Opening balance 10000 | Growth rate 5% | Asset cost 12000 |
                    Salvage value 2000 | Life (periods) 5 | Interest rate 6%
Forecast!B6         =Assumptions!$B$2*(1+GrowthRate)       GrowthRate = Assumptions!$B$3
```

**What it means** The `absolute` regions of the classifier (`Forecast!B2`,
`Forecast!B7:F7`, the named range `GrowthRate`) read single input cells. Each input is a
parameter someone changes and watches the model respond to.

**The Malloy** The declarations in Setup. A `given:` is typed, has a default, and becomes a
control in the Console, notebooks and data apps, with no code.

**Defaults can drift from the sheet.** The givens are a copy of the cells. Keep a guard
that compares them with the sheet, and run it whenever the workbook changes:

```malloy
source: assumptions is duckdb.sql("""
  SELECT "Assumption" AS name, "Value" AS sheet_value
  FROM read_xlsx('data/fixture.xlsx', sheet = 'Assumptions', range = 'A1:B7', header = true)
""") extend {
  dimension: given_value is
    pick $OPENING when name = 'Opening balance'
    pick $GROWTH_RATE when name = 'Growth rate'
    pick $ASSET_COST when name = 'Asset cost'
    pick $SALVAGE when name = 'Salvage value'
    pick $LIFE when name = 'Life (periods)'
    pick $INTEREST_RATE when name = 'Interest rate'
    else null
  dimension: drift is abs(sheet_value - given_value) > 1e-12
}
```

```malloy
run: assumptions -> { select: name, sheet_value, given_value, drift; order_by: name }
```

Run it with no overrides. All six rows come back `drift = false`. Label: `executed`.

**The sheet is the source of the default, not of the live value.** A request override
changes what the model computes, not what the cell says. Anything that reads the cell (a
report, a pivot) still sees the saved value.

Validation, a dropdown over a fixed list, or a slider: see [SC10](#sc10).

<a id="sc2"></a>

## SC2 - Opening plus running flows

**The Excel**

```
Forecast!B3:F3   =Assumptions!B11        flow per period
Forecast!B9:F9   =SUM($B$3:B3)           cumulative flow
Forecast!C2:F2   =B5                     opening = last period's closing
```

**What it means** A running total down a period axis. `Forecast!B9:F9` is the region the
classifier calls `range_aggregate`; `Forecast!C2:F2` is a relative reference with an
offset, a prior-period read.

**The Malloy** A cumulative window over the period dimension:

```malloy
run: flows -> {
  group_by: fiscal_year
  aggregate: flow
  calculate:
    cumulative_flow is sum_cumulative(flow)
    closing_no_interest is $OPENING + sum_cumulative(flow)
    opening_no_interest is $OPENING + sum_cumulative(flow) - flow
  order_by: fiscal_year
}
```

| fiscal_year | flow | `cumulative_flow` | cached `Forecast!B9:F9` | `closing_no_interest` | `opening_no_interest` |
|--:|--:|--:|--:|--:|--:|
| 2025 | 1000 | 1000 | 1000 | 11000 | 10000 |
| 2026 | 1200 | 2200 | 2200 | 12200 | 11000 |
| 2027 | 1500 | 3700 | 3700 | 13700 | 12200 |
| 2028 | 1800 | 5500 | 5500 | 15500 | 13700 |
| 2029 | 2000 | 7500 | 7500 | 17500 | 15500 |

`cumulative_flow` is `executed`. The two `*_no_interest` columns are `semantics-cited (hand-derived)`:
the workbook's closing balance includes interest from the circularity in
[SC6](#sc6), so there is no cached cell for a roll-forward without it
(10000 + 1000 = 11000, + 1200 = 12200, and so on).

**What it costs** `sum_cumulative` is a query-level analytic. A `measure:` cannot hold it
(`Cannot use an analytic field in a measure declaration`) and it is refused inside
`aggregate:` too. Every consumer repeats the `calculate:` block, and the window shares its
stage with the query's `where:`. A date filter on the query restarts the running total at
the first visible period; use a two-stage query when the workbook's total ran from the
start. Executed: with `where: fiscal_year >= 2027` the totals are 1500, 3300, 5300, not the workbook's 3700, 5500, 7500. `skill:malloy-powerbi-review` documents the same cost and the two-stage fix (its `cookbook-time` reference, T1).

An opening balance that depends on the **previous closing** only needs this recipe when
the closing is a plain sum. The moment the closing feeds back into itself (interest on
the average balance) it is a circularity: [SC6](#sc6).

<a id="sc3"></a>

## SC3 - Compounding growth

**The Excel**

```
Forecast!B6      =Assumptions!$B$2*(1+GrowthRate)
Forecast!C6:F6   =B6*(1+GrowthRate)
```

**The Malloy** A product over periods is a sum of logarithms. The growth rate can differ by
period, which is the case that justifies the form:

```malloy
source: growth_by_period is flows extend {
  dimension: growth is $GROWTH_RATE
  dimension: ln_growth is ln(1 + growth)
  measure: ln_step is ln_growth.sum()
}
```

```malloy
run: growth_by_period -> {
  group_by: fiscal_year
  aggregate: ln_step
  calculate: compounded is $OPENING * exp(sum_cumulative(ln_step))
  order_by: fiscal_year
}
```

| fiscal_year | `compounded` | cached `Forecast!B6:F6` |
|--:|--:|--:|
| 2025 | 10500 | 10500 |
| 2026 | 11025 | 11025 |
| 2027 | 11576.250000000002 | 11576.25 |
| 2028 | 12155.062500000002 | 12155.0625 |
| 2029 | 12762.815625000003 | 12762.815625000001 |

`executed`; agreement is to about 1e-12, not bit for bit, because `exp(ln(x))` is not
exact. State a tolerance (1e-9 relative is plenty).

**What it costs** Readability. `exp(sum_cumulative(ln(1 + g)))` says nothing to someone
who knows the workbook's `*(1+g)` copied right, and it inherits the query-level cost of
SC2. When the rate is **constant**, as it is here, skip all of it: the balance in period
`n` is `OPENING * pow(1 + $GROWTH_RATE, n)`. The logarithm form earns its place only for
a rate that varies by period (`B6*(1+C$4)` with a rate row), and then `ln_growth` comes
from that row, not from one given.

<a id="sc4"></a>

## SC4 - Forecast periods that have no rows

**The Excel** A forecast that runs to 2031 while the inputs stop in 2029. Excel shows
blank or zero cells; a source built from the sheet has no row for 2030 or 2031, so a
`group_by: fiscal_year` skips them and a cumulative window never carries the total
forward.

**The Malloy** Generate the periods and join the data to them:

```malloy
given: FIRST_YEAR :: number is 2025
given: LAST_YEAR :: number is 2031

source: year_spine is duckdb.sql("""SELECT y AS fiscal_year FROM generate_series(2020, 2040) t(y)""") extend {
  where: fiscal_year >= $FIRST_YEAR and fiscal_year <= $LAST_YEAR
  join_one: f is flows on f.fiscal_year = fiscal_year
}
```

```malloy
run: year_spine -> {
  group_by: fiscal_year
  aggregate: flow is coalesce(f.flow, 0)
  calculate: cumulative_flow is sum_cumulative(flow)
  order_by: fiscal_year
}
```

| fiscal_year | flow | cumulative_flow |
|--:|--:|--:|
| 2025 to 2029 | 1000, 1200, 1500, 1800, 2000 | 1000, 2200, 3700, 5500, 7500 |
| 2030 | 0 | 7500 |
| 2031 | 0 | 7500 |

With `LAST_YEAR = 2027` the query returns the first three rows. `semantics-cited (hand-derived)` (no cached
cell): the carried total of 7500 is the 2029 value, held.

**What it costs** The spine's bounds are a decision the sheet made implicitly (how far the
columns run). Make them givens, as above, and a month spine is the same shape with
`generate_series(date '2024-01-01', date '2024-12-01', interval 1 month)`. A spine is a
stopgap: a real date dimension belongs in the model once more than one source needs it.

<a id="sc5"></a>

## SC5 - Recurrences (depreciation, debt waterfalls)

**The Excel**

```
Forecast!B7:F7   =(Assumptions!$B$4-Assumptions!$B$5)/Assumptions!$B$6      straight-line charge
Forecast!B8      =Assumptions!$B$4-B7
Forecast!C8:F8   =B8-C7                                                     net book value
```

**What it means** Each period's value is the previous period's value moved by a rule. The
classifier sees a relative reference with an offset (`B8-C7`), a prior-period read.

**The Malloy** A `WITH RECURSIVE` source in the model. The assumptions come in through
`%{ inputs_q }`:

```malloy
source: depreciation is duckdb.sql("""
  WITH RECURSIVE r(period, dep, nbv) AS (
    SELECT 1, ((a.cost - a.salvage) / a.life)::DOUBLE, (a.cost - (a.cost - a.salvage) / a.life)::DOUBLE
    FROM (%{ inputs_q }) a
    UNION ALL
    SELECT r.period + 1, r.dep, r.nbv - r.dep
    FROM r WHERE r.period < (SELECT life FROM (%{ inputs_q }))
  )
  SELECT * FROM r
""")
```

```malloy
run: depreciation -> { select: *; order_by: period }
```

| period | `dep` | cached `Forecast!B7:F7` | `nbv` | cached `Forecast!B8:F8` |
|--:|--:|--:|--:|--:|
| 1 | 2000 | 2000 | 10000 | 10000 |
| 2 | 2000 | 2000 | 8000 | 8000 |
| 3 | 2000 | 2000 | 6000 | 6000 |
| 4 | 2000 | 2000 | 4000 | 4000 |
| 5 | 2000 | 2000 | 2000 | 2000 |

`executed`. Because the charge here is constant, a closed form
(`ASSET_COST - period * (ASSET_COST - SALVAGE) / LIFE`) is simpler and needs no recursion.
Reach for the recursion when the rule needs the previous value: declining balance
(`nbv * (1 - rate)`), a debt waterfall, a reserve that feeds itself.

**A debt waterfall** is the same shape with two prior-period reads. The payment is
the annuity formula `PMT(rate, life, -principal)`, written out because DuckDB has no `PMT`:

```malloy
source: loan is duckdb.sql("""
  WITH RECURSIVE a AS (
    SELECT opening AS principal, rate, life,
           opening * rate / (1 - power(1 + rate, -life)) AS payment
    FROM (%{ inputs_q })
  ),
  r(period, opening_balance, interest, payment, principal_paid, closing_balance) AS (
    SELECT 1, a.principal::DOUBLE, a.principal * a.rate, a.payment,
           a.payment - a.principal * a.rate, a.principal - (a.payment - a.principal * a.rate)
    FROM a
    UNION ALL
    SELECT r.period + 1, r.closing_balance, r.closing_balance * a.rate, a.payment,
           a.payment - r.closing_balance * a.rate,
           r.closing_balance - (a.payment - r.closing_balance * a.rate)
    FROM r, a WHERE r.period < a.life
  )
  SELECT * FROM r
""")
```

```malloy
run: loan -> { select: period, opening_balance, interest, payment, closing_balance; order_by: period }
```

Executed with `OPENING = 10000`, `INTEREST_RATE = 0.06`, `LIFE = 5`: the payment is
2373.9640043118948, interest runs 600, 493.56, 380.74, 261.14, 134.38, and the closing
balance after period 5 is 9.5e-12. `semantics-cited (hand-derived)`: the fixture has no loan, but the
expected endpoint is exact arithmetic (a fully amortising loan ends at zero, here to
floating-point noise), and the payment matches
`10000 * 0.06 / (1 - 1.06^-5)` worked by hand.

**Extra payments and a payoff row.** A loan with an extra-payment rule changes the payment
to `min(balance + interest, scheduled + extra)`, so the last payment is short and the balance
stops at zero. `IPMT` and `PPMT` on a running balance are the interest on the opening balance
(`opening_balance * rate`) and the payment less that interest; a workbook that wraps them in
`IFERROR` returns 0 or blank past the payoff, which is `coalesce(.., 0)` once the balance is
zero. The payoff row, found in Excel by a descending `MATCH` or `MAX(row)` where the balance is
positive, is `max(period) where opening_balance > 0`. Worked on DuckDB 1.4.5 with a balance of
1000, 1% a period, six periods, a scheduled payment of 172.548 and an extra 100: interest runs
10.000, 7.375, 4.723, 2.045; the payment is 272.548 for three periods and 206.497 in the fourth;
the closing balance is 737.452, 472.278, 204.452, then 0; the payoff period is 4. In the
recursive source above, replace the payment with `least(r.closing_balance * (1 + a.rate), a.payment + a.extra)`
(`extra` is a given). *(semantics-cited for the Malloy; the arithmetic ran in DuckDB)*

**What it costs** The model is a source, not a measure: a recursive CTE cannot be
filtered by a query-level `where:` before it recurses, and each period depends on the one
before, so it cannot be sliced to a single period without computing the others. That is
cheap at forecast scale (tens of periods) and wrong at a million rows. A recursion that
reads no givens can be persisted ([Persistence](#persistence)); one that does cannot.

<a id="sc6"></a>

## SC6 - Circular references (`iterate="1"`)

**The Excel**

```
Forecast!B4:F4   =Assumptions!$B$7*AVERAGE(B2,B5)      interest on the average balance
Forecast!B5:F5   =B2+B3+B4                              closing = opening + flow + interest
calcPr           iterate="1" iterateCount="100" iterateDelta="0.001"
```

**What it means** Closing depends on interest and interest depends on closing, a real
cycle (the classifier reports `iterate=1 and a cycle was found: a real circularity`, 10
cells in 5 loops). `iterate="1"` alone is only a setting; a workbook can carry it with no
cycle. Excel recalculates the chain until a pass changes no cell by more than
`iterateDelta`, or `iterateCount` passes have run, whichever comes first.
Regions inside a reported cycle route C with reason `circular`; under `iterate="1"` a
self-inclusive range (an aggregate over a range holding its own cell, `iterates_when_iterate_on`)
iterates too, so it is an SC6 loop and routes C, while with iterate off it stays a flagged non-loop.

**The Malloy** The same stopping rule, as a recursion that iterates a period's interest
until it settles, then moves on to the next period. N and the tolerance are the workbook's own
(`MAX_ITER`, `TOLERANCE`; read them from `workbook_props.calc.effective` in the JSON, which
holds `iterateCount` 100 and `iterateDelta` 0.001 for the fixture, with the defaults applied
when `calcPr` omits an attribute):

```malloy
source: interest_loop is duckdb.sql("""
  WITH RECURSIVE
  a AS (SELECT * FROM (%{ inputs_q })),
  f AS (SELECT row_number() OVER (ORDER BY fiscal_year) AS period, fiscal_year, flow FROM (%{ flows_q })),
  last_period AS (SELECT max(period) AS p FROM f),
  it(period, pass, opening, interest, delta) AS (
    SELECT 1, 0, a.opening::DOUBLE, 0::DOUBLE, 1e18::DOUBLE FROM a
    UNION ALL
    SELECT
      CASE WHEN it.delta < a.tol OR it.pass >= a.max_iter THEN it.period + 1 ELSE it.period END,
      CASE WHEN it.delta < a.tol OR it.pass >= a.max_iter THEN 0 ELSE it.pass + 1 END,
      CASE WHEN it.delta < a.tol OR it.pass >= a.max_iter THEN it.opening + f.flow + it.interest ELSE it.opening END,
      CASE WHEN it.delta < a.tol OR it.pass >= a.max_iter THEN 0.0
           ELSE a.rate * (it.opening + (it.opening + f.flow + it.interest)) / 2 END,
      CASE WHEN it.delta < a.tol OR it.pass >= a.max_iter THEN 1e18
           ELSE abs(a.rate * (it.opening + (it.opening + f.flow + it.interest)) / 2 - it.interest) END
    FROM it
    JOIN f ON f.period = it.period
    CROSS JOIN a
    WHERE NOT (it.period = (SELECT p FROM last_period) AND (it.delta < a.tol OR it.pass >= a.max_iter))
  )
  SELECT f.fiscal_year, f.period, it.opening, f.flow, it.interest, it.pass AS passes,
         it.opening + f.flow + it.interest AS closing, it.delta < a.tol AS converged
  FROM (SELECT *, row_number() OVER (PARTITION BY period ORDER BY pass DESC) AS rn FROM it) it
  JOIN f ON f.period = it.period
  CROSS JOIN a
  WHERE it.rn = 1 AND it.period <= (SELECT p FROM last_period)
""")
```

```malloy
run: interest_loop -> { select: fiscal_year, interest, passes, converged, closing; order_by: fiscal_year }
```

| fiscal_year | `interest` | cached `Forecast!B4:F4` | `closing` | cached `Forecast!B5:F5` | passes |
|--:|--:|--:|--:|--:|--:|
| 2025 | 649.4845203 | 649.4845276 | 11649.4845203 | 11649.4845276 | 5 |
| 2026 | 757.7000550 | 757.7000640 | 13607.1845753 | 13607.1845915 | 5 |
| 2027 | 888.0732511 | 888.0732620 | 15995.2578264 | 15995.2578536 | 5 |
| 2028 | 1045.0674690 | 1045.0674824 | 18840.3252955 | 18840.3253359 | 5 |
| 2029 | 1227.2365864 | 1227.2366026 | 22067.5618818 | 22067.5619386 | 5 |

Every cell is within 6e-5 of the cached one, inside the 0.01 tolerance the fixture
documents for the circular block. `converged` is true on every row. Label:
`semantics-cited`. The committed fixture is the `python` build, whose cached values are
the generator's own 100-pass, 0.001 iteration. Excel stops at the same change threshold
but visits cells in its own order, so it can land a few millionths away. The number to
compare is within tolerance of the workbook; only an Excel save makes it
`executed (Excel)`.

**Where it can fool you**

- **The cap hides non-convergence.** With `MAX_ITER = 2` the same query returns interest
  648.9, 756.98, 887.19, 1043.99, 1225.93: five wrong numbers, with `passes = 2` and
  `converged = false`. Always select `converged` and fail the parity check on a `false`.
  Excel also stops silently at its cap and shows the last pass, but its `iterateCount`
  caps the whole workbook's recalculation, not each period's loop as `MAX_ITER` does here,
  so a capped workbook and a capped query do not land on the same wrong numbers. Treat any
  capped result as a non-match to investigate rather than a number to reproduce.
- **Tight tolerances change the last digits, not the answer.** `TOLERANCE = 1e-9` takes 9
  passes and lands on 649.4845360824614, 757.7000743968384, ... 1227.2366216546616.
- **A linear circularity has a closed form, and it is the better check.** Here
  `interest = rate * (2*opening + flow) / (2 - rate)`: 649.4845360824743 for 2025. It
  is an algebraic identity, not an iteration, so use it to confirm the loop, and use
  the loop when the feedback is not linear (a cash sweep with a `MIN`, a `MAX`, an
  `IF`).
- **A different rate, same query.** `INTEREST_RATE = 0.1` converges in 6 passes with
  2025 interest 1105.263140625. No cached cell exists for it; the closed form gives
  `0.1 * 21000 / 1.9 = 1105.263157...`, which differs by 1.7e-5, the loop's tolerance.
- **Excel's cache is where Excel stopped, not the fixed point.** Excel stops at
  `iterateDelta` or `iterateCount`, so a loop that stopped at `iterateDelta` 0.001 can sit
  about 1e-4 from the true fixed point, and a cache that converged further can differ from
  a solve that stopped at the same threshold by about that much. To compare, solve the
  recurrence tightly (about 1e-12, or a high fixed pass count), but compare at no less than
  `iterateDelta` (0.01 on the fixture), and report both numbers: the solve tolerance and the
  comparison tolerance. Read `iterateDelta` and `iterateCount` from the classifier's `calc`
  object (`calc.effective` has the defaults applied; `graph.iterate` says whether the
  setting is on). Say which tolerance was used when labelling a row `executed`.
- **The cost is the unrolling.** The workbook's cycle is implicit; the Malloy source is
  eight lines of recursion nobody will edit casually. Say so in the migration report, and
  keep N and the tolerance as givens so the reader sees them.

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
  fetched with `Publisher.query(...)`; see `skill:malloy-html-data-apps` and
  `skill:malloy-html-data-app-runtime`. Vendor Pyodide into `public/vendor/` rather than
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
build plan listed it under `refusedSources` with `reason: given_in_persisted_query` and the
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
