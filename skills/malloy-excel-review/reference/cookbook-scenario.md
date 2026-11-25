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
| Monte Carlo (`RAND`) | C for the model, X for the cells | [SC7](cookbook-scenario-2.md#sc7) seeded draws in DuckDB |
| What-If Data Tables, discrete Goal Seek and Solver | C | [SC8](cookbook-scenario-2.md#sc8) a precomputed scenario grid |
| Continuous Goal Seek and Solver | X | [SC9](cookbook-scenario-2.md#sc9) stays in Excel, or Pyodide in a data app |
| VBA-driven logic | X | [Stays in Excel](cookbook-scenario-2.md#stays-in-excel) |
| Scenario Manager, form controls, data validation lists | T | [SC10](cookbook-scenario-2.md#sc10) a scenario table and a `given:` |

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

Validation, a dropdown over a fixed list, or a slider: see [SC10](cookbook-scenario-2.md#sc10).

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
reads no givens can be persisted ([Persistence](cookbook-scenario-2.md#persistence)); one that does cannot.

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

SC7 onward (Monte Carlo through what the recipes were checked against) is in `reference/cookbook-scenario-2.md`.
