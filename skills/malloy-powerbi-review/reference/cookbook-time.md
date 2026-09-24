<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Cookbook: Time Intelligence (Step 5)

> Worked transpiles for DAX time intelligence. Two of the six recipes are
> **stopgaps** - the Malloy works, but it is a workaround and the file says so.

DAX time intelligence rests on a table *marked as the date table*, with a
contiguous date column, which Malloy does not have as a concept. That does not
make the functions untranslatable - it makes them a rewrite of the intent.

**Every recipe's Malloy was executed** against the AdventureWorks package on Malloy
0.0.434. DAX-side numbers are `semantics-cited`, never measured.

**Before anything else - the type gotcha.** A Power BI date table exported to
parquet carries `TIMESTAMP_NS`, which Malloy 0.0.434 types as `sql native` and
refuses to truncate (`Cannot do time truncation on type 'sql native'`). One cast at
the source fixes every recipe below:

```malloy
source: s is sales extend {
  dimension: order_month is order_date.full_date::timestamp.month
}
```

Watch the reserved words too: `date`, `month`, `months` and `days` all need
backticks as field names, and a Power BI date table usually has at least one of them.

---

## T1 - Year to date / month to date

**The DAX**

```dax
Sales YTD = TOTALYTD ( [Total Sales], 'Date'[Date], "6/30" )
```

**What it means** The running total from the start of the fiscal year to this row.
The fourth argument is what makes it fiscal: `year_end_date` defaults to `"12/31"`,
so the bare three-argument form is a *calendar* YTD and partitions by calendar year
instead. Read it before choosing the `partition_by` column.

**The Malloy**

```malloy
run: s -> {
  group_by:
    fy is order_date.fiscal_year
    ym is order_month
  aggregate: monthly is total_sales
  calculate: ytd is sum_cumulative(total_sales) {
    partition_by: fy
    order_by: ym
  }
}
```

**Verified:** `executed`

| fy | ym | monthly | ytd |
|---|---|---:|---:|
| FY2018 | 2017-09 | 2,523,947.55 | 6,005,207.32 |
| FY2018 | 2017-10 | 561,681.48 | 6,566,888.80 |
| FY2018 | 2017-11 | 4,764,920.16 | 11,331,808.96 |
| FY2018 | 2017-12 | 596,746.56 | 11,928,555.52 |
| FY2018 | 2018-01 | 1,327,674.63 | 13,256,230.15 |

**What it costs** Nothing structural. This is a correct YTD, and `partition_by: fy`
is what makes it reset at the fiscal year boundary rather than running forever.

**Why a running sum and not `lag()`.** `sum_cumulative` accumulates over the rows
the window actually contains, so a month with no sales simply is not there and the
next month's running total is still right. A positional `lag()` would be wrong on
the same data - see T4. MTD is the same recipe with `partition_by: fy, ym`, but only once the `group_by`
goes to day grain - on the month-grain query above each partition holds one row and
the running total just repeats `monthly`.

`sum_cumulative`, `partition_by` and `order_by` all work with **no experiment
flag**, despite the upstream docs listing the latter two as experimental.

---

## T2 - Same period last year

**The DAX**

```dax
Sales SPLY = CALCULATE ( [Total Sales], SAMEPERIODLASTYEAR ( 'Date'[Date] ) )
```

**What it means** The same span of days, shifted back one year.

**The Malloy** - an explicit range per side, not a shift:

```malloy
run: s -> {
  group_by: category is products.category
  aggregate:
    fy2019 is total_sales { where: order_date.fiscal_year = 'FY2019' }
    fy2018 is total_sales { where: order_date.fiscal_year = 'FY2018' }
    yoy_pct is
      (total_sales { where: order_date.fiscal_year = 'FY2019' }
       - total_sales { where: order_date.fiscal_year = 'FY2018' })
      / total_sales { where: order_date.fiscal_year = 'FY2018' }
}
```

**Verified:** `executed`

| category | fy2019 | fy2018 | yoy_pct |
|---|---:|---:|---:|
| Accessories | 138,901.55 | 36,814.85 | 2.7730 |
| Bikes | 28,544,881.62 | 22,590,983.47 | 0.2636 |
| Clothing | 757,224.19 | 66,327.53 | 10.4164 |
| Components | 4,629,101.14 | 1,166,765.32 | 2.9675 |

**What it costs** The comparison periods are **named in the query**, so the measure
is not reusable across periods the way the DAX is. Where the period must follow the
user's selection, carry both ends as givens (`$PERIOD`, `$PRIOR_PERIOD`) rather
than hard-coding them.

This is the real time-intelligence gap, and it is narrower than "no time
intelligence": what is missing is a *measure-level shift relative to the selected
period*. Ranges you can name, Malloy expresses fine.

**Do not reach for `lag()` here.** It reads the previous row, not the previous year.
On a query grouped by category it would compare Bikes to Accessories.

---

## T3 - Period-over-period growth

**The DAX**

```dax
Sales MoM % =
VAR Prior = CALCULATE ( [Total Sales], DATEADD ( 'Date'[Date], -1, MONTH ) )
RETURN DIVIDE ( [Total Sales] - Prior, Prior )
```

**The Malloy** - `lag()`, **only where every period is present**:

```malloy
run: s -> {
  group_by: ym is order_month
  aggregate: monthly is total_sales
  calculate:
    prev_row is lag(total_sales)
    growth_pct is (total_sales - lag(total_sales)) / lag(total_sales)
  order_by: ym
}
```

**`order_by: ym` is load-bearing.** `lag()` follows the query's ordering, and the
default for a time dimension is *descending* - drop it and every row is compared to
the month *after* it. No error, and the numbers still look like growth rates.

**Verified:** `executed` - on the full fact table, where every month has sales:

| ym | monthly | prev_row | growth_pct |
|---|---:|---:|---:|
| 2017-08 | 2,057,902.45 | 1,423,357.32 | 0.4458 |
| 2017-09 | 2,523,947.55 | 2,057,902.45 | 0.2265 |
| 2017-10 | 561,681.48 | 2,523,947.55 | -0.7775 |
| 2017-11 | 4,764,920.16 | 561,681.48 | 7.4833 |

**What it costs** Correctness the moment a period is missing. `lag()` is positional:
it reads the previous *row*, and a row only exists where there were sales. Where the
series can be sparse, use T4 or name the ranges explicitly as in T2.

---

## T4 - Date spine / densification (STOPGAP)

**Nothing in Malloy densifies sparse periods.** There is no generated date range and
no equivalent of a marked date table that supplies the missing rows. The escape is a
raw SQL source, as in S6 - the dialect has the generator, the language does not. This recipe is a workaround, and its existence is the evidence behind
the upstream ask.

**The failure, executed.** `Socks` sell in only 22 of the 36 months in the data:

| ym | monthly | prev_row | growth_pct | |
|---|---:|---:|---:|---|
| 2017-08 | 785.39 | 245.10 | 2.2044 | |
| 2017-09 | 1,003.31 | 785.39 | 0.2775 | |
| 2017-11 | 1,400.09 | **1,003.31** | **0.3955** | ← October is missing; this is a **two-month** change reported as one |
| 2018-01 | 256.50 | **1,400.09** | **-0.8168** | ← December missing too |

No error. The chart is labelled "month over month" and two of its four points are
not.

**The stopgap** - generate a calendar and join the aggregate to it:

```malloy
source: calendar is duckdb.sql("""
  SELECT CAST(d AS TIMESTAMP) AS month_start
  FROM generate_series(DATE '2017-07-01', DATE '2020-06-01', INTERVAL 1 MONTH) AS t(d)
""")

// The spine cannot join straight onto `s`: a join's ON clause cannot reach through
// `s`'s own join to the date table. Join to a pre-aggregated stage instead.
source: monthly_socks is s -> {
  where: products.subcategory = 'Socks'
  group_by: ym is order_month
  aggregate: monthly is total_sales
}

source: spine is calendar extend {
  join_one: monthly_socks on month_start.month = monthly_socks.ym
}

run: spine -> {
  group_by: ym is month_start.month
  aggregate: monthly is monthly_socks.monthly.sum() ?? 0
  calculate: growth_pct is
    (monthly_socks.monthly.sum() ?? 0) / lag(monthly_socks.monthly.sum() ?? 0) - 1
  order_by: ym
}
```

**Verified:** `executed`

| ym | monthly | growth_pct |
|---|---:|---:|
| 2017-09 | 1,003.31 | 0.2775 |
| 2017-10 | **0** | -1 |
| 2017-11 | 1,400.09 | null |
| 2017-12 | **0** | -1 |
| 2018-01 | 256.50 | null |

**What it costs** Three named objects and raw SQL where DAX needed a marked date
table and one function. The generated range is hard-coded, so it goes stale. The
join to a pre-aggregated stage is forced - joining the spine directly onto the fact
fails with `Referenced table order_date_0 not found`, because a join's ON clause
cannot reach through another join. And the honest result for a month following a
gap is `null`, not a growth rate, which is correct but is not what the DAX returned.

---

## T5 - Semi-additive closing balance (STOPGAP)

**The DAX**

```dax
Closing Balance = CLOSINGBALANCEMONTH ( [Balance], 'Date'[Date] )
Last Known    = CALCULATE ( [Balance], LASTNONBLANK ( 'Date'[Date], [Balance] ) )
```

**What it means** Not a sum over time - the value *as at* the last date in context.
Stock levels, headcount, account balances.

**The two DAX forms are not the same function, and the Malloy below is the second
one.** `CLOSINGBALANCEMONTH` evaluates at the *calendar* last day of the month and
returns BLANK when that day has no rows; `row_number() ... order_by desc` picks the
last day that *exists*, which is `LASTNONBLANK`. Where the balance is carried
forward from a prior day the two agree; where it is not, they differ silently. To
get the calendar reading, join the T4 spine first so the missing day is a row.

**Why it is a stopgap.** `last_value` cannot be a measure:

```malloy
measure: closing_balance is last_value(sales_amount)
```

**Verified:** `executed` - fails with

```
Parameter 1 ('value') of last_value must be literal, constant or output, but received input
Cannot use an analytic field in a measure declaration
```

`first_value` and `last_value` return `calculation`, so they are window functions
and only legal in `calculate:`. And the obvious workaround does not work either:

```malloy
measure: closing is max_by!(sales_amount, order_date_key)
```

**Verified:** `executed` - fails with `Cannot use a scalar field in a measure
declaration`. A raw `fn!()` inherits its arguments' expression type, so over scalar
columns it is scalar. (`max_by`/`min_by` *are* real aggregates where they exist -
but only in the standardsql and trino dialects, not duckdb or postgres.)

**The stopgap** - three stages, because the last value has to be found positionally:

```malloy
run: s -> {
  group_by:
    category is products.category
    ym is order_date.full_date::timestamp.month
  aggregate: monthly is total_sales
} -> {
  group_by: category, ym, monthly
  calculate: rn is row_number() { partition_by: category, order_by: ym desc }
} -> {
  where: rn = 1
  select: category, ym, closing is monthly
}
```

**Verified:** `executed`

| category | ym | closing |
|---|---|---:|
| Accessories | 2020-06 | 71,339.87 |
| Bikes | 2020-06 | 2,716,128.86 |
| Clothing | 2020-06 | 99,919.61 |
| Components | 2020-06 | 577,852.35 |

That is one row per category at the end of the whole data set - a grand-total
reading. A DAX visual grouped by month wants a *series*, which needs day grain and a
two-column partition:

```malloy
run: s -> {
  group_by:
    category is products.category
    ym is order_date.full_date::timestamp.month
    `day` is order_date.full_date::timestamp.day
  aggregate: daily is total_sales
} -> {
  group_by: category, ym, `day`, daily
  calculate: rn is row_number() { partition_by: category, ym, order_by: `day` desc }
} -> {
  where: rn = 1
  select: category, ym, closing is daily
  order_by: category, ym
}
```

**Verified:** `executed`

| category | ym | closing |
|---|---|---:|
| Accessories | 2017-07 | 423.92 |
| Accessories | 2017-08 | 181.68 |
| Accessories | 2017-09 | 363.36 |
| Accessories | 2017-11 | 545.04 |
| Accessories | 2018-01 | 242.24 |

October and December are **absent rather than blank** - the `LASTNONBLANK` reading
again. `CLOSINGBALANCEMONTH` would return a row with BLANK.

**What it costs** Three stages instead of one measure, and the result is a *query*,
not something the model can publish and every downstream query reuse. Any dashboard
tile wanting a closing balance has to carry the whole pipeline. That is the gap, and
it is the first of the three upstream asks.

---

## T6 - `CALENDAR()` / `CALENDARAUTO()` calculated tables

**The DAX**

```dax
Date = CALENDAR ( DATE ( 2017, 7, 1 ), DATE ( 2020, 6, 30 ) )
```

**What it means** The model's date table, built in DAX rather than sourced. Most
models build the marked date table this way, so it comes up early - usually before
any measure.

**The Malloy** - generate a real source:

```malloy
source: calendar is duckdb.sql("""
  SELECT
    CAST(d AS TIMESTAMP)                                     AS day,
    CAST(DATE_TRUNC('month', d) AS TIMESTAMP)                AS month_start,
    EXTRACT(YEAR  FROM d)                                    AS cal_year,
    EXTRACT(MONTH FROM d)                                    AS cal_month,
    'FY' || CAST(EXTRACT(YEAR FROM d) +
      CASE WHEN EXTRACT(MONTH FROM d) >= 7 THEN 1 ELSE 0 END AS VARCHAR) AS fiscal_year
  FROM generate_series(DATE '2017-07-01', DATE '2020-06-30', INTERVAL 1 DAY) AS t(d)
""")
```

**Verified:** `executed`

| fiscal_year | day_count |
|---|---:|
| FY2018 | 365 |
| FY2019 | 365 |
| FY2020 | 366 |

**What it costs** The range is fixed at compile time where `CALENDARAUTO()` derived
it from the data. Prefer a real date dimension in the warehouse where one exists -
generating it in the model is the fallback, and it is the same object T4 needs, so
build it once.

Every `LocalDateTable_<guid>` and `DateTableTemplate_<guid>` in the source model is
an artifact of a setting, not a modeling decision. Drop all of them and propose
this one table - see `cookbook-structure.md#s7`.
