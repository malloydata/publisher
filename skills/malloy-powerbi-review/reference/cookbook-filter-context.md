<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Cookbook: Filter Context (Step 5)

> Worked transpiles for the DAX that touches filter context. This is where a
> faithful-looking translation returns a different number with no error, so every
> recipe states what it costs as well as what it produces.

Read `translate-measures.md` for how a measure is routed here. Read `_concepts.md`
for the syntax tables. This file is the answers.

**Every recipe's Malloy below was executed** against the AdventureWorks package
(`sales.parquet`, 121,253 order lines, grand total **$109,809,274.20**) on Malloy
0.0.434, and the numbers shown are what came back. The **DAX** column of a
divergence table is *not* executed - there is no open-source DAX engine - and is
marked `semantics-cited` with the rule it follows. Never present a cited number as
a measured one.

---

## FC1 - Filtered aggregate that must not widen

**The DAX**

```dax
Bike Sales = CALCULATE ( [Total Sales], Product[Category] = "Bikes" )
```

**What it means** Total sales, restricted to bikes - and in DAX that restriction
*replaces* any filter already on `Product[Category]`.

**The Malloy**

```malloy
measure: bike_sales is sum(sales_amount) { where: products.category = 'Bikes' }
```

Write it against the leaf aggregate, not against `total_sales`: a filter refining
an already-derived measure is refused by preaggregation. **Every snippet below is a fragment.** The `measure:` and
`aggregate:` lines belong inside `sales extend { ... }`, and `run: sales -> ...`
is shorthand for running against that extended source - pasted alone they do not
compile, which is the convention and not a defect.

**Verified:** `executed`

| Query context | `total_sales` | `bike_sales` (Malloy, executed) | DAX (`semantics-cited`) | Agree? |
|---|---:|---:|---:|---|
| no filter | 109,809,274.20 | 94,620,526.21 | 94,620,526.21 | yes |
| `where: products.category = 'Accessories'` | 1,272,057.89 | **0** | **94,620,526.21** | **no** |
| `where: order_date.fiscal_year = 'FY2018'` | 23,860,891.17 | 22,590,983.47 | 22,590,983.47 | yes |

**What it costs** Nothing, *if* nothing ever filters `category`. The middle row is
the whole problem: Malloy intersects to zero, DAX overwrites and answers with the
full bike total. Both are silent. The measure is only safe on a column no report
filters, and the model file cannot tell you that - `report.json` can, and this
skill does not read it.

**The tell that it is safe.** `CALCULATE([Total], KEEPFILTERS(Product[Category] =
"Bikes"))` asks DAX to intersect, which is what Malloy does natively. Expect the
tell to be absent: `PBIASEngine` uses `KEEPFILTERS` in **0** of its 79 `CALCULATE`
calls. Divergence is the default case, not the exception.

---

## FC2 - Percent of total

**The DAX**

```dax
Pct of Total = DIVIDE ( [Total Sales], CALCULATE ( [Total Sales], ALL ( Sales ) ) )
```

**What it means** This row's share of everything.

**The Malloy**

```malloy
run: sales -> {
  group_by: category is products.category
  aggregate:
    total is total_sales
    pct_of_all is total_sales / all(total_sales)
}
```

**Verified:** `executed`

Unfiltered, the two agree. Add `where: order_date.fiscal_year = 'FY2018'` and they
do not:

| category | total (FY2018) | Malloy `all()` (executed) | DAX `ALL(Sales)` (`semantics-cited`) |
|---|---:|---:|---:|
| Accessories | 36,814.85 | 0.001543 | 0.000335 |
| Bikes | 22,590,983.47 | 0.946779 | 0.205729 |
| Clothing | 66,327.53 | 0.002780 | 0.000604 |
| Components | 1,166,765.32 | 0.048899 | 0.010625 |
| **column sums to** | | **1** (before rounding) | **0.217294** |

**What it costs** The denominator. `all()` removes *grouping* and keeps the query's
`where:`, so the column sums to 1. DAX `ALL` removes *filters too*, so its
denominator is the all-years 109,809,274.20 and the column sums to the year's share
of all time. Both are defensible readings of "percent of total"; they are not the
same number, and the divergence is invisible until something is filtered.

If the DAX reading is the one the business wants, it is spellable - but by the move
in `cookbook-filter-context.md#fc8`, putting the year filter in the *numerator*
instead of the query:

```malloy
run: sales -> {
  group_by: category is products.category
  aggregate: pct_of_all_time is
    total_sales { where: order_date.fiscal_year = 'FY2018' } / all(total_sales)
}
```

**Verified:** `executed` - 0.000335 / 0.205729 / 0.000604 / 0.010625, the DAX column
above exactly. The query stays unfiltered, so `all()` spans all time.

---

## FC3 - Percent of visible total

**The DAX**

```dax
Pct of Visible = DIVIDE ( [Total Sales], CALCULATE ( [Total Sales], ALLSELECTED () ) )
```

**What it means** This row's share of what the user can currently see - the slicers
still apply, only the visual's own grouping is removed.

**The Malloy**

```malloy
aggregate: pct_of_visible is total_sales / all(total_sales)
```

**Verified:** `executed` - identical Malloy to FC2, identical numbers (the FY2018
column above, summing to 1.000000).

**What it costs** Nothing. **This is an exact match**, and it is the single most
valuable line in this file. `all()` keeps the query's filters and drops the
grouping, which is `ALLSELECTED` semantics precisely. Earlier revisions of this
skill called `ALLSELECTED` "no equivalent" and gave `ALL` the clean mapping; that
was backwards, and it is why `ALLSELECTED` measures were being written off.

The practical consequence for sizing a migration: `ALLSELECTED` appears in **25 of
`PBIASEngine`'s 126 measures**, and none of them is untranslatable.

**Read the argument before reaching for `all()`.** Bare `ALLSELECTED()` is the exact
match above. `ALLSELECTED(Table[Column])` restores only *that* column's filter, so in
a category > subcategory matrix `CALCULATE([X], ALLSELECTED(Product[Subcategory]))`
is the **category** total, not the grand total - that is `exclude(x, subcategory)`,
FC5's shape with FC3's filter semantics. Both spellings occur in `PBIASEngine`:

| DAX argument | Malloy | Reading |
|---|---|---|
| `ALLSELECTED()` | `all(expr)` | everything the user can see |
| `ALLSELECTED(Table)` | `all(expr)` | that table's grouping removed |
| `ALLSELECTED(Table[Column])` | `exclude(expr, column)` | one level up, not the top |

**The one real difference** is what "visible" means. In Power BI the scope is the
visual's, set by the report. In Malloy the scope is the query's `where:`. Those
coincide when the query is generated from a dashboard's own filter controls - but
only if the host binds a control by substituting a value **inside** the measure. A
host that binds by appending `+ { where: ... }` to the tile puts the filter at query
level, where `all()` cannot escape it, and this recipe silently stops matching. If
you own the dashboard layer, check which of the two it does before promising
`ALLSELECTED` parity.

---

## FC4 - Percent within a group

**The DAX**

```dax
Pct within Category =
DIVIDE ( [Total Sales], CALCULATE ( [Total Sales], ALLEXCEPT ( Product, Product[Category] ) ) )
```

**What it means** This subcategory's share of its own category.

**The Malloy**

```malloy
run: sales -> {
  group_by:
    category is products.category
    subcategory is products.subcategory
  aggregate:
    total is total_sales
    pct_within_category is total_sales / all(total_sales, category)
}
```

**Verified:** `executed`

| category | subcategory | total | pct_within_category |
|---|---|---:|---:|
| Accessories | Bike Racks | 237,096.16 | 0.186388 |
| Accessories | Bike Stands | 39,591.00 | 0.031124 |
| Accessories | Bottles and Cages | 64,274.79 | 0.050528 |

237,096.16 / 1,272,057.89 = 0.186388. Correct.

**What it costs** The same divergence as FC2, plus the chance of writing
`exclude()` here by mistake.

`ALLEXCEPT(Product, Product[Category])` removes **every** other filter on Product,
a slicer on `Product[Color]` included; `all(x, category)` keeps it. The two agree
only while nothing else on that table is filtered - which is why this recipe is
routed divergent, not direct.

And `ALLEXCEPT` **keeps** the columns it names and removes the rest, so it is
`all(x, keep)`. `exclude(x, c)` is the `ALL(T[c])` analog and does the opposite.
The two read alike and both compile.

---

## FC5 - Remove one dimension

**The DAX**

```dax
Category Total = CALCULATE ( [Total Sales], REMOVEFILTERS ( Product[Subcategory] ) )
```

**What it means** Keep every filter except the one on subcategory.

**The Malloy**

```malloy
aggregate: category_total is exclude(total_sales, subcategory)
```

**Verified:** `executed` - every Accessories row returns 1,272,057.89, which is the
Accessories total and matches FC1's Accessories row exactly.

**What it costs** The same `ALL`-versus-`all()` divergence as FC2, one dimension
narrower: DAX drops the *filter* on subcategory, Malloy drops the *grouping*. They
agree when the grouping is the only thing filtering that column - which is the
common case for this shape, and why it is less dangerous than FC2.

**A structural constraint:** a dimension named in `all()` or `exclude()` must be an
output field of the query. So `exclude(x, subcategory)` written as a source-level
`measure:` only compiles in queries that actually `group_by: subcategory`.

---

## FC6 - Dynamic top-N driven by a slicer

**The DAX** - real, from `PBIASEngine`'s `Top N Selector` table:

```dax
Rank_By_Duration =
IF (
    ISINSCOPE ( Operation[EventText] ),
    INT (
        RANKX (
            CALCULATETABLE (
                GROUPBY ( Operation, Operation[EventText], Operation[ReportId], … ),
                ALLSELECTED ( Operation[EventText], Operation[ReportId], … )
            ),
            CALCULATE ( PERCENTILE.INC ( Operation[Duration (ms)], .5 ) )
        ) <= MAX ( 'Top N Selector'[Value] )
    )
)
```

**What it means** Rank the visible groups by a measure, and return 1 for the top N,
where N comes from a disconnected slicer table the user drives. It is used as a
visual-level filter, not displayed.

**The Malloy** - two moves: the rank becomes a `calculate:`, and the slicer's N
becomes a `given:`.

```malloy
##! experimental.givens
given: TOP_N :: number is 3

run: sales -> {
  group_by: subcategory is products.subcategory
  aggregate: total is total_sales
  calculate: sales_rank is rank() { order_by: total_sales desc }
} -> {
  where: sales_rank <= $TOP_N
  select: subcategory, total, sales_rank
  order_by: sales_rank
}
```

**Verified:** `executed`

| subcategory | total | sales_rank |
|---|---:|---:|
| Road Bikes | 43,878,790.997 | 1 |
| Mountain Bikes | 36,445,443.941 | 2 |
| Touring Bikes | 14,296,291.270 | 3 |

**What it costs** The `IF(ISINSCOPE(...))` guard has no analog and does not need
one: it exists because a DAX measure cannot know what the visual grouped by, and a
Malloy query always does. Drop it.

The `<= N` comparison must move to a **second stage** - a `calculate:` field cannot
be filtered or ordered on in the stage that defines it.

**This recipe is the one that changes a migration estimate.** Every measure this
skill previously called untranslatable in `PBIASEngine` was `ALLSELECTED`-triggered,
and 8 of them were this exact shape. Seven route here, one to FC7. None of them
exercises a real gap.

---

## FC7 - Rank within the visible set

**The DAX**

```dax
Sales Rank = RANKX ( ALLSELECTED ( Product[Subcategory] ), [Total Sales] )
```

**The Malloy**

```malloy
run: sales -> {
  group_by: subcategory is products.subcategory
  aggregate: total is total_sales
  calculate: sales_rank is rank() { order_by: total_sales desc }
  order_by: subcategory          // deliberately NOT the rank order
}
```

**Verified:** `executed`

| subcategory | total | sales_rank |
|---|---:|---:|
| Bib-Shorts | 166,739.71 | 18 |
| Bike Racks | 237,096.16 | 14 |
| Bike Stands | 39,591.00 | 30 |
| Bottles and Cages | 64,274.79 | 24 |

The rows come back alphabetically while the ranks are by sales. **`rank()` orders by
whatever you give it, independently of the query's own ordering.** A `TODO` in the
compiler source asks whether anyone would ever want that; they would, it already
works, and an earlier revision of this skill wrongly published the opposite.

Ranking within a group adds `partition_by:`:

```malloy
calculate: rank_in_category is rank() {
  partition_by: category
  order_by: total_sales desc
}
```

| category | subcategory | total | rank_in_category |
|---|---|---:|---:|
| Bikes | Road Bikes | 43,878,790.997 | 1 |
| Bikes | Mountain Bikes | 36,445,443.941 | 2 |
| Clothing | Jerseys | 752,259.388 | 1 |
| Clothing | Shorts | 413,522.527 | 2 |

**What it costs** A stage. Ranking is a query-level calculation, not a measure, so
it cannot be published on the source and reused - each query that wants it declares
it. And `order_by:` on the `calculate:` field itself needs a second stage.

`partition_by` and `order_by` inside `calculate:` **need no experiment flag**: every
query on this page ran without one. The upstream docs still list them under
`##! experimental { function_order_by partition_by aggregate_limit }`; that label is
stale, and believing it generates false gap reports.

---

## FC8 - Escaping a query-level filter

**There is no escape, and that is correct.** This is the recipe for the situation
where a translated measure needs to *widen* past the filter the query already set.

**The wrong shape**, carried over from the DAX habit of putting the modifier in the
visual and letting the measure overwrite it:

```malloy
run: sales -> {
  where: products.category = 'Accessories'   // the visual's filter
  aggregate: bikes is bike_sales             // wants to widen back to Bikes
}
```

**Verified:** `executed` - returns `0`. There is no `all()`, `exclude()` or
refinement that gets Bikes back, because a query `where:` is not a filter the
measure is allowed to see past.

**The right shape** - the modifier moves from the query into the measure:

```malloy
run: sales -> {
  aggregate:
    accessories is sum(sales_amount) { where: products.category = 'Accessories' }
    bikes      is sum(sales_amount) { where: products.category = 'Bikes' }
}
```

**Verified:** `executed` - `accessories` 1,272,057.89, `bikes` 94,620,526.21.

**What it costs** A change in how the author thinks, not a capability. The DAX model
is "the query sets context, the measure overrides it"; the Malloy model is "each
measure carries its own context and they compose". Both express the same report. Do
not raise this as a language gap - a maintainer will answer "use a filtered
aggregate", and they will be right.

The practical migration rule: when a `CALCULATE` measure and a page-level filter
touch the same column, the page-level filter is the thing that has to move.
