<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Cookbook: Model Structure (Step 5)

> Worked transpiles for the Power BI constructs that are model shape rather than
> measure logic - relationships, parameter tables, calculation groups, hierarchies.
> Two of the seven recipes are **stopgaps** and say so.

Read these **before** classifying measures. Three of them (S1, S2, S3) are model
properties that change what a measure means without appearing anywhere in its DAX,
which is why `translate-measures.md` reads `relationships.tmdl` at step 0.

**Every recipe's Malloy was executed** against the AdventureWorks package on Malloy
0.0.434. DAX-side numbers are `semantics-cited`.

---

## S1 - Role-playing dimension / `USERELATIONSHIP`

**The DAX**

```dax
Sales by Due Date =
CALCULATE ( SUM ( Sales[Sales Amount] ),
            USERELATIONSHIP ( Sales[DueDateKey], 'Date'[DateKey] ) )
```

**What it means** One date table serves three roles - order, due, ship. Power BI
allows only one *active* relationship between two tables, so the other two are
declared inactive and switched on inside a measure.

**The Malloy** - name all three join paths; there is nothing to switch:

```malloy
source: sales is duckdb.table('data/sales.parquet') extend {
  join_one: order_date is dates with order_date_key
  join_one: due_date   is dates with due_date_key
  join_one: ship_date  is dates with ship_date_key
}
```

**Verified:** `executed`

| fiscal year | by `order_date` | by `due_date` |
|---|---:|---:|
| FY2018 | 23,860,891.17 | 23,348,493.12 |
| FY2019 | 34,070,108.50 | 33,636,168.52 |
| FY2020 | 51,878,274.54 | 52,824,612.56 |
| **total** | **109,809,274.20** | **109,809,274.20** |

The grand totals reconcile and the distributions differ - which is exactly the
property a `USERELATIONSHIP` measure exists to produce, and the parity check to run.

**What it costs** Nothing, and it is usually an improvement: the role is in the
field name at every call site instead of hidden inside one measure. But it is not a
transcription - the *measure* disappears and becomes a *join path*, so every
downstream reference changes shape. Budget for that, and flag every measure using
the inactive relationship, not just the relationship.

---

## S2 - Many-to-many via a bridge table

**The Power BI** A relationship with `fromCardinality: many` and
`toCardinality: many`, resolved by an invisible internal table.

**The Malloy** Malloy has no M:M declaration. Model the bridge the grain actually
has:

```malloy
source: product_tag is duckdb.sql("""
  SELECT 477 AS product_key, 'clearance' AS tag UNION ALL
  SELECT 477, 'seasonal' UNION ALL
  SELECT 480, 'clearance'
""")

source: tagged_sales is sales extend {
  join_many: product_tag on product_key = product_tag.product_key
}
```

**Verified:** `executed`

| tag | lines | total |
|---|---:|---:|
| clearance | 8,042 | 36,886.76 |
| seasonal | 4,688 | 28,654.16 |
| **sum of the rows** | **12,730** | **65,540.92** |
| **true distinct** | **8,042** | **36,886.76** |

**What it costs** The fan-out, and it must be named out loud. Product 477 carries
two tags, so its sales appear under both and the column does not add up to the
total. Power BI's M:M does the same thing - this is not a Malloy defect - but a
Power BI author is used to the total row silently de-duplicating, and here it will
not. Where the total matters, aggregate before joining the bridge.

`skill:malloy-model` `reference/bridge-tables.md` has the general pattern.

---

## S3 - Bidirectional cross-filtering

**The Power BI** `crossFilteringBehavior: bothDirections` on a relationship, or
`CROSSFILTER ( ..., BOTH )` inside a single measure.

**There is no Malloy equivalent**, and there is nothing to add: Malloy's grammar has
no join-direction setting at all. But the interesting half is the opposite of what
you would expect.

**The divergence is on the SINGLE-direction side.** A filter on a *dimension* column
propagates to the fact in both products, so those agree. A filter on a *fact* column
does not travel back up to the dimension in DAX - but in Malloy it always does.

**Verified:** `executed` - filter `order_quantity > 30` (31 order lines):

| | Malloy (executed) | DAX, single-direction (`semantics-cited`) |
|---|---:|---:|
| `count(products.subcategory)` | **5** | **37** |

DAX answers 37 because `DISTINCTCOUNT(Product[Subcategory])` still sees the whole
unfiltered dimension. Malloy answers 5 because the join only reaches the
subcategories those 31 lines actually touch. **Turning the Power BI relationship
bidirectional makes DAX answer 5** - so Malloy's join behaves like Power BI's
*bidirectional* setting for this purpose, and it is the default-direction models
that need re-checking, not the bidirectional ones.

**What it costs** Every `DISTINCTCOUNT` or `COUNTROWS` over a dimension under a
fact-side filter is a parity risk, in both directions. Check them individually.

**Why this is the highest-blast-radius flag in the file.** Bidirectional is a
model-level switch, invisible in every measure's DAX, and it lands on the fact
table. Of the two Microsoft-published models sampled:

| model | measures | bidirectional relationships | measures affected |
|---|---:|---:|---:|
| `PBIASEngine` | 126 | 0 | 0 |
| `FabricASEngineAnalytics` | 117 | 2, both onto `ExecutionMetrics` | **108** |

Same publisher, same domain, and one of them has 92% of its measures touching a
concept Malloy does not have. Reading `relationships.tmdl` is not optional.

---

## S4 - What-if parameter / disconnected slicer table

**The DAX**

```dax
Adjusted Sales = [Total Sales] * ( 1 + SELECTEDVALUE ( 'Param'[Value], 0 ) )
```

**What it means** A table with no relationships, generated by `GENERATESERIES`,
whose selected value the user drives with a slicer. `PBIASEngine`'s
`'Top N Selector'` is one of these.

**The Malloy** - a given. This is a direct analog, not a workaround:

```malloy
##! experimental.givens
given: UPLIFT :: number is 0.10

source: s is sales extend {
  measure: adjusted_sales is total_sales * (1 + $UPLIFT)
}
```

**Verified:** `executed`

| category | total | adjusted (`$UPLIFT` = 0.10) |
|---|---:|---:|
| Accessories | 1,272,057.89 | 1,399,263.68 |
| Bikes | 94,620,526.21 | 104,082,578.83 |
| Clothing | 2,117,613.45 | 2,329,374.79 |
| Components | 11,799,076.66 | 12,978,984.32 |

**What it costs** Nothing in the model; the work moves to the host, which has to
supply the value per query. `SELECTEDVALUE(Param[Value], 0)`'s fallback maps to the
given's own `is 0` default, so the shape survives intact. Where a dashboard binds
the control, check that it substitutes the given rather than appending a query-level
`where:` - see `cookbook-filter-context.md#fc3`.

**Do not route these to "skip".** A disconnected parameter table reads like
report-layer furniture and is not - it carries a real business input.

**Field parameters are different.** A field parameter swaps *which measure or
column* a visual shows. A given is a value, not a field reference, so this one has
no mapping today - it is a question for whatever renders the dashboard, not for the
model. Flag it rather than faking it with one tile per measure.

---

## S5 - Calculation groups (STOPGAP)

**The Power BI** A calculation group rewrites measures at query time: define
`Time Calc` with items `Current`, `YTD`, `PY`, and every measure in the model gains
three variants without being rewritten. It is the highest-leverage object in DAX and
it has no equivalent.

**The Malloy** - view composition with `+`:

```malloy
source: s is sales extend {
  view: by_category is {
    group_by: category is products.category
    aggregate: total is total_sales
  }
}

run: s -> by_category + { where: order_date.fiscal_year = 'FY2018' }
```

**Verified:** `executed` - Bikes 22,590,983.47, Components 1,166,765.32,
Clothing 66,327.53, Accessories 36,814.85.

A named boolean dimension makes the lens readable:

```malloy
dimension: is_fy2018 is order_date.fiscal_year = 'FY2018'
run: s -> by_category + { where: is_fy2018 }
```

**Verified:** `executed` - identical numbers.

**What it costs, and the limit that keeps it a stopgap.** A view cannot be
filter-only, so the lens cannot be named as a view and reused:

```malloy
view: fy2018 is { where: order_date.fiscal_year = 'FY2018' }
```

**Verified:** `executed` - fails with
`Can't determine view type (group_by / aggregate / nest, project, index)`.

So the composition is one-sided: the *base* is reusable, the *lens* is not. Every
call site respells the filter, or reaches for a boolean dimension that carries the
condition but is not itself composable as a view. Where Power BI multiplied N
measures by M calculation items for free, this is N × M spellings.

That asymmetry - not "no calculation groups" - is the second upstream ask. Frame it
as view-level composability, and acknowledge the two things that already work, or
the answer is "use a refinement".

---

## S6 - Parent-child `PATH` hierarchy (STOPGAP)

**The DAX** `PATH`, `PATHITEM`, `PATHCONTAINS`, `PATHLENGTH` over a
self-referencing `manager_id` - an org chart, a chart of accounts, a BOM.

**Malloy has no recursive CTE in any dialect**, so an unbounded hierarchy cannot be
walked. Two options, both with a cost.

**Preferred: flatten upstream.** Materialize the path in the warehouse, where a
recursive CTE is available, and model the flat result.

**The stopgap: a bounded self-join per level.**

```malloy
source: org is employee extend {
  join_one: l1 is employee on manager_id = l1.id
  join_one: l2 is employee on l1.manager_id = l2.id
}

run: org -> {
  select: name, manager is l1.name, grandmanager is l2.name
}
```

**Verified:** `executed`

| name | manager | grandmanager |
|---|---|---|
| Ann | null | null |
| Bob | Ann | null |
| Cat | Bob | Ann |
| Dan | Cat | Bob |

**What it costs** One join per level, written by hand, and a depth you have to
guess. Depth 4 needs `l3`; a hierarchy that grows a level needs a model change and
a redeploy. `PATHCONTAINS` ("is anyone in my management chain X?") has no bounded
form at all beyond the depth you declared.

This is the third upstream ask, and it is the one with the clearest single answer:
recursive CTE support.

---

## S7 - Auto date tables

**The Power BI** A hidden `LocalDateTable_<guid>` per date column, plus one
`DateTableTemplate_<guid>`. AdventureWorks has four.

**The Malloy** Drop every one of them and propose a single real date dimension -
`cookbook-time.md#t6` builds it.

**What it costs** Nothing. These are an artifact of the *Auto date/time* setting,
not a modeling decision, and no one will miss them. Do not count them as tables
when you size the migration, and do not report them as dropped coverage in Step 7 -
say why they are absent instead, or a coverage review reads four missing tables as
four gaps.
