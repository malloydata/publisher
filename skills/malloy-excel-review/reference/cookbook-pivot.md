<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Cookbook: Pivot Tables (Step 3)

> A pivot table is a saved view over a range: a set of row and column fields, a
> value computed per cell, a filter on the page, and a snapshot of the answer. A Malloy
> view is the same thing with the snapshot taken out. This file maps each part of a
> pivot to Malloy, and says what to compare when the snapshot and the data disagree.

Read `translate-formulas.md` for routing and `recover-sources.md` for the `sales`
source used here (the `Data` table).

## Status of the evidence

**The committed fixture has no pivot.** `fixtures/fixture.xlsx` is the `python` build, and
that build ships without the `Pivot` sheet: a pivot is created in Excel, and Excel
silently repairs or strips one a script writes. So no recipe here compares against a
cached pivot cell. The recipes are labelled in place. Where one says it ran, the Malloy
ran on a local Publisher (0.9.0, DuckDB v1.5.5) over the `Data` table, and the expected
numbers are worked by hand from the same eleven rows:

| Label | Meaning |
|---|---|
| `semantics-cited (hand-derived)` | The fixture has no cached pivot cell. The Malloy ran; the expected value was worked out by hand from the `Data` rows, shown with the sum. |
| `semantics-cited` | What Excel does is cited from the file format or from how Excel behaves. It was not re-checked against a saved pivot. |
| `executed` | The Malloy ran and the result is not an Excel-quirk route (no recipe here is labelled so for a pivot cell, because there is none to compare). |

**Which recipes could ever become `executed (Excel)`.** The Excel procedure in
`fixtures/README.md` builds one pivot: `OrderDate` in Rows (grouped by months), `Region` as a
page field, a calculated field `Rev108 = Revenue * 1.08`, and `Revenue` shown as "% of Grand
Total". Its cached cells could therefore be compared with these, and only these:

| Recipe | Comparable to that pivot? |
|---|---|
| [P6](#p6) date grouping by months | yes, including the stale July cell |
| [P2](#p2) the `Region` page field at "(All)" | yes |
| [P3](#p3) `Rev108` and [P4](#p4) `percentOfTotal` | yes, **by month**: the recipes below are written by Product, so re-key them on the month first |
| [P1](#p1), [P4](#p4) row and column percentages, [P5](#p5), [P7](#p7), [P8](#p8) | no: they need `Product` in Rows (and `Region` across), which that pivot does not have. They stay `semantics-cited (hand-derived)` until someone saves a pivot with that layout. |

The pivot recipes can become `executed (Excel)` once the `Pivot` sheet exists (`fixtures/README.md`),
and only the rows above qualify. Real Excel-saved pivots were read for what the classifier
reports, not compared with these queries: their data is not the `Data` table.

What the classifier does read from a pivot was checked on a synthetic workbook of the
unit-test shape (hand-written XML, not an Excel save); its output is below.

## What the classifier reports

The text report prints one line per pivot, then a line for each selection it carries. On a
hand-built workbook with a hidden row item, a multi-select page filter, a calculated item
and an x14 "show values as", it printed:

```
- pivot PivotTable1 on P at A3:B8: source {"type": "worksheet", "sheet": "Data", "ref": "A1:C4"}, refreshed 2023-03-15, 1 value field(s), 0 calculated field(s)
  - page filter: Zone = East, North
  - hidden items in Region (2): West, North
  - hidden items in Zone (1): West
  - value field Sum of Amount is shown as percentOfParentRow
  - calculated item in Region: East+West
```

`classify --json` carries the same in `pivots[]`:

| Key | Holds |
|---|---|
| `row_fields`, `col_fields`, `page_fields` | field names |
| `page_items` | one entry per page field: `{"field": "Zone", "item": "East, North", "multi": true, "hidden": ["West"]}`. A single selected item is `{"field", "item"}`; "(All)" is `item: "(All)"`; a multi-select page with nothing hidden is also "(All)". |
| `hidden_items` | per field, the items switched off by hand (`<item h="1">`): `{"field": "Region", "hidden": ["West", "North"], "count": 2}`. Written for row, column and page fields. |
| `data_fields` | `name`, `field`, `subtotal`, and `show_data_as`: the x14 `pivotShowAs` when the file has one, else `showDataAs`, else `normal` |
| `calculated_fields` | `name`, `formula` (a `cacheField formula`) |
| `calculated_item_formulas` | `field`, `formula` (a calculated item: `East+West`); `calculated_items` is their count |
| `date_grouping` | `field`, `group_by` and `grouped_on_axis` (true when the grouped field is on the row, column or page axis; false means the cache groups it and the pivot does not show it). The text report prints `date grouping on Date: by months, placed on an axis` |
| `numeric_grouping` | a numeric field grouped into ranges: `{"field", "group": {"kind": "numeric", "start", "end", "interval"}, "grouped_on_axis"}` (from `rangePr startNum`, `endNum`, `groupInterval`; none on a masked pivot). The text report prints `numeric grouping on Age: 0 to 50 in steps of 10` |
| `top_n_filters` | a count of `top10` elements |
| `value_filters` | each value filter on an axis field: `{"field", "kind": "top_n"\|"top_percent"\|"top_sum"\|"bottom_n"..., "n", "measure", "direction"}`. A label or date filter is `{"field", "kind": "unsupported", "type"}`: it exists, and nothing here translates it ([P5](#p5)) |
| `cache_source` | worksheet range or table name, or `{"type": "external", "connection_id": ...}` |
| `external_cache` | present on an external source: `kind` is `data_model`, `relational`, `olap` or `external` ([P9](#p9)) |
| `refreshed_date` | the raw serial, here `"45000.5"`, which is 2023-03-15 12:00. `refresh` carries `refreshed` (the ISO date), `modified` (the `dcterms:modified` date) and `days_before_save`; the text report prints `refreshed 2023-03-15` and `pivot predates last save by N day(s)` |
| `records` | a `pivotCacheRecords` part exists (never read) |

It also marks the pivot's `location` as never lifted, so those cells are not read as data.
The sheet holding a pivot is classed `report`. A pivot over a hidden sheet reports none of
its selections or hidden items.

A `GETPIVOTDATA` formula routes **NR** with the flag `getpivotdata`, and the region lists the
pivot it reads (`pivots`: name, location, source). It is a read of the snapshot ([P8](#p8)).

| In the file | Reads as | Becomes in Malloy |
|---|---|---|
| `pivotTableDefinition` `rowFields`, `colFields` | row and column fields | `group_by:` (and a `nest:` or a second dimension for columns) |
| `pageFields`, with the selected `item` | report filters | a `where:`, or a given ([P2](#p2)) |
| `<item h="1">` in a `pivotField` | an item switched off by hand | a `where:` that excludes it ([P1](#p1), [P2](#p2)) |
| `dataFields` `subtotal=` | how a value is aggregated | the aggregate ([P1](#p1)) |
| `dataFields` `showDataAs=`, x14 `pivotShowAs=` | "show values as" | `all()` and window calculations ([P4](#p4)) |
| `cacheField formula=` | a calculated field | an expression over sums ([P3](#p3)) |
| `filters` with `top10` | a Top-N filter | `order_by:` and `limit:` ([P5](#p5)) |
| `cacheField` `fieldGroup` `rangePr groupBy=` | date or number grouping | a truncation, an extraction or a bucket ([P6](#p6)) |
| `calculatedItems` | a calculated item | a filtered aggregate ([P7](#p7)) |
| `GETPIVOTDATA` in a formula | a read of the pivot's output | the same view, queried ([P8](#p8)) |
| `cacheSource type="external"` | a pivot on a data model, a database or a cube | by `external_cache.kind` ([P9](#p9)) |

Not reported: the row and column order, sort type, and the Grand Total
and subtotal settings. Compare those against the saved cells.

## The snapshot problem

A pivot's displayed numbers are stored in the sheet as bare `<v>` cells inside the
`location ref`, computed from `pivotCacheRecords` as of the cache's `refreshedDate`. They
are a **snapshot**, and the data they summarised may have changed since. Parity must say
which two things it compared:

1. **The pivot's cached cells against Malloy over the current sheet.** The usual
   comparison. If they disagree, first read the report's `pivot predates last save by N day(s)`
   line (`pivots[].refresh.days_before_save`, the refresh date against the workbook's
   `dcterms:modified` in `docProps/core.xml`): a pivot refreshed before the last edit to its
   source is stale, and a disagreement is the workbook's, not the translation's. The advisory is not an error: the cache may still match, so compare the cells anyway and record the outcome either way. A pivot
   that was never refreshed after an edit is a finding to report, not a number to
   reproduce.
2. **The pivot's cached cells against the cache's own records.** That would prove the
   translation matches what the pivot summarised, but the script never reads
   `pivotCacheRecords` (it only notes that the part exists), so this comparison is not
   available from the tools here.

The fixture's own procedure builds a stale cache on purpose: `Data!D8` is a real date in
the pivot cache and text in the sheet, so a pivot grouped by month will show July's
7.25 where a Malloy view over the sheet's `order_date` alone will not (it needs
`order_date_any`, the data decision in `recover-sources.md`). That difference is exactly
what comparison 1 versus comparison 2 distinguishes.

The script already converts the serial (`refresh.refreshed`); to do it by hand,
`date '1899-12-30' + floor(refreshedDate)::int` is the day (a 1904 workbook starts at
`1904-01-01`), and the fraction is the time of day. Take the floor: a plain `::int` cast rounds, so `45000.5` would
read as the 16th instead of the 15th.

## Setup

The recipes use the `sales` source, plus one given for the page field:

```malloy
##! experimental.givens

// pivot page field Region; an empty string is "(All)"
given: PAGE_REGION :: string is ''
```

<a id="p1"></a>

## P1 - Rows, columns and values

The pivot's row field is `Product`, its value `Sum of Revenue`, its page field `Region`
set to (All), with a calculated field and a percentage next to it (P3, P4):

```malloy
run: sales -> {
  where: $PAGE_REGION = '' or lower(region) = lower($PAGE_REGION)
  group_by: product
  aggregate:
    sum_of_revenue is revenue.sum()
    rev108 is revenue.sum() * 1.08
    revenue_pct_of_grand_total is revenue.sum() / all(revenue.sum())
  order_by: product
}
```

| Product | `sum_of_revenue` | `rev108` | `revenue_pct_of_grand_total` |
|---|--:|--:|--:|
| Gadget | 184.5 | 199.26 | 0.5179 |
| Gizmo | 21.75 | 23.49 | 0.0611 |
| Widget | 150 | 162 | 0.4211 |
| **Grand Total** | **356.25** | **384.75** | **1** |

`semantics-cited (hand-derived)`: Gadget = 102.5 + 20.5 + 41 + 20.5 = 184.5; Gizmo = 7.25 + 14.5; Widget = 30 + 20 + 40 + 0 + 60;
the total is `Data!F13` = 356.25, and `384.75 = 356.25 * 1.08`. The Grand Total row is
the same query without `group_by:`.

**Items hidden by hand are not in the totals.** `hidden_items` lists the items a user switched
off in a row, column or page field. A relational pivot leaves them out of its Grand Total
too (`semantics-cited`; not checked against a saved pivot), so exclude them in a `where:` and
say which ones. With `Gizmo` hidden in the Product field:

```malloy
run: sales -> {
  where: lower(product) not in ('gizmo')
  group_by: product
  aggregate:
    sum_of_revenue is revenue.sum()
    revenue_pct_of_grand_total is revenue.sum() / all(revenue.sum())
  order_by: product
}
```

| Product | `sum_of_revenue` | `revenue_pct_of_grand_total` |
|---|--:|--:|
| Gadget | 184.5 | 0.5516 |
| Widget | 150 | 0.4484 |
| **Grand Total** | **334.5** | **1** |

`semantics-cited (hand-derived)`: 356.25 - 21.75 = 334.5, and 184.5 / 334.5 = 0.5516. A
translation that forgets the hidden item reproduces the unfiltered 356.25 and every
percentage in the pivot is off. Put the hidden items in the report beside the page filter.

A value field's aggregation is `dataField subtotal=` (the script reports it; "sum" when
the attribute is absent):

| `subtotal=` | Pivot "Summarize by" | Malloy | Watch for |
|---|---|---|---|
| `sum` (default) | Sum | `x.sum()` | Excel defaults a field to Count, not Sum, when the column holds text or blanks (`semantics-cited`). The fixture's `Qty` does (a text `"1"` and a blank). |
| `count` | Count | non-null count of the field | **A pivot "Count of X" counts non-blank ROWS, not distinct values, unless the pivot says "Distinct Count" (Power Pivot only).** It counts all non-empty cells, like `COUNTA`. Malloy `count(x)` is a **distinct** count. See `cookbook-lookup-aggregate.md#la5`. |
| `countNums` | Count Numbers | non-null count of the numeric column | counts numbers only, like `COUNT`. |
| `average` | Average | `x.avg()` | skips blanks and text, like `AVERAGE`. |
| `max`, `min`, `product` | Max, Min, Product | `x.max()`, `x.min()`, no direct aggregate | `product` has no aggregate; `exp(sum(ln(x)))` for positive values. |
| `stdDev`, `stdDevp`, `var`, `varp` | StdDev, StdDevp, Var, Varp | `x.stddev()` is the **sample** deviation (executed: 0.03 for 0.05, 0.08, 0.02; the population value is 0.0245) | `stdDevp` needs the population scaling, `stddev * sqrt((n - 1) / n)`; `varp` is `var * (n - 1) / n`, with no square root. |

The case-insensitive grouping applies to row fields: Excel's pivot merges `East`, `east`
and `EAST` into one item, showing the first spelling it met (`semantics-cited`; not
checked against a saved pivot). `group_by: region` in Malloy gives three groups, so
group on `lower(region)` and show the spelling you want. A blank cell is the item
`(blank)`; in Malloy it is a `null` group.

<a id="p2"></a>

## P2 - Page fields

A `pageField` is a filter above the table. "(All)" is no filter, one item is an equality
filter, and several items are an `in` filter (`multipleItemSelectionAllowed`). The script
reports which: `page_items` names the selected item, or every shown item of a multi-select
page, and `hidden` the ones left out. The given declared in Setup carries a single choice, and
an empty string stands for "(All)". The comparison is case-insensitive on both sides, like
the pivot's:

```malloy
run: sales -> {
  where: $PAGE_REGION = '' or lower(region) = lower($PAGE_REGION)
  aggregate: sum_of_revenue is revenue.sum(), rev108 is revenue.sum() * 1.08
}
```

With `givens: {"PAGE_REGION": "east"}` the query returns `sum_of_revenue = 77.75` and
`rev108 = 83.97`: the three spellings of East together. `semantics-cited (hand-derived)`: 30 + 20 + 7.25 + 0 +
20.5 = 77.75, and the same two numbers are cached in the report sheet as `Report!B3`
(77.75) and `Report!B13` (83.97), which come from `SUMIFS` rather than a pivot and agree.
The page filter changes the Grand Total, and so every percentage of it: with East
selected the Gadget row is 20.5 of 77.75, 0.2637.

**A multi-select page is not "(All)".** When the report says `item: "East, West"` with
`"hidden": ["North"]`, the pivot's totals cover only the shown items, and a single-valued
given cannot say so. Use the shown set:

```malloy
run: sales -> {
  where: lower(region) in ('east', 'west')
  aggregate: sum_of_revenue is revenue.sum()
}
```

260.75, which leaves out North (54.5) **and the blank region** (41): a blank is its own item,
`(blank)`, and is not selected here. `semantics-cited (hand-derived)`: 77.75 + 183 = 260.75.
Take the shown set from `page_items` and the hidden one from `hidden`; do not infer either from the
data. For a choice the viewer should change, make the given a `filter<string>` with the
shown items as its default.

Select the item that was **chosen** in the file, not the one that sorts first: `page_items`
has it. A page field with no `item` and no hidden items is "(All)".

<a id="p3"></a>

## P3 - Calculated fields

`cacheField formula="Revenue*1.08"` defines `Rev108`. A pivot calculated field is evaluated
on the **sums of the fields it names**, not row by row. For a formula that is linear in one
field, as `Revenue * 1.08` is, the two agree. For a product of two fields they do not:

```malloy
run: sales -> {
  group_by: product
  aggregate:
    pivot_calc_field is qty_num.sum() * price.sum()
    row_level is revenue.sum()
  order_by: product
}
```

| Product | `pivot_calc_field` = Sum(Qty) * Sum(Price) | `row_level` = Sum(Qty * Price) |
|---|--:|--:|
| Gadget | 656 | 184.5 |
| Gizmo | 43.5 | 21.75 |
| Widget | 750 | 150 |

`semantics-cited (hand-derived)`: Widget's quantities sum to 3 + 2 + 4 + 0 + 6 = 15 and its prices to
10 * 5 = 50, so 750 against 150. The pivot's Sum ignores text, so `qty_num` is the quantity (the text `"1"`
in `Data!C5` is a Gadget row: `qty_coerced` would give 9 and 738, the pivot gives 8 and 656 = 8 * 82); the row-level `revenue` still
counts it, as Excel's `Qty * Price` does. The Malloy ran on the fixture and returned these figures *(`executed`)*. Port the calculated field with the pivot's
semantics (a product of sums) when the goal is to reproduce the pivot, and say so, because
the row-level `revenue` is almost always what the author meant. Report the discrepancy as a
finding, never silently pick one. `semantics-cited`: that Excel computes a calculated field
on sums.

In a cell with no source rows a calculated field shows `0`, or `#DIV/0!` when its formula divides. The cached cell is the oracle: port the division as `a / nullif(b, 0)` and compare an error cell to null (`parity.md` section 1). A localized subtotal or grand-total row label ("Total", "Grand Total" translated) is part of the cached labels: compare the data rows by label and the totals rows by value and position, never by their text.

<a id="p4"></a>

## P4 - "Show values as"

`dataField showDataAs=` is the "Show Values As" menu. The enumeration comes from the file
format; the Malloy forms were executed. Two dimensions (`product` down, `region_key` across) so row,
column and total all mean something:

```malloy
run: sales -> {
  group_by: product, region_key is lower(region)
  aggregate:
    rev is revenue.sum()
    percent_of_row is revenue.sum() / all(revenue.sum(), product)
    percent_of_col is revenue.sum() / all(revenue.sum(), region_key)
    percent_of_total is revenue.sum() / all(revenue.sum())
  order_by: product, region_key
}
```

| `showDataAs` | Meaning | Malloy |
|---|---|---|
| `normal` | the value | the aggregate |
| `percentOfTotal` | share of the grand total | `x / all(x)` |
| `percentOfRow` | share of the row total | `x / all(x, row_dim)` |
| `percentOfCol` | share of the column total | `x / all(x, col_dim)` |
| `percent` ("% Of") | the value as a multiple of the base item's value in the same row (`baseField`, `baseItem`) | `x / all(x { where: base_field = 'item' }, other_dim)` |
| `difference`, `percentDiff` | change from the base item, or from `(previous)` | `x - lag(x)`; `(x - lag(x)) / lag(x)` in a `calculate:` |
| `runTotal` | running total down a base field | `sum_cumulative(x)` in a `calculate:` |
| `index` | `(x * grand total) / (row total * column total)` | `x * all(x) / (all(x, row_dim) * all(x, col_dim))` |

**`all(x, dim)` keeps `dim`; it removes every other group.** The second argument names the
dimension to *retain*, so the denominator for a row percentage keeps the row field:
`all(rev, product)` is the product's total across all regions. Reading it the other way
round swaps row and column percentages with no error. Executed on the table above:

| product | region | `rev` | `percent_of_row` | `percent_of_col` | `percent_of_total` |
|---|---|--:|--:|--:|--:|
| Gadget | east | 20.5 | 0.1111 | 0.2637 | 0.0575 |
| Gadget | west | 123 | 0.6667 | 0.6721 | 0.3453 |
| Gadget | (blank) | 41 | 0.2222 | 1 | 0.1151 |
| Gizmo | east | 7.25 | 0.3333 | 0.0932 | 0.0204 |
| Gizmo | north | 14.5 | 0.6667 | 0.2661 | 0.0407 |
| Widget | east | 50 | 0.3333 | 0.6431 | 0.1404 |
| Widget | north | 40 | 0.2667 | 0.7339 | 0.1123 |
| Widget | west | 60 | 0.4 | 0.3279 | 0.1684 |

`semantics-cited (hand-derived)`: Gadget/west is 123 of the Gadget row's 184.5 = 0.6667, and 123 of the west
column's 183 = 0.6721; the whole table is 356.25.

The running total, the difference and the percent difference, executed with `product` as
the base field in alphabetical order:

```malloy
run: sales -> {
  group_by: product
  aggregate: rev is revenue.sum()
  calculate:
    running_total is sum_cumulative(rev)
    difference_prev is rev - lag(rev)
    pct_diff_prev is (rev - lag(rev)) / lag(rev)
  order_by: product
}
```

Gadget 184.5, 184.5, null, null; Gizmo 21.75, 206.25, -162.75, -0.8821; Widget 150, 356.25,
128.25, 5.8966. `semantics-cited (hand-derived)`: 21.75 - 184.5 = -162.75 and -162.75 / 184.5 = -0.8821.

**"% Of" keeps the other axis.** With `baseField` Region and `baseItem` East, each cell is
divided by the East cell *of its own row*, so the denominator keeps `product` and filters the
region. Leaving `product` out divides every cell by one number, East's grand total, and
gives 1.582 where Excel gives 6.0 for Gadget/west (123 / 20.5):

```malloy
run: sales -> {
  group_by: product, region_key is lower(region)
  aggregate:
    rev is revenue.sum()
    percent_of_east is revenue.sum() / all(revenue.sum() { where: lower(region) = 'east' }, product)
  order_by: product, region_key
}
```

| product | region | `rev` | `percent_of_east` |
|---|---|--:|--:|
| Gadget | east | 20.5 | 1 |
| Gadget | west | 123 | 6 |
| Gadget | (blank) | 41 | 2 |
| Gizmo | east | 7.25 | 1 |
| Gizmo | north | 14.5 | 2 |
| Widget | east | 50 | 1 |
| Widget | north | 40 | 0.8 |
| Widget | west | 60 | 1.2 |

`semantics-cited (hand-derived)`: 123 / 20.5 = 6, 41 / 20.5 = 2, 14.5 / 7.25 = 2, 40 / 50 = 0.8, 60 / 50 = 1.2.
The base cell is itself 1 (100%). Every product here has an East cell; a row without one has no denominator, and that case was not run.

**What it costs.** Window calculations are query-level: every consumer repeats the
`calculate:`, and the order is the `order_by:`, which must match the pivot's sort
(`sortType`, or a manual order held in the item indexes). An `all()` denominator keeps the
query's `where:` (a page filter changes it, as in a pivot) but not a `limit:`: see P5.
`percent`, `difference` and `percentDiff` against a **named** base item need the item
value in the model, and `lag` only covers `(previous)`; a base item is a filter inside
`all()`, shown above, which ran but was not compared with a pivot.

### The x14 "show values as" options

Excel 2010 added six options that the base `showDataAs` attribute cannot hold. They are
stored in an `extLst` under the data field, as `pivotShowAs` on an `x14:dataField` element.
When both are present the x14 value is the real one, and the classifier reports it as
`show_data_as` (on the demo above, `percentOfParentRow` although the base attribute said
`percentOfRow`). Treat the six names as **storage details taken from the schema**: only
`percentOfParentRow` was exercised, on a hand-built file; the other five are spelled as the
schema spells them and were not seen in a saved pivot.

| Menu item | `pivotShowAs` | Malloy |
|---|---|---|
| % of Parent Row Total | `percentOfParentRow` | `x / all(x, parent_row_field)`: the share inside the outer row field |
| % of Parent Column Total | `percentOfParentCol` | `x / all(x, parent_col_field)`: the same on the column axis |
| % of Parent Total | `percentOfParent` | `x / all(x, fields_down_to_the_parent_of_base)`; with the outermost field as base this is `x / all(x)` |
| % Running Total In | `percentOfRunningTotal` | the share of the grand total, accumulated down the base field (below) |
| Rank Smallest to Largest | `rankAscending` | `rank()` in a `calculate:` over `order_by: x asc` |
| Rank Largest to Smallest | `rankDescending` | `rank()` in a `calculate:` over `order_by: x desc` |

The parent forms need a nested field; with one row field the parent is the grand total, and
what Excel shows for an outermost item was not checked. With `product` as the outer row field
and `region` inside it, the parent row total of Gadget/west is the Gadget total, which is the
`percent_of_row` column of the table above (0.6667); the Malloy is the same `all(rev, product)`.

```malloy
run: sales -> {
  group_by: product
  aggregate:
    rev is revenue.sum()
    pct is revenue.sum() / all(revenue.sum())
  order_by: product
} -> {
  select: product, rev, pct
  calculate: running_pct is sum_cumulative(pct)
  order_by: product
}
```

| product | `rev` | `pct` | `running_pct` |
|---|--:|--:|--:|
| Gadget | 184.5 | 0.5179 | 0.5179 |
| Gizmo | 21.75 | 0.0611 | 0.5789 |
| Widget | 150 | 0.4211 | 1 |

`semantics-cited (hand-derived)`: (184.5 + 21.75) / 356.25 = 0.5789. The running total is a
second stage because `sum_cumulative` does not accept an `all()` expression directly. The
accumulation order is the `order_by:`, which must match the pivot's.

```malloy
run: sales -> {
  group_by: product
  aggregate: rev is revenue.sum()
  calculate: rank_desc is rank()
  order_by: rev desc
}
```

Gadget 184.5 is rank 1, Widget 150 is 2, Gizmo 21.75 is 3; with `order_by: rev asc` and `rank_asc`
the order is reversed, Gizmo 1, Widget 2, Gadget 3. `semantics-cited (hand-derived)`. Ties:
`rank()` gives equal values the same rank and skips the next, like Excel's `RANK`; no tie
exists in these three rows, so that was not run.

<a id="p5"></a>

## P5 - Top-N filters

`filters` with `<top10 val="N">` (count, percent or sum, top or bottom) keep the first N
items of a row field ranked by a value field:

```malloy
run: sales -> {
  group_by: product
  aggregate: rev is revenue.sum()
  order_by: rev desc
  limit: 2
}
```

Gadget 184.5 and Widget 150. `semantics-cited (hand-derived)`.

**What it costs: the percentages and totals.** A `limit:` applies after the percentage is
computed, so `x / all(x)` in the same query still divides by the total of *all* products
(0.5179 for Gadget), not the total of the two shown (0.5516). The report lists each
filter under `value_filters` (field, kind, N, measure, direction), and a pivot's Grand Total
after a Top-N filter covers only the visible items, so a Grand Total in the saved cells is the
visible total, not the all-items total. The tie at the cut is undefined: Excel and `limit:`
may keep different rows. The two-stage form divides by the visible total:

```malloy
run: sales -> {
  group_by: product
  aggregate: rev is revenue.sum()
  order_by: rev desc
  limit: 2
} -> {
  group_by: product, rev
  aggregate: pct_of_visible is rev.sum() / all(rev.sum())
  order_by: rev desc
}
```

Gadget 184.5 / 334.5 = 0.5516 and Widget 150 / 334.5 = 0.4484. `semantics-cited (hand-derived)`. Ties at the
cut are the other trap: `limit:` keeps an arbitrary one of equal values, so add a
tie-breaker to `order_by:` and check the boundary against the saved pivot (how Excel breaks
ties was not checked).

**Top N percent and Top N sum are not a count.** `<top10 percent="1" val="50">` keeps the smallest set of top items whose cumulative value reaches 50% of the total, not 50% of the item count; "Top N sum" does the same against N itself. The item that crosses the threshold is included. An item is kept while the running total *before* it is still under the threshold, which is the window form below. On `50, 30, 15, 5` (total 100) a Top 50 percent keeps only the 50, and Top 80 percent keeps 50 and 30 (this SQL ran on DuckDB 1.4.5):

```sql
SELECT k, v FROM (
  SELECT k, v,
         sum(v) OVER (ORDER BY v DESC, k) - v AS cum_before,
         sum(v) OVER () AS total
  FROM items)
WHERE cum_before < 0.5 * total   -- N/100 * total; for "sum", compare with N itself
```

The Malloy form keeps the cumulative sum in the first stage and filters in the next (ran on a live Publisher; `0.5` is N/100, and for "sum" compare `cum - v` with N itself):

```malloy
run: items -> {
  group_by: k
  aggregate: v is v.sum(), total is all(v.sum())
  calculate: cum is sum_cumulative(v)
  order_by: v desc, k
} -> {
  select: k, v
  where: cum - v < 0.5 * total
}
```

Use the stanza wrapper or the Malloy form above. The Excel rule is `semantics-cited`: the documented one, not checked against a saved pivot. A tie at the cut is undefined: Excel and the `k` tie-breaker above may keep different items. Bottom N percent is the same with `ORDER BY v ASC`.

<a id="p6"></a>

## P6 - Date and number grouping

`cacheField` `fieldGroup` with `rangePr groupBy=` groups a field: `seconds`, `minutes`,
`hours`, `days`, `months`, `quarters`, `years`, or `range` for numbers
(`startNum`, `endNum`, `groupInterval`). Several can be applied together, and **what is
selected matters**:

| Pivot grouping | Meaning | Malloy |
|---|---|---|
| Years and Months | each calendar month | `order_date.month` (a truncation, `2024-03-01`) |
| Months alone | the month of the year, all years pooled | `month(order_date)` (an extraction, `3`) |
| Quarters, Years | likewise | `.quarter`, `.year`; `quarter()`, `year()` |
| numbers by interval | a bucket of width `groupInterval` | `floor(x / interval) * interval` |

Choosing the wrong pair gives a plausible chart with merged years (Months alone folds
January 2024 into January 2025; `semantics-cited`). The saved `groupBy` list says which; the report line
`date_grouping` lists the field and the grouping.

```malloy
run: sales -> {
  where: $PAGE_REGION = '' or lower(region) = lower($PAGE_REGION)
  group_by: month_no is month(order_date_any)
  aggregate: sum_of_revenue is revenue.sum(), pct is revenue.sum() / all(revenue.sum())
  order_by: month_no
}
```

| month | `sum_of_revenue` |
|--:|--:|
| 1 | 30 |
| 2 | 20 |
| 3 | 102.5 |
| 4 | 20.5 |
| 5 | 40 |
| 6 | 41 |
| 7 | 7.25 |
| 8 | 0 |
| 10 | 14.5 |
| 11 | 20.5 |
| (null) | 60 |

Months 9 and 12 have no rows here. Whether a saved pivot lists them ("show items with no
data") is read from the pivot; a spine (`cookbook-scenario.md#sc4`) supplies them.
`semantics-cited (hand-derived)`: the rows add to 356.25. Two inputs are data decisions:

- **The null row's 60** is the serial-60 row (`Data!D10`). The `sales` source lifts the
  phantom 1900-02-29 as null, as the printed stanza does for a serial below 61 (`recover-sources.md`), so it has no month. The workbook shows
  it as 1900-02-29; how a saved pivot groups that date is not asserted here. Read the
  February cell from the save and decide whether that row belongs to February.
- **July's 7.25** is the text date `2024-07-15` read as July (`order_date_any`). Excel will
  not group a field that holds text, which is why the fixture's procedure retypes the
  cell as a date before grouping and back after (`fixtures/README.md`); the stale cache
  then has July where the sheet does not (see "The snapshot problem").

A `range` group on a number is a bucket. On `price` with an interval of 10:

```malloy
run: sales -> {
  group_by: price_bucket is floor(price / 10) * 10
  aggregate: rows is count(), rev is revenue.sum()
  order_by: price_bucket
}
```

Bucket 0 holds 2 rows (21.75), bucket 10 holds 5 (150) and bucket 20 holds 4 (184.5).
`semantics-cited (hand-derived)`: the prices are 7.25 (2 rows), 10 (5 rows) and 20.5 (4 rows).

A group that does not start at 0 is `floor((x - start) / interval)` buckets, each covering
`[start, start+interval)`, lower bound included and upper bound excluded; the saved
`numeric_grouping` gives `start`, `end` and `interval`, and values outside `start` to `end`
land in the `<start` and `>end` items. With `start` 5 and `interval` 10, the values 7, 14, 15
and 31 fall in buckets 0, 0, 1 and 2, which Excel labels `5-14`, `5-14`, `15-24` and `25-34` for whole numbers (compare against the cached label strings rather than building them)
(`semantics-cited (hand-derived)`; compile-check an edit):

```malloy
group_by: bucket is floor((price - 5) / 10)
```

<a id="p7"></a>

## P7 - Calculated items

`calculatedItems` add a **row** to a field (`East + West` as a new Region item) computed
from other items. A Malloy `group_by:` cannot add a group that overlaps others, so the
translation is a **column**:

```malloy
run: sales -> {
  group_by: product
  aggregate:
    east is revenue.sum() { where: lower(region) = 'east' }
    west is revenue.sum() { where: lower(region) = 'west' }
    east_plus_west is revenue.sum() { where: lower(region) in ('east', 'west') }
  order_by: product
}
```

Gadget 20.5, 123, 143.5; Gizmo 7.25, 0, 7.25; Widget 50, 60, 110. `semantics-cited (hand-derived)`. The cost
is the shape: a calculated item is a row of the pivot and a column here. The script reports
each calculated item's field and formula (`calculated_item_formulas`, `East+West` on the demo
above), including ones stored in the cache definition, so the translation does not start from
a guess.
A calculated item also changes how Excel computes the pivot's grand totals; compare the
saved Grand Total before trusting a match.

<a id="p8"></a>

## P8 - GETPIVOTDATA

`=GETPIVOTDATA("Sum of Revenue", P!$A$3, "Product", "Widget")` reads one cell of the
pivot's output by field and item. Its value is the pivot's snapshot (the cache's
`refreshedDate`), not a computation. The classifier routes it NR with the flag `getpivotdata`
and names the pivot (name, location and source) the first argument points into. That NR is
recipe-backed, not a dead end: the region carries `reason_id: "pivot_recipe"` and the text report
counts it as "NR (pivot recipe)", apart from the NR regions with no recipe. Translate it
by querying the view with the same filters, one filter per field and item pair:

```malloy
run: sales -> {
  where: lower(product) = 'widget'
  aggregate: sum_of_revenue is revenue.sum()
}
```

150, the Widget row's Grand Total cell. Naming a second pair, `"Region", "East"`, adds a
filter and reads the intersection:

```malloy
run: sales -> {
  where: lower(product) = 'widget' and lower(region) = 'east'
  aggregate: sum_of_revenue is revenue.sum()
}
```

50. Both ran on the fixture and returned 150 and 50 *(`executed`)*. `lower()` on `product` too: GETPIVOTDATA compares the item name case-insensitively, as a pivot item does, and a bare `=` in Malloy does not. `semantics-cited (hand-derived)`: Widget is 30 + 20 + 40 + 0 + 60 = 150, and its East rows are 30 + 20 + 0 = 50.
The comparison to make is the formula's cached value against that query, with the same
snapshot caveat: if the pivot is stale, `GETPIVOTDATA` is stale with it. Excel's own
documentation says `GETPIVOTDATA` returns `#REF!` when the arguments name a cell the pivot
is not showing, so a cached `#REF!` here means a hidden or removed item (check
`hidden_items`), not a translation error.

**`GETPIVOTDATA` returns the value as the pivot displays it.** A data field set to a "Show Values As" mode caches the transformed number, not the raw sum, so the query above matches only a `normal` field. Read `show_data_as` for the field (`data_fields`), apply the same transform to the same filters, and compare that:

| `show_data_as` | The cached cell is | Compare against |
|---|---|---|
| `normal` | the aggregate | the aggregate |
| `percentOfTotal` | share of the grand total | `x / all(x)` |
| `percentOfCol`, `percentOfRow` | share of the column or row total | `x / all(x, col_dim)`, `x / all(x, row_dim)` |
| `percentOfParentRow`, `percentOfParentCol`, `percentOfParent` | share of the parent item's total | `x / all(x, parent_dim)` (P4) |
| `runTotal` | running total down the base field | `sum_cumulative(x)` in a `calculate:` |
| `difference`, `percentDiff` | change from the base item or `(previous)` | `x - lag(x)`, `(x - lag(x)) / lag(x)` |
| `rankAscending`, `rankDescending` | the item's rank among the base field's items | `rank()` over `order_by: x asc` or `x desc`, ties sharing a rank |

The Malloy forms are the P4 recipes. `semantics-cited` for the Excel side: a raw sum compared to a percentage mismatches by construction, which is a wrong comparison, not a translation bug.

**Month names and other labels follow the workbook's locale.** A pivot label or `TEXT(date, "mmm")` caches the language and abbreviation length the saving Excel used (some locales write a four-letter abbreviation), so never assume English abbreviations. Compare the labels against the cached strings, and key the Malloy on the date rather than on a label.

**Positional reads.** A formula such as `=A4` beside a pivot reads a pivot ROW by position, not by item. The report does not print the pivot's sort order, so reproduce the sort from the cached order (the oracle read of the pivot's cells) and say so; if the pivot is refreshed or re-sorted, the formula reads a different item. `semantics-cited`.

<a id="p9"></a>

## P9 - A pivot on an external cache

`cacheSource type="external"` (with a `connectionId`) means the pivot's rows are not on a
sheet. That is **not always the Power Pivot model**. The script resolves the connection and
reports `external_cache.kind`:

| `kind` | Meaning | What to do |
|---|---|---|
| `data_model` | the workbook's own Power Pivot model: fields are model columns and measures, calculated fields are DAX | `power-pivot.md`, and `skill:malloy-powerbi-review`. The pivot's layout (rows, columns, filters) is rebuilt here from P1 to P6. |
| `relational` | a pivot over a database through an ordinary connection | none of this file's recipes apply to the source rows. "Decide the data question" in `discover.md`: point Malloy at the database, then rebuild the pivot from P1 to P6. |
| `olap` | a pivot over a cube | `discover.md`: Publisher has no cube connection; ask for the underlying warehouse tables. |
| `external` | a source the script cannot resolve | `discover.md`: decide the data question. |

Only `data_model` pivots count in `external.pivot_external_caches` ("data-model pivot
cache(s)" in the text). The others set `cache_of_database` and print under "External data".

## P10 - A filter on a field that is on no axis

A pivot can be filtered by a field that appears in no row, column, page or value area: hidden
items on that field, or a slicer selection. The pivot shows nothing of it, but the rows are
gone. The script lists each as an entry in the pivot's `filters` (`via` is `filter` or
`slicer`, with the field, the `kept` items and the `hidden` ones) and sets
`filter_on_non_axis_field`. A slicer whose selection it cannot map to a cache field sets
`slicer_filter_unresolved` instead; open the slicer in Excel and read its selection by hand.
Each pivot also carries `slicer_status`: `none` (no slicer targets it), `all_selected` (a slicer
targets it and filters nothing), `filtering` or `unresolved`. Only `none` means nobody drove it.
A slicer or timeline is bound to a pivot by its sheet and name, so same-named pivots on different
sheets each get only their own. A timeline is a date range, listed in the pivot's `timelines`
(`field`, `start`, `end`, `bounds_start`, `bounds_end`, `filtering`): when `filtering` is true the
selection is narrower than the bounds, and that is a `where:` on the date field. An unreadable
timeline cache makes `slicer_status` `unresolved`.

Carry each such filter into the Malloy view as a `where:`, or a given when the reader should
choose the items. Leaving it out sums rows Excel excluded, and the totals will not match.

```malloy
run: sales -> {
  where: lower(region) in ('east', 'north')
  group_by: product
  aggregate: total_amount is revenue.sum()
}
```

```malloy
// timeline narrowed to 2020-03-01 through 2020-06-30
run: sales -> {
  where: order_date ~ f'2020-03-01 to 2020-07-01'
  group_by: product
  aggregate: total_amount is revenue.sum()
}
```

`executed` (both compiled and ran on the fixture's `sales`; the first returns Gadget 20.5, Gizmo 21.75, Widget 90, the second no rows because the fixture has no 2020 dates). Compare with `lower()`: a pivot item
is case-insensitive, and `region ~ f'East, North'` is case-sensitive, so it drops `east` and `EAST`
(Widget 70 instead of 90).

<a id="p11"></a>

## P11 - Nested row fields (compact layout)

Two row fields (`Region`, then `Product`) in the compact layout show each region as a
subtotal row with its products indented under it, plus a Grand Total. The Malloy is a
`group_by:` per level: the outer level's `aggregate:` is the subtotal row and a `nest:`
holds the inner level.

```malloy
run: sales -> {
  group_by: region_key is lower(region)
  aggregate: region_total is revenue.sum()
  nest: by_product is {
    group_by: product
    aggregate: rev is revenue.sum()
    order_by: rev desc
  }
  order_by: region_key
}
```

The Grand Total row is the same query without the outer `group_by:`. Add a `nest:` per
further row field. `semantics-cited` (not executed here; compile-check the fragment).

- **Aggregate each level from the rows; never add up the nested rows.** A subtotal computed
  by the outer `aggregate:` is right for `avg`, distinct counts and percentages, where a sum of
  the inner rows is not.
- **Group on `lower(region)`, as in P1.** Excel merges `East` and `east` into one item; two
  levels give two chances to split it.
- **Order each level to match the pivot.** A pivot sorts items ascending by default and a
  sort by value is its own setting (`sortType`); an unordered `nest:` is an arbitrary order.
- A pivot that puts subtotals at the bottom of each group is a presentation choice, not a
  different number.

## Reporting a pivot

One row per pivot cell range you compared, naming the snapshot:

| Pivot | Cell | Excel cached (as of refreshedDate) | Malloy (context) | Match | Label |
|---|---|---|---|---|---|

Include the pivot's `refreshedDate` and `dcterms:modified` in the report, the page-field
item that was selected (`page_items`), and every hidden item (`hidden_items`). A pivot that matches at the Grand Total and nowhere else is a
mismatch, not a match.
