<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

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
