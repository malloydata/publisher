<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Cookbook: Aggregates and Lookups (Step 3)

> Worked ports of the Excel functions that look the same in Malloy and are not:
> `SUMIFS`, `COUNTIF`, `AVERAGE`, `SUMPRODUCT`, `VLOOKUP`, `MATCH`. This is where a
> faithful-looking translation returns a different number with no error, so every
> recipe states what it costs as well as what it produces.

Read `translate-formulas.md` for how a region is routed here and
`recover-sources.md` for the `sales` source used throughout.

**Each recipe is labelled `executed` or `semantics-cited` in its own entry; trust the entry, not
this paragraph.** Where an entry says the Malloy ran, it ran on a local Publisher (0.9.0, DuckDB
v1.5.5) against `fixtures/fixture.xlsx` (or on DuckDB 1.5.5 over the rows the entry shows), and the
numbers shown are what came back, compared with the value the workbook cached next to the formula.
A snippet whose entry says to compile-check it has not been compiled here: compile-check the
fragment before relying on it. The labels:

| Label | Meaning |
|---|---|
| `executed` | The Malloy ran and the cached value is not an Excel-quirk route: a plain sum, join or window. |
| `semantics-cited` | The route is an Excel quirk (case-insensitive match, text numbers, approximate match, `#N/A`, `TODAY`). Excel's behavior is cited from its documentation or from the fixture generator's model of it; the Malloy was executed separately and compared with that value, and may or may not reproduce it (the table says which). The committed fixture is the `python` build, so a cached value is the generator's model, not an Excel save. Re-label `executed (Excel)` only once `fixtures/README.md` records one. |
| `semantics-cited (hand-derived)` | The fixture has no cached cell for this formula. The Malloy ran; the expected value was worked out by hand from the data, shown with the sum. |

**Every snippet below is a fragment.** The `given:` declarations and the sources in
Setup belong in the model file; a `run:` block is a query against them.

## Setup

```malloy
##! experimental.givens

// Report!A1, the number SUMIFS compares against
given: MIN_QTY :: number is 1
// Report!B2 (serial 45292)
given: REPORT_START :: date is @2024-01-01
// TODAY() pinned to docProps/core.xml dcterms:modified
given: AS_OF :: date is @2024-06-30
// the 1.08 typed inside Report!B13
given: UPLIFT :: number is 1.08

// Lookup!A1:B6 (sorted) and D1:E6 (the unsorted copy): [lo, hi) brackets
source: rates_sorted is duckdb.sql("""
  SELECT "Threshold" AS lo,
         lead("Threshold") OVER (ORDER BY "Threshold") AS hi,
         lag("Threshold") OVER (ORDER BY "Threshold") AS prev_lo,
         lag("Threshold") OVER (ORDER BY file_row) AS prev_in_file,
         "Rate" AS rate
  FROM (SELECT *, row_number() OVER () AS file_row
        FROM read_xlsx('data/fixture.xlsx', sheet = 'Lookup', range = 'A1:B6', header = true))
""")
source: rates_unsorted is duckdb.sql("""
  SELECT "Threshold" AS lo,
         lead("Threshold") OVER (ORDER BY "Threshold") AS hi,
         lag("Threshold") OVER (ORDER BY "Threshold") AS prev_lo,
         lag("Threshold") OVER (ORDER BY file_row) AS prev_in_file,
         "Rate" AS rate
  FROM (SELECT *, row_number() OVER () AS file_row
        FROM read_xlsx('data/fixture.xlsx', sheet = 'Lookup', range = 'D1:E6', header = true))
""")

// The keys the Lookup formulas hardcode (VLOOKUP(750, ...)), one row per formula cell.
// In a real workbook the key is a column of the fact source, and the joins below hang off that.
source: probes is duckdb.sql("""
  SELECT * FROM (VALUES ('H2', 750), ('H3', 750), ('H4', 750), ('H6', -5), ('H7', 750), ('H8', 750), ('X1', 500)) t(cell, key)
""")

// Lookup!J1:K4, a text-key table, and the two keys its tests look up (any case)
source: text_keys is duckdb.sql("""
  SELECT row_number() OVER () AS pos, "Key" AS key_text, "Code" AS code
  FROM read_xlsx('data/fixture.xlsx', sheet = 'Lookup', range = 'J1:K4', header = true)
""")
source: text_probes is duckdb.sql("""
  SELECT * FROM (VALUES ('H9', 'east'), ('H10', 'EAST')) t(cell, key)
""")

// Report!G1:H9
source: units is duckdb.sql("""
  SELECT "Month" AS month_no, "Units" AS units
  FROM read_xlsx('data/fixture.xlsx', sheet = 'Report', range = 'G1:H9', header = true)
""") extend {
  dimension: doubled is units * 2
}
```

`sales` has `region`, `revenue`, `qty_raw`, `qty_coerced` (what Excel's `*` sees),
`qty_num` (numbers only), `order_date` (serials), `order_date_text` and
`order_date_any`. The fixture's `Data` sheet is built to hit each trap: `East`, `east`
and `EAST`; one blank region (row 7); a text `"1"` in `Qty` (row 5) and a blank
(row 9); a text date (row 8) and serial 60 (row 10).

---

<a id="la1"></a>

## LA1 - `SUMIFS` / `COUNTIFS` equality is case-insensitive

**The Excel**

```
Report!B3   =SUMIFS(Data!$F$2:$F$12, Data!$A$2:$A$12, "east")
Report!B11  =COUNTIF(Data!A:A, "east")
```

**What it means** Revenue and rows where the region is "east", **whatever the case**.
The data holds `East` (3 rows), `east` (1) and `EAST` (1).

**The Malloy**

```malloy
run: sales -> {
  aggregate:
    east_revenue is revenue.sum() { where: lower(region) = 'east' }
    east_rows is count() { where: lower(region) = 'east' }
    east_revenue_naive is revenue.sum() { where: region = 'east' }
    east_rows_naive is count() { where: region = 'east' }
    east_qty2 is revenue.sum() { where: lower(region) = 'east' and qty_num >= 2 }
}
```

| | Excel cached | Malloy `lower()` | Malloy `region = 'east'` |
|---|--:|--:|--:|
| `Report!B3` revenue | 77.75 | **77.75** | 20 |
| `Report!B11` rows | 5 | **5** | 1 |

**Verified:** `semantics-cited`. The `region = 'east'` column is the naive port: it
matches only the one lowercase row, and it compiles and runs.

The same rule breaks grouping. A pivot or `COUNTIFS` groups the five spellings
together; `group_by: region` does not:

```malloy
run: sales -> {
  group_by: region_key is lower(region)
  aggregate: rows is count(), total_revenue is revenue.sum()
  order_by: region_key
}
```

| `region_key` | rows | `total_revenue` |
|---|--:|--:|
| east | 5 | 77.75 |
| north | 2 | 54.5 |
| west | 3 | 183 |
| (null) | 1 | 41 |

Plain grouping returns six groups: `EAST` 1, `East` 3, `North` 2, `West` 3, `east` 1,
null 1.

```malloy
run: sales -> { group_by: region; aggregate: rows is count(); order_by: region }
```

Several criteria are `and`ed inside one `where:`:
`=SUMIFS(F, A, "east", C, ">=2")` is the `east_qty2` aggregate above, which returned
**50** (`semantics-cited (hand-derived)`: the two East/east rows with Qty 3 and 2, 30 + 20).

**What it costs** A `lower()` on every text criterion and every text group key, on the
data side. Define `region_key` once as a dimension and use it everywhere; keep the
original column for display. The text key you display is no longer one of the
spellings, so decide which one the report shows.

---

<a id="la2"></a>

## LA2 - Comparison criteria: `">="&A1`

**The Excel**

```
Report!B4   =SUMIFS(Data!$F$2:$F$12, Data!$C$2:$C$12, ">="&A1)      -- A1 = 1
Report!B15  =SUMIFS(Data!$F$2:$F$12, Data!$D$2:$D$12, ">="&B2)      -- B2 = 2024-01-01
```

**What it means** The criterion string is an operator glued to a cell, and the cell
becomes a `given:`. A comparison criterion matches **numbers only**: the text `"1"` in
`Data!C5` and the text date in `Data!D8` never satisfy `>=`.

**The Malloy**

```malloy
run: sales -> {
  aggregate:
    by_qty is revenue.sum() { where: qty_num >= $MIN_QTY }
    by_qty_naive is revenue.sum() { where: qty_coerced >= $MIN_QTY }
    by_date is revenue.sum() { where: order_date >= $REPORT_START }
    by_date_text_as_date is revenue.sum() { where: order_date_any >= $REPORT_START }
}
```

| | Excel cached | Malloy (matches Excel) | Malloy (text read as a number or date) |
|---|--:|--:|--:|
| `Report!B4` | 335.75 | **335.75** (`qty_num`) | 356.25 (`qty_coerced`) |
| `Report!B15` | 289 | **289** (`order_date`) | 296.25 (`order_date_any`) |

**Verified:** `semantics-cited`. The two right-hand numbers are what a careful
translation produces if it cleans the data first: the text `"1"` becomes a 1 and the
text date becomes a date.

**What it costs** A decision. The workbook's answer excludes a row that is plainly a
sale on a plainly valid date, so the numbers disagree with a *better* model. Port
what the workbook says (`qty_num`, `order_date`), report the difference
(`356.25 - 335.75 = 20.50`, the text-`"1"` row; `296.25 - 289 = 7.25`, the text-date
row), and let the owner decide whether to fix the cells.

The criterion cell stays a `given:`, so a dashboard can offer it as a widget.

**15 significant digits** A criterion built with `&` (`">"&x`, `"<="&MAX(...)`) is text, so
Excel converts `x` to at most 15 significant digits before comparing. A strict `>` against
full float precision then disagrees on values that differ only beyond digit 15: Excel treats
them as equal. The classifier's `criteria_comparison` detail says `15sig` for these.
Round both sides to 15 significant digits, or compare with a tolerance. *(semantics-cited)*

```malloy
// 15 significant digits: 14 minus the exponent of the leading digit; 0 has no exponent
dimension: amount_15 is pick amount when amount = 0 else round(amount, (14 - floor(log(abs(amount), 10)))::"INTEGER")
```

Malloy has no `log10()`, so use `log(x, 10)`, and `round(DOUBLE, DOUBLE)` is rejected: the digits
argument must be an integer. Only the quoted `::"INTEGER"` cast parses (`::integer` and
`cast(.. as integer)` are parse errors). This form compiled and ran on a live Publisher.

Round the given the same way before `amount_15 > $THRESHOLD_15`.

---

<a id="la3"></a>

## LA3 - Wildcards: `*` and `?`

**The Excel**

```
Report!B5   =SUMIFS(Data!$F$2:$F$12, Data!$A$2:$A$12, "*st")
```

**What it means** `*` is any run of characters and `?` is exactly one, and the match
ignores case. `"*st"` is `East`, `east`, `EAST` and `West`.

**The Malloy**

```malloy
run: sales -> {
  aggregate:
    ends_in_st is revenue.sum() { where: lower(region) ~ '%st' }
    w_any_st is revenue.sum() { where: lower(region) ~ 'w_st' }
}
```

| | Excel cached | Malloy |
|---|--:|--:|
| `Report!B5` `"*st"` | 260.75 | **260.75** |
| `"w?st"` (no cached cell) | | 183 (`semantics-cited (hand-derived)`: `West`, 102.5 + 20.5 + 60) |

**Verified:** `semantics-cited` for `*st`, `semantics-cited (hand-derived)` for `w?st`.

**What it costs** Here `~` is a `LIKE` pattern, so `*` becomes `%` and `?` becomes
`_`. That is only safe while the Excel criterion has no `%` or `_` of its own. In Excel
both are literal characters; in `LIKE` they are wildcards, so a criterion such as
`"SKU_1*"` becomes `LIKE 'sku_1%'`, where `_` matches any character, and it
**over-matches with no error**. Malloy's `~` does take an escape: write `\\_` and `\\%`
(a doubled backslash inside the Malloy string) for a literal `_` and `%`, and it compiles to
`LIKE ... ESCAPE`. Or translate the criterion to an anchored regular expression: escape the
regex metacharacters, turn `*` into `.*` and `?` into `.`, and anchor both ends (an Excel `~*`
is a literal `*`):

```malloy
source: skus is duckdb.sql("""
  SELECT * FROM (VALUES ('SKU_1A'), ('sku_1b'), ('SKUx1C'), ('SKU_2A')) t(sku)
""")

run: skus -> {
  aggregate:
    like_port is count() { where: lower(sku) ~ 'sku_1%' }
    escaped_like is count() { where: lower(sku) ~ 'sku\\_1%' }
    anchored_regex is count() { where: lower(sku) ~ r'^sku_1.*$' }
}
```

| | Excel `COUNTIF(range, "SKU_1*")` | Malloy |
|---|--:|--:|
| `LIKE` port (`~ 'sku_1%'`) | 2 | 3 (`SKUx1C` matches by mistake) |
| escaped `LIKE` (`~ 'sku\\_1%'`) | 2 | **2** |
| anchored regex | 2 | **2** |

**Verified:** `semantics-cited (hand-derived)`. `skus` is an illustration, not a
fixture sheet; Excel's count of 2 is worked out from the criterion, and the Malloy counts (3, 2, 2) ran on DuckDB 1.5.5. A `%` or `_` in the
*data* is harmless, since only the pattern side is interpreted. A text criterion with
no wildcard stays an equality (LA1).

---

<a id="la4"></a>

## LA4 - Blank criteria: `""`, `"<>"`, `"<>x"`

**The Excel**

```
Report!B6   =SUMIFS(Data!$F$2:$F$12, Data!$A$2:$A$12, "")
```

**What it means** Revenue where the region cell is **empty**. The one blank region is
`Data!A7` (revenue 41).

**The Malloy**

```malloy
run: sales -> {
  aggregate:
    blank_region is revenue.sum() { where: (region ?? '') = '' }
    non_blank is revenue.sum() { where: region is not null }
    not_east is revenue.sum() { where: lower(region) != 'east' }
}
```

| | Excel | Malloy |
|---|--:|--:|
| `Report!B6` `""` | 41 (cached) | **41** |
| `"<>"` (non-blank), no cached cell | | 315.25 (`semantics-cited (hand-derived)`: 356.25 - 41) |
| `"<>east"`, no cached cell | | 278.5 (`semantics-cited (hand-derived)`: 356.25 - 77.75) |

**Verified:** `semantics-cited`.

**What it costs** Blank is not null in general. A blank cell comes back from
`read_xlsx` as null, but a cell holding an empty string comes back as `''` (verified
on a scratch workbook, not the fixture), and Excel's `""` criterion matches both.
`(region ?? '') = ''` matches both. `region is null` matches only the first, and
happens to give the same 41 on this fixture because it has no empty-string cell.

`"<>east"` matches blanks too. Malloy's `!=` already keeps the null row (adding
`or region is null` returned the same 278.5), so no extra clause is needed. Malloy's raw
`a != b` is TRUE for null against null and for null against `''` (run on a live Publisher),
where Excel's `<>` calls a blank and `""` equal; compare with `coalesce(x, '') != coalesce(y, '')`.

---

<a id="la5"></a>

## LA5 - `COUNT`, `COUNTA`, `COUNTIF`, and Malloy's `count()`

**The Excel**

```
Report!B8   =COUNT(Data!C2:C12)
Report!B9   =COUNTA(Data!C2:C12)
Report!B10  =COUNTIF(Data!C:C, 1)
```

**What it means**

- `COUNT` counts **numbers**. The text `"1"` does not count.
- `COUNTA` counts every non-blank cell, text included.
- `COUNTIF(range, 1)` matches the number 1 **and the text `"1"`**.

**The Malloy**

```malloy
run: sales -> {
  aggregate:
    count_numbers is count() { where: qty_num is not null }
    count_nonblank is count() { where: qty_raw is not null }
    countif_one is count() { where: qty_coerced = 1 }
    distinct_trap is count(qty_num)
}
```

| | Excel cached | Malloy |
|---|--:|--:|
| `COUNT` (`Report!B8`) | 9 | **9** |
| `COUNTA` (`Report!B9`) | 10 | **10** |
| `COUNTIF(.., 1)` (`Report!B10`) | 3 | **3** |
| `count(qty_num)` | | 6 |

**Verified:** `semantics-cited` (the `COUNT`/`COUNTIF` text-number split is the quirk).

**What it costs** The last row is the trap. In Malloy `count(field)` is a **distinct
count** (`count(qty_num)` is the six distinct values 1, 2, 3, 4, 5, 6) and
`count(distinct x)` is a parse error. It compiles and returns a plausible small
number. The row count is `count()` with a `where:` on null-ness. `COUNT` and
`COUNTIF(.., 1)` need *different* columns, `qty_num` and `qty_coerced`, because they
disagree about the text `"1"` on the same cell.

---

<a id="la6"></a>

## LA6 - `AVERAGE`, `AVERAGEIF(S)`

**The Excel**

```
Report!B7   =AVERAGE(Data!C2:C12)
```

**What it means** The mean of the numeric cells. Blanks and text are skipped, so the
denominator is 9, not 11.

**The Malloy**

```malloy
run: sales -> {
  aggregate:
    mean_qty is qty_num.avg()
    mean_qty_naive is qty_coerced.avg()
    west_mean is qty_num.avg() { where: lower(region) = 'west' }
    nobody_mean is qty_num.avg() { where: lower(region) = 'nowhere' }
}
```

| | Excel | Malloy |
|---|--:|--:|
| `Report!B7` | 2.8889 (cached) | **2.8889** (`qty_num`); 2.7 (`qty_coerced`, text `"1"` counted) |
| `AVERAGEIFS(.., "west")`, no cached cell | | 5.5 (`semantics-cited (hand-derived)`: (5 + 6) / 2, the text `"1"` West row skipped) |
| `AVERAGEIFS` with no match | `#DIV/0!` | null; compare a cached `#DIV/0!` to null only where the report states that mapping (`translate-formulas.md`, section 0) |

**Verified:** `semantics-cited`.

**What it costs** `avg()` skips nulls, so it matches `AVERAGE` as soon as the column
holds nulls for exactly the cells Excel skips. A blank read as 0 (`coalesce(qty, 0)`,
which is right for `*`) would put 11 in the denominator. Which column to average is the
same choice as in LA5. A no-match average is null, and the `#DIV/0!` it stands for is
not an oracle until the report says which cells wrap it in `IFERROR` and which do not.

---

<a id="la7"></a>

## LA7 - `SUMPRODUCT`, both shapes

**The Excel**

```
=SUMPRODUCT((Data!A2:A12="East")*Data!C2:C12*Data!E2:E12)       -- boolean mask
=SUMPRODUCT(Data!C2:C12, Data!E2:E12)                           -- plain product of ranges
```

**What it means** The classifier tells them apart (`sumproduct_mask`,
`sumproduct_product`). A mask is a conditional sum. A plain product is a sum of row
products; both are a `sum()` over an expression, never a join.

**The Malloy**

```malloy
run: sales -> {
  aggregate:
    mask_east is sum(coalesce(qty_coerced, 0) * price) { where: lower(region) = 'east' }
    mask_west_star is sum(coalesce(qty_coerced, 0) * price) { where: lower(region) = 'west' }
    mask_west_comma is sum(coalesce(qty_num, 0) * price) { where: lower(region) = 'west' }
    product_star is sum(coalesce(qty_coerced, 0) * price)
    product_comma is sum(coalesce(qty_num, 0) * price)
}
```

| Excel form | Excel | Malloy |
|---|---|--:|
| mask, East (`=` ignores case) | no cached cell | 77.75 (`semantics-cited (hand-derived)`: 30 + 20 + 7.25 + 0 + 20.5) |
| `(A="West")*C*E` (`*` form) | | 183 |
| `SUMPRODUCT((A="West")*1, C, E)` (comma form) | | 162.5 |
| `SUMPRODUCT(C*E)` | | 356.25 |
| `SUMPRODUCT(C, E)` | | 335.75 |

**Verified:** `semantics-cited (hand-derived)`. The Malloy was executed; the Excel
column is cited from Excel's documented behavior and was not run in Excel. The fixture
has no `SUMPRODUCT` cell.

**What it costs** The two forms differ on text. With `*`, a numeric-looking text
(`"1"`) is coerced; with comma-separated arrays, any non-number is treated as zero.
So the same cells give 183 and 162.5 for West, and 356.25 and 335.75 overall. The
arithmetic is `qty_coerced` for the first and `qty_num` for the second. Pick by the
punctuation in the formula. A text that does not look like a number would return
`#VALUE!` under `*`; the fixture has none.

---

<a id="la8"></a>

## LA8 - Exact lookups: `VLOOKUP(.., FALSE)`, `INDEX`/`MATCH(.., 0)`, `XLOOKUP`

**The Excel**

```
Lookup!H4   =VLOOKUP(750, A2:B6, 2, FALSE)                   -- cached #N/A
Lookup!H5   =IFERROR(VLOOKUP(750, A2:B6, 2, FALSE), 0)       -- cached 0
```

**What it means** An equality join on the key, returning the matching row's column. No
match is `#N/A`; `IFERROR` replaces it.

**The Malloy**

```malloy
run: probes extend {
  join_one: ex is rates_sorted on key = ex.lo
} -> {
  where: cell = 'H4' or cell = 'X1'
  group_by: cell, key
  aggregate:
    rate is ex.rate.max()
    rate_or_zero is ex.rate.max() ?? 0
  order_by: cell
}
```

| cell | key | Excel | Malloy `rate` | Malloy `rate_or_zero` |
|---|--:|---|---|--:|
| `H4` | 750 | `#N/A` (cached) | **null** | |
| `H5` (the same lookup in `IFERROR(.., 0)`) | 750 | 0 (cached) | | **0** |
| `X1` | 500 | no cached cell | 0.1 (`semantics-cited (hand-derived)`: `Lookup!B4`) | 0.1 |

**Verified:** `semantics-cited` (`#N/A` and `IFERROR` are quirk routes).

**What it costs** A `join_one` is a left join, so no match is null: **null stands for
`#N/A`** in the parity table, and `?? 0` stands for `IFERROR(.., 0)` /
`XLOOKUP(.., if_not_found)`. `IFERROR` also hides real errors (a divide by zero, a bad
reference). A faithful port of `IFERROR(x, 0)` hides them too; flag each one so the
owner decides. `XLOOKUP`'s default is an exact match, and `INDEX(rate, MATCH(key, thr,
0))` is the same join.

**Text keys are case-insensitive too.** `VLOOKUP("east", J2:K4, 2, FALSE)` and
`MATCH("EAST", J2:J4, 0)` find `East`. An equality join on the text finds nothing:

```malloy
run: text_probes extend {
  join_one: tk is text_keys on lower(tk.key_text) = lower(key)
  join_one: tk_naive is text_keys on tk_naive.key_text = key
} -> {
  group_by: cell, key
  aggregate:
    code is tk.code.max()
    pos_found is tk.pos.max()
    code_naive is tk_naive.code.max()
  order_by: cell
}
```

| cell | Excel cached | Malloy `lower()` join | Malloy plain join |
|---|--:|--:|---|
| `Lookup!H9` `VLOOKUP("east", ..)` | 1 | **1** (`code`) | null |
| `Lookup!H10` `MATCH("EAST", .., 0)` | 1 | **1** (`pos_found`) | null |

**Verified:** `semantics-cited`. `pos_found` is the row number the lift assigns
(`pos`), which equals `MATCH`'s position only because the source starts at the first
data row. Mind `lower()` on both sides: it matches Excel for ASCII keys; other scripts
are not exercised here.

**Wildcards in a key.** `VLOOKUP` and `HLOOKUP` with `range_lookup` FALSE, `MATCH` with
`match_type` 0, and the `COUNTIF`/`SUMIF`/`AVERAGEIF` families treat `*` and `?` in a text
lookup value as wildcards (`~` escapes), so `"a*"` matches `Abc`, not only a literal `a*`.
`XLOOKUP` and `XMATCH` do not unless `match_mode` is 2 (LA9); at the default exact mode
they compare literally. The `lower()` join above is a literal equality, so it is right for
`XLOOKUP` and differs from the others only on a key that holds `*` or `?`. For an exact-match
`VLOOKUP`/`HLOOKUP`/`MATCH`, check whether any lookup key or criterion cell holds `*` or
`?`: if none does, say so and accept the difference; if one does, port it as the anchored regular expression of LA3, or `like`-escape the key
(`\\%` and `\\_` in a `~` pattern, LA3) when the port must stay literal.
`semantics-cited`.

**A repeated key returns the first match.** `VLOOKUP`, `XLOOKUP` and `MATCH` over a key that
occurs more than once return the **first** match in sheet order; a `join_one` on a repeated
key fans out instead. A printed stanza has no row number, but `read_xlsx` returns rows in
sheet order, so add one in a wrapper over the printed stanza and join on the smallest row
per key:

```malloy
source: first_rate is duckdb.sql("""
  WITH r AS (
    SELECT *, row_number() OVER () AS row_n
    FROM (VALUES ('East', 1), ('east', 2), ('West', 3)) t(key_text, code)  -- the printed stanza goes here
  )
  SELECT r.* FROM r
  JOIN (SELECT lower(key_text) AS k, min(row_n) AS row_n FROM r GROUP BY 1) f
    ON lower(r.key_text) = f.k AND r.row_n = f.row_n
""")
```

`"east"` joins to code 1, the first of the two spellings. **Verified:** `semantics-cited`
(the SQL ran in DuckDB over the literal rows; the first-match rule is Excel's documented
one). Report the duplicated keys in the findings list: a repeated key in a lookup table is
usually a data defect, so say which keys repeat and ask whether to dedupe the source or keep
the first-match behaviour. The same `row_n` wrapper over an oracle read gives per-row parity.

In a real workbook the key is a column, not a literal. The same join on a column of the
fact source:

```malloy
run: sales extend {
  join_one: rs is rates_sorted on revenue >= rs.lo and (rs.hi is null or revenue < rs.hi)
} -> {
  group_by: rate is rs.rate
  aggregate: sales_rows is count(), total_revenue is revenue.sum()
  order_by: rate
}
```

returned rate 0: 10 rows, 253.75; rate 0.05: 1 row, 102.5 (`semantics-cited (hand-derived)`: only the
102.5 sale reaches the 100 threshold). That one is an approximate lookup (LA9).

---

<a id="la9"></a>

## LA9 - Approximate lookups: `VLOOKUP(.., TRUE)`, `LOOKUP`, `XLOOKUP` modes -1 and 1

**The Excel**

```
Lookup!H2   =VLOOKUP(750, A2:B6, 2, TRUE)        -- cached 0.1
Lookup!H3   =VLOOKUP(750, D2:E6, 2, TRUE)        -- the unsorted copy, cached 0
Lookup!H6   =VLOOKUP(-5, A2:B6, 2, TRUE)         -- cached #N/A
```

**What it means** The 4th argument `TRUE` (or omitted) searches a **sorted** table for
the last key at or below the target. It is a range join, not an equality join. Below the
first key the result is `#N/A`.

**The Malloy**

```malloy
run: probes extend {
  join_one: rs is rates_sorted on key >= rs.lo and (rs.hi is null or key < rs.hi)
} -> {
  where: cell = 'H2' or cell = 'H6'
  group_by: cell, key
  aggregate: rate is rs.rate.max()
  order_by: cell
}
```

| cell | key | Excel cached | Malloy |
|---|--:|---|---|
| `H2` | 750 | 0.1 | **0.1** |
| `H6` | -5 | `#N/A` | **null** |
| `H3` (unsorted copy) | 750 | 0 | 0.1 (does not match) |

`H3` joins the unsorted copy, read by value:

```malloy
run: probes extend {
  join_one: ru is rates_unsorted on key >= ru.lo and (ru.hi is null or key < ru.hi)
} -> {
  where: cell = 'H3'
  group_by: cell, key
  aggregate: rate is ru.rate.max()
}
```

**Verified:** `semantics-cited` for `H2` and `H6`. `H3` is a **finding**: the Malloy
does not reproduce the cached value.

The `[lo, hi)` bracket comes from `lead()` in the source (Setup). Excel's `TRUE`
search is a binary search, so on an unsorted table it returns whatever the probes land
on. `H3`'s 0 is the generator's model of that, and Excel's exact probe sequence on
unsorted data is unverified. Reading the same table as sorted gives 0.1, the
intended answer. **Do not try to reproduce the garbage.** For `VLOOKUP`/`HLOOKUP`/`LOOKUP`
(other than the last-match idiom below) with `TRUE` or no 4th argument, and `MATCH` with no 3rd, check the table for
sortedness first:

```malloy
run: rates_unsorted -> {
  aggregate: rows is count(), out_of_order is count() { where: prev_in_file > lo }
}
```

`rates_sorted` returned 0 out-of-order rows; `rates_unsorted` returned **2** of 5. A
table with out-of-order rows is a defect in the workbook: report the cell, show both
numbers, and port the intended lookup.

**`XLOOKUP` is different.** `match_mode` -1 (exact or next smaller) and 1 (exact or
next larger) scan linearly and are correct on an unsorted table; only `search_mode`
2 or -2 (binary search) assumes sorted data, and `match_mode` 2 is a wildcard match
(LA3), not a range join. So the same range joins, with brackets built by value, port
`XLOOKUP` correctly on the unsorted copy, and the sortedness check above does **not**
apply to them. Executed on the unsorted `D1:E6` (next smaller for 750 is the 500 row,
next larger is the 1000 row):

```malloy
run: probes extend {
  join_one: next_smaller is rates_unsorted on key >= next_smaller.lo and (next_smaller.hi is null or key < next_smaller.hi)
  join_one: next_larger is rates_unsorted on key <= next_larger.lo and (next_larger.prev_lo is null or key > next_larger.prev_lo)
} -> {
  where: cell = 'H3'
  group_by: cell, key
  aggregate:
    next_smaller_rate is next_smaller.rate.max()
    next_larger_rate is next_larger.rate.max()
}
```

returned 0.1 and 0.15 (`semantics-cited (hand-derived)`: `Lookup!E2` and `E6`; the
fixture has no `XLOOKUP` cell).

**What it costs** For `VLOOKUP`/`MATCH`/`LOOKUP` the join stands for the intended
answer only on a sorted table with unique keys, and the sortedness check is a step the
Excel formula never does. `lead()` and `lag()` run in the SQL source, so a table edited
in Excel is re-bracketed on the next load.

**`LOOKUP(2, 1/(condition), results)` is not an approximate search.** Dividing 1 by a
boolean array gives `#DIV/0!` where the condition is false and `1` where it is true,
and `LOOKUP` skips errors while it searches for a number larger than every `1`, so it
lands on the **last** row that meets the condition. The classifier flags it
`last_match_idiom` (route C) in place of `approx_match`: there is no sort order to
check and no range join to build. Port it as the result on the row with the greatest
row number that meets the condition, which needs the sheet row (or any column that
keeps the sheet order) in the source:

```
=LOOKUP(2, 1/(B2:B6<>""), C2:C6)    -- last non-blank status; with the rows below, 40
```

```malloy
source: ledger is duckdb.sql("""
  SELECT * FROM (VALUES (1, 'open', 10.0), (2, 'closed', 20.0), (3, NULL, 30.0), (4, 'open', 40.0), (5, NULL, 50.0))
    t(row_n, status, amount)
""")

run: ledger -> {
  where: status is not null and status != ''
  select: row_n, amount
  order_by: row_n desc
  limit: 1
}
```

returned row 4, amount 40; `aggregate: last_row is max(row_n)` over the same `where:`
returned 4. **Verified:** `semantics-cited` (the Excel side is hand-derived from
`LOOKUP`'s documented error-skipping; the Malloy compiled and ran in the Malloy runtime
over DuckDB). A text comparison inside the condition is case-insensitive in Excel
(`ci_match`, LA8).

---

<a id="la10"></a>

## LA10 - `MATCH` with the 3rd argument omitted

**The Excel**

```
Lookup!H7   =MATCH(750, A2:A6)         -- cached 3
Lookup!H8   =MATCH(750, D2:D6)         -- the unsorted copy, cached 2
```

**What it means** An omitted third argument is `1`: **approximate**, the position of
the largest value at or below the target, over sorted data. `XLOOKUP` defaults to
exact; `MATCH` does not.

**The Malloy**

```malloy
run: probes extend {
  join_many: rs is rates_sorted on rs.lo <= key
  join_many: ru is rates_unsorted on ru.lo <= key
} -> {
  where: cell = 'H7' or cell = 'H8'
  group_by: cell
  aggregate: sorted_position is rs.count(), unsorted_position_if_sorted is ru.count()
  order_by: cell
}
```

| cell | Excel cached | Malloy |
|---|--:|--:|
| `H7` (sorted) | 3 | **3** |
| `H8` (unsorted) | 2 | 3 (does not match) |

**Verified:** `semantics-cited` for `H7`; `H8` is a **finding**, for the same reason
as `H3` in LA9.

**What it costs** The position of the last key at or below the target equals the
**count** of keys at or below it, but only for a sorted column. `MATCH` is usually the
half of an `INDEX`/`MATCH` pair, and that pair is LA8 or LA9. A bare position is rarely
worth carrying into the model; ask what it was used for.

---

<a id="la11"></a>

## LA11 - Full-column references and totals rows

**The Excel**

```
Report!B12  =SUM(Data!F:F)         -- cached 712.5
Data!F13    =SUBTOTAL(109, tbl_Sales[Revenue])    -- the Table's totals row, cached 356.25
```

**What it means** `F:F` includes the Table's totals row, so the sheet adds the data
(356.25) and its own total (356.25).

**The Malloy**

```malloy
run: sales -> {
  aggregate: revenue_total is revenue.sum(), data_rows is count()
}
```

| | Excel cached | Malloy |
|---|--:|--:|
| `Data!F13` totals row | 356.25 | **356.25** (11 rows) |
| `Report!B12` `SUM(Data!F:F)` | 712.5 | 356.25 (**does not match, by design**) |

**Verified:** `executed`.

**What it costs** This recipe deliberately does **not** reproduce the cached value. The
source stops at row 12 (`recover-sources.md`), so Malloy gives the right total and the
workbook cell double counts. Report it as a defect in `Report!B12`, with both numbers.
Reproducing 712.5 would mean lifting the totals row as data, which is the double count
the lift rules exist to prevent. The totals row itself is a useful oracle: a Table's
`SUBTOTAL` should equal the Malloy aggregate of the same column. Mind the code:
`SUBTOTAL(109, ...)` ignores manually hidden rows and `SUBTOTAL(9, ...)` does not, while a
filter excludes its rows under both (see the `Ledger` example in `recover-sources.md`).

**Lookups over a full column.** A lookup range of `A:B` includes the header row, so a key equal to a header text hits the header. A blank lookup value matches nothing (`#N/A`), but a lookup that lands on a blank target cell returns `0`, not blank: port the blank target as `?? 0` when the cached value is 0. *(semantics-cited)*

---

<a id="la12"></a>

## LA12 - Hardcoded constants, plugs and `TODAY()`

**The Excel**

```
Report!B13  =B3*1.08                     -- cached 83.97
Report!I2:I9  =H2*2 ... (I6 holds the typed value 999)
Report!B14  =TODAY()-B2                  -- cached 181
```

**What it means** Three things the workbook did without saying so: a rate typed into a
formula, one cell of a copied-down region overwritten by hand, and a clock.

**The Malloy**

```malloy
run: sales -> {
  aggregate:
    east_with_uplift is revenue.sum() { where: lower(region) = 'east' } * $UPLIFT
    days_since_start is max(days($REPORT_START to $AS_OF))
}
```

```malloy
run: units -> { select: month_no, units, doubled; order_by: month_no }
```

| Cell | Excel cached | Malloy |
|---|--:|--:|
| `Report!B13` | 83.97 | **83.97** (`77.75 * $UPLIFT`) |
| `Report!B14` | 181 | **181** (`AS_OF` = 2024-06-30) |
| `Report!I2:I9` months 1-4, 6-8 | 20, 40, 60, 80, 120, 140, 160 | **same** |
| `Report!I6` month 5 | **999** | 100 (**does not match**) |

**Verified:** `executed` for the plug and the copied-down cells; `semantics-cited` for
the constant (`B13` multiplies `B3`, a case-insensitive `SUMIFS`) and for
`TODAY()`: the generator pins `TODAY` to the same date it then uses as the oracle, so
181 proves nothing about Excel, and Excel's `TODAY` is the local date while
`dcterms:modified` is a UTC stamp.

**What it costs**

- **The constant** becomes a `given:` with the typed value as default (`UPLIFT`), and a
  question: is 1.08 a tax rate, a markup, or a stale one? The classifier flags it
  (`hardcoded_constant`); never leave the literal inline. The literal `750` in the
  `Lookup` formulas is the same flag: it is a test key, not a column.
- **The plug** is not translated. `I6` was overwritten with 999, so the region's
  formula gives 100 and the sheet shows 999. Show both and ask the owner which is
  right; do not make the dimension return 999 for month 5. A typed text or boolean in the
  middle of a run is flagged `typed_overwrite` on both neighbouring regions: report it as a
  workbook defect and keep the typed value as a given, never as a rule of the formula.
- **`TODAY()`** becomes a `given:` pinned to the file's saved date, so the parity run
  is reproducible. Whether a live version should use the current date instead is a
  decision for the user.

---

<a id="la13"></a>

## LA13 - `RANK`, `MEDIAN`, `PERCENTILE`, `STDEV`

Rank is a window in Malloy; the distribution statistics are DuckDB functions in a
`duckdb.sql` stanza. The numbers below come from four rows, `10, 20, 20, 40`, with a tie:

```malloy
source: scores is duckdb.sql("""
  SELECT * FROM (VALUES ('a', 10), ('b', 20), ('c', 20), ('d', 40)) t(k, x)
""")
```

```malloy
run: scores -> {
  group_by: k, x
  calculate: rank_desc is rank()
  order_by: x desc
}
```

`RANK.AVG` in Malloy: number the rows in rank order, then average the number per value (ran on a
live Publisher, 2.5 for the two tied 20s of `10, 20, 20, 40` ranked descending):

```malloy
run: scores -> {
  group_by: k, x
  calculate: rn is row_number()
  order_by: x desc
} -> {
  group_by: x
  aggregate: rank_avg is rn.avg()
  order_by: x desc
}
```

`median` and `quantile_cont` are DuckDB functions, not Malloy ones (`Unknown function`, and a
`!`-call is refused in a restricted query), so they work only in a stanza:

```malloy
source: scores_stats is duckdb.sql("""
  SELECT median(x) AS med, quantile_cont(x, 0.9) AS p90_inc,
         stddev_samp(x) AS sd_s, stddev_pop(x) AS sd_p,
         var_samp(x) AS var_s, var_pop(x) AS var_p
  FROM (VALUES (10), (20), (20), (40)) t(x)
""")
```

| Excel | Port | Value on `10, 20, 20, 40` | Gotcha |
|---|---|--:|---|
| `RANK`, `RANK.EQ(x, range, 0)` | `rank()` in `calculate:`, `order_by: x desc` | `b` and `c` rank 2, `a` ranks 4 | ties share a rank and the next is skipped; `, 1` (ascending) is `order_by: x asc` |
| `RANK.AVG` | number the rows with `row_number()` in the same order, then average that number per value (below) | `b` and `c` rank 2.5 | averaging the tied `rank()` values is wrong: both are 2, so the average is 2, not 2.5 |
| `MEDIAN` | `median(x)` inside a `duckdb.sql` stanza only; not a Malloy function | 20 | skips blanks and text, as DuckDB skips null; a text number in the column is the lift's problem (`recover-sources.md`) |
| `PERCENTILE`, `PERCENTILE.INC`, `QUARTILE` | `quantile_cont(x, k)` inside a `duckdb.sql` stanza only; not a Malloy function. `k` must be a constant there, so a given-driven percentile is interpolated by hand (below) | 34 at `k = 0.9` | linear interpolation at position `1 + k * (n - 1)`; `quantile_disc` is a different function and returns a data value |
| `PERCENTILE.EXC`, `QUARTILE.EXC` | none direct | n/a | interpolates at `k * (n + 1)` and errors outside `1/(n+1)` to `n/(n+1)`; port by hand or route NR |
| `STDEV`, `STDEV.S` | Malloy's `x.stddev()` (sample; ran on a live Publisher), or `stddev_samp` in a stanza | 12.583 | sample, `n - 1` |
| `STDEVP`, `STDEV.P` | `stddev_pop` in a stanza | 10.897 | population, `n` |
| `VAR`, `VAR.S` | `var_samp` in a stanza, or `pow(x.stddev(), 2)` | 158.33 | the variance, not the standard deviation: the square of the `STDEV.S` row |
| `VARP`, `VAR.P` | `var_pop` in a stanza | 118.75 | the square of the `STDEV.P` row |

**A given-driven `PERCENTILE`** (`k` is a given, so `quantile_cont` cannot take it) is interpolated
by hand, *(semantics-cited)*: sort ascending and number the rows `i` with `row_number()`; with `n`
rows the position is `p = k * (n - 1) + 1`; the answer is `x_floor(p) + (p - floor(p)) * (x_ceil(p) - x_floor(p))`,
taking `x` at the rows numbered `floor(p)` and `ceil(p)` (the same row when `p` is whole). Check it
against the stanza's `quantile_cont` at a constant `k`.

`semantics-cited (hand-derived)`: the median is the mean of the middle two, 20; the 0.9
percentile sits at position 3.7, `20 + 0.7 * 20 = 34`; the sums of squares are 475, so
the sample variance is `475 / 3 = 158.33` and the population variance `475 / 4 = 118.75`, and their
square roots are `12.583` and `10.897`. The stanza above ran on DuckDB 1.5.5 and returned those
six figures, and `pow(x.stddev(), 2)` returned 158.33 as a Malloy measure *(`executed`)*. A DuckDB function that Malloy does not wrap, such as `SKEW`, `KURT`, `median` or
`quantile_cont`, is refused as a `!`-call in a Publisher query: compute it inside a `duckdb.sql`
stanza, or fall back to raw moments (`sum(x)`, `sum(x*x)`, `sum(x*x*x)` and the textbook formula). A `RANK` whose range drifts down a copied formula (`$B$2:$B$10`, then
`$B$3:$B$11`) is a workbook defect: reproduce it and list it as a finding
(`parity.md` section 9).

---

<a id="la14"></a>

## LA14 - Distinct counts and duplicate flags: `SUMPRODUCT(1/COUNTIF(rng, rng))`

**The Excel**

```
=SUMPRODUCT(1/COUNTIF(A2:A9, A2:A9))              -- distinct count
=SUMPRODUCT((A2:A9<>"")/COUNTIF(A2:A9, A2:A9&"")) -- the blank-guarded form
B2          =COUNTIF($A$2:A2, A2)>1                -- duplicate flag, copied down
```

**What it means** Each row contributes 1/(its value's frequency), so a value seen n times sums to 1. `COUNTIF` ignores case, so `East` and `east` are one value. It is a distinct count, not a product, though the classifier flags it `sumproduct_product`. On a truly blank cell the first form divides by zero (`#DIV/0!`); the guarded form skips blanks, and it skips an empty string `""` too, because `A2<>""` is false for both.

**The Malloy**

```malloy
run: sales -> {
  aggregate:
    distinct_regions is count(nullif(lower(region), ''))
}
```

`count(x)` is Malloy's distinct count over non-null values (LA5), so a blank (null) region is skipped. The guarded form also skips `""`, so `nullif(..., '')` turns it into null first; `count(lower(region))` alone counts `""` as one more value and over-counts by one against the guarded form, and `count(coalesce(lower(region), ''))` does the same for a null. `lower()` merges the spellings, as `COUNTIF` does. *(`executed` on DuckDB 1.5.5 over `a, A, b, null, ''`: the guarded form returned 2, and the other two returned 3. Excel side `semantics-cited`)*

**The expanding-range duplicate flag.** `COUNTIF($A$2:A2, A2) > 1` is true from the second occurrence of a value on, in sheet order, ignoring case. Compute it in the stanza wrapper, where `sheet_row` is available (the first-match recipe in LA8):

```sql
row_number() OVER (PARTITION BY lower(x) ORDER BY sheet_row) > 1 AS is_dup
```

On `a, A, b, B, a` it returns false, true, false, true, true (run on DuckDB 1.4.5). The first spelling is kept, the later ones flagged, so the order must be the sheet order. Label the Excel side `semantics-cited`.

---

<a id="la15"></a>

## LA15 - Database functions: `DSUM`, `DCOUNT`, `DAVERAGE`, `DMAX`, `DMIN`, `DGET`

**The Excel**

```
G1:I3   region | product | amt      <- criteria range: header cells, then criteria rows
        East   | W       |
        West   |         | >20
H8      =DSUM(A1:C8, "amt", G1:I3)
```

**What it means** `DSUM(database, field, criteria)` aggregates the `field` column of the database rows that satisfy the criteria range. The first criteria row is a header (it must repeat the database's header text); each row below it is one alternative, so **rows are OR and the cells within a row are AND**. A blank criteria cell adds no condition, and a fully blank row matches every row. A text criterion is a **begins-with** match, ignoring case (`W` matches `Widget` and `widget2`); `=East` requires the whole value; `>20`, `<=5` and `<>x` are operators inside the cell. `DCOUNT` counts the numeric cells of the field, `DCOUNTA` the non-blank ones, and `DGET` returns `#VALUE!` when no row matches and `#NUM!` when more than one does. The classifier routes these C.

**The Malloy** The criteria range is cells, so it is a `given:` or a small source, never a hardcoded filter; the `where:` is built from it. For the range above, written out:

```malloy
run: orders -> {
  aggregate:
    dsum is amt.sum() {
      where: (lower(region) ~ 'east%' and lower(product) ~ 'w%')
          or (lower(region) ~ 'west%' and amt > 20)
    }
}
```

Each criteria row is one parenthesised `and` group and the rows are joined with `or`. The `~` carries the LA3 caveat: a `%` or `_` typed into a criterion over-matches, so use the anchored regular expression there. `DAVERAGE`, `DMAX`, `DMIN` and `DCOUNT` swap `amt.sum()` for `amt.avg()`, `amt.max()`, `amt.min()` and a count of the numeric cells; `DGET` is a `where:` plus a check that the result has exactly one row.

| Rows `region, product, amt` | Excel `DSUM` | Check |
|---|--:|---|
| `East Widget 10`, `East Gadget 5`, `East Widget2 7`, `West Widget 30`, `West Gizmo 15`, `West Gizmo 25`, `east widget 1` | 73 | 18 (row 1: 10 + 7 + 1) + 55 (row 2: 30 + 25) |

The SQL equivalent, `sum(amt) FILTER (WHERE (lower(region) LIKE 'east%' AND lower(product) LIKE 'w%') OR (lower(region) LIKE 'west%' AND amt > 20))`, ran on DuckDB 1.4.5 over those rows and returned 73 (18 and 55 for the two rows alone). The lower-case `east widget` is included, which is the case-insensitive begins-with. An exact `=East` form, `lower(region) = 'east'`, gave 23 on the same rows. *(Excel side `semantics-cited`; compile-check the Malloy fragment)*

---

<a id="la16"></a>

## LA16 - Aggregating over the groups of another aggregate

**The Excel**

```
=B2/SUM(B$2:B$4)     -- share of total, over a column of group totals
=MAX(B2:B4)          -- the largest group, or LARGE(B2:B4, 1)
=AVERAGE(B2:B4)      -- the average across groups, not across rows
```

**What it means** `B2:B4` holds group totals (each a `SUMIFS`), and the formula aggregates those cells. A single Malloy aggregate cannot do that: `v.avg()` averages rows, not groups.

**The Malloy** A two-stage query: stage 1 groups and aggregates, stage 2 aggregates over stage 1's rows (the shape LA13 uses for `RANK.AVG`).

```malloy
run: lines -> {
  group_by: g
  aggregate: tot is v.sum()
} -> {
  aggregate:
    avg_group_total is tot.avg()
    max_group_total is tot.max()
}
```

A share of total stays in one stage with `all()` (the P4 form): `aggregate: tot is v.sum(), share is v.sum() / all(v.sum())`. In stage 2 of the pipeline above, keep the group rows with `group_by: g, tot` and aggregate the stage 1 column: `aggregate: share is tot.sum() / all(tot.sum())`. (`calculate: share is tot / all(tot)` does not compile: `all()` takes an aggregate.) To keep the group rows beside the figure, `nest:` the group view inside an outer `aggregate:` query instead of repeating the stages.

Over groups `a` (10 + 30), `b` (20 + 40) and `c` (60), the group totals are 40, 60, 60 (sum 160): the shares are 0.25, 0.375, 0.375, the maximum is 60 and the average across groups is 53.33, against 32 for the row average. The one-stage `all()` form, the stage-2 `group_by: g, tot` form and the `avg`/`max` stage all compiled and returned these figures on DuckDB 1.5.5 over a `VALUES` table of those rows. *(`executed`)*

---

<a id="la17"></a>

## LA17 - Many oracle stanzas, one source

A workbook with hundreds of report regions does not need hundreds of registered sources. Take the printed oracle stanzas' text unchanged (`parity.md` section 3) and combine them in ONE `duckdb.sql` source with `UNION ALL`, adding a `region_id` column so each branch stays identifiable:

```malloy
source: oracle_all is duckdb.sql("""
  SELECT 'R1' AS region_id, row_number() OVER () AS row_n, "A" AS label, "B" AS cached
  FROM read_xlsx('data/book.xlsx', sheet = 'Report', range = 'A3:B15', header = false)
  UNION ALL
  SELECT 'R2', row_number() OVER (), "A", "B"
  FROM read_xlsx('data/book.xlsx', sheet = 'Report', range = 'A20:B31', header = false)
""")
```

Compare per region with `group_by: region_id`. Every branch must return the same column count and compatible types, so read cells as text (`all_varchar = true`) when a region holds error values. The `parity.md` timeout warning applies to hundreds of SEPARATE sources; one combined source registered in seconds in practice *(observed once; not a guarantee)*. The `UNION ALL` shape itself ran on DuckDB 1.4.5.

---

## Parity contexts

Each reporting measure is checked in **two filter contexts**, and the second one is
chosen to hit the case-insensitive, blank and text-number paths again. Context 1 is
`Report!B3:B15`. Context 2 is `Report!D3:D15`: another spelling (`WEST`, `NORTH`), other
operators (`<>`, `N*`, `COUNTIFS` with `">=0"` and `"<>"`), and the criterion cells
overridden through the request's `givens`: `MIN_QTY = 3` (`Report!D1`) and
`REPORT_START = 2024-06-01` (`Report!D2`).

**Tolerance.** Counts compare exactly. Sums, averages and products compare with an
absolute tolerance of **1e-9**, on the unrounded numbers (the table prints full
precision; a displayed `2.89` is not a comparison). The `Forecast` circularity needs 0.01
(`fixtures/README.md`) and is not in this file.

Context 1, no overrides:

```malloy
run: sales -> {
  aggregate:
    b3 is revenue.sum() { where: lower(region) = 'east' }
    b4 is revenue.sum() { where: qty_num >= $MIN_QTY }
    b5 is revenue.sum() { where: lower(region) ~ '%st' }
    b6 is revenue.sum() { where: (region ?? '') = '' }
    b7 is qty_num.avg()
    b8 is count() { where: qty_num is not null }
    b9 is count() { where: qty_raw is not null }
    b10 is count() { where: qty_coerced = 1 }
    b11 is count() { where: lower(region) = 'east' }
    b12 is revenue.sum()
    b13 is revenue.sum() { where: lower(region) = 'east' } * $UPLIFT
    b14 is max(days($REPORT_START to $AS_OF))
    b15 is revenue.sum() { where: order_date >= $REPORT_START }
}
```

Context 2, with `givens: {"MIN_QTY": 3, "REPORT_START": "2024-06-01"}`:

```malloy
run: sales -> {
  aggregate:
    d3 is revenue.sum() { where: lower(region) = 'west' }
    d4 is revenue.sum() { where: qty_num >= $MIN_QTY }
    d5 is revenue.sum() { where: lower(region) ~ 'n%' }
    d6 is revenue.sum() { where: region is not null }
    d7 is qty_num.avg() { where: lower(region) = 'west' }
    d8 is count() { where: lower(region) = 'west' and qty_num >= 0 }
    d9 is count() { where: lower(region) = 'west' and qty_raw is not null }
    d10 is count() { where: qty_coerced = 2 }
    d11 is count() { where: lower(region) = 'north' }
    d12 is revenue.sum() { where: lower(region) = 'west' }
    d13 is revenue.sum() { where: lower(region) = 'west' } * $UPLIFT
    d14 is max(days($REPORT_START to $AS_OF))
    d15 is revenue.sum() { where: order_date >= $REPORT_START }
}
```

## Parity summary

Every cell below was cached by the workbook and compared with the Malloy above, in
both contexts, on the unrounded values. The Malloy column is what Publisher returned;
the Excel column is the cached value (an error is printed as Excel prints it, and
`null` stands for `#N/A`).

| Cell | Check | Excel cached | Malloy | abs diff | Within 1e-9 | Label |
|---|---|--:|--:|--:|---|---|
| `Report!B3` | ctx1 SUMIFS east | 77.75 | 77.75 | 0 | yes | `semantics-cited` |
| `Report!B4` | ctx1 SUMIFS >= A1 | 335.75 | 335.75 | 0 | yes | `semantics-cited` |
| `Report!B5` | ctx1 SUMIFS *st | 260.75 | 260.75 | 0 | yes | `semantics-cited` |
| `Report!B6` | ctx1 SUMIFS blank | 41.0 | 41 | 0 | yes | `semantics-cited` |
| `Report!B7` | ctx1 AVERAGE | 2.888888888888889 | 2.888888888888889 | 0 | yes | `semantics-cited` |
| `Report!B8` | ctx1 COUNT | 9.0 | 9 | 0 | yes | `semantics-cited` |
| `Report!B9` | ctx1 COUNTA | 10.0 | 10 | 0 | yes | `semantics-cited` |
| `Report!B10` | ctx1 COUNTIF 1 | 3.0 | 3 | 0 | yes | `semantics-cited` |
| `Report!B11` | ctx1 COUNTIF east | 5.0 | 5 | 0 | yes | `semantics-cited` |
| `Report!B12` | ctx1 SUM(F:F) | 712.5 | 356.25 | 356.25 | **no** | `executed` |
| `Report!B13` | ctx1 B3*1.08 | 83.97 | 83.97 | 0 | yes | `semantics-cited` |
| `Report!B14` | ctx1 TODAY()-B2 | 181.0 | 181 | 0 | yes | `semantics-cited` |
| `Report!B15` | ctx1 SUMIFS date | 289.0 | 289 | 0 | yes | `semantics-cited` |
| `Report!D3` | ctx2 SUMIFS WEST | 183.0 | 183 | 0 | yes | `semantics-cited` |
| `Report!D4` | ctx2 SUMIFS >= D1 | 232.5 | 232.5 | 0 | yes | `semantics-cited` |
| `Report!D5` | ctx2 SUMIFS N* | 54.5 | 54.5 | 0 | yes | `semantics-cited` |
| `Report!D6` | ctx2 SUMIFS <> | 315.25 | 315.25 | 0 | yes | `semantics-cited` |
| `Report!D7` | ctx2 AVERAGEIFS west | 5.5 | 5.5 | 0 | yes | `semantics-cited` |
| `Report!D8` | ctx2 COUNTIFS west numeric | 2.0 | 2 | 0 | yes | `semantics-cited` |
| `Report!D9` | ctx2 COUNTIFS west non-empty | 3.0 | 3 | 0 | yes | `semantics-cited` |
| `Report!D10` | ctx2 COUNTIF "2" | 3.0 | 3 | 0 | yes | `semantics-cited` |
| `Report!D11` | ctx2 COUNTIF NORTH | 2.0 | 2 | 0 | yes | `semantics-cited` |
| `Report!D12` | ctx2 SUMIFS F:F west | 183.0 | 183 | 0 | yes | `semantics-cited` |
| `Report!D13` | ctx2 D3*1.08 | 197.64000000000001 | 197.64000000000001 | 0 | yes | `semantics-cited` |
| `Report!D14` | ctx2 TODAY()-D2 | 29.0 | 29 | 0 | yes | `semantics-cited` |
| `Report!D15` | ctx2 SUMIFS date >= D2 | 76.0 | 76 | 0 | yes | `semantics-cited` |
| `Data!F13` | totals row | 356.25 | 356.25 | 0 | yes | `executed` |
| `Lookup!H2` | VLOOKUP TRUE | 0.1 | 0.1 | 0 | yes | `semantics-cited` |
| `Lookup!H3` | VLOOKUP TRUE unsorted | 0.0 | 0.1 | 0.1 | **no** | `semantics-cited` |
| `Lookup!H4` | VLOOKUP FALSE | #N/A | null | n/a | yes | `semantics-cited` |
| `Lookup!H5` | IFERROR | 0.0 | 0 | 0 | yes | `semantics-cited` |
| `Lookup!H6` | VLOOKUP TRUE below | #N/A | null | n/a | yes | `semantics-cited` |
| `Lookup!H7` | MATCH sorted | 3.0 | 3 | 0 | yes | `semantics-cited` |
| `Lookup!H8` | MATCH unsorted | 2.0 | 3 | 1 | **no** | `semantics-cited` |
| `Lookup!H9` | VLOOKUP text key | 1.0 | 1 | 0 | yes | `semantics-cited` |
| `Lookup!H10` | MATCH text key | 1.0 | 1 | 0 | yes | `semantics-cited` |
| `Report!I2` | copied-down doubled, month 1 | 20.0 | 20 | 0 | yes | `executed` |
| `Report!I3` | copied-down doubled, month 2 | 40.0 | 40 | 0 | yes | `executed` |
| `Report!I4` | copied-down doubled, month 3 | 60.0 | 60 | 0 | yes | `executed` |
| `Report!I5` | copied-down doubled, month 4 | 80.0 | 80 | 0 | yes | `executed` |
| `Report!I6` | copied-down doubled, month 5 | 999.0 | 100 | 899 | **no** | `executed` |
| `Report!I7` | copied-down doubled, month 6 | 120.0 | 120 | 0 | yes | `executed` |
| `Report!I8` | copied-down doubled, month 7 | 140.0 | 140 | 0 | yes | `executed` |
| `Report!I9` | copied-down doubled, month 8 | 160.0 | 160 | 0 | yes | `executed` |

`Report!B13`, `D12` and `D13` depend on a case-insensitive `SUMIFS` (`B3`, `D3`, and `D12` itself), so
they are `semantics-cited` like the rows they build on.

Four rows are outside tolerance, and they are the useful output. `Report!B12`, `I6`,
`Lookup!H3` and `H8` are defects in the workbook, not in the port: a full-column sum
that includes its own totals row, a hand-typed plug, and two lookups over an unsorted
table. `Report!D12`, the same full-column reference with a criterion that excludes the
totals row, matches. The text number and the text date in LA2 match the cache only
because the port reproduces what Excel skipped; they are data decisions.

Not covered here: pivots (`cookbook-pivot.md`) and the `Forecast` sheet
(`cookbook-scenario.md`). A `semantics-cited` row is as strong as the fixture's engine
(`python`): it shows the port agrees with the generator's model of Excel, not with an
Excel save.
