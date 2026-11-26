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
v1.5.5) against the repository's `fixtures/fixture.xlsx` (not shipped with this skill; or on DuckDB 1.5.5 over the rows the entry shows), and the
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

LA9 onward (approximate lookups through the parity contexts and summary) is in `reference/cookbook-lookup-aggregate-2.md`.
