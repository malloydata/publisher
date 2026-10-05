<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Cached-Value Parity (Step 5)

> An `.xlsx` stores the last computed value of every formula cell next to the formula.
> That makes it the one migration source with its own answer key: every cell can be
> compared, mechanically, at every context the workbook already shows. This file says
> what to compare, how to read the key, when not to trust it, and what a mismatch means.

Read `translate-formulas.md` section 0 for the verdict on the cache, and `discover.md` for
how the file was classified. Parity is the step that decides whether the migration is
trusted: coverage counts (`review-coverage.md`) describe effort, parity numbers describe
correctness.

## 1. The oracle gate

Do not compare anything until the **Oracle** section says the cache can be trusted. Every
row below is a tell the classifier prints with a count.

**Untrusted: ask for a recalculated save.** Any one of these makes the whole file's cache
untrusted, because it means a library, not Excel, wrote the numbers.

| Tell | Why |
|---|---|
| `full_calc_on_load` | `fullCalcOnLoad="1"`: the writer expects Excel to recompute on open. |
| `missing_calc_id` | No `calcPr calcId`. Excel always writes one. |
| `formula_without_v` | A formula cell with no `<v>`. Libraries that write `<v>0</v>` for every formula write a wrong value, not a missing one, which is why `full_calc_on_load` alone is enough. |
| `manual_calc` | `calcMode="manual"`: values may be stale. |
| `calc_on_save_off` | `calcOnSave="0"`: the cache may predate the last edit. |

`docProps/app.xml` `Application: Microsoft Excel` is **not** a tell: some libraries write
it too. "Saved by Excel" means a `calcId` and a cached value on every
formula, and the report says so in the Oracle section when both hold:
`cache looks Excel-saved: calcId 191029, 304 formula cells all cached` (`oracle.excel_saved`
in `--json`; the `calcId` is also printed on the `Cached values` line, or `none`). It is
the absence of the untrusted tells above, not proof: a library can copy both.
The `volatile` caveat names the functions it actually found, and its pin advice
applies to `TODAY`/`NOW` only. The bundled fixture is untrusted on purpose (`full_calc_on_load`), so its parity
is against the generator's model of Excel, which is why most rows below are
`semantics-cited`.

**When the cache is untrusted and no recalculated save will ever come** (the author is gone, the file is all there is), do not compare against it. Hand-derive the expected value at **at least two contexts** per reporting measure from the lifted data, run the Malloy, and label each row `semantics-cited (hand-derived)`; never `executed (Excel)`, since no Excel run backs it. The cache may be read **after** the derivation, and whether it agrees is recorded as `cache_inspection` (informational: it is the generator's number, not parity, and a disagreement is a note, not a mismatch).

**Caveats: compare, but say what the number is.**

| Tell | What the cached value is | Do |
|---|---|---|
| `volatile` | as of the last calculation. `TODAY`/`NOW` change at every open. | Pin `TODAY`/`NOW` to `docProps/core.xml` `dcterms:modified` and expose it as a `given:` (`AS_OF`, `cookbook-lookup-aggregate.md#la12`). `RAND*` is X: never compared cell for cell (`cookbook-scenario.md#sc7`). `OFFSET`/`INDIRECT` are NR. |
| `cached_errors` | `t="e"` cells: `#N/A`, `#DIV/0!`, `#VALUE!` | `#N/A` is a legitimate no-match oracle (Malloy null). Others map to null only with the `IFERROR` routing stated: say which cells the workbook wraps and which it does not. An error value in the cache matches null, never a number: label that row `semantics-cited` (error cell == null in the port). Port a division as `a / nullif(b, 0)`: a bare `/` in DuckDB gives NaN for `0/0` and inf for `x/0`, which no cached error matches. Read the cell as text to tell an error from an empty value (below). |
| `pivot_snapshot` | the pivot as of `refreshedDate`, from `pivotCacheRecords` | Name which two things were compared (`cookbook-pivot.md`, "The snapshot problem"). A `pivot predates last save` line is an advisory, not an error: the cache may still match, so compare the pivot cells against the current data and record the outcome either way (match, or a mismatch attributed to the stale pivot). |
| `subtotal_ui_state` | `SUBTOTAL(1xx)` and `AGGREGATE` over an autofiltered or hidden range depend on what the user had hidden when the file was saved | Never bake the filter into the model. The code matters only for manually hidden rows (`SUBTOTAL(9)` counts them, `SUBTOTAL(109)` does not; `recover-sources.md`, `Ledger`); a filter excludes its rows under every code, so the port needs the filter as a `where:`, reported as a finding. |
| `autofilter`, `slicers`, `timelines` | UI state; the filtered columns and filter types of a sheet or an Excel Table are listed (value lists only on visible sheets) | Not part of the model. A number that depends on it is a number about a screen. |
| `date_serial_60` | a date-styled cell holding the phantom 1900-02-29 | Not a date; no translation maps it back (`recover-sources.md#dates`). |
| `external_link_snapshot` | cells that read through an `externalLink`, directly (`external_ref`) or through a defined name that points into another workbook (`external` in `defined_names`) | Flagged, **not compared**. The target workbook is never resolved, and the cached value is a snapshot as of the last link refresh. |
| `external_data` | the sheet is a cache of a database | The cache is as old as the last refresh. Decide the data question first (`discover.md`). |
| `code_snapshot` | values produced by an add-in, a feed, a UDF or Python in Excel | **Matches as of save, at best.** A snapshot, not an oracle. |

## 2. Three granularities

1. **Per cell.** Every cell of a report region that is a formula: the cached value against
   the Malloy value at the context the formula states. Cheap and exact.
2. **Per region.** A region is one formula copied over N cells. Compare every cell when
   N is small (`Report!I2:I9`, 8 cells), and otherwise the first, the last, a handful in
   between and **every cell the classifier names in `plug_candidates`**. A plug cell
   (`Report!I6` = 999 inside a copied-down `=H*2`) is the point of the check, and it is
   invisible if you sample. A typed text, boolean or error cell inside a run is the same
   defect: the classifier names it as `typed_overwrite` on both neighbouring regions.
3. **Lifted data.** For every source: the row count against the sheet, and one column sum
   against the sheet's own total (an Excel Table's totals row is the best oracle:
   `Data!F13` caches 356.25).

## 3. Reading the oracle

`read_xlsx` returns each cell's cached value at full precision. Reading it is how you build
the key; do it in the model so it is one query.

The classifier prints that read for you: each unmasked region (formula, array, spill, what-if data table) and each pivot with a
known location carries an `oracle_stanza` in `--json` (null on a hidden sheet), a `read_xlsx` of
exactly that region's own cells (an array or spill region counts every cell of its ref), marked ORACLE in its first comment. Run it as printed. Reading
the oracle is not lifting a source, so it does not break the rule never to widen a source stanza
or hand-write a range for one: a source stanza is still the only way data enters the model. Edit
an oracle read only for what the examples below show (`all_varchar = true` for an error cell). If Malloy rejects a printed column name, quote it with backticks or rename it in a wrapper `SELECT ... AS`. A `row_number() OVER () AS row_n` in that wrapper gives per-row parity: the read order is the sheet order, and a printed stanza carries no row number of its own.
A run past the 2000-stanza cap says how many were left out; those oracle reads are the only ranges written by hand.
The oracle read has `header = false`, so it returns columns named by sheet column (`A`, `B`, ...), plus the `row_n` (or `sheet_row`) the wrapper adds.
A printed stanza names the workbook by its bare file name, so the workbook must sit in the package folder next to the model: copy it in. Keep scratch `.malloy` files outside the package folder until they are registered.
When a lifted stanza is missing or too narrow, log `stanza_gap`, copy nothing across by hand and ask the user; the one exception is the oracle read above, which is printed. Typed inputs a stanza leaves out now appear as given candidates instead.
A region on a hidden sheet has `hidden_dep: true` and no oracle read: its route is unchanged,
but it is a hidden dependency the key cannot compare, so say so in the findings.
A visible region that reads a hidden sheet (`hidden_dep_via: "direct"`), or reads a region that does (`"transitive"`),
is just as unverifiable: report it the same way. The flag is conservative (it propagates through labels and unevaluated `IF` branches): when most of a model is `hidden_dep`, ask for the hidden toggles, then compare.

```malloy
// Report!A3:D15: label, context 1, an empty column, context 2
source: report_cached is duckdb.sql("""
  SELECT row_number() OVER () + 2 AS sheet_row, "A" AS label, "B" AS ctx1, "D" AS ctx2
  FROM read_xlsx('data/fixture.xlsx', sheet = 'Report', range = 'A3:D15', header = false)
""")

// Lookup!G2:H8 holds #N/A cells, so read the cached values as text
source: lookup_cached is duckdb.sql("""
  SELECT row_number() OVER () + 1 AS sheet_row, "G" AS label, "H" AS cached
  FROM read_xlsx('data/fixture.xlsx', sheet = 'Lookup', range = 'G2:H8', header = false, all_varchar = true)
""")
```

```malloy
run: report_cached -> { select: sheet_row, label, ctx1, ctx2; order_by: sheet_row; limit: 3 }
```

```malloy
run: lookup_cached -> { select: sheet_row, label, cached; order_by: sheet_row }
```

**Observed once:** `col.count()` inside a `%{ }` given or query expression made registration or compile return HTTP 500; use a precomputed measure instead.

**Scale.** A model with hundreds of SEPARATE `duckdb.sql` sources can time out at registration: split it across several packages, with the sources grouped by sheet, or combine the oracle stanzas into one `UNION ALL` source with a `region_id` column (`cookbook-lookup-aggregate.md#la17`; one such source registered in seconds in practice, observed once). A Publisher ad hoc query (`run:` sent to the endpoint) cannot call `duckdb.sql` itself, so every stanza lives in the model file and the query names the source. *(semantics-cited; not run here)*

A formula whose result is the empty string `""` is read back from the cache as NULL, so the key cannot tell `""` from a blank on a formula cell: compare with `coalesce(x, '')` on both sides and say so.

Executed: `Report!B7` comes back `2.888888888888889`, not the `2.8889` its cell format
displays, so **compare values, never what `numFmt` shows**. A numeric read of `Lookup!H4`
fails (`Could not convert string '#N/A' to DOUBLE`), and with `all_varchar = true` the
error comes back as the text `#N/A`, so the key can tell an error from a blank. Read the
numbers back with `try_cast`. Label: `executed`.

**Reading a cache a non-Excel producer wrote.**

- **Booleans where Excel would store numbers.** A formula body coerced with `--`, `N()` or arithmetic on a comparison can be cached as `t="b"` by some producers. Compare booleans as 1 and 0.
- **Short ratios.** Such a producer's cached ratios can carry only about 10 significant digits. Compare hand-derived values to 10 significant digits (relative 1e-9), and say so.
- **The producer is inferred, not named.** The classifier's Oracle verdict reports the tells in section 1 and does not print the `docProps/app.xml` `Application` field, and that field is not proof either way. Say "inferred from tells" in the report.
- **Hand-deriving hundreds of cells.** Port the formulas in Malloy and verify them with an independent SQL formulation as a second implementation; the two agreeing is the check. Read the cache afterwards and record it as `cache_inspection`.

## 4. Tolerance

Say the number in the report. The defaults:

| Kind of cell | Tolerance |
|---|---|
| counts, positions | exact |
| sums, averages, products of decimals the workbook computed once | absolute 1e-9, on the unrounded values |
| anything the workbook rounds with `ROUND` | exact on the rounded value; the Malloy rounds the same way |
| iterative calculation (`iterate="1"`) | the workbook's own `iterateDelta` is the floor; the fixture documents 0.01 (`fixtures/README.md`). Excel's cache is where it stopped, not the fixed point, so solve tightly (about 1e-12) but compare at no less than `iterateDelta` (0.01 on the fixture); report both numbers (`cookbook-scenario.md#sc6`) |
| `XIRR` | Excel's iteration stops near 1e-8, so compare at about 1e-7 and label the residual `float_noise` with the measured diff |
| Monte Carlo | statistics within sampling error, never draws (`cookbook-scenario.md#sc7`) |
| a snapshot you cannot recompute | "matches as of save", with no tolerance claimed |

A displayed `2.89` is not a comparison. A tolerance chosen after seeing the difference is
not a tolerance.

## 5. Contexts: at least two per reporting measure

One context proves a formula, not a model. For each reporting measure give **at least two
filter contexts, and make one of them hit the case-insensitive and blank paths on purpose**
(a mixed-case key such as `East`/`east`/`EAST`, a blank, a text number in a numeric
column). The workbook usually shows several already; harvest them from the formulas'
criteria:

| Context | Cell | Revenue (cached) | Malloy |
|---|---|--:|--:|
| everything | `Data!F13` | 356.25 | 356.25 |
| region `east`, any case | `Report!B3` | 77.75 | 77.75 |
| region blank | `Report!B6` | 41 | 41 |
| region ends in `st` | `Report!B5` | 260.75 | 260.75 |
| Qty at least 1, numbers only | `Report!B4` | 335.75 | 335.75 |

(The Malloy column is from `cookbook-lookup-aggregate.md`, which runs these on the same
fixture.) The fixture also carries a second context for each measure in `Report!D3:D15`,
with other spellings and operators and its own inputs in `D1:D2`, so the second context is
a cell comparison, not a hand calculation. When the workbook shows no second context, derive
one by hand from the data, run it, and label it `semantics-cited (hand-derived)`.

A measure that matches at the unfiltered total and nowhere else is not validated.

## 6. Building the parity table

1. `classify_workbook.py classify book.xlsx --json`. The `regions` array has, per region:
   `id`, `sheet`, `ref`, `cells`, `example` (a representative formula), `route`, `flags`,
   `plug_candidates`. The `oracle` object has the status, each tell with its count, and
   `cached_errors`.
2. Choose the cells (section 2). Skip any region whose flags include `external_ref` or whose
   route is X or NR: those are listed as not compared, with the reason.
3. Build the key: one `read_xlsx` source per sheet range (section 3).
4. Compute the Malloy value at the context the formula states. Take the names from
   `get_context`/`list_packages`, never from memory. Where the formula uses a criterion
   cell (`Report!A1`), the Malloy reads a `given:` that defaults to the same value.
5. Compare with the tolerance, record the two numbers in full precision, and **do not
   round the difference**.

The result, one row per cell:

| Cell | Check | Excel cached | Malloy | Abs diff | Within tolerance | Label |
|---|---|--:|--:|--:|---|---|

`cookbook-lookup-aggregate.md` holds the filled table for the fixture's reporting and
lookup regions: 40 of its 44 rows match and 4 do not, and the four are findings.

### Row cap and looping contexts

Publisher caps a model query at 1000 rows unless the Malloy states its own `limit:`
(`PUBLISHER_DEFAULT_QUERY_ROW_LIMIT`), and nothing reports the cut. A per-cell comparison
over more rows silently truncates: put an explicit `limit:` in the query, above the row
count you expect (the hard cap is `PUBLISHER_MAX_QUERY_ROWS`, 100000 by default; the REST
body has no row-limit field), and compare the returned row count with the key's.

To check one view at several criteria contexts, loop the values through `givens` and
compare each result with the cached cell (stdlib only, as in `AGENTS.md` section 7):

```python
import json, urllib.request

URL = ("http://localhost:4000/api/v0/environments/examples/packages/storefront"
       "/models/storefront.malloy/query")
cached = {"east": 77.75, "west": 183.0}  # the workbook's cached cell per criterion

for region, want in cached.items():
    body = {"query": "run: sales -> by_region", "givens": {"REGION": region},
            "compactJson": True}
    req = urllib.request.Request(URL, json.dumps(body).encode(),
                                 {"content-type": "application/json"})
    rows = json.loads(json.load(urllib.request.urlopen(req))["result"])
    got = sum(r["revenue"] for r in rows)
    print(region, want, got, abs(want - got) <= 1e-9)
```

Label: `semantics-cited`. The names are illustrative; take the real ones from `get_context`,
and read the cached cells from the key (section 3).

## 7. Row counts and sums for lifted data

```malloy
source: data_rows is duckdb.sql("""
  SELECT count(*) AS n FROM read_xlsx('data/fixture.xlsx', sheet = 'Data', range = 'A2:F12', header = false, all_varchar = true)
""")
```

```malloy
run: data_rows -> { select: n }
```

```malloy
run: sales -> { aggregate: lifted_rows is count(), revenue_total is revenue.sum() }
```

Executed: 11 rows on the sheet, 11 lifted, and a recomputed revenue of 356.25 against the
cached `Data!F13` of 356.25. Both must hold before any measure is trusted: a lift that
drops a row or double-counts a totals row fails here, long before a measure looks wrong.
A mixed column needs the extra check in `recover-sources.md` ("Check the lift"): count
how many cells are text.

## 8. What a mismatch is

A mismatch is a finding, not a failure to hide. Classify each one, with both numbers and
the context:

| It is | Example in the fixture | Report as |
|---|---|---|
| a **workbook defect** | `Report!B12` double-counts its own totals row; `Report!I6` is a typed plug | the cell, both numbers, and the owner to ask. Do not reproduce it. |
| a **quirk the port cannot reproduce faithfully** | `VLOOKUP(..., TRUE)` over an unsorted table (`Lookup!H3`, `H8`) | a finding; the cache is the generator's model of a binary search, unverified against Excel. |
| a **data decision** | the text `"1"` and the text date, which match the cache only because the port reproduces what Excel skipped | the decision and who made it. |
| a **translation bug** | a Malloy value that differs for no reason above | fix it. |
| a **stale or untrusted oracle** | a pivot older than its source; an untrusted cache | not a mismatch; the comparison was not valid. |

A report with no mismatches and no C rows is a red flag when the data exercises the flagged
quirks, not a clean bill of health: any real workbook has cells that disagree with a
faithful translation. Where the data cannot hit a quirk, the absence of a mismatch says
nothing about it (section 9).

## 9. Findings and quirks not exercised

**A quirk the data never exercised is `not_exercised`.** A case-insensitive match, a text
number, an approximate lookup or a blank criterion matches the naive port on data that
holds no mixed case, no text number, no unsorted table and no blank. Record the row as
`not_exercised`, name the context that would hit it, and never imply parity for it:
"no mismatches" is evidence only for a quirk the data actually exercises, so a flagged
quirk the data cannot hit (one spelling, all-exact lookups, no text numbers) is never
reported as parity. Add the
context if the user can supply data, or label it `semantics-cited (hand-derived)` on a
constructed row.

**Workbook defects reproduced faithfully go in a findings list, not "fixed" silently.** A
formula that references the prior row on a flat sheet, a rank range that drifts down a copied
formula, a copied-down formula with an anomalous offset (one that looks two rows back where its neighbours look one, or an `INDEX(range, 0)`), and a typed-in total over a formula column all reproduce on purpose, because the
parity target is the workbook's number. List each one with the cell, both numbers and the
question for the owner; do not change the Malloy to the number the owner probably meant.

A formula that differs by one term from its neighbours in a copied-down row or column (Excel's green-triangle case) is the same kind of finding: reproduce it and list it with the cell, both numbers and the question for the owner.

**Ask when a flat sheet references the prior row.** A formula copied down a flat (non-ledger)
sheet that reads the row above (`=B3-B2`, `=C2+D3`) is either a running calculation or a
copy error, and the data cannot say which. Ask the user whether the prior-row reference is
intended before choosing `lag()`, a cumulative window or a plain per-row field.

## What is not covered yet

Each of these has been exercised only by constructed probes, never against a real saved workbook, so treat a clean result as weak evidence and label it accordingly:

- approximate match (`VLOOKUP(..., TRUE)`, `MATCH(.., 1)`) on UNSORTED data
- text numbers read by the compared formulas
- the 1904 date system, and serial 60 on a real file
- wildcard criteria on real data
- iterative circularity compared against the cache of a real loop (SC6 depth only)
