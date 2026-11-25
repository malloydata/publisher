# Recipe fixture (CC0)

`fixture.xlsx` and `fixture_1904.xlsx` are the workbooks the `malloy-excel-review` cookbook recipes run against; each recipe is labelled `executed` or `semantics-cited` in place. They are original, synthetic and dedicated to the public domain (CC0). The `_Config` password is a dummy.

## Provenance

The tests read the rows below, so keep the `| label | value |` shape when you update them.

| | |
|---|---|
| Generator | `build_fixture.py` in this directory (stdlib only, Python 3.9+) |
| Generator source SHA-256 | `9c9a1361b36a5874e24abdb41e4792dd9ade0b52d4f50e5c2e666dde2fba3ef4` |
| Engine of the committed binary | **`python`** (cached values computed by the generator, not by a spreadsheet application) |
| Pivot | **none**: the `python` build ships without the `Pivot` sheet |
| `fixture.xlsx` SHA-256 | `1df5646586c3b8575d968687b8a9b5202c57785205cfa2a8b22478854808862e` |
| `fixture_1904.xlsx` SHA-256 | `991159f40a3c36428452f5b39813f80d8d6a021c09b51b4f58b5100b82285705` |
| Excel save recorded | **no** |

A commit cannot name its own hash, so the generator is pinned by its source SHA-256: editing `build_fixture.py` without updating this table and rebuilding fails the tests. When the engine is `python`, the tests also unzip the committed binaries and compare every part to a fresh build. The zip timestamps are fixed, so a rebuild on the same Python and zlib reproduces these bytes; the tests compare parts, not the zip container.

## Rebuild

```bash
python3 skills/malloy-excel-review/fixtures/build_fixture.py                       # --recalc python (default)
python3 skills/malloy-excel-review/fixtures/build_fixture.py --recalc libreoffice  # soffice --headless --convert-to xlsx
python3 skills/malloy-excel-review/fixtures/build_fixture.py --recalc excel        # writes the files and prints the steps
```

Every engine writes formula cells with no `<v>` and `<calcPr fullCalcOnLoad="1" iterate="1" .../>`; only `python` then writes the values it computes.

**`libreoffice`** needs `brew install --cask libreoffice` and was not exercised for this commit. It converts with a private profile so a running soffice cannot swallow the job. LibreOffice may write `iterate`/`date1904` as `true`/`false` (the tests accept both), rewrites `dcterms:modified` (so `TODAY()` moves off 2024-06-30 and `Report!B14` shifts; `B15` compares against the constant `B2` and does not), and whether it round-trips the data table, structured references and `totalsRowCount` faithfully is unverified.

**`excel`** is the only route to an Excel oracle. The printed procedure, exactly:

1. Quit Excel and open `fixture.xlsx` in a **fresh** instance with no other workbook open (the iterative-calculation setting is taken from the first workbook opened). Confirm iterative calculation is on (Windows: File > Options > Formulas; macOS: Excel > Settings > Calculation) and that Forecast shows no circular-reference warning.
2. Select `tbl_Sales`, Insert > PivotTable > New Worksheet, rename the sheet `Pivot`, then right-click its tab > Move or Copy > move to end, so the sheet order stays the eight fixture sheets then `Pivot`.
3. Leave "Add this data to the Data Model" **unchecked** (an OLAP pivot offers neither calculated fields nor this grouping). Put `Region` in Filters (a page field), `OrderDate` in Rows (grouping needs it), add a calculated field `Rev108 = Revenue * 1.08`, and `Revenue` as a value shown as "% of Grand Total".
4. Excel will not group a field that holds text. Retype `Data!D8` as a real date (2024-07-15), refresh the pivot once, group `OrderDate` by Months, then retype `Data!D8` back to the text `2024-07-15` (format the cell as Text first). `Data!D8` must end as a string.
5. Never refresh the pivot again. Its cache keeps `D8` as a date while the sheet holds text; that stale cache is deliberate, and a test asserts `D8` is still a string in the committed file.
6. Save as .xlsx over `fixture.xlsx`, open `fixture_1904.xlsx` once and save it too, then update the table above (engine `excel`, Excel version, both SHA-256 values). With engine `excel` the tests also require the pivot's page field, calculated field, `percentOfTotal` data field and `OrderDate` grouping, and the classifier test tolerates the extra `Pivot` sheet.

This procedure is reasoned from Excel's pivot-refresh and grouping behaviour; it has not been run in Excel.

## What the committed binary is

The committed binary is the `python` build. Its cached values are the generator's model of Excel's semantics (case-insensitive SUMIFS/COUNTIF, `COUNTIF(C:C,1)` matching the text `"1"`, AVERAGE skipping text, `VLOOKUP`/`MATCH` approximate binary search, `#N/A`, serial 60, a 100-pass / 0.001 iteration of the interest circularity, `TODAY()` pinned to `dcterms:modified`). It is not an Excel save, so **every quirk-route recipe is `semantics-cited` (the python value is the oracle) until an Excel save is recorded in the table above.** The unsorted-`VLOOKUP` and `MATCH` values assume a standard binary search; Excel's exact probe sequence on unsorted data is unverified.

Parity tolerance for the circular cells (`Forecast!B4:F5` and everything downstream of them) is at least 0.01: Excel stops iterating at a 0.001 change and its cell order may land on slightly different values than the generator's.

## Honesty labels

| Recipe | `python` (committed today) | `libreoffice` | `excel` |
|---|---|---|---|
| Plain sums, joins, windows, unpivot, cumulative, recursive CTE, depreciation | `executed` | `executed` | `executed` |
| Case-insensitive SUMIFS/COUNTIF; `COUNTIF(A:A,1)` matching text `"1"`; AVERAGE skipping text | `semantics-cited` | `executed (LibreOffice: not Excel's oracle)` | `executed (Excel)` |
| `VLOOKUP`/`MATCH` approximate match, sorted and unsorted; `#N/A`; `IFERROR` | `semantics-cited` | `executed (LibreOffice: not Excel's oracle)` | `executed (Excel)` |
| Serial 60 (the 1900 leap-year bug); the 1904 offset | `semantics-cited` | `executed (LibreOffice: not Excel's oracle)` | `executed (Excel)` |
| `iterate` convergence of the Forecast circularity | `semantics-cited` | `executed (LibreOffice: not Excel's oracle)` | `executed (Excel)` |
| `TODAY()` pinned to `dcterms:modified` (`Report!B14` = 181) | `semantics-cited`: the generator pins TODAY to the same date it then uses as the oracle, so 181 proves nothing about Excel (and Excel's TODAY is local time, not the UTC `modified` stamp) | `semantics-cited`: LibreOffice rewrites `modified` | `executed (Excel)` only for the pin-to-`modified` translation, since Excel itself recomputes with the real clock |
| Pivot recipes (`cookbook-pivot`, `refreshedDate`, records snapshot) | `semantics-cited (hand-derived)`: no pivot in this build | same | `executed (Excel)` once the `Pivot` sheet exists, for the recipes whose pivot layout matches the Excel procedure (OrderDate in Rows; see `cookbook-pivot.md` for which) |
| A recipe on a quirk route with no cached cell (the expectation is worked out by hand from the data) | `semantics-cited (hand-derived)` | `semantics-cited (hand-derived)` | `semantics-cited (hand-derived)` until an Excel save exists |
| RAND / Monte Carlo | the seeded DuckDB recipe is `executed`; the workbook's own draws are not reproducible by any engine | same | same |

A recipe moves to `executed (Excel)` only once the table above records an Excel save.

## Sheets and expected cached values (python build)

| Sheet | Contents |
|---|---|
| `Data` | Table `tbl_Sales` `A1:F13` (data `A1:F12`, totals row 13), `Revenue` is a calculated column; `Region` mixes `East`/`east`/`EAST` and a blank (A7); `Qty` has a text `"1"` (C5) and a blank (C9); `OrderDate` has a text date (D8) and serial 60 (D10). Revenue total `F13 = 356.25` |
| `Ledger` | `A1:D10`, merged header `A1:A2`, `B1:B2`, `C1:D1`; hidden row 5; `SUBTOTAL(9)` rows 6 and 10 |
| `Lookup` | sorted `A1:B6`, unsorted copy `D1:E6`; tests in `H2:H8` (`0.1`, `0` garbage, `#N/A`, `0`, `#N/A`, `3`, `2`); a text-key table `J1:K4` (`East`, `West`, `North`) with two case-insensitive tests in `H9:H10` (`VLOOKUP("east",..)` = `1`, `MATCH("EAST",..,0)` = `1`) |
| `Report` | `A1 = 1`; `B3:B15` (`77.75`, `335.75` for Qty `>=1`: numeric only, a text-coercing translation gives `356.25`; `260.75`, `41`, `2.8889`, `9`, `10`, `3`, `5`, `712.5` double count, `83.97`, `181`, `289` for OrderDate `>= B2`, which drops the text date and serial 60); copied-down `I2:I9` with the plug `I6 = 999`; a second context for every measure in `D3:D15` (inputs `D1 = 3` and `D2` = 2024-06-01; `183`, `232.5`, `54.5`, `315.25`, `5.5`, `2`, `3`, `3`, `2`, `183`, `197.64`, `29`, `76`) |
| `Assumptions` / `Forecast` | inputs `B2:B7`; a constant wide block `A10:F11` (years 2025..2029, flows 1000, 1200, 1500, 1800, 2000) that `Forecast!B3:F3` reads; What-If table `E3:E6` on `Assumptions`; defined names `GrowthRate`, `SalesData` |
| `MonteCarlo` | `B2:B1001` `NORM.INV(RAND(),100,15)`, seeded values in the python build |
| `_Config` | veryHidden; `A2` `Password`, `B2` a dummy value |
| `Pivot` | Excel build only |

`fixture_1904.xlsx` holds the same dates (serial 60 excluded; it has no 1904 equivalent) with `date1904="1"`.
