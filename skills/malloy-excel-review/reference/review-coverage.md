<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Excel Coverage Review (Step 6)

> Compare the Malloy model against the workbook it came from. Account for every sheet,
> region, pivot, connection and code mechanism as translated, translated at a cost,
> staying in Excel, or not resolved, each with a reason. Coverage without parity
> numbers (`parity.md`) is not a review.

The four outcomes are the classifier's routes, and a report uses them as they are:

| Outcome | Route | Meaning |
|---|---|---|
| **translated** | T | The Malloy does what the Excel does, and parity says so. |
| **at cost** | C | Translated, and the cost is stated: a different shape, a query-level window, a recursion, an approximation. |
| **stays** | X | Stays in Excel. The seam is named, with what crosses it. |
| **NR** | NR | Not resolved: a question for the user is open, or the logic is not in the file. |

Anything that is none of the four is a hole in the review. The totals below come from the
classifier, so a missing item shows up as a count that does not add up.

## 1. Sheet coverage

One row per sheet, from the **Sheets** table (`discover.md` section 3):

| Sheet | Class | State | Outcome | Reason |
|---|---|---|---|---|

Left out is allowed; it needs a reason. A sheet that was hidden, `veryHidden`, `scratch` or
`config` gets a row anyway, because the visible sheets may depend on it. A `config` sheet's
values are never shown or copied into the report.

## 2. Region coverage

Group by what the classifier emits and report counts first, then detail. The counts must
match the **Routes** table:

| Route | Regions | Formula cells | Translated | At cost | Stays | NR |
|---|--:|--:|--:|--:|--:|--:|
| T | | | | | | |
| C | | | | | | |
| X | | | | | | |
| NR | | | | | | |

For the bundled fixture the classifier reports 55 regions: T 40 regions / 77 cells, C 13 /
23, X 2 / 1001, NR 0 / 0. A T region that failed parity moves out of "translated"; a region
the classifier routed T is not thereby correct.

Then a row for every region that is not a clean T translation, and for every T region with
a flag (`plug`, `hardcoded_constant`, `ci_match`, ...):

| Region | Cells | Route | Flags | Outcome | Malloy | Reason |
|---|---|---|---|---|---|---|

Examples of what a row says: `Lookup!H3` (C, `approx_match`): at cost, an unsorted table,
a finding; `Report!I2:I9` (T, `plug`): translated as the formula, `I6` reported as a
workbook defect; `MonteCarlo!B2:B1001` (X, `volatile`): stays, replaced by a seeded
source; `Forecast!B4:F5` (C, `circular`): at cost, a bounded recursion.

## 3. Source coverage

Every source lifted, and every range that was not:

| Range | Source | Rows (sheet / lifted) | Column sum check | Excluded | Reason |
|---|---|--:|---|---|---|

"Excluded" lists the cells never lifted: formula columns, array and spill ranges, What-If
data table cells, pivot output. A source with no row-count line is unverified
(`parity.md` section 7).

## 4. Pivot coverage

| Pivot | Sheet | Source | Rows / columns / values | Page items, hidden items | Calculated fields, items | Top-N, grouping, show-as | Outcome | Compared against |
|---|---|---|---|---|---|---|---|---|

"Page items, hidden items" are the selections the report prints (`page_items`, `hidden_items`),
since a translation that ignores them reproduces the unfiltered totals. "Compared against" names
the snapshot: the pivot's cached cells and its `refreshedDate` (`cookbook-pivot.md`). A pivot
whose `external_cache.kind` is `data_model` is one row, outcome "handed to
`skill:malloy-powerbi-review`" (`power-pivot.md`); a `relational`, `olap` or `external` one is
a data question (`discover.md` section 5), not a hand-off.

## 5. Data connections

| Connection or query | System | Publisher connection | Outcome | Reason |
|---|---|---|---|---|

Every connection in the **External data** section, SQL Server first when present. State the
answer to the data question for each (`discover.md` section 5): point at the source,
credentials missing, no source, or a vendor feed. Do not repeat a credential, a connection
string or a server password; say that one was present and that it was rotated or must be.

## 6. Code attached to the workbook

One row per mechanism in the **Code attached** section, with the cells that depend on it:

| Mechanism | Count | Route | Cells that depend on it | Outcome | Seam |
|---|--:|---|---|---|---|

A cell fed by code outside the file is a snapshot (`parity.md` section 1). The row says what
crosses the seam and in which direction: the values the macro wrote, or the data the
add-in returned. A mechanism the script cannot see (Office Scripts, Power Automate, COM
add-ins) is listed as "not detectable" with the question asked of the user
(`limitations.md`).

## 7. Parity results

**This is the section that decides whether the migration is trusted.** The table is the one
in `parity.md` section 6, with a row for every cell compared:

| Cell | Context | Excel cached | Malloy | Match | Label |
|---|---|--:|--:|---|---|

Requirements:

- Cover **every non-T region**, each at a context that exercises its quirk.
- Cover a sample of T regions, including at least one with a `ci_match` or `criteria_*` flag.
- Include **two contexts** per reporting measure, one of them on the case-insensitive and
  blank paths.
- Include row count and one column sum per lifted source.
- State the oracle verdict (`trusted`, `caveats`, `untrusted`) and the fixture or save it
  came from. A `semantics-cited` row is as strong as that engine, not stronger.

A mismatch is reported with both numbers and the context, and classified: workbook defect,
quirk the port cannot reproduce, data decision, or translation bug (`parity.md` section 8).

## 8. Known gaps

Close with what the Malloy model does **not** do that the workbook did, stated plainly:

- Regions that stay in Excel (X) and regions with no answer yet (NR), by intent and not by
  function name.
- Workbook behaviour with no model equivalent: UI state, a pivot's layout and sort,
  conditional formatting, macros that write cells.
- Anything that depends on the refresh, when the data was lifted from a cache.
- Values that were snapshots of code outside the file.
- Everything in the classifier's **Not read** list that this workbook happens to use
  (`limitations.md`).
- Credentials that were found in the file and need to be rotated.

A review that reports only what was achieved is not usable for the decision the user has to
make.
