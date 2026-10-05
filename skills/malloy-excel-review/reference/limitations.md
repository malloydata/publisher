<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# What the script reads, and what it does not (any step)

> `scripts/classify_workbook.py` recovers the model inside a workbook and gates its
> cached values. Everything it does not read is a place its counts are silently
> incomplete rather than wrong, and a count that is silently incomplete is the failure
> this skill exists to avoid. This file is the one inventory. Quote the parts that apply to the
> workbook in hand in the migration report.

## What it reads

It reads the `.xlsx` zip directly (standard library only). It never extracts to disk and
never executes anything it reads.

| Read | What it yields |
|---|---|
| `xl/workbook.xml` sheets, `definedNames`, `calcPr`, `workbookPr` | the sheet list, names, `iterate`, `iterateCount`, `iterateDelta`, `calcMode`, `calcOnSave`, `fullCalcOnLoad`, `date1904`, and an `effective` block with Excel's defaults applied |
| `xl/_rels/workbook.xml.rels` | the only mapping from a sheet to its XML part; never `sheetId` or the file name |
| `xl/worksheets/*.xml` formulas and cached values | formula regions, with shared formulas expanded and R1C1 normalised; cached value types (`str`, `e`, `b`) |
| `xl/tables/*.xml` | Excel Tables: `ref`, totals row, `calculatedColumnFormula` |
| `xl/pivotTables/*`, `xl/pivotCache/*` definitions | source, row, column, page and value fields, the selected page items (a multi-select page names its shown and hidden items), manually hidden items per field, `subtotal`, `showDataAs` and the x14 `pivotShowAs` that overrides it, calculated fields, calculated-item formulas, date grouping, Top-N count, `refreshedDate`, and for `cacheSource type="external"` the `external_cache` kind (`data_model`, `relational`, `olap`, `external`) |
| `xl/connections.xml`, `xl/queryTables/*`, `customXml` `DataMashup` | external data: connection strings (credentials withheld), commands, parameters, Power Query connectors |
| `xl/externalLinks/*`, `docProps/custom.xml`, `docProps/core.xml` | link and DDE counts (targets never resolved), VSTO tells, sensitivity labels, `dcterms:modified` |
| `xl/ctrlProps/*`, `customUI/*`, `xl/webextensions/*`, `xl/richData/*` | code-attached mechanisms, counted separately |
| sheet `<scenarios>` | each scenario's name, input cells and values, in the top-level `scenarios` (the comment and user fields are dropped; none from a hidden sheet) |
| `styles.xml` number formats | which cells are dates (to flag serials before 61) |

Hygiene on every part: the member list is capped (20,000 entries, 256 MiB per part, 1 GiB in
total, counting bytes actually read, not the size a crafted header claims); absolute and `..`
member names are ignored; a part containing a `<!DOCTYPE` or `<!ENTITY` is rejected before
the XML parser sees it (UTF-16 with a BOM included); strict OOXML namespaces are accepted.
The region dependency graph stops at 2,000,000 edges and says so
(`edge_budget_exceeded`). Large sheets are streamed.

**Each kind is counted separately.** A formula cell, a constant, a pivot, a connection and
an add-in function are different things. Summing them under one heading overstates the job.

## What it does not read

Each of these is a decision, not an oversight. The **Not read** section of every report
lists the ones that apply.

| Not read | Why, and where it is handled |
|---|---|
| `xl/vbaProject.bin` | An OLE container with compressed source. Detected and counted, never parsed. Ask the user to export the VBA modules (`discover.md` section 4). |
| `xl/model/item.data` | The Power Pivot VertiPaq backup. Detected, never parsed: `power-pivot.md`. |
| Office Scripts, Power Automate flows, COM add-ins | **Leave no trace in the file.** They live in OneDrive or in the application. If a sheet's data "just appears", ask. |
| `.xls` and `.xlsb`, encrypted workbooks | Not zips of XML. Exit code 2 with an `unreadable` flag; ask for a re-save as `.xlsx` (`discover.md` section 1). |
| `pivotCacheRecords` | Presence only. A snapshot of the data at `refreshedDate`, never compared here (`cookbook-pivot.md`). |
| the cell values of hidden, `veryHidden` and config-named sheets | Withheld on purpose. |
| `sharedStrings` text beyond header rows | Cell values are never emitted. |
| `xl/calcChain.xml` | The dependency graph is built from the formulas. |
| `xl/metadata.xml` | Spills are found by the `cm` attribute. |
| drawings, charts, sheet `extLst` (x14 conditional formats, sparklines) | Not part of the model the script recovers. |
| comments and `threadedComments` text | Scanned for secrets, never emitted. |
| `customXml` parts other than a `DataMashup` | Counted, never read. |
| `xl/externalLinks/*` targets, `xl/embeddings/*` | Never resolved, never opened. A formula that reads through a link (`external_ref`), or through a defined name that points into another workbook (`external` in `defined_names`), routes NR and its value is a snapshot as of the last link refresh (`translate-formulas.md`). |
| The pivot's own row and column layout beyond the field names | Field order, sort type, manual item order, and the item positions of a Top-N result are not reported. Compare against the saved cells. |
| the M transformation steps of a Power Query | The sheet holds Power Query's output, and a snapshot runs no M. Only the connector calls and their literal arguments are read. A computed argument (a variable or a parameter) is skipped, so a query built from a parameter yields no host. |

## Tells that are spec-derived, not confirmed against a real file

These mechanisms have a tell the script looks for. It was written from the file-format
specification and tested on hand-built XML only, then run over real
workbooks to see which tells a real file carries. The report prints the ones still
unseen under **Unconfirmed tells**, and a JSON consumer finds them in `unconfirmed_tells`.

| Tell id | The tell | If it is absent |
|---|---|---|
| `xll_udf` | an `_xll.` prefix, or a bare function name that is not built in, in `<f>` | Weak: a bare unknown name is also what an add-in function looks like when it is *not* an add-in (a VBA UDF, a typo, a removed function). In a workbook that carries a VBA project the same tell is reported as `vba_udf` instead, and that is what the two corpus workbooks with bare unknown names produce. No real file reaches `xll_udf` itself: neither the `_xll.` prefix nor a bare name without VBA is in any of them. |
| `unresolved_function` | an `_xludf.` prefix in `<f>` (a cached `#NAME?` is confirmed on real files) | Weak for the prefix half. |
| `addin_link` | `[n]!Func(...)` formulas into an `.xla` or `.xlam` add-in (the `externalLink` target half is confirmed) | Weak for the formula half. The `[n]!Name` strings real files carry are `macro=` attributes on button shapes, which are not formulas. |
| `python_in_excel` | `_xlfn._xlws.PY(` in `<f>`; the code is taken as the first string literal argument, which is a guess | Weak. |
| `web_extension` | `xl/webextensions/` parts | Weak. |
| `dde` | `<ddeLink>` in `xl/externalLinks/` | Weak. |
| `custom_ui` | `customUI/customUI*.xml` with `onAction` | Weak. |

Confirmed against a real Excel save, so not listed above: `form_control` (an `xl/ctrlProps/`
part with `fmlaLink`), `scenario_manager` (`<scenarios>` in the sheet XML) and `vba_udf`
(bare unknown function names beside a `vbaProject.bin`).

Absence of an unconfirmed tell is weaker evidence than absence of a confirmed one. Say so
when a workbook "has no add-ins" on the strength of one.

The external-data shapes are in the same position. The `connections.xml` and `DataMashup`
parsers were tested on **hand-built fixtures**: the `commandType` numbering and the
`Microsoft.Mashup.OleDb.1` stub string are from the specification, and a connection written
by a real Excel may carry fields the parser has not seen. The embedded Power Pivot connection
(`Data Source=$Embedded$`, usually named `ThisWorkbookDataModel`) is recognised as
`data_model`; `WorksheetConnection_*` and type 100 or 102 connections as `workbook`. Neither
is NR and neither is a cache of a database (`power-pivot.md`).

## Secret masking is best effort

`--json` and the text report mask what they can recognise. Rotation is the real control.

| Masked | Not masked |
|---|---|
| cells named by `--secret-cell` | a secret that appears only inside a **formula string literal**, with no label and no recognisable shape |
| cells labelled `password`, `pwd`, `secret`, `token`, `api key` and common translations (de, es, fr, pt, nl, sv, ru, ja, zh, ko), and the first filled cell beside or below | `DBPWD` in all capitals: it cannot be split into words, so it is not recognised as a label |
| values shaped like secrets: `Password=...`, `user:pass@host`, `AKIA...`, `sk-...`, `ghp_...`, `xox[bp]-...`, a JWT, a 24 or more character high-entropy string | a plain-word password in a cell that is neither labelled nor secret-shaped |
| defined-name constants and comments, scanned and flagged | |
| anything from a hidden, `veryHidden` or config-named sheet | |

Known quirks of the heuristic: the scrub can over-mask (`NoPassword=1`, or a header
beside a `Password` label) and it will also mask a column literally called `Pass` when
the cell beside it looks like a password. Past 50 labels on one sheet, the script flags
`secret_label_cap` and keeps masking by position only; past 500 candidate strings it flags
`secret_scrub_cap`.

## Where the routing misleads

The route is a priority order, not a verdict: it cannot prove a formula safe.

- **The function table is hand-written** (about 290 built-ins). A real built-in missing from
  it routes NR with "which add-in?". That is the safe direction, and it will show up as a
  false NR on a workbook that uses an unusual function.
- **`GETPIVOTDATA` routes NR** with the flag `getpivotdata`, because its value is a read of
  the pivot's snapshot. Each region lists the pivot it reads under `pivots` (name, location,
  source), or an empty list when its first argument is outside every pivot
  (`cookbook-pivot.md#p8`).
- **Sheet classes are heuristics.** They are tested on the fixture and on hand-built
  workbooks. Confirm each with the user.
- **A relative reference with an offset** is read as a prior-period reference. A
  genuine same-row field and a deliberate offset look alike in R1C1.
- **`INDIRECT`, `OFFSET`, `CHOOSE` used as a reference** route NR and the script prints
  "dependency graph incomplete: N cells" rather than present a partial graph as complete.
  Names, links and code the script cannot read are not in the graph either.
- **`iterate="1"` is only a setting.** The script also finds cycles in the graph; a workbook
  can carry the setting with no cycle. The `calc` object carries `iterateCount`,
  `iterateDelta` and an `effective` block with Excel's defaults applied (100 and 0.001 when
  the file is silent). Three shapes are not cycles and are not counted as one: an `OFFSET`
  anchored on its own cell (it reads nothing there, only its target), a space
  intersection such as `(RowName ColName)`, which is one cell and carries the flag
  `intersection`, and an aggregate over a range that contains its own cell, which is
  reported separately as `self_inclusive_range` (the `graph.self_inclusive_range` list). A cycle the script does report can still be false: a
  large cost model can report one cycle that may be real or may be a
  false one the static graph cannot tell apart. Settle it by asking what the
  author expected to iterate, not by trusting the count.
- **A one-cell dynamic array is not a spill.** A formula marked `cm` whose array `ref` is one
  cell is a flagged scalar (`dynamic_array_scalar`), not an exclusion of the neighbouring
  cells from lifting. A real multi-cell spill is still excluded.
- **`vba_udf` and `xll_udf` are different counts.** An unknown bare function name in a
  workbook with a `vbaProject.bin` is counted as `vba_udf`; without one, as `xll_udf`.
  Both route NR. Neither confirms the owner: the script does not read the VBA.
- **An embedded credential in a file is flagged** (`connections_embedded_credential`,
  `data_mashup`, `secret_*`). Whether it is still valid is not known: rotate it.

## What the cached values can and cannot tell you

- The cache is only an oracle when Excel computed it (`parity.md` section 1). On a library-written
  file it is a model of Excel, and the bundled fixture's committed binary is exactly that
  (`fixtures/README.md`). No quirk-route recipe in this skill is `executed (Excel)` until
  an Excel save is recorded.
- A cell that reads through an external link, a feed or an add-in is a snapshot.
- The `refreshedDate` of a pivot and the `dcterms:modified` stamp are the only clocks in
  the file. Excel's `TODAY` is the local date at calculation; `dcterms:modified` is UTC at
  save, so a pin to it can be a day off near midnight.

## Where it runs

Claude Code and Cursor. The Credible app's agent has no shell tool, and the MCP skills
bundle carries the `SKILL.md` body and each `reference/*.md` file (served as a prompt named
`malloy-excel-review/<name>`, fetched on request) but no `scripts/`.
**So `SKILL.md` and the reference files have to be usable without the script**, and the script exists to do what
reading sheets one at a time cannot: expand shared formulas, find the regions, build the
dependency graph and its cycles, gate the oracle, and read the external data without a
credential entering the transcript.

- The fixture holds no date serial between 1 and 59, so the serial-60 handling is exercised only by unit tests, not by the fixture.
- Pivot field names are masked by position against the source range's header cells. A pivot whose cache source cannot be resolved to a range (an external or OLAP source) keeps its field names as written.
