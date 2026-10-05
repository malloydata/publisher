---
name: malloy-excel-review
description: Move an Excel workbook (.xlsx or .xlsm) onto Malloy - recover the model hiding in its cells, translate its formulas and pivots, and prove the numbers match against the values the file already stores. Use when a workbook is present, when a user wants to migrate or switch off a spreadsheet, and whenever a specific formula has to become Malloy - SUMIFS, COUNTIF, SUMPRODUCT, VLOOKUP, XLOOKUP, INDEX/MATCH, pivot tables, assumption cells, roll-forwards, circular references, Monte Carlo, or a Power Pivot model. Carries worked recipes, each labelled executed or semantics-cited in place, and states what each port costs. Works with or without a database connection.
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Excel to Malloy

> **Purpose:** Move a working Excel workbook onto Malloy, fast, and prove the numbers match. The user already has a workbook their business agreed on; the job is to carry it across without silently changing what it means.

> **Tool names** are written bare here - `get_context`, `execute_query`, `search_malloy_docs`. The exact prefixed name depends on the host surface; match each against the tools you actually have.

> **This is NOT a blind conversion.** A workbook has no declared model, only cells, and Excel and Malloy disagree about matching, blanks, text numbers and dates. A formula that translates cleanly on sight can return a different number with no error raised. Knowing which ones change meaning is the work, and then saying what the port costs.

## The migration, end to end

1. **Discover.** Establish the input shape, run the classifier, inventory and classify every sheet, and decide where the data really comes from. `reference/discover.md`.
2. **Recover sources.** Turn ranges into sources, lifting only cells that are data, and check the lift against the workbook's own totals. `reference/recover-sources.md`.
3. **Translate formulas.** Route every formula region, then port it from a recipe: `reference/translate-formulas.md` decides where each goes, and the cookbooks carry the worked Malloy. A routing table is not a migration; the recipe is the deliverable.
   - Aggregates and lookups: `reference/cookbook-lookup-aggregate.md`
   - Pivot tables: `reference/cookbook-pivot.md`
   - Assumptions, roll-forwards, circularity, Monte Carlo: `reference/cookbook-scenario.md`
4. **Power Pivot handoff**, only when the workbook carries a data model. `reference/power-pivot.md`.
5. **Prove the numbers.** Cached-value parity at more than one context. `reference/parity.md`.
6. **Say what changed.** Every cell accounted for as translated, translated at a cost, staying in Excel, or unresolved. `reference/review-coverage.md`.

The user's question is "can I move off this workbook and still trust my numbers?" Steps 3 and 5 are the answer; the rest is bookkeeping. `Step N` here and in the reference files is this numbering.

## When to Use

- **Auto-detected:** an `.xlsx` or `.xlsm` file with formulas or pivot tables, and the user confirms they want it carried across.
- **Explicitly requested:** "migrate off Excel", "turn this spreadsheet into a model", "replace this workbook", or a path to a workbook.
- **One formula at a time:** the user pastes a formula and asks what it becomes. Go straight to the recipe (`reference/translate-formulas.md` section 5 names the cookbook); the routing procedure is for a whole workbook.
- **Not this skill:** a workbook with no formulas and no pivots is a data file; take it to the modeling workflow. A Power BI file is `malloy-powerbi-review`.

## Input Shapes

| Shape | How to read it |
|---|---|
| `.xlsx`, `.xlsm` | Read directly with `scripts/classify_workbook.py`. Best case. A macro workbook is read, never opened or run. |
| `.xls`, `.xlsb` | Binary. Ask the user to re-save as `.xlsx` in Excel. |
| Encrypted workbook | An OLE container, not a zip. Ask for an unencrypted save. |
| CSV, TSV | Data only; every formula is already gone. Lift it as a table. |
| Google Sheets | File > Download > `.xlsx`. |
| Written by a library, no cached values | The Oracle section reports it untrusted. Ask for a save recalculated in Excel before promising parity. |
| `xl/model/` inside the file | Power Pivot. `reference/power-pivot.md`. |

## Two Modes

| Mode | When | Behavior |
|---|---|---|
| **Workbook + live data** | The workbook is a cache of a database, or a connection to the real source exists | Point Malloy at the source (the data question, `reference/discover.md` section 5); the workbook is prior art and the parity oracle. |
| **Workbook only** | No database behind it | The sheets are the data. DuckDB `read_xlsx` lifts them; the model is a snapshot until the user decides where next month's data comes from. |

## Hard Rules

These are not preferences. They exist because a workbook can carry credentials and code the cells never show.

- **Never read the secrets file or `publisher.db`.** Not with `cat`, not to "check" it.
- **Never dump `xl/connections.xml`, `customXml/`, `xl/queryTables/` or `xl/externalLinks/` raw** (`unzip -p`, `cat`, a hex dump). That is the realistic leak path while debugging the script. Use the report.
- **If a secret reaches the transcript anyway** (pasted, shown in a dump, printed by a bug), stop and tell the user plainly: that credential is now in this session's history and logs and must be rotated. Never repeat the value. A secret found inside the file is rotated whatever happens next; it has been shipping with the workbook.
- **`--json` masking is best effort; rotation is the real control.** The JSON carries customer structure (names, formulas, server names, SQL), so it stays local and never lands in a PR, or a shared doc.
- **A workbook with an `MSIP_Label_*` sensitivity label never enters a corpus or a PR.**
- **Never resolve `externalLinks` or DDE, never run a macro, never fetch a `WEBSERVICE` URL.** Skip `xl/embeddings/` entirely. Do not open a macro workbook to recalculate it without telling the user.
- **A flagged secret inside a bulk data column stays by reference.** When the classifier masks or flags cells shaped like secrets inside a data column, report the column and the count only, never the values; compare the column excluding those cells (`not_compared`, as for `hidden_dep`) and ask the user what they are.
- **Security flags come first.** The report prints them above everything; resolve them before you translate anything.

## Running the Script

`scripts/classify_workbook.py` is stdlib-only Python 3. It runs where you have a shell; the Credible app's agent has no shell tool and the skills bundle ships markdown only, so the prose stands alone and `reference/limitations.md` says what the script would have told you.

```bash
python3 scripts/classify_workbook.py book.xlsx             # Markdown report (same as `classify`)
python3 scripts/classify_workbook.py classify book.xlsx --json [--secret-cell 'Sheet!B3=VAR' ...]
python3 scripts/classify_workbook.py connections book.xlsx --config-out conn.json \
    [--secret-cell 'Sheet!B3=VAR' ...] [--secrets-out PATH] [--force]
python3 scripts/classify_workbook.py run --secrets PATH -- npx @malloy-publisher/server@latest
```

`classify` answers what reading sheets one at a time cannot: which formula cells are one formula copied down, whether each is a dimension, a measure or a given, which cells a source may lift, whether the cached values can be trusted, and what attached code cannot be translated. It emits a routing table, not a verdict: **T** translate, **C** translate at a stated cost, **X** stays in Excel, **NR** ask the user. Read the report in order and stop at the first section that needs a human.

`connections` proposes a Publisher `connections` block. Every secret is written as `${VAR}` in `--config-out`; the values go only to `--secrets-out`, a `0600` file that must sit outside a work tree or be git-ignored, and the output names variables, never values. Merge the block under an environment in `publisher.config.json`. `run` reads that file and starts the server with the variables in its environment, so no value reaches argv, `ps` or shell history. Never use `env $(cat file | xargs)`.

Two facts about starting the server, both executed (`reference/discover.md` section 5):

- **Supply the variables on every start.** An unset `${VAR}` fails `PUBLISHER_INIT_FAILED` at each boot, not only the first.
- **Config added after the first boot is ignored.** Put the connection block in `publisher.config.json` before the first boot, or restart once with `--init`, which wipes `publisher.db` and anything created in the UI. After the first boot Publisher keeps the resolved connection, plaintext included, in `publisher.db` in the server root; keep that root out of any work tree.

**Editing after registering a package.** A package registered by `location` is copied into the server's `publisher_data`; editing the original folder afterwards does nothing, and `?reload=true` re-reads the copy. After editing, `DELETE` the package and `POST` it again, or edit the served copy under `publisher_data/<env>/<pkg>/` and reload. A package whose model has a compile error fails to register (HTTP 424) and leaves no package behind: fix the compile error first, then `POST` again. Every `.malloy` file in the package folder is compiled at registration, so a stale scratch fragment left there fails it too: keep scratch fragments outside the package folder, or delete them.

Not every connection maps. SQL Server leads the list of those that do not (Publisher has no SQL Server connection type), then Oracle, Teradata, SAP HANA, DB2, SSAS, Access, SharePoint, machine-local ODBC DSNs and integrated Windows auth. Those are NR, first in the report, with the real options stated: a replica or export in a supported warehouse, or DuckDB over an extract.

## Scope: What Translates

Reporting workbooks translate in full: actuals, rollups, budget against actual, pivots, lookups. Scenario and financial models split three ways (`reference/cookbook-scenario.md`):

| Construct | Route |
|---|---|
| Actuals, input cells (`given:`), roll-forwards as a cumulative window | **T** |
| Compounding, forecast periods with no rows, recurrences, circular references, Monte Carlo, discrete Goal Seek and Data Tables | **C**, as DuckDB recursive-SQL or generated-series sources; the cost is stated in the recipe |
| Continuous Solver and Goal Seek, VBA-driven logic, live vendor feeds | **X**, stays in Excel |

The seam is named, not hidden: for each **X** item say what stays in Excel and what feeds across to Malloy (typically the result table, read through `read_xlsx` or an export). Malloy runs no Python in Publisher, so notebooks and dashboards stay declarative.

## Parity

The workbook is the one migration source that ships its own answer key: every formula cell stores its last computed value. `reference/parity.md` is the procedure; the rules that matter:

1. **Gate the oracle first.** `fullCalcOnLoad`, a formula with no cached value, or a missing `calcId` means the cache is not Excel's; manual calc or calc-on-save off means its values may be stale. Ask for a recalculated save; if none will ever come, hand-derive at least two contexts, label them `semantics-cited (hand-derived)` (never `executed (Excel)`), and record the cache's agreement afterwards as `cache_inspection`, informational and not parity (`reference/parity.md` section 1).
2. **Never lift a formula cell as data.** A lifted calculated column makes parity compare a number to itself. A source lifts only cells with no formula.
3. **Compare at more than one context**, including one that hits the case-insensitive, blank and text-number cases on purpose. A match at the grand total proves little.
4. **Compare values at a stated tolerance**, not what the number format displays.
5. **Report a table:** region, cell, recipe, route, Excel cached, Malloy, match, label. "It looks right" is not a result. A report with no mismatches and no **C** rows is a red flag when the data exercises the flagged quirks: real workbooks have cells that disagree with a faithful translation. "No mismatches" is evidence only for a quirk the data hits; one it cannot hit is `not_exercised` or `semantics-cited`, never parity (below).

**Labels say how a recipe was checked.** `executed` means the Malloy ran and returned the number printed. `semantics-cited` means the behaviour is Excel's documented one and the expected value comes from the cache or the spec, not from a run of Excel. `semantics-cited (hand-derived)` means no cached cell exists and the expectation was worked out by hand from the data. The bundled fixture is a generator build, not an Excel save, so every quirk route (case-insensitive matching, approximate lookup, text numbers, serial 60, `iterate`, `TODAY`) is `semantics-cited` until a recalculated Excel save is recorded in `fixtures/README.md`. Say so in the report rather than implying Excel confirmed it. A quirk the data never hit (a single spelling, all-exact lookups, no text numbers) is `not_exercised`, or `semantics-cited (hand-derived)` when a constructed probe row was worked out by hand; never a match, and a workbook defect reproduced on purpose goes in a findings list, not a silent fix (`reference/parity.md` section 9).

## Reference Files

Each reference file is loaded by the step that needs it. You do not need to read them all at once.

| Reference File | Step | What It Does |
|---|---|---|
| `reference/discover.md` | 1 | Input shape, classifier run, sheet classes, attached code, the data question, secrets handling, prior-art notes |
| `reference/recover-sources.md` | 2 | Ranges to sources, `read_xlsx` behaviour, dates, merged headers, wide to long, checking the lift |
| `reference/translate-formulas.md` | 3 | The oracle verdict, the three-way split, routes, the must-not-misfile flags, `LET` and dynamic arrays |
| `reference/cookbook-lookup-aggregate.md` | 3 | `SUMIFS` and friends, `SUMPRODUCT`, exact and approximate lookups, plugs, `TODAY` |
| `reference/cookbook-pivot.md` | 3 | Rows, columns, values, page fields, calculated fields, show-values-as, Top-N, grouping |
| `reference/cookbook-scenario.md` | 3 | Givens, cumulative flows, compounding, recurrences, circularity, Monte Carlo, the stays-in-Excel list |
| `reference/power-pivot.md` | 4 | Detect a data model and hand it to `malloy-powerbi-review` |
| `reference/parity.md` | 5 | The oracle gate, granularities, contexts, tolerances, what a mismatch means |
| `reference/review-coverage.md` | 6 | Account for every sheet, region, pivot, connection and code mechanism |
| `reference/limitations.md` | any | What the script reads and what it does not |

`reference/_concepts.md` is the Excel to Malloy mapping table (types, functions, references, dates, errors, blanks), used by the recover and translate steps.

## What Comes Across for Free

- **Names the business agreed on:** sheet, column, Excel Table and defined-name labels.
- **Excel Tables:** a `ref`, a totals row (excluded from the lift and used to check it) and `calculatedColumnFormula` as a free field signal.
- **Formula logic**, often years of it, already grouped into regions by the classifier.
- **The answer key:** every cached value, at every context the workbook shows.
- **Input cells and validation lists:** each becomes a `given:`, and a list becomes its allowed values.

## What to Skip

- **Formatting, charts, drawings, conditional formats and sparklines.** Presentation, not model. A conditional format by expression that carries logic is flagged, not ported.
- **Scratch sheets** (empty or almost empty). Ask why they exist; usually leave them out.
- **Autofilter, slicer and timeline state, and hidden rows as a filter.** UI state; never bake it into a source.
- **Cached pivot records as data.** A pivot is a view; its snapshot is a parity oracle only.
- **Hidden and `veryHidden` sheet values.** The script withholds them on purpose; ask what they hold.

## What to Flag for User Decision

- **Any NR route.** No recipe by design: ask what the number means and rewrite the intent together.
- **Any region on a quirk route** (`ci_match`, `approx_match`, `count_numbers_only`, `full_column`, text numbers, plugs). State which context makes the faithful port diverge from the naive one.
- **A formula that references the prior row on a flat sheet.** Ask whether that is intended before choosing `lag()` or a cumulative window (`reference/parity.md` section 9).
- **Hardcoded constants and plugs** (a literal inside a formula, a typed value overwriting one cell of a copied-down formula). Translate what the formula says, show where the workbook disagrees, ask which is right.
- **The data question,** when the workbook is a cache of a database, a query table or a Power Query: where does next month's data come from?
- **Volatile functions.** `TODAY` and `NOW` pin to the save date as a `given:`; `RAND*` is **X**.
- **Code attached** (VBA, add-ins, vendor feeds, Python in Excel): each is a seam with its own route.
- **Circular references and recurrences:** the iteration count and tolerance become stated parameters.
- **Where the cache is not Excel's,** the fixture-style generator case, and any cell fed by an add-in (`matches as of save` at best).

> Excel and Microsoft are trademarks of Microsoft Corporation. This skill is not affiliated with or endorsed by Microsoft.
