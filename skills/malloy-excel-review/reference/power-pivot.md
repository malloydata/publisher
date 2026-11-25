<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Power Pivot: Workbooks With a Data Model (Step 4)

> A workbook with a Power Pivot data model is not a spreadsheet with some formulas: it
> carries a compressed analytic model (tables, relationships, DAX measures) next to the
> sheets. Nothing in this skill reads that model. This file says how to recognise one,
> how to get it out in a form `skill:malloy-powerbi-review` can read, and what stays
> with this skill.

## Detect it

The classifier reports a data model when any of these holds. Not every external pivot cache
is one: a pivot over an SQL Server table or an OLAP cube also says `cacheSource
type="external"`, and only some of them are the workbook's own model.

| Tell | Where | What it means |
|---|---|---|
| `xl/model/item.data` | a part in the zip | the model itself, a VertiPaq backup. The classifier checks that it exists and never opens it. |
| a connection whose provider is `MSOLAP` with `Data Source=$Embedded$` (Excel names it `ThisWorkbookDataModel`) | `xl/connections.xml` | the model's own connection. The classifier gives it the status `data_model`: it is not an external server and has no login. |
| `CUBEVALUE`, `CUBEMEMBER`, `CUBESET`, `CUBESETCOUNT`, `CUBEMEMBERPROPERTY`, `CUBERANKEDMEMBER`, `CUBEKPIMEMBER` | formulas | cells that read the model, or an OLAP cube, by member and measure name; they route `NR` |
| `cacheSource type="external"` | `xl/pivotCache/pivotCacheDefinition*.xml` | a pivot whose source is **not a worksheet range**. `external_cache.kind` in the JSON says which of four it is (below). |

A pivot's `external_cache.kind` decides where it goes:

| `kind` | The pivot reads | Goes to |
|---|---|---|
| `data_model` | the workbook's own model (its connection is `data_model`, or there is no connection record and `item.data` exists) | this file, and `skill:malloy-powerbi-review` |
| `relational` | a database through an ordinary connection | "Decide the data question" in `discover.md`: point Malloy at that database; no Power BI path |
| `olap` | an SSAS or other cube (`commandType` cube, or an SSAS system) | `discover.md`: Publisher has no cube connection, so ask for the underlying warehouse tables |
| `external` | a source the script cannot resolve (no matching connection record) | `discover.md`: decide the data question |

Only `data_model` counts in `external.pivot_external_caches` and prints "data-model pivot
cache(s)". The other three set `cache_of_database` instead.

Two connection statuses are not external data and are not NR: `data_model` (above) and
`workbook` (a `WorksheetConnection_*` or a type 100 or 102 connection, which feeds a worksheet
range or Table into the model or into Power Query). The text report counts the `workbook` ones
in a single line ("workbook-internal connection(s)") and lists the model once.

The `$Embedded$` provider string and the `ThisWorkbookDataModel` name are what Excel is
generally known to write; they were not confirmed against a real file here
(`limitations.md`).

On a hand-built workbook of those shapes (not an Excel save) the classifier printed:

```
- conn 1 "ThisWorkbookDataModel" is the workbook's own Power Pivot data model, not an external connection: see power-pivot.md (skill:malloy-powerbi-review).
- pivot PivotTable1 on P at A3:B8: source {"type": "external", "connection_id": "1"}, refreshed 2023-03-15, ...
| R1 | C | A1 | formula | 1 | constant | NR | CUBEVALUE | |
- R1 C!A1: CUBEVALUE reads the Power Pivot data model or an OLAP connection: route with power-pivot.md / the connection
External: 1 connection(s), 0 query table(s), 0 external link(s), 1 data-model pivot cache(s), Power Query: no, Power Pivot model: yes.
- xl/model/item.data (Power Pivot VertiPaq backup: detected, never parsed)
```

and in `--json`, `workbook_props.power_pivot` is `true`, the pivot carries
`"external_cache": {"kind": "data_model", "connection": "conn 1", ...}`, and
`external.pivot_external_caches` counts the data-model pivots. The `CUBE*` formulas route
`NR`, and `item.data` is listed under "Not read".

Neither the `data_model` connection nor its sheets are reported as "a cache of a database":
there is no server and no login behind `$Embedded$`, so the next step is this document, not
asking the user for credentials. The model's *own sources* (Power Query queries, if any)
are a different matter, and are external data in the ordinary way.

## What you can and cannot do from here

| You can | You cannot |
|---|---|
| Say a model is present, and how many pivots and `CUBE*` formulas depend on it | List its tables, columns, relationships, hierarchies or KPIs |
| Translate the **worksheet** formulas, Tables and worksheet-source pivots in the same workbook | Read a DAX measure, or its dependencies |
| Report a data-model pivot or a `CUBE*` cell as a snapshot as of save | Recompute either of them |
| Route the model to the Power BI skill | Parse `item.data`: it is a proprietary backup format, and a reverse-engineered reader can return plausible but wrong values |

## The conversion path

A Power Pivot model is the same engine as a Power BI semantic model. Power BI Desktop can
take the model out of the workbook; once it is a Power BI project (PBIP) its TMDL is plain
text, and `skill:malloy-powerbi-review` reads that.

1. **In Power BI Desktop, import the workbook's model.** The import that carries the model
   with its measures and relationships is *File > Import > Power Query, Power Pivot, Power
   View* ("Import Excel workbook contents"). Choose the workbook and Start. If the
   import fails or is not offered, *Get Data > Excel workbook* reads only the worksheets
   and Tables as flat tables: it brings the data but **not** the measures or relationships,
   so use it to lift data, not to recover the model.
2. **Save it as a project.** *File > Save as*, and pick the Power BI project (`.pbip`) type.
   It does not appear until the preview feature is on (*File > Options and settings > Options >
   Preview features > Power BI Project (.pbip) save option*, then restart Desktop); the
   steps and the reason are in `skill:malloy-powerbi-review`, `reference/discover.md` section 1.
3. **Hand off.** Give the `.pbip`'s `*.SemanticModel/` folder to
   `skill:malloy-powerbi-review`. It holds `definition/` (TMDL) when Desktop saved the
   project as TMDL and `model.bim` otherwise; the Power BI skill reads both. It reads the TMDL, transpiles the DAX and proves the
   numbers. The measures, relationships and calculated columns are its job from here.

**Not exercised here.** There is no Power BI Desktop in this environment, so menu names and
the preview toggle are as documented by Microsoft and by the Power BI skill, not tried.
Menus differ between Desktop releases; if one is missing, look for the equivalent rather
than falling back to guessing the model from the sheets.

If Desktop is not available to the user, ask for the measure definitions another way (a
DAX tool connected to the open workbook can list them) and the table list. Do not infer
measures from a pivot's numbers.

## What stays with this skill

The workbook has two halves, and you work both.

| Part | Where it goes |
|---|---|
| Sheet formulas, Tables, named ranges, worksheet-source pivots | this skill: `translate-formulas.md`, `cookbook-pivot.md`, the cookbooks |
| The model: tables, relationships, DAX measures, calculated columns, RLS roles | `skill:malloy-powerbi-review` |
| A data-model pivot's fields and values | the measures it names, translated by the Power BI skill; the pivot's *layout* (rows, columns, filters) is rebuilt here from `cookbook-pivot.md` P1 to P6 |
| A `CUBEVALUE("ThisWorkbookDataModel", "[Measures].[Total Sales]", ...)` cell | a query of the translated measure at the member context the formula names |
| The Power Query that fills the model | `discover.md`, "Decide the data question": the model's tables are a cache of those sources |

Parity for the model's numbers is the Power BI skill's, with one addition from here: every
data-model pivot and `CUBE*` cell is a snapshot as of the workbook's last save and last
model refresh (`parity.md`). Say which one a comparison used.

## Report it

In `review-coverage.md`, list the model once as a single item with the outcome "handed to
`skill:malloy-powerbi-review`", the number of data-model pivots and `CUBE*` cells that
depend on it, and whether the user could produce a `.pbip`. A workbook whose reporting is
mostly in the model and that never got a `.pbip` is `NR` for those parts, with the reason.
