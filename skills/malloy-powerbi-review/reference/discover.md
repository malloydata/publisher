<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Power BI Discovery (Step 1)

> Inventory a Power BI semantic model, establish which input shape you have, classify storage mode, extract architecture-level candidates, and capture prior-art notes in the conversation. Does NOT translate measures; that is deferred to `translate-measures.md`.

## 1. Establish the Input Shape

Scan the project directory and immediate subdirectories. Three shapes, in descending order of how much you should like them:

| Found | Shape | Action |
|-------|-------|--------|
| `*.SemanticModel/definition/` with `.tmdl` files | PBIP | Read the TMDL directly. Best case. |
| A `definition/` folder of `.tmdl` files | TMDL export | Read directly. Equally good. |
| `*.pbix` | Binary | Ask for a re-save first; see below. |
| `model.bim` (single JSON file) | Legacy BIM | Read directly. Same content as TMDL, JSON syntax. |

Confirm with the user: "I found a Power BI model. Use it as prior art?"

### If All You Have Is a `.pbix`, Ask Before Extracting

A `.pbix` stores the model as a compressed binary part. Reading it means a reverse-engineered third-party library, and those libraries have open issues where a column decodes to **plausible but wrong values** instead of failing.

Ask first: "Can you open this in Power BI Desktop and use `File > Save as` to save a `.pbip` project? It writes the model as text, which I can read exactly, with no extraction step that can corrupt a value."

That request costs the user one dialog and removes an entire class of silent error. Only fall back to extracting the `.pbix` when they cannot, and when you do, say plainly in your notes that every number from it is unverified until checked against Power BI itself.

## 2. Read the Model Root

From `definition/model.tmdl` (or the BIM equivalent):

- **Culture / locale**: affects date and decimal parsing on anything lifted out
- **`defaultPowerBIDataSourceVersion`**, compatibility level: note it, it bounds which features can appear
- Whether a table is marked as the **date table**: time intelligence measures depend on it

From `definition/relationships.tmdl`, capture every relationship. This is the whole join graph in one file and it is the highest-value thing in the export. For each: from column, to column, cardinality both sides, `crossFilteringBehavior`, and `isActive`.

## 3. Classify Storage Mode Before Promising Anything

Read the `partition` block of each table. The `mode:` decides what you actually have.

| Mode | Data present in the file | What you can validate |
|------|--------------------------|----------------------|
| `import` | Yes, a compressed copy | Everything, if you extract it |
| `directQuery` | No | Nothing locally; you need the warehouse |
| `dual` | Sometimes | Treat as DirectQuery for planning |
| Live connection to a remote model | No | Nothing; the model lives in the service |

**Say this out loud to the user early.** A DirectQuery or live-connection model translates fine, but there is no data in the file, so no number can be checked until a connection to the same warehouse exists. Discovering that after translating forty measures is a bad afternoon.

Mixed-mode models are common. Record the mode per table, not per model.

## 4. Extract the Real Source Tables from M

Each import or DirectQuery partition carries M (Power Query) code naming where the data came from. You are reading it for **one thing**: the server, database, schema, and table. That is what a Malloy source points at.

```
partition Sales = m
    mode: import
    source =
            let
                Source = Sql.Database("server.example.com", "SalesDW"),
                dbo_FactSales = Source{[Schema="dbo",Item="FactSales"]}[Data]
            in
                dbo_FactSales
```

That is `conn.table('dbo.FactSales')`.

**Do not translate the M transformation steps.** On an import model the stored data is Power Query's *output*, so a snapshot runs no M at all. M matters only when the user needs the refresh reproduced, which is a separate decision. Flag any partition whose M does substantial reshaping (merges, appends, unpivots, custom columns) as work that has to land somewhere, and say where you think it belongs: upstream in the warehouse, or in the Malloy source.

Note any partition whose source is a **local file path or a spreadsheet**. Those break on any machine but the author's and are a migration blocker worth raising immediately.

## 5. Extract Table Candidates

For each table that is not an auto date table:

- **Table reference**: from the M above
- **Storage mode**: from step 3
- **Grain**: inferred from the key columns and the relationships pointing at it
- **Role**: Fact (many side of relationships, carries measures) or Dimension (one side, lookup)
- **Row count**, if available from the extraction
- **Calculated table?**: if the partition is a DAX expression rather than M, flag it

### Skip the Auto Date Tables

Power BI generates a hidden `LocalDateTable_<guid>` for **every** date column when auto date/time is on, plus one `DateTableTemplate_<guid>`. A model with thirty date columns has thirty-one junk tables. They are a setting, not a design. Skip all of them, note how many you skipped, and propose a single real date dimension if the model lacks one.

## 6. Inventory Measures Without Translating Them

Count and list measures per table. Capture the DAX text but **do not translate yet**. What you want at this stage is the shape of the problem:

- How many measures total
- How many reference `CALCULATE`
- How many reference time intelligence functions
- How many reference other measures (the dependency depth)

Measures live on a table but are global in the DAX namespace, so the table a measure sits on is organizational, not semantic. Do not assume a measure belongs to the source built from its host table; read what it actually aggregates.

Report the counts to the user before translating. "There are 340 measures, 210 use CALCULATE, 48 use time intelligence" is a scope conversation worth having up front.

## 7. Extract Relationship Candidates

From the relationships captured in step 2, build the join graph:

- **Active many-to-one** relationships: the normal case, these become `join_one:`
- **Inactive** relationships: flag each, and find which measures reference it via `USERELATIONSHIP`
- **Bidirectional** relationships: flag each, ask what it was for. It is usually one of: a many-to-many bridge, a slicer that needed to filter backwards, or an accident that has been changing numbers quietly for years.
- **Many-to-many**: flag, and look for the bridge table that should exist

## 8. Extract Visibility Seeds

Capture a compact summary, not full field extraction:

- Tables and columns with `isHidden: true`: note whether they are key columns, intermediate calculation inputs, or clutter
- **Perspectives**: each is a curated subset of the model someone already decided on. Valuable for Step 8.
- **RLS roles**: list role names and the tables each filters. Do not translate the DAX yet; that is `rls-roles.md`.

## 9. Extract Documentation Seeds

TMDL carries descriptions as `///` comments directly above the object:

```
/// Net revenue after returns and discounts, excluding tax.
measure 'Net Revenue' = ...
```

These are usually better than anything that will be written fresh, because they were written by someone who had to answer for the number. Capture object name and description text for every one that says something a name does not.

## 10. Decide the Data Question

If the user is moving **off** Power BI rather than alongside it, ask where next month's data comes from. Three answers, and they lead to different work:

1. **The warehouse is still there.** Point Malloy at it. The import was only a cache. This is the clean case.
2. **The warehouse is there but nobody has credentials.** A people problem, and a real one; raise it now.
3. **There is no warehouse.** The `.pbix` is the only copy, refreshed from files or a service by someone's Desktop install. Lifting the data gives a working model today and a stale one next month. Say so explicitly rather than letting it be discovered later.

## 11. Capture Prior-Art Notes

Hold a lean routing summary in the conversation with these sections:

- **Source**: shape (TMDL / PBIP / PBIX), location, storage modes present, mode, confidence
- **Table Candidates**: table with real source table, grain, role, row count, storage mode
- **Relationship Candidates**: from, to, cardinality, direction, active
- **Measure Inventory**: counts by category, not translations
- **Flags**: numbered list of situations requiring attention
- **Visibility Seeds**: hidden objects, perspectives, RLS role names
- **Documentation Seeds**: compact table of objects with good descriptions
- **Decisions Made During Discovery**: input shape accepted, auto date tables skipped, the data question from step 10

No measure-level detail beyond counts. Architecture + flags + decisions only.
