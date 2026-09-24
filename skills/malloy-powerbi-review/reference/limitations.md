<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# What the script reads, and what it does not

> `scripts/classify_measures.py` routes DAX at scale. Everything it does not read
> is a place its counts are silently incomplete rather than wrong, and a count
> that is silently incomplete is the failure mode this whole skill exists to
> avoid. This file is the one inventory; the notes below used to be scattered
> across three files or absent.

## What it reads

Everything under `definition/`, by shape:

| Read | What it yields |
|---|---|
| `tables/*.tmdl` → `measure` | measures |
| `tables/*.tmdl` → `calculationItem` | calculation groups (`S5`) |
| `tables/*.tmdl` → `column X = <DAX>` | calculated columns, and their `dataType` |
| `tables/*.tmdl` → `partition X = calculated` | calculated tables - where `CALENDAR()` and `GENERATESERIES()` actually live |
| `tables/*.tmdl` → `formatStringDefinition` | dynamic format strings, kept with their measure |
| `functions.tmdl` | user-defined DAX functions |
| `roles/*.tmdl` → `tablePermission` | row-level security predicates (`RLS`) |
| `relationships.tmdl` | bidirectional, many-to-many and inactive flags |

Auto date tables (`LocalDateTable_*`, `DateTableTemplate_*`) are detected and
**excluded**, then reported as a model-level count. They are an artifact of a
setting rather than a modeling decision, and they are not a rounding error: in
the 50-model corpus they are 126 tables carrying 756 calculated columns, against
129 calculated columns in the whole rest of the corpus. Counting them would
swamp every real number in the file.

**Each kind is counted separately.** A user-defined function and a calculation
item are DAX, and they are not measures. Summing them under one "measures"
heading published 1,622 measures for a corpus that held 1,406.

## What it does not read

Each of these is a decision, not an oversight:

| Not read | Why, and where it is handled |
|---|---|
| `expressions.tmdl` | Power Query (M), not DAX. It holds the source binding and endpoints, and it is where a partition's `Server`/`Database` are declared - `discover.md` §4 tells the agent to read it by hand, because on an import model the M has already run. |
| `partition X = m` / `= entity` | Same: M and Direct Lake bindings. Their `source` blocks are consumed so no line inside one is mistaken for a declaration, then discarded. |
| `model.tmdl`, `database.tmdl` | No DAX. `model.tmdl` is the only authoritative table roster, so read it by hand if the table list ever disagrees with `tables/`. |
| `cultures/`, `perspectives/` | No DAX. Locale affects date and decimal parsing on anything lifted out of the file (`discover.md`); perspectives are visibility (`rls-roles.md`). |
| `roles/*.tmdl` → `columnPermission`, `metadataPermission` | Object-level security, not a row filter. A different control with a different answer; `rls-roles.md` covers it, and `review-coverage.md` §5 requires it be reported separately. |
| `DAXQueries/*.dax`, `TMDLScripts/*.tmdl` | Outside `definition/`. Authoring scratch, not part of the model. |
| `report.json`, `*.Report/` | The reason the routing is **a priority order, not a verdict**: whether a report ever filters an overwritten column is decided here, so the script cannot prove a measure safe. |

## Where the parse gives up

Two TMDL shapes are read wrongly rather than skipped. Neither is produced by
Microsoft's own TMDL writer, which is why they are accepted rather than fixed:

- **A body line at exactly the property indent** (`base + 1`) ends the body
  there, so the measure is routed on the part above it.
- **`T [Col]`, spaced**, reads as a measure reference rather than a column.
  The rule is deliberate and runs the other way round: whitespace before `[`
  means a measure reference, because `AND [Gross]` was reading as a column of a
  table named `AND` and dropping the edge from the dependency graph - which is
  the edge divergence propagates along. The spaced-column shape is the price.

## What it does not route

**A string-typed what-if selector.** `SELECTEDVALUE('Aircraft Type Parameter'[Aircraft Type],
"A330")` reads a table with no relationships, which is `S4`'s test exactly - and it returns a
string, which is step 1's test exactly. Step 1 runs first, so it routes `SKIP`. That looks
like a bug until you try to fix it: `Selected page = SELECTEDVALUE('Current page'[Current page])`
is the same shape to the character, and it is the canonical report-layer measure. So is
`"Top " & SELECTEDVALUE('Top N Selector'[SelectorSort]) & " reports"`. **Whether a disconnected
string table is a given or a page-title selector is decided in `report.json`**, which this script
does not read, and a rule that routed the first correctly moved ten of `PBIASEngine`'s captions
out of the report layer. 35 measures in the 50-model corpus are in this bucket; most are genuinely
titles. Check a `SKIP` that reads a disconnected table by hand before trusting it.

**A count on the *one* side of a relationship.** `DISTINCTCOUNT(dim_customer[customer_id])`
routes `DIRECT`, and `DIRECT` means only that no Power BI-specific recipe applies -
never that the translation is guaranteed equivalent. In a single-direction model the
fact table's filters do not reach the dimension, so the measure answers "how many
customers exist" whatever the report filtered on the fact; write it in Malloy as a
`count(distinct …)` over a joined source and it answers "how many customers in the
filtered rows", which is a different number. The script has no recipe for this because
deciding it needs the join direction *and* which side the aggregate sits on, and the
routing is per measure rather than per query. **38 measures across 12 of the 50 corpus
models are this shape**, so check it by hand wherever a count names a dimension table.

## Routes with no trigger

`T4` (date spine) and `FC8` (escaping a query-level filter) are **teaching
recipes**: no DAX function requests either, so no code path emits them. Their
zero is guaranteed by construction and must never be published as a measured
result - `T4` was, once, inside a "fires zero times across 1,622 measures"
claim that was vacuous for exactly that one route. They are declared in
`TEACHING_ONLY`, and a test asserts against the module's own source that every
other recipe is reachable.

`S7` (auto date tables) is a **model-level** route. It is emitted once per
model, with a count, and never appears in the per-measure recipe totals.

## Where it runs

Claude Code and Cursor. The Credible app's agent has no shell tool, and the MCP
skills bundle ships `SKILL.md` and `reference/*.md` only - it never opens a
skill's `scripts/` directory. **So the prose has to be complete without the
script**, and the script exists to do what an LLM reading measures one at a time
cannot: the dependency graph, return-type inference from the model's own column
types, and the relationship flags that appear in no measure's DAX.
