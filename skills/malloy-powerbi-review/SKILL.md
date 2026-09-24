---
name: malloy-powerbi-review
description: Analyze a Power BI semantic model (TMDL, PBIP, or .pbix) as prior art for Malloy modeling, and transpile its DAX. Use when Power BI artifacts are present during Step 1 (DISCOVER), and whenever a specific DAX measure has to become Malloy - CALCULATE, ALL/ALLSELECTED/ALLEXCEPT, RANKX and top-N, time intelligence, USERELATIONSHIP, calculation groups, what-if parameters, or an RLS role. Carries a worked, executed transpile per construct and states what each port costs. Works with or without a database connection.
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Power BI Review

> **Purpose:** Evaluate a Power BI semantic model as prior art for building a Malloy semantic model. This skill coordinates the Power BI adapter. The implementation lives in reference files under `reference/`.

> **Tool names** are written bare here - `get_context`, `execute_query`, `search_malloy_docs`. The exact prefixed name depends on the host surface; match each against the tools you actually have.

> **This is NOT a blind conversion.** DAX and Malloy disagree about what a filter means. A measure that translates cleanly on sight can still return a different number, with no error raised. Knowing which measures change meaning on the way across is the work - and then saying what the port costs, because "untranslatable" has done nothing for the customer.

## When to Use

- **Auto-detected:** The agent finds a `.pbix`, a `.pbip` project, or a `definition/` folder of `.tmdl` files during Step 1 (DISCOVER) and the user confirms they should be used as prior art.
- **Explicitly requested:** The user says "model from Power BI", "convert this PBIX", "migrate off Power BI", or provides a path to Power BI artifacts.
- **One measure at a time:** The user pastes a DAX expression and asks what it becomes in Malloy. Go straight to the recipe in `reference/cookbook-*.md`; the routing procedure is for a whole model.

## Two Modes

| Mode | When | Behavior |
|------|------|----------|
| **Power BI + live data** | A connection is configured against the same warehouse the model reads | Power BI provides prior art; the data validates it. Full data-driven proposals. |
| **Power BI only** | No connection, or the model is import-mode with no warehouse behind it | The model file provides all context. Proposals flagged as **unvalidated**. |

In Power BI-only mode, warn the user: "No database connection found. I'll use the Power BI model as the sole source of context, but proposals cannot be validated against live data."

## Input Shapes: Prefer Text Over Binary

Three shapes arrive, and they are not equally trustworthy. Establish which one you have before anything else.

| Shape | What it is | How to read it |
|-------|-----------|----------------|
| **TMDL folder** (`definition/*.tmdl`) | The model in text form | Read directly. No extraction step, nothing to go wrong. **Preferred.** |
| **PBIP project** (`*.SemanticModel/definition/`) | A `.pbip` save format that contains the TMDL folder above | Read the TMDL directly, same as above. |
| **`.pbix`** | A zip whose model is a compressed binary part | Requires third-party extraction. **See the parity section below before you trust a number out of it.** |

**Always ask for TMDL or PBIP before accepting a `.pbix`.** It removes the entire extraction risk class from the migration. Power BI Desktop saves to PBIP directly, but it is still a **preview** feature that has to be enabled under `Options > Preview features` first, and it is unavailable in Desktop for Report Server, so ask with those instructions rather than just naming the format (`reference/discover.md` has the wording). A `.pbix` is what you fall back to when the user cannot re-save.

The `.pbix` also carries the **data**, where TMDL carries only the **model**. If the goal is a working model over a warehouse the user still has, TMDL is sufficient. If the goal includes lifting the imported data out, see `reference/discover.md`.

## Numeric Parity Validation (do this before you trust anything)

Two independent mechanisms can hand back a wrong number with no error. Neither announces itself.

**1. Extraction can be silently wrong.** Reading a `.pbix` means a reverse-engineered reader, not a Microsoft tool. The readers available have open issues in which a column decodes to *plausible but wrong values* rather than failing. A wrong number that looks like a number survives every check except comparison against the source. This risk does not exist on the TMDL path, which is one more reason to ask for it.

**2. DAX and Malloy disagree about filters.** This one applies on every path, including TMDL, and it is the one that costs a migration its credibility. A DAX Boolean filter argument **overwrites** the existing filter on that column; Malloy's `where:` **intersects**. The same measure, faithfully transcribed, answers a different question. `reference/translate-measures.md` is entirely about telling these apart.

**So validate numerically, not visually:**

1. Pick the measures the business actually looks at, not the easy ones. Ask the user which numbers are on the dashboard people open every morning.
2. Get the current value from Power BI itself, at a stated filter context (a specific year, region, whatever the report shows). A screenshot of the report is fine and is often faster than arranging API access.
3. Run the Malloy equivalent at the same filter context with `execute_query` and compare.
4. Compare at more than one filter context. The overwrite-versus-intersect divergence is **invisible at the grand total** and only appears once a filter on the same column is active. A measure that matches unfiltered and diverges when sliced is the signature of this bug, not a coincidence.
5. For a lifted `.pbix`, also compare row counts and one column sum per table before modeling anything on top of it.

**Report parity as a table of measure, context, Power BI value, Malloy value, and match.** "It looks right" is not a result. A migration is trusted or abandoned on these numbers.

## Reference Files

Each reference file is loaded by the workflow phase that needs it. You do not need to read them all at once.

| Reference File | Phase | What It Does |
|------------|-------|-------------|
| `reference/discover.md` | Step 1 (DISCOVER) | Inventory the model, classify storage mode, extract source candidates, capture prior-art notes |
| `reference/propose-fields.md` | Step 4 (DEFINE) | Extract field proposals from tables, columns, and relationships |
| `reference/translate-measures.md` | Step 5 (BUILD) | Route every DAX measure to a recipe, and say what the routing does not prove |
| `reference/cookbook-filter-context.md` | Step 5 (BUILD) | Worked transpiles: `CALCULATE`, `ALL` / `ALLSELECTED` / `ALLEXCEPT`, ranking, top-N |
| `reference/cookbook-time.md` | Step 5 (BUILD) | Worked transpiles: YTD, prior period, growth, date spines, semi-additive |
| `reference/cookbook-structure.md` | Step 5 (BUILD) | Worked transpiles: role-playing dimensions, M:M, bidirectional, what-if parameters, hierarchies |
| `reference/rls-roles.md` | Step 8 (CURATE) | Map RLS roles and hidden objects to access modifiers and gate annotations |
| `reference/review-coverage.md` | Step 7 (REVIEW) | Compare the Malloy model against the Power BI model: table, measure, and relationship coverage |
| `reference/document.md` | Step 9 (DOCUMENT) | Extract TMDL descriptions as `#(doc)` tag seeds |
| `reference/limitations.md` | any | What the script reads and what it does not, and which recipes have no trigger |
| `reference/corpus.md` | any | The 50 public models the coverage numbers are measured against, with commits |

**The three cookbook files are the deliverable of Step 5.** Each recipe carries the
real DAX, the Malloy, whether the Malloy was **executed** or only `semantics-cited`,
and what the port costs. Route with `translate-measures.md`, then transpile from the
recipe - do not hand the user a classification and call it a migration.

### Shared Reference

`reference/_concepts.md` is the Power BI to Malloy concept mapping table. Referenced by `propose-fields.md`, `translate-measures.md`, and `rls-roles.md` for type mapping and syntax translation.

### Script

`scripts/classify_measures.py` runs the routing at scale: dependency graph, return-type
inference from the model's own column types, and the relationship flags that never
appear in a measure's DAX. It also has a `--json` mode for the `.pbix` path, which has
no TMDL. It runs where you have a shell (Claude Code, Cursor); the Credible app's agent
has no shell tool and the MCP skills bundle ships markdown only, so the prose stands alone.

**DAX is not only in `measure` declarations, and the rest is not a rounding error.**
The script also reads calculation groups, `functions.tmdl`, calculated columns,
calculated-table partitions and `roles/*.tmdl`. Across 50 public models, three recipes
fire **zero** times in any measure body and are not rare at all: `CALENDAR()` is only
ever in a calculated-table partition, the calculation items carry a model's time
intelligence, and not one `USERPRINCIPALNAME` in the corpus is in a table file. Match
only `measure` and the model reports no calculation groups, no date spine and no
row-level security.
`reference/limitations.md` is the full inventory of what is read and what is not, and
it counts each kind separately - a user-defined function is DAX and is not a measure.

## What Power BI Provides

- Table, column, and measure names the business already agreed on (accelerates Step 4)
- Relationships with explicit cardinality and filter direction (accelerates Step 3)
- Measure definitions carrying real business logic, often years of it (accelerates Step 5)
- Visibility decisions via `isHidden` and perspectives (accelerates Step 8)
- Descriptions in `///` comments, which are usually better than what anyone will write fresh (accelerates Step 9)
- Format strings that map to render tags

## What to Skip

- **Auto date/time tables.** Power BI generates a hidden `LocalDateTable_<guid>` per date column plus a `DateTableTemplate_<guid>`. These are an artifact of a setting, not a modeling decision. Skip all of them and propose one real date dimension (`reference/cookbook-structure.md#s7`).
- **Report layout** (`report.json`, `*.Report/`): visuals, pages, bookmarks, themes. Analysis is a separate workflow.
- **Report-layer measures.** Button captions, tooltips, dynamic titles, selected-page names, conditional-format colors, SVG sparklines. They return text and belong to the canvas, not the model. How many there are varies more than any other figure here - a third of `PBIASEngine` (42 of 126), an eighth across 50 public models (217 of 1,881) - so count them for the model in front of you rather than assuming a share. **Type the return value rather than looking for a quote character** - the canonical example, `Selected page = SELECTEDVALUE('Current page'[Current page])`, has no string literal at all. `reference/translate-measures.md` step 1 has the tells.
- **Implicit measures.** A numeric column aggregated in a visual with no defined measure. Note which columns are used this way, do not manufacture a measure per column.
- **`summarizeBy` defaults**, except as a hint about which columns are facts and which are keys.
- **Display folders**, `lineageTag`, `ordinal`, and other authoring metadata.
- **Hierarchies** as structure: Malloy has no hierarchy object. Note the level order as drill intent and move on.
- **Query Editor / M** on an import-mode model. The imported data is Power Query's **output**, so a snapshot runs no M. M matters only if you are reproducing the refresh, which is a separate decision (see `reference/discover.md`).

## What to Flag for User Decision

- **Any measure routed to a divergent recipe.** This is the flag that matters most and it must reach the user, never be resolved quietly. Say which filter context makes it diverge.
- **Storage mode.** DirectQuery and live-connection models contain no data; the model still translates but nothing can be validated locally.
- **Bidirectional cross-filtering** (`crossFilteringBehavior: bothDirections`, or `CROSSFILTER(..., BOTH)` inside a measure). It is a model-level switch with a model-wide blast radius - 111 of 117 measures in `FabricASEngineAnalytics`, and the second most demanded recipe across 50 public models - and it is invisible in every measure's DAX. Ask what it was for; `reference/cookbook-structure.md#s3` has the divergence worked out.
- **Many-to-many relationships**: model the bridge the grain actually has, and name the fan-out out loud (`#s2`).
- **Inactive relationships** (`isActive: false`): they exist to be switched on by `USERELATIONSHIP` inside a measure. In Malloy they become named join paths (`#s1`), which changes every call site - the measure disappears rather than translating.
- **Calculated columns and calculated tables**: DAX evaluated at refresh. Decide per object whether it becomes a Malloy dimension, a computed source, or work pushed upstream.
- **RLS roles that do not fit the gate grammar**: most will not, and a translated role is usually *weaker* than the original unless it sits behind a trusted tier. See `reference/rls-roles.md`.
- **The four stopgaps**: date spines (`cookbook-time.md#t4`), semi-additive measures (`#t5`), calculation groups (`cookbook-structure.md#s5`) and parent-child hierarchies (`#s6`). All four ship working Malloy at a cost worth stating before the customer discovers it. The router flags the last three; **no DAX function asks for a date spine**, so `#t4` is one you have to recognize yourself, from a report that shows periods with no rows.
- **Where next month's data comes from**, if the user is lifting data out of a `.pbix`. A snapshot answers today's question and goes stale.

> Power BI, Microsoft and Fabric are trademarks of Microsoft Corporation. This skill is not affiliated with or endorsed by Microsoft.
