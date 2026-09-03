---
name: malloy-modeling
description: Build semantic models with Malloy for the Malloy Publisher. Read this skill whenever the user asks about modeling data or specifically mentions Malloy.
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# STOP - READ BEFORE WRITING ANY MALLOY CODE

> **AI AGENTS: You MUST review this file before writing Malloy code.** Cross-skill references below use logical `skill:` names; load the referenced skill before acting. Before writing code, also read the gotcha skills: `skill:malloy-gotchas-modeling`, `skill:malloy-gotchas-queries`, and `skill:malloy-gotchas-rendering`.

## Pre-Flight Checklist

1. **Discover first**: ground yourself before writing ANY code, with the tool that matches what you are modelling.
   - Modelling data **already in a package**: `malloy_getContext` returns that package's sources, views, and fields (with their docs).
   - Modelling **a database with no package yet**: `malloy_getContext` has nothing to return, so use `malloy_searchDatabaseSchema` instead. It walks the connection's schemas and tables, ranks them against a plain-English description, and gives you each table's columns plus the `source:` line to start from. Take those names verbatim into step 5.
   Never guess field names either way.
2. **Search docs proactively**: call `malloy_searchDocs` BEFORE writing unfamiliar patterns (window functions, query-based sources, pipelines). Don't guess. Malloy syntax is specific and SQL intuition is often wrong.
3. **Use `skill:malloy-patterns`** to discover available doc topics (YoY, cohorts, rendering, window functions).
4. **Check diagnostics** after writing: fix the FIRST error first, errors cascade.
5. **Read the gotcha skills**: `skill:malloy-gotchas-modeling`, `skill:malloy-gotchas-queries`, and `skill:malloy-gotchas-rendering` prevent the most common mistakes.

**Quick syntax reminders:**
1. **Backtick reserved words:** `` `Date` ``, `` `Hour` ``, `` `Timestamp` ``, `` `Type` ``, `` `number` ``, `` `source` ``
2. **Use `having:` for aggregate filters**: not `where:` on measures
3. **Alias joined fields in `group_by`** if using them in `order_by`
4. **`count()` counts rows; `count(x)` counts distinct values of `x`**: `count(distinct x)` is deprecated, write `count(x)`
5. **One tag per line**: `# label="Revenue"` and `# currency` on separate lines
6. **No fixed scale on measures**: use `# currency` not `# currency=usd0m`
7. **Cast strings for aggregates:** `avg(score::number)` not `avg(score)`
8. **Boolean columns:** use `= true` not `= 'true'` (no quotes!)
9. **Read data files in place:** `.csv`, `.parquet`, `.json`, `.ndjson`, and `.xlsx` all work as-is through `duckdb.table('data/file.ext')`. Never convert a file to another format first, and never read one with python or jq to "have a look" first: query it. For `.xlsx`, check the row count before trusting it: a workbook with a title row or a blank spacer reads short and reports no error. (Per-format quirks: `skill:malloy-gotchas-modeling`)

## Planning and `modeling-notes.md`

If the IDE has a native plan mode, use it for the high-level approach: do data exploration during planning, then present a concrete plan for user approval before writing any files.

`modeling-notes.md` is an expected output of the workflow, not an optional extra. Start it at step 2 (Propose Scope) and grow it as you work: it persists alongside the model, and its value is as the thing the user argues with at step 3, before source files exist; written after the build it can only document decisions already baked in. Record findings and problems as they are found during discovery (`skill:malloy-discover`), and every unconfirmed decision as an open item. Only when there is no writable workspace do the notes live in the conversation instead.

Keep it compact, with these sections:

```markdown
# Modeling notes - <package>
## Scope           what was confirmed, what the model is FOR, skip list with reasons
## Grain and keys  proven by query, not by column name
## Coverage        coverage cliffs; columns excluded for nullity
## Decisions       each with its evidence
## Open decisions  ASSUMPTIONS, NOT CONFIRMED: every threshold or definition the user
                   has not settled, one entry each, mirrored by a hedge in its #(doc)
## Validation      reconciliation checks performed, and their results
```

## 8-Step Modeling Workflow

The agent orchestrates all steps. Steps marked **(user)** pause for input. Each step has a dedicated skill with full instructions. Read each step's skill **before starting that step**, including the decision skills for steps 1–4 (`skill:malloy-discover`, `skill:malloy-scope`, `skill:malloy-define`). They govern what the model says; skipping them to reach the build skills is how unreviewed business logic ships.

**A field is not complete until it has its definition, `#(doc)` tag, and rendering tags, and any threshold or business convention in it is user-confirmed, distribution-derived, or explicitly flagged in its `#(doc)`** (see `skill:malloy-document` § Mark conventions as conventions). Documentation is part of defining a field, not a separate activity. Read `skill:malloy-document` for full documentation standards (doc string writing, tag ordering).

```
DISCOVER → SCOPE → SOURCES → DEFINITIONS → BUILD BASE → BUILD JOINED → REVIEW → CURATE
 (silent)  (user)   (user)      (user)       (agent)      (agent)      (user)   (user)
```

| Step | Skill | What Happens |
|------|-------|-------------|
| 1. Discover | `skill:malloy-discover` | Read the model and data; scan sources, fields, distributions; detect prior art. With no package yet, start from `malloy_searchDatabaseSchema` to find the tables in the connection |
| 2. Propose Scope | `skill:malloy-scope` | Present findings, user selects focus |
| 3. Propose Sources | `skill:malloy-define` | Propose source plan, user confirms architecture |
| 4. Propose Definitions | `skill:malloy-define` | Propose fields per base source, user confirms logic |
| 5. Build Base Sources | `skill:malloy-model` | Write fully documented base source files (one per table), check diagnostics. Read `skill:malloy-document` for doc standards. |
| 6. Build Joined Sources | `skill:malloy-model` | Write fully documented joined source files, validate. Read `skill:malloy-document` for doc standards. |
| 7. Review | (none) | Present the review checklist below; user confirms or corrects |
| 8. Curate | `skill:malloy-model` | Propose access controls (`explores`, `queryableSources`, access modifiers); always propose, the user decides whether to apply |

### The pauses are the point

These are governed semantic models: the business decisions in them must be confirmed by a human subject-matter expert, and the **(user)** steps exist to collect that confirmation. They are real stops, not progress reports. A model can be complete, compiling, and fully documented and still be wrong everywhere it guessed; a capable agent can build the whole thing without pausing once, which is exactly the failure mode this workflow exists to prevent.

When a decision goes unanswered (the user explicitly declines to decide, or nobody is there to ask), do not silently proceed as if it were settled. Take your best-supported position, label it an assumption in the field's own `#(doc)` (see `skill:malloy-document` § Mark conventions as conventions), record it under "Open decisions" in `modeling-notes.md`, and raise it again at Review. An unlabeled assumption is indistinguishable from a confirmed fact, and misleads everyone downstream.

### Step 7 Review is a checklist, not a summary

Present these to the user, with answers:

- **Which definitions did the user actually confirm?** List them; everything else is an assumption.
- **Which thresholds and bucket boundaries did you choose?** For each: the evidence (distribution query, metadata, prior art) and the `#(doc)` hedge that marks it.
- **Which questions were left unanswered?** Each must already carry a labeled assumption and an "Open decisions" entry.
- **Does the headline metric have more than one defensible definition?** If yes, that is a blocking question: put the candidate definitions to the user with their counts side by side, not in a footnote.

The user confirming this checklist is what makes the model governed. A summary of what you built is not a checkpoint.

Publishing is out of scope for open-source v1. Self-hosters move a finished model into a served package via git and the host's publish path; see `skill:malloy-publish` for the local-to-served handoff.

**Two paths to a model: both produce the same fully documented result:**
- **Schema-first:** "Model my data" → 8-step workflow above using the relevant skills
- **Analysis-first:** a data question arrives before any model exists → `skill:malloy-model-as-you-go`. It answers the question with `skill:malloy-analysis`, then codifies what the answer assumed into the model, one question at a time, confirming binding decisions first. The model exists by the end; there is no separate formalize step.
- **Open-ended exploration** with no intent to keep anything: `skill:malloy-analyze`. If it turns into something worth keeping, formalize via `skill:malloy-model` (`reference/analysis-to-model.md`).

## Agent Behavior

**Research before asking.** Present proposals with evidence. Never ask open-ended questions: propose with data and let the user confirm.

**Use business language.** Say "I simplified the column name" not "reserved word replaced." Don't expose Malloy internals unless the user asks.

**Describe what you're doing, not which step you're on.** The user doesn't have the skill files open. Say "I'll propose which tables to include and how they relate" not "Steps 3 and 4." Say "Now I'll write the source files" not "Moving to Step 5." Explain the purpose of each phase in plain language before doing it.

**Present choices as A/B/C.** When asking the user to choose, use lettered options with one-line descriptions. Mark your recommendation.

**Complete all workflow steps.** Once modeling begins, complete through Review and propose Curate. A field without documentation is not finished. If you lose track, re-read the model and your notes. Suggest notebooks at the end.

## Route by Intent

| User says... | Route to |
|-------------|----------|
| "Model my data", "create a model" | 8-step workflow (`skill:malloy-discover`) |
| "Model from LookML" | 8-step with prior art via `skill:malloy-lookml-review` |
| "Explore this data", "what's interesting?", "show me the top X" | `skill:malloy-analyze` (EDA) |
| "Build a dashboard", "create views" on existing model | `skill:malloy-analyze` (views), plus `skill:malloy-charts` or `skill:malloy-notebooks` as needed |
| "Build a model but not sure what metrics" | `skill:malloy-model-as-you-go`: answer their first real question, codify what it assumed, repeat |

**If the user's first message is a data question** (not "build me a model"), route to `skill:malloy-model-as-you-go`. It answers with `skill:malloy-analysis` and grows the model from what each answer assumed, so there is nothing to formalize afterwards.

## Additional Support Skills

These supplemental skills may also be loaded as needed:

- **`skill:malloy`**: Index of Malloy skills and routing guide
- **`skill:malloy-debug`**: Fix compile errors and interpret diagnostics

## Publisher MCP Tools

Ensure the Publisher MCP tools are configured before modeling. No server yet? `skill:malloy-getting-started` covers setup, including the one-command scaffolder (`npm create @malloy-publisher/malloy-package@latest <name>`) and why local authoring needs `--watch-env <env>`: start the server without it and your saved edits are never read.

| Tool | Purpose |
|------|---------|
| `malloy_getContext` | Ground yourself in a package: its sources, views, and fields |
| `malloy_executeQuery` | Run ad-hoc queries for validation |
| `malloy_compile` | Compile-check a change and get diagnostics back without running a query |
| `malloy_reloadPackage` | Recompile a package from disk so a saved edit becomes queryable by name |
| `malloy_searchDocs` | Search Malloy docs (call BEFORE unfamiliar patterns) |
| `malloy_searchDatabaseSchema` | Find the tables in a database connection by plain-English description, when modelling data that is not in a package yet. Returns each table's columns and the `source:` line to start from. Names and types only: no row value is returned |

Never guess field names. Ground yourself with `malloy_getContext` to see the sources and fields a package defines.

### The edit-and-run loop

Publisher compiles each configured package at boot and serves that cached model, so a source or view you add afterwards is not queryable by name until you reload the package. The loop is:

1. **Validate** the change with `malloy_compile`, picking the scope that matches what you are doing:
   - Adding a new definition or query: the default (`scope: "append"`) compiles your text in the model's namespace. Note its diagnostic positions land in the model-plus-your-text concatenation.
   - **Editing an existing definition: `scope: "file"`**, with the whole edited file as `source`. It compiles your text AS the file (append would collide with "Cannot redefine"), and diagnostics land at the true line numbers of your text.
   - Before saving a change other files import: `scope: "package"` with the edited file as `source` runs reload's worker compiler over every `.malloy` and `.malloynb` file against your edit, so a rename that breaks an importer surfaces now instead of at reload. Each diagnostic carries `model`, the file it points at; files hidden from discovery can appear. If `modelPath` does not exactly match an existing file, a warning says the source was treated as new.
2. **Save** it to the package's model file.
3. **Reload** with `malloy_reloadPackage`.
4. **Run** the new view with `malloy_executeQuery`.

A reload that fails to compile is safe: your files are left alone and the previously compiled model keeps serving, with the compile errors returned to you. Compile first anyway for faster feedback, and a `scope: "package"` dry-run with no `source` uses reload's compiler and file selection (imports across files, every `.malloy` and `.malloynb` file as saved) without touching the served model. Keep the source of truth outside `publisher_data/`, which is not version-controlled and is wiped by a `--init` restart. If these tools are missing, the Publisher you are connected to predates them; fall back to validating with a throwaway `malloy_executeQuery`. An older Publisher that has `malloy_compile` but rejects `scope` supports only the append behavior.

## SQL-to-Malloy Quick Reference

| SQL | Malloy |
|-----|--------|
| `COUNT(*)` | `count()` |
| `COUNT(DISTINCT x)` | `count(x)` |
| `NOW()` | `now` |
| `CASE WHEN...END` | `pick...when...else` |
| `col IN ('a','b')` | `col ? 'a' \| 'b'` |
| `COALESCE(a,b)` | `a ?? b` |
| `CAST(x AS type)` | `x::type` |
| `DATEDIFF(day, a, b)` | `days(a to b)` |
| `CONCAT(a, b)` or `a \|\| b` | `concat(a, b)` |
| `TIMESTAMP_DIFF(a, b, SECOND)` | `seconds(b to a)` |

## Critical Rules

1. **All keywords require colons**: `source:`, `dimension:`, `measure:`, `view:`
2. **Use `is` not `as`**: `dimension: name is expression`
3. **Arrow operator required**: `run: source -> { operations }`
4. **Specify join type**: `join_one:`, `join_many:`, `join_cross:`
5. **Safe division**: `revenue / nullif(count, 0)`
6. **Group definitions under one keyword**: `measure:` then indent fields beneath

## Common Anti-Patterns

```
WRONG: source flights is ...           RIGHT: source: flights is ...
WRONG: dimension: x as y               RIGHT: dimension: y is x
WRONG: count(*)                        RIGHT: count()
WRONG: count(distinct x)               RIGHT: count(x)
WRONG: revenue / order_count           RIGHT: revenue / nullif(order_count, 0)
WRONG: run: src { ... }                RIGHT: run: src -> { ... }
```

## Reserved Words: Scan Schema First

**Malloy has many reserved words. When in doubt, backtick it.** Most likely to appear as column names:

```
date, time, day, month, year, quarter, week, hour, minute, second,
number, string, boolean, type, table, source, index, count, sum, avg, min, max,
true, false, null, is, on, with, all, from, by, in, to, for, select, order_by,
top, bottom, desc, asc, row, range, current, window, rank
```

- `number`: only the bare word needs backticking; `account_number` is fine
- `source`: reserved; use a different alias like `traffic_source`
- `string`, `boolean`, `true`, `false`: backtick any column with these exact names

## Gotcha Skills: Read Before Writing Code

The following skills contain detailed WRONG/RIGHT patterns that prevent the most common Malloy errors. **Read them before writing code:**

- **`skill:malloy-gotchas-modeling`**: Reserved words, NULL checks, date functions, type casts, rename pitfalls, query-based source gotchas, `conn.sql()` anti-pattern
- **`skill:malloy-gotchas-queries`**: Chart constraints, aggregate filters, joined field aliasing, time truncation vs extraction
- **`skill:malloy-gotchas-rendering`**: Tag syntax, scale rules, sparkline setup, big_value patterns
