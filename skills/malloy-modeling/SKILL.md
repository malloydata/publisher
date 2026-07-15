---
name: malloy-modeling
description: Build semantic models with Malloy for the Malloy Publisher. Read this skill whenever the user asks about modeling data or specifically mentions Malloy.
---

# STOP - READ BEFORE WRITING ANY MALLOY CODE

> **AI AGENTS: You MUST review this file before writing Malloy code.** Cross-skill references below use logical `skill:` names; load the referenced skill before acting. Before writing code, also read the gotcha skills: `skill:gotchas-modeling`, `skill:gotchas-queries`, and `skill:gotchas-rendering`.

## Pre-Flight Checklist

1. **Discover first**: ground yourself with `malloy_getContext` before writing ANY code. It returns the package's sources, views, and fields (with their docs), so you build on what actually exists. Never guess field names.
2. **Search docs proactively**: call `malloy_searchDocs` BEFORE writing unfamiliar patterns (window functions, query-based sources, pipelines). Don't guess. Malloy syntax is specific and SQL intuition is often wrong.
3. **Use `skill:malloy-patterns`** to discover available doc topics (YoY, cohorts, rendering, window functions).
4. **Check diagnostics** after writing: fix the FIRST error first, errors cascade.
5. **Read the gotcha skills**: `skill:gotchas-modeling`, `skill:gotchas-queries`, and `skill:gotchas-rendering` prevent the most common mistakes.

**Quick syntax reminders:**
1. **Backtick reserved words:** `` `Date` ``, `` `Hour` ``, `` `Timestamp` ``, `` `Type` ``, `` `number` ``, `` `source` ``
2. **Use `having:` for aggregate filters**: not `where:` on measures
3. **Alias joined fields in `group_by`** if using them in `order_by`
4. **Use `count(x)` not `count(distinct x)`**: Malloy's count() is always distinct
5. **One tag per line**: `# label="Revenue"` and `# currency` on separate lines
6. **No fixed scale on measures**: use `# currency` not `# currency=usd0m`
7. **Cast strings for aggregates:** `avg(score::number)` not `avg(score)`
8. **Boolean columns:** use `= true` not `= 'true'` (no quotes!)

## Planning and `modeling-notes.md`

If the IDE has a native plan mode, use it for the high-level approach: do data exploration during planning, then present a concrete plan for user approval before writing any files. Once approved, you can write a `modeling-notes.md` during execution to record decisions (scope, sources, key choices, prior art, gaps). This file persists alongside the model. Otherwise, keep the proposal and decisions in the conversation; Publisher has no separate workspace document store to write them to.

## 8-Step Modeling Workflow

The agent orchestrates all steps. Steps marked **(user)** pause for input. Each step has a dedicated skill with full instructions; load the relevant skill when needed.

**A field is not complete until it has its definition, `#(doc)` tag, and rendering tags.** Documentation is part of defining a field, not a separate activity. Read `skill:malloy-document` for full documentation standards (doc string writing, tag ordering).

```
DISCOVER → SCOPE → SOURCES → DEFINITIONS → BUILD BASE → BUILD JOINED → REVIEW → CURATE
 (silent)  (user)   (user)      (user)       (agent)      (agent)      (user)   (user)
```

| Step | Skill | What Happens |
|------|-------|-------------|
| 1. Discover | `skill:malloy-discover` | Read the model and data; scan sources, fields, distributions; detect prior art |
| 2. Propose Scope | `skill:malloy-scope` | Present findings, user selects focus |
| 3. Propose Sources | `skill:malloy-define` | Propose source plan, user confirms architecture |
| 4. Propose Definitions | `skill:malloy-define` | Propose fields per base source, user confirms logic |
| 5. Build Base Sources | `skill:malloy-model` | Write fully documented base source files (one per table), check diagnostics. Read `skill:malloy-document` for doc standards. |
| 6. Build Joined Sources | `skill:malloy-model` | Write fully documented joined source files, validate. Read `skill:malloy-document` for doc standards. |
| 7. Review | (none) | Present structure, assumptions, and doc coverage; user confirms |
| 8. Curate | `skill:malloy-model` | Propose access controls, user approves: optional, ask user |

Publishing is out of scope for open-source v1. Self-hosters move a finished model into a served package via git and the host's publish path; see `skill:malloy-publish` for the local-to-served handoff.

**Two paths to a model: both produce the same fully documented result:**
- **Schema-first:** "Model my data" → 8-step workflow above using the relevant skills
- **Analysis-first:** "Explore this data" → `skill:malloy-analyze` → formalize via `skill:malloy-model` (`reference/analysis-to-model.md`)

After analysis completes, **always recommend formalizing into a model.**

## Agent Behavior

**Research before asking.** Present proposals with evidence. Never ask open-ended questions: propose with data and let the user confirm.

**Use business language.** Say "I simplified the column name" not "reserved word replaced." Don't expose Malloy internals unless the user asks.

**Describe what you're doing, not which step you're on.** The user doesn't have the skill files open. Say "I'll propose which tables to include and how they relate" not "Steps 3 and 4." Say "Now I'll write the source files" not "Moving to Step 5." Explain the purpose of each phase in plain language before doing it.

**Present choices as A/B/C.** When asking the user to choose, use lettered options with one-line descriptions. Mark your recommendation.

**Complete all workflow steps.** Once modeling begins, complete through review. A field without documentation is not finished. If you lose track, re-read the model and your notes. Suggest notebooks at the end.

## Route by Intent

| User says... | Route to |
|-------------|----------|
| "Model my data", "create a model" | 8-step workflow (`skill:malloy-discover`) |
| "Model from LookML" | 8-step with prior art via `skill:lookml-review` |
| "Explore this data", "what's interesting?", "show me the top X" | `skill:malloy-analyze` (EDA) |
| "Build a dashboard", "create views" on existing model | `skill:malloy-analyze` (views), plus `skill:malloy-charts` or `skill:malloy-notebooks` as needed |
| "Build a model but not sure what metrics" | `skill:malloy-analyze` first, then formalize via `skill:malloy-model` |

**If the user's first message is a data question** (not "build me a model"), route to `skill:malloy-analyze`. After analysis completes, **always recommend formalizing via the analysis-to-model workflow** (`skill:malloy-model` → `reference/analysis-to-model.md`).

## Additional Support Skills

These supplemental skills may also be loaded as needed:

- **`skill:malloy`**: Index of Malloy skills and routing guide
- **`skill:malloy-debug`**: Fix compile errors and interpret diagnostics

## Publisher MCP Tools

Ensure the Publisher MCP tools are configured before modeling.

| Tool | Purpose |
|------|---------|
| `malloy_getContext` | Ground yourself in a package: its sources, views, and fields |
| `malloy_executeQuery` | Run ad-hoc queries for validation |
| `malloy_compile` | Compile-check a change and get diagnostics back without running a query |
| `malloy_reloadPackage` | Recompile a package from disk so a saved edit becomes queryable by name |
| `malloy_searchDocs` | Search Malloy docs (call BEFORE unfamiliar patterns) |

Never guess field names. Ground yourself with `malloy_getContext` to see the sources and fields a package defines.

### The edit-and-run loop

Publisher compiles each configured package at boot and serves that cached model, so a source or view you add afterwards is not queryable by name until you reload the package. The loop is:

1. **Validate** the change with `malloy_compile`, which reads the model fresh from disk and returns diagnostics without running anything.
2. **Save** it to the package's model file.
3. **Reload** with `malloy_reloadPackage`.
4. **Run** the new view with `malloy_executeQuery`.

Compile first is a safety gate, not just a speed-up: a reload whose models do not compile removes the package's on-disk copy under `publisher_data/` and drops the cached package, and a second reload cannot bring it back. Watch-mode symlink mounts are exempt. Keep the source of truth outside `publisher_data/`, which is not version-controlled and is wiped by a `--init` restart. If these two tools are missing, the Publisher you are connected to predates them; fall back to validating with a throwaway `malloy_executeQuery`.

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

- **`skill:gotchas-modeling`**: Reserved words, NULL checks, date functions, type casts, rename pitfalls, query-based source gotchas, `conn.sql()` anti-pattern
- **`skill:gotchas-queries`**: Chart constraints, aggregate filters, joined field aliasing, time truncation vs extraction
- **`skill:gotchas-rendering`**: Tag syntax, scale rules, sparkline setup, big_value patterns
