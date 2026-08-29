---
name: malloy-modeling
description: Build semantic models with Malloy. Read this skill whenever the user asks about modeling data or specifically mentions Malloy.
---

# STOP - READ BEFORE WRITING ANY MALLOY CODE

> **AI AGENTS: You MUST review this file before writing Malloy code.** Cross-skill references below use logical `skill:` names; load the referenced skill before acting. Before writing code, also read the gotcha skills: `skill:malloy-gotchas-modeling`, `skill:malloy-gotchas-queries`, and `skill:malloy-gotchas-rendering`.

> **Tool names** are written bare here - `get_context`, `execute_query`, `search_malloy_docs`. The exact prefixed name depends on the host surface; match each against the tools you actually have.

The host's own setup skill names the exact tools and how to reach them, including host-only capabilities such as compile-checking a change and reloading a package so a saved edit is queryable by name.

## Pre-Flight Checklist

1. **Discover first**: ground yourself before writing ANY code, with the tool that matches what you are modelling.
   - Modelling data **already in a package**: `get_context` returns that package's sources, views, and fields (with their docs).
   - Modelling **a database with no package yet**: `get_context` has nothing to return, so use `search_database_schema` instead. It walks the connection's schemas and tables, ranks them against a plain-English description, and gives you each table's columns plus the `source:` line to start from. Take those names verbatim into step 5.
   Never guess field names either way.
2. **Search docs proactively**: call `search_malloy_docs` BEFORE writing unfamiliar patterns (window functions, query-based sources, pipelines). Don't guess. Malloy syntax is specific and SQL intuition is often wrong.
3. **Use `skill:malloy-patterns`** to discover available doc topics (YoY, cohorts, rendering, window functions).
4. **Check diagnostics** after writing: fix the FIRST error first, errors cascade.
5. **Read the gotcha skills**: `skill:malloy-gotchas-modeling`, `skill:malloy-gotchas-queries`, and `skill:malloy-gotchas-rendering` prevent the most common mistakes. Syntax reminders, the SQL-to-Malloy mapping, reserved words, critical rules, and anti-patterns live there and in `skill:malloy-queries`; do not rely on a summary here.

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
| 1. Discover | `skill:malloy-discover` | Read the model and data; scan sources, fields, distributions; detect prior art. With no package yet, start from `search_database_schema` to find the tables in the connection |
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

Publishing is out of scope of this workflow. The host's own publish path is how a finished model becomes a served package; the host's setup or publish skill names that path.

**Two paths to a model: both produce the same fully documented result:**
- **Schema-first:** "Model my data" is the 8-step workflow above.
- **A question that arrives before a model:** `skill:malloy-model-as-you-go`. Answer first with `skill:malloy-analysis`, then write down what the answer assumed.

After analysis completes, **always recommend formalizing into a model.**

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
| "Explore this data", "what's interesting?", "show me the top X" | `skill:malloy-analysis` |
| "Build a dashboard" | the host's dashboard skill |
| "Create views" on existing model | `skill:malloy-charts` or `skill:malloy-notebooks` |
| "Build a model but not sure what metrics" | `skill:malloy-analysis` first, then `skill:malloy-model-as-you-go` |

**If the user's first message is a data question** (not "build me a model"), route to `skill:malloy-analysis`. After analysis completes, write down what the answer assumed with `skill:malloy-model-as-you-go`.

## Additional Support Skills

These supplemental skills may also be loaded as needed:

- The host's skill index names the rest of the surface.
- **`skill:malloy-debug`**: Fix compile errors and interpret diagnostics
