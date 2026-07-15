---
name: lookml-review
description: Analyze LookML files as prior art for Malloy modeling. Used during Step 1 (DISCOVER) when .lkml files are present. Coordinates reference files that extract business logic, relationships, and curation decisions. Works with or without a database connection.
---

# LookML Review

> **Purpose:** Evaluate a LookML project as prior art for building a Malloy semantic model. This skill coordinates the LookML adapter. The implementation lives in reference files under `reference/`.

> **This is NOT a blind conversion.** Each LookML pattern is evaluated for quality and relevance to Malloy. Bad practices, Looker-specific UI patterns, and performance-only constructs are identified and skipped.

## When to Use

- **Auto-detected:** The agent finds `.lkml` files during Step 1 (DISCOVER) and the user confirms they should be used as prior art.
- **Explicitly requested:** The user says "model from LookML", "convert LookML", or provides a path to LookML files.

## Two Modes

| Mode | When | Behavior |
|------|------|----------|
| **LookML + live data** | A connection is configured and you can query the data | LookML provides prior art; the data validates it. Full data-driven proposals. |
| **LookML only** | No connection, or queries return nothing | LookML provides all context. Proposals flagged as **unvalidated**. |

If in LookML-only mode, warn the user: "No database connection found. I'll use LookML as the sole source of context, but proposals cannot be validated against live data."

## Numeric Parity Validation (preflight before you trust the Looker path)

To prove the Malloy numbers match Looker, there are two channels, and the "obvious" one fails silently more often than you'd expect.

**Preflight the Looker-API path before attempting it.** Running the original explore through the Looker API only works if the API service account **satisfies that explore's `required_access_grants`**. A service account that doesn't (e.g. its `org_id` user attribute is empty/`NULL`, or an `*_user_id` attribute the grant keys on is unset) gets a **404 on every restricted explore**, indistinguishable at a glance from "explore not found", and cannot self-provision without `administer`/`sudo`. So before you build a parity harness on the Looker API:

1. Identify the target explore's `required_access_grants` and the user attributes they key on.
2. Verify the service account actually has non-empty values for those attributes. If it doesn't, the Looker path is a dead end: don't spend time discovering that through 404s.

**SQL-level parity against the same warehouse is a first-class fallback, not a consolation prize.** When the Looker path is blocked (or just as the primary method), validate by running equivalent SQL directly against the **same warehouse** the LookML explore reads and comparing to the Malloy result (`malloy_executeQuery`). This is what actually validates the numbers in practice: reach for it first if access grants are in doubt.

## Reference Files

Each reference file is loaded by the workflow phase that needs it (via dispatch tables in each phase skill). You do not need to read them all at once.

| Reference File | Phase | What It Does |
|------------|-------|-------------|
| `reference/discover.md` | Step 1 (DISCOVER) | Inventory .lkml files, extract source candidates, capture prior-art notes |
| `reference/propose-fields.md` | Step 4 (DEFINE) | Extract field proposals from .lkml views |
| `reference/build-derived-tables.md` | Step 5 (BUILD) | Classify and convert LookML derived tables |
| `reference/build-unnest.md` | Step 5 (BUILD) | Convert UNNEST joins and struct field access |
| `reference/curate-visibility.md` | Step 8 (CURATE) | Map LookML visibility mechanisms to Malloy access modifiers |
| `reference/document.md` | Step 9 (DOCUMENT) | Extract LookML descriptions as `#(doc)` tag seeds |
| `reference/review-coverage.md` | Step 7 (REVIEW) | Compare Malloy model against LookML: source, field, and join coverage with rationale for gaps |

### Shared Reference

`reference/_concepts.md` is the LookML to Malloy concept mapping table. Referenced by `propose-fields.md` and `build-derived-tables.md` for type mapping and syntax translation.

## What LookML Provides

- Field names, descriptions, and business logic (accelerates Step 4)
- Join relationships and cardinality (accelerates Step 3)
- Field visibility decisions: `hidden: yes`, `fields` exclusions, `required_access_grants` (accelerates Step 8)
- Organizational structure via `group_label` and `view_label` (informs source design)
- Derived table intent: NDTs to computed sources, PDTs to evaluate

## What to Skip

- Looker UI patterns (`link:`, `drill_fields:`, `html:`, `action:`)
- Liquid templating (`{% %}`, `{{ }}`): strip and note intent
- `parameter:` definitions: note the business intent, don't replicate
- PDT optimization (`partition_keys:`, `datagroup_trigger:`, `increment_key:`)
- Dashboard files (`.dashboard.lookml`)
- `sql_always_where:`: document as context, don't bake into Malloy

## What to Flag for User Decision

- Complex SQL dimensions (50+ lines): default is simplify or push upstream
- Derived tables: classify as performance-only, transformation, or aggregation
- Refinement structure (`+view`): consolidate vs. preserve layering via `extend`
- Synthetic primary keys: ask about actual grain
