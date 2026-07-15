---
name: malloy-publish
description: Package Malloy models for serving by Malloy Publisher. Use when user asks to "publish", "package", "deploy", or wants to share models with others.
---

# Packaging Malloy models for Publisher

> **CRITICAL: Only package or prepare a release when the user explicitly asks.** Making model changes, adding documentation, or building notebooks is NOT a publish request. Never auto-package after completing other tasks.

## Publishing in open-source Publisher

Automated publishing is not part of the open-source Malloy Publisher tool surface yet. There is no publish tool to call from this skill. What this skill does is get a package into a publishable shape: a valid `publisher.json`, a flat layout, and the right files in the package root.

Once the package is in shape, self-hosters publish it through their own host: commit the package to git, then run the host's publish path (for example, the deploy step that points a Publisher server at the package directory or repository). The mechanics of that path depend on how the Publisher instance is deployed, so confirm with the user how their instance is served rather than assuming a hosted control plane.

## Prerequisites

- Malloy model (`.malloy`) and/or notebook (`.malloynb`) files ready
- The Publisher MCP tools configured (used by the modeling and analysis skills, not by a publish step)

## Step 1: Verify publisher.json

Check if `publisher.json` exists in the package root. If it does, proceed to Step 2.

If it doesn't exist, create one. Suggest a package name based on the model content, write a brief description, and default to version `0.0.1`.

```json
{
  "name": "package-name",
  "version": "0.0.1",
  "description": "Brief description of the package"
}
```

**Naming conventions:**
- `name`: lowercase, hyphens allowed (e.g., `ecommerce`, `sales-analytics`)
- `version`: semver format (e.g., `0.0.1`, `1.2.3`)

### Curating discovery & the query boundary (optional)

By default a package exposes **everything**: every model is listed and every source is directly queryable. That's the right behavior for most packages, and it's the safe default: **omit these fields and nothing changes.** Reach for them when you have raw/staging/scaffolding sources that exist to build a curated entry point and you don't want agents landing on (or querying) them directly.

Two optional fields opt the package into curation:

```json
{
  "name": "ecommerce",
  "version": "0.0.1",
  "description": "Orders, customers, and revenue analysis",
  "explores": ["order_analysis.malloy", "customer_health.malloy"],
  "queryableSources": "declared"
}
```

- **`explores`** (`string[]`) — an allowlist of **model file paths** (relative to the package root, not source or view names) whose models agents should discover and land on. Declaring it is the single opt-in for all discovery curation. With `explores` set, listings narrow to those files, and within each file to its `export { ... }` closure (below), so anything a listed file doesn't export is also dropped. Other files still compile, and can still be imported or joined, but are hidden from listings. Leaving `explores` **absent or empty** means every model is listed, unchanged from today, so existing packages don't break when this field is added. An entry that doesn't resolve to a real `.malloy` file surfaces in `exploresWarnings`; publishing a package that has any is rejected, so fix the path before publishing.
- **`queryableSources`** (`"declared"` | `"all"`, default `"declared"`) — the query boundary. Only takes effect once `explores` is set. `"declared"` makes queryable == discoverable: only the `explores` files and their `export {}` closure are valid top-level query targets; other sources still compile, import, and join, but a direct query against one is denied. `"all"` curates discovery only: every compiled source stays queryable even though `explores` narrows what's listed.

**About `export { … }`:** `explores` filters which *files* are listed; `export { … }` (a Malloy statement) filters which *sources within a file* are exposed, the two compose. You usually don't write it: a file with **no** `export` exposes all of its own top-level sources. Add `export { orders, customers }` to a file to expose only those and keep imported/scaffolding helpers out of discovery (it must appear after the definitions it names). See [Malloy: Imports & Exports](https://docs.malloydata.dev/documentation/language/imports).

**Why curate here:** declaring `explores` routes agents to the well-documented curated sources instead of raw tables, and `queryableSources: "declared"` keeps them from reaching the hidden sources by name. The two axes compose: list a file in `explores` for its models to be discoverable, and `export` a source within that file for it to be a landing point.

> **Not access control.** `queryableSources` gates the query surface (query / compile / MCP), not raw file retrieval by exact path, and it doesn't restrict *who* may query, only *what* is queryable by name. To gate access by caller-supplied identity/role, use `#(authorize)` on the source, see `skill:malloy-model` § Access Control and `docs/authorize.md`. Discovery curation and `#(authorize)` are independent layers.

The manifest also carries a `scope` field (`"package"` | `"version"`, default `"package"`) controlling whether persisted/materialized artifacts are shared across published versions or owned by a single version, and a `materialization` field configuring that persistence policy (a cron `schedule` or a `freshness` window). Both are unrelated to discovery curation; there is no per-source `sharing` or `schedule` field, that was retired in favor of the single package-level `scope` and `materialization`.

## Step 2: Confirm the package layout

With a valid `publisher.json` in place, confirm the package is in the flat, publishable shape described below. There is no publish tool to call in open-source v1; hand the package off to the host's publish path (git plus the deploy step for your Publisher instance).

## Package Structure

All `.malloy` files must be in the package root (flat layout: the publisher does not support cross-directory imports yet).

```
<package-name>/
  publisher.json
  customers.malloy              # Base source file
  orders.malloy                 # Base source file
  user_order_facts.malloy       # Computed source
  order_analysis.malloy         # Source file (joins base sources)
  customer_health.malloy        # Source file
  monthly_report.malloynb       # Notebook (optional)
```

Publishable contents:
- `.malloy` files - Semantic model definitions (base sources + joined sources)
- `.malloynb` files - Notebooks for exploration/documentation (see `skill:malloy-notebooks`)
- Data files (CSV/Parquet) - Embedded data published with package

## Version Management

- Treat each published version as immutable once it is served.
- "Latest" determines the default version consumers resolve.
- Keep older versions available so existing consumers keep working.
- Bump the version in `publisher.json` when you cut a new release, or if a publish step rejects a version that already exists.

## Workflow

1. Verify `publisher.json` exists; if not, create it (suggest name from model content, default `0.0.1`).
2. Confirm the flat package layout: all `.malloy` files in the package root.
3. Hand the package to the host's publish path (commit to git, then run the deploy step for your Publisher instance). If a version-already-exists conflict occurs, bump the patch version in `publisher.json` and retry.
4. Confirm with the user how their package is served so they can verify it is reachable.

## Common Issues

- **Cross-directory imports fail**: Move all `.malloy` files into the package root; the publisher uses a flat layout.
- **Version already exists**: Bump the patch version in `publisher.json` before re-publishing.

## Done

Step complete. Output: package is in publishable shape (valid `publisher.json`, flat layout), ready for the host's publish path.
