---
name: malloy-publish
description: Package Malloy models for serving by Malloy Publisher. Use when user asks to "publish", "package", "deploy", or wants to share models with others.
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Packaging Malloy models for Publisher

> **CRITICAL: Only package or prepare a release when the user explicitly asks.** Making model changes, adding documentation, or building notebooks is NOT a publish request. Never auto-package after completing other tasks.

## Publishing in open-source Publisher

Automated publishing is not part of the open-source Malloy Publisher tool surface yet. There is no publish tool to call from this skill. What this skill does is get a package into a publishable shape: a valid `publisher.json`, a flat layout, and the right files in the package root.

Once the package is in shape, self-hosters publish it through their own host: commit the package to git, then run the host's publish path (for example, the deploy step that points a Publisher server at the package directory or repository). The mechanics of that path depend on how the Publisher instance is deployed, so confirm with the user how their instance is served rather than assuming a hosted control plane.

## Prerequisites

- Malloy model (`.malloy`) and/or notebook (`.malloynb`) files ready
- The Publisher MCP tools configured (used by the modeling and analysis skills, not by a publish step)

## Connections: a flat-file package needs none

A package backed by data files (CSV/Parquet/XLSX/JSON) needs **no `connections` entry at all** in `publisher.config.json`: every loaded package automatically gets its own DuckDB sandbox connection named `duckdb`, which is what `duckdb.table('data/file.csv')` resolves against. That name is **reserved**: declaring an environment-level connection named `duckdb` fails the whole environment at init (name an env-level DuckDB connection something like `shared_duckdb` instead). See `docs/connections.md`.

Two more facts about how a Publisher server sees the package, both easy to get wrong:

- **A package `location` is treated as local only when it starts with `./`, `../`, `~/`, or `/`.** A bare name like `"spotify"` is silently not local; write `"./spotify"`.
- **Local authoring means `--watch-env`.** Without `--watch-env <env>`, Publisher **copies** each local package into `publisher_data/` at boot and serves the copy; edits to your source directory are never read, however many times you save. Start the server with `--watch-env <env>` (mounts the package in place and live-reloads), the same command `skill:malloy-html-data-apps` uses:

  ```sh
  npx @malloy-publisher/server --server_root . --port 4000 --watch-env <env>
  ```

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

A package with no `index.malloy` and no `explores` exposes **everything**: every model is listed and every source is directly queryable.

To curate, add an **`index.malloy`** at the package root. Publisher reads it as the package's published surface, so no manifest field is involved:

```malloy
// index.malloy
import "order_analysis.malloy"
import "staging.malloy"

export { orders, customers }
```

What it exports is what agents discover **and** what may be queried. Everything else still compiles, and other models can import, join and extend it, but a direct query against it is refused with a 404. Where nothing in the model is gated, the 404 says the source is off the surface and how to publish it; where a gate is in play, it reads exactly like a source that does not exist. Reach for this when you have raw/staging/scaffolding sources that exist to build a curated entry point and you don't want agents landing on, or querying, them directly.

**Address queries to the surface.** Once a package has an `index.malloy`, `.../models/staging.malloy/query` is no longer a query entry point, *even for a source that file declares itself*. Use `.../models/index.malloy/query`. If you are debugging a refusal rather than authoring, `skill:malloy-source-unreachable` covers the three ways a source can be out of reach and how to tell them apart.

**A surface can be layered.** An `index.malloy` may front a file that fronts another. A source re-exported through a chain of files stays queryable through the surface at any depth, because admission follows the declaration rather than the path taken to it.

**Curation hides a landing point, not a column.** A published source may `join` an unpublished one, and a query grouping by a joined field returns that field's values normally. If a column must not be readable, do not join it into something you publish; gate it with `#(authorize)` instead.

**Leaving a source out does not put it out of reach, but there is a condition.** `export { ... }` also decides what an *importing* file may see, which is Malloy's rule rather than Publisher's. A file that declares no `export` hands an importer everything it declares, so an unpublished source stays importable and joinable from the file that declares it. A file that *does* declare one hands over exactly that list: importing a file whose `export` omits a source and then naming it fails to compile with `Reference to undefined object`. Put an `export` on a mid-layer file only when you mean to narrow what its importers can build on, not just what Publisher lists.

**About `export { … }`:** the surface filters which *files* are listed; `export { … }` (a Malloy statement) filters which *sources within a file* are exposed, and the two compose. You usually don't write it in a leaf model: a file with **no** `export` exposes all of its own top-level sources. It must appear after the definitions it names. See [Malloy: Imports & Exports](https://docs.malloydata.dev/documentation/language/imports).

### The older manifest fields

Both still work and are not going away in this release. Both are deprecated where `index.malloy` replaces them, and a package using them that way gets a load-time warning naming the replacement. The two uses it cannot replace, an `explores` naming several files and `queryableSources: "all"`, stay supported without a warning. Do not add either key to a new package that does not need one of those two.

```json
{
  "name": "ecommerce",
  "explores": ["order_analysis.malloy", "customer_health.malloy"],
  "queryableSources": "all"
}
```

- **`explores`** (`string[]`) - model file paths relative to the package root, naming the surface. Reach for it for the one thing `index.malloy` cannot express: a surface spanning **several** files. An explicit `explores` always wins over the convention, and a package with both an `index.malloy` and an `explores` that omits it carries a warning saying so. An entry that doesn't resolve to a real `.malloy` file surfaces in `exploresWarnings`; publishing a package that has any is rejected, so fix the path before publishing. An explicit `"explores": []` means "do not curate" and suppresses the convention.
- **`queryableSources`** (`"declared"` | `"all"`, default `"declared"`) - the query boundary. `"declared"` is already the default, so the key changes nothing. **`"all"` is the exception, and `index.malloy` does not replace it:** it curates listings while leaving every source queryable by name, and a surface derived from an `index.malloy` always enforces the boundary. If you want listings-only curation, keep both keys.


> **Not access control.** The surface gates the query surface (the query endpoints, REST and MCP alike), not compile and not raw file retrieval by exact path: `/compile` and `compile_model` are deliberately exempt, because compile is the authoring loop and the boundary is discovery curation. It doesn't restrict *who* may query, only *what* is queryable by name. Queryable sources are the union of every listed file's `export {}` closure, whichever listed model path a query addresses them through. To gate access by caller-supplied identity/role, use `#(authorize)` on the source (and `#(access_filter)` to scope rows), see `skill:malloy-model` § Access Control and `docs/authorize.md`. Discovery curation and these gates are independent layers.

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
- Data files (CSV/Parquet/XLSX) - Embedded data published with package

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
