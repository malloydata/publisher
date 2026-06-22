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
