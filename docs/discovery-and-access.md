# Discovery surface & query boundary

> What this is: how a package controls **which** models and sources are visible and queryable. This
> is a different axis from [givens](givens.md)-based access control: it shapes the *surface* (what
> exists and what is a valid query target) regardless of who is asking. To gate **who** may query a
> source by caller identity, see [authorize.md](authorize.md); to scope **which rows** they see, see
> [row-level-access.md](row-level-access.md).

Declaring `explores` in `publisher.json` is the **single opt-in** for curated discovery. When absent
or empty, every model is listed with its full source set — today's backward-compatible behavior.

A package's manifest can scope which models and sources appear in listings (the surface that drives
discovery and chat), at two granularities that **both apply only after `explores` is declared**:

- **File level — `explores`.** An optional `string[]` of `.malloy` file paths (relative to the
  package root) that form the package's public surface. When present, only those models are returned
  by `listModels()`; every other `.malloy` file still compiles for import/join resolution and stays
  queryable, but is hidden from listings. When absent or empty, every model is listed. Notebooks are
  always listed regardless of this field (they can't be imported, so they have nothing to hide
  behind).

  ```json
  {
    "name": "sales",
    "description": "Sales models",
    "explores": ["index.malloy"]
  }
  ```

- **Within a file — `export { … }`.** Once `explores` is declared, the discovery accessors list only
  the model's re-export closure (`modelDef.exports`), matching what Malloy's `modelInfo`/`sourceInfos`
  expose. A model with no `export { … }` exports all of its locally-declared top-level sources;
  declaring `export { customers }` lists only `customers` and keeps imported/internal helpers out.

The two compose: `explores` decides which files are listed, and `export { … }` decides which sources
within a listed file are shown.

## Query boundary — `queryableSources`

Controls whether that discovery surface is *also* a query boundary. `"declared"` (the default) makes
**queryable == discoverable**: when `explores` is declared, only `explores` files — and within them
only the `export {}` closure — are valid top-level query targets; every other source still compiles,
imports, joins, and extends, but a direct query against it is denied with a `404` (indistinguishable
from a non-existent target). `"all"` decouples the axes — `explores`/`export {}` gate discovery only
and every compiled source stays directly queryable. When `explores` is absent there is no curated
surface, so both modes are equivalent (everything queryable).

```json
{ "name": "sales", "explores": ["index.malloy"], "queryableSources": "all" }
```

For gradual migration, use `explores` with `queryableSources: "all"` to curate listings while keeping
every source queryable by name; switch to `"declared"` when ready to enforce the boundary.

> **`explores`/`export {}` are a discovery filter; `queryableSources` decides if they also gate
> queries; `#(authorize)` is the identity gate.** With `queryableSources: "all"`, hiding a source
> only removes it from listings — it stays queryable by name. To restrict *who* can query (as opposed
> to *what* is queryable), gate the source with `#(authorize)` (see [authorize.md](authorize.md));
> those gates are enforced against the complete source set and are never weakened by listing or
> boundary curation.
>
> The `queryableSources` boundary applies to the *query* surface (`getQueryResults`, the MCP query
> tool, and `/compile`). It does **not** cover raw retrieval by exact path — a hidden model's file
> text and its compiled metadata are still fetchable by path — by design; use `#(authorize)` when the
> contents themselves must be protected, not just removed from discovery.

## Runnable example

[`examples/governed-analytics`](../examples/governed-analytics) curates its surface in
[`publisher.json`](../examples/governed-analytics/publisher.json):

```json
{
  "explores": ["orders.malloy", "secured.malloy"],
  "queryableSources": "declared"
}
```

`orders_base` lives in [`internal.malloy`](../examples/governed-analytics/internal.malloy), which is
**not** listed — so the public models still `import` it, but it is hidden from discovery and, because
the boundary is `"declared"`, a direct query is denied:

```bash
API=http://localhost:4000/api/v0/environments/examples/packages/governed-analytics/models
curl -s -X POST $API/internal.malloy/query -H 'content-type: application/json' \
  -d '{"query":"run: orders_base -> { aggregate: c is count() }"}'   # → 404 (indistinguishable from non-existent)
```

## Validation

Validation is asymmetric by design: **publishing** a package with an `explores` entry that doesn't
resolve to a real model is rejected with a `400`, while at **startup/reload** the package still serves
but hides the unresolved entry (it never falls back to listing everything) and surfaces the reason in
the package's `exploresWarnings` field.
