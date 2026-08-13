# Discovery surface & query boundary

> What this is: how a package controls **which** models and sources are visible and queryable. This
> is a different axis from [givens](givens.md)-based access control: it shapes the *surface* (what
> exists and what is a valid query target) regardless of who is asking. To gate **who** may query a
> source by caller identity, see [authorize.md](authorize.md); to scope **which rows** they see, see
> [row-level-access.md](row-level-access.md).

A package gets a curated discovery surface in one of two ways: by having an `index.malloy`, or by
declaring `explores` in `publisher.json`. With neither, every model is listed with its full source
set, which is the backward-compatible behavior.

A package's manifest can scope which models and sources appear in listings (the surface that drives
discovery and chat), at two granularities that **both apply only once the package has a surface**:

- **File level: `index.malloy`, or `explores`.** If the package root holds a file called
  `index.malloy`, that file is the package's published surface and no configuration is needed. To
  name a different set, declare `explores`: an optional `string[]` of `.malloy` file paths (relative
  to the package root). Either way, only those models are returned by `listModels()`, and every
  other `.malloy` file still compiles for import/join resolution but is hidden from listings.
  Notebooks are always listed regardless (they can't be imported, so they have nothing to hide
  behind).

  **The two are not equivalent for queries.** A surface from `index.malloy` hides models from
  listings and nothing more: every source stays queryable by name. Declaring `explores` also turns
  on the query boundary, so unlisted sources start being refused. That is the whole difference
  between them, and the [next section](#query-boundary--queryablesources) is about it.

  So this package needs no manifest key at all:

  ```
  sales/
    publisher.json     { "name": "sales" }
    index.malloy       import "orders.malloy"
                       export { orders }
    orders.malloy
  ```

  and this one curates a two-file surface the convention cannot express:

  ```json
  {
    "name": "sales",
    "description": "Sales models",
    "explores": ["orders.malloy", "secured.malloy"]
  }
  ```

  An explicit `explores` always wins over the convention. If a package has both and they disagree,
  the explicit key is used and the package carries a warning saying so rather than the server
  guessing. That warning is on the package itself, in the `warnings` of
  `GET /api/v0/environments/{env}/packages/{pkg}`, not in the server log.

  Declaring `"explores": []` is a third, explicit state: an empty array means "do not curate", and
  it suppresses the convention. Only a package with no `explores` key **and** no `index.malloy` is
  uncurated by default.

  > **Upgrading an existing package.** If you already have a package with a root `index.malloy` and
  > no `explores`, this changes what it lists: the surface becomes that one file, so your other
  > models drop out of listings and `export { … }` curation starts applying inside it. Nothing
  > becomes unreachable, because the convention never gates queries (see the boundary section
  > below), so anything you did not mean to hide is still queryable by name while you fix it.
  >
  > That last point is about query access, not about listings. Anything that reads the listing
  > rather than querying by name narrows with it: a catalog, a chat surface, MCP
  > `malloy_getContext`, or a search indexer sees only the surface at its next pass. Curation is
  > the whole point of the convention, so that is intended, but it is worth knowing before you
  > wonder where an indexed source went.
  >
  > **To keep exactly the old behavior, add `"explores": []` to the package's own `publisher.json`.**
  > One key, no rename, no boundary: the empty array is read as a deliberate "do not curate", so
  > listings, `export {}` filtering and query access are all unchanged.
  >
  > It has to go in the package source. Setting it through the API writes into the server's
  > `publisher_data/` copy, so `--init`, a fresh server root, or a replica that re-copies the
  > package all revert it, and a deployment with `"frozenConfig": true` refuses the call outright.
  > If you run packages you do not own, that means there is no fix on your side: either accept the
  > narrowed listings or ask each package's owner to add the key.
  >
  > Two things not to reach for. Renaming the file changes the model's identity, so
  > `…/models/index.malloy` starts returning 404, and if any sibling `import`s `"index.malloy"` the
  > dangling import fails the compile, which takes the **whole package** out of service rather than
  > just that file: it disappears from `GET …/packages` entirely and the only trace is
  > `loadErrors` in `GET /api/v0/status`. And declaring `explores` with your old file list does
  > **not** restore the old behavior either: it turns on the query boundary and `export {}`
  > filtering, which is a larger change than the one you are undoing.
  >
  > **Watch for one shape in particular.** If your `index.malloy` is an aggregator, all `import`s
  > and no `export { … }`, it exports nothing, so the package now lists one model with no sources
  > and looks empty. Either add `export { … }` naming the sources you want published, or take the
  > `"explores": []` route above.

- **Within a file: `export { … }`.** Once the package has a surface, the discovery accessors list only
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
and every compiled source stays directly queryable. With no curated surface at all, both modes are
equivalent (everything queryable).

```json
{ "name": "sales", "explores": ["index.malloy"], "queryableSources": "all" }
```

For gradual migration, use `explores` with `queryableSources: "all"` to curate listings while keeping
every source queryable by name; switch to `"declared"` when ready to enforce the boundary.

> **Declaring `explores` is what turns the boundary on, and an `index.malloy` alone never does.**
> A surface that came from the convention curates listings only: every source stays queryable by
> name whatever `queryableSources` says. This is deliberate. The boundary denies with a `404` that
> cannot be told apart from "does not exist", so enforcing it because a file with a particular name
> appeared would revoke query access on an existing package whose author changed no configuration,
> and give them no error they could act on. Adding an `index.malloy` is therefore always safe: it
> can hide a source from listings, never make one unreachable. When you do want the boundary, write
> the surface out as `explores` and it applies as described above.

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
