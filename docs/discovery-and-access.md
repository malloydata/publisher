<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Discovery surface & query boundary

> What this is: how a package controls **which** models and sources are visible and queryable. This
> is a different axis from [givens](givens.md)-based access control: it shapes the _surface_ (what
> exists and what is a valid query target) regardless of who is asking. To gate **who** may query a
> source by caller identity, see [authorize.md](authorize.md); to scope **which rows** they see, see
> [row-level-access.md](row-level-access.md).

**A package's published surface is its `index.malloy`.** Put a file with that name at the package
root, `import` your models, and `export { … }` the sources you publish. What it exports is what
Publisher lists **and** what callers may query. Nothing goes in `publisher.json`.

```
sales/
  publisher.json     { "name": "sales" }
  index.malloy       import "orders.malloy"
                     export { orders }
  orders.malloy      declares `orders` and `orders_staging`
```

`orders_staging` is now a building block: it still compiles, and other models can import, join and
extend it by importing `orders.malloy`, but it is not listed and a direct query against it is
refused. A source reached through a join is read as normal — hiding a source does not hide the
fields a published source joins in. A package with no
`index.malloy` and no `explores` publishes everything, which is the behavior every package had
before this convention existed.

## The two granularities

- **File level.** Only the surface's files are returned by `listModels()`. Every other `.malloy`
  file still compiles for import and join resolution, but is hidden. `GET .../models/{path}` for a
  hidden file answers 404, with the same message a query to it gets. For a file it does return,
  the response lists only the names that file publishes, and `sourceText` is left out when the file
  text names a source it does not publish. A join to a hidden source keeps only its name and the
  names and types of its fields, which is what querying through it needs; the hidden source's table,
  SQL and connection are left out.

  Notebooks and dashboards (`dashboards/*.malloy` files with an `# artifact` tag) are always
  listed, whatever the surface. To hide a dashboard, remove its tag. Any other file under
  `dashboards/` is neither listed nor queryable, even when `explores` names it.

  Listed is not a way around the surface. A notebook cell may read only sources on the surface. A
  cell over a hidden source answers 404, and 404 rather than 403 even when the source is also
  gated. A source an earlier cell declares on top of a published one
  (`source: mine is customers extend { … }`) still works. A cell's own source over a raw table
  (`duckdb.table(…)` or `.sql(…)`) has no published source under it, so on a curated package it is
  refused. The notebook GET and each cell's response show only the sources the notebook may read.
  A query a caller sends to the notebook's path is held to the surface the same way.

  Dashboards follow the same rule. A tile, a dashboard's single query, and a filter `suggest` may
  read only sources on the surface, and a tile over a hidden source answers 404. The package load
  warns about each one; see [dashboards.md](dashboards.md#what-publisher-checks-at-load).

- **Within a file — `export { … }`.** The discovery accessors list only the model's re-export
  closure (`modelDef.exports`), matching what Malloy's `modelInfo`/`sourceInfos` expose. A model
  with **no** `export { … }` exports all of its locally-declared top-level sources; declaring
  `export { customers }` lists only `customers` and keeps imported and internal helpers out.

  `export { … }` also decides what an **importing file** can see, which is Malloy's rule rather than
  Publisher's, and it is the one that surprises people. A file that declares no `export` hands an
  importer everything it declares, so a hidden source stays importable and joinable from the file
  that declares it. A file that _does_ declare one hands over exactly that list: importing a model
  whose `export` omits a source and then referencing it fails to compile with
  `Reference to undefined object`. So "hidden, not out of reach" means reachable **through a file
  that exports it, or that exports nothing** — not through any file that happens to mention it.

The two compose: the surface decides which files are listed, and `export { … }` decides which
sources within a listed file are shown.

## The three answers

Three separate mechanisms decide whether a caller gets rows, and they answer differently on
purpose. Curation hides; `#(authorize)` denies; hiding is not denying.

| the caller's situation            | the mechanism                                              | the answer                                                                |
| --------------------------------- | ---------------------------------------------------------- | ------------------------------------------------------------------------- |
| the source is off the surface     | not in any listed file's `export { … }` closure            | **404**, saying why, unless a gate is in play (below)                     |
| the source is locked to them      | [`#(authorize)`](authorize.md) their givens do not satisfy | **403**                                                                   |
| the source is row-scoped for them | [`#(access_filter)`](row-level-access.md)                  | **200**, with their rows — zero of them if it admits none                 |

The 404 is deliberate: a 403 would confirm that a hidden name exists, which is how a curated
package becomes an enumeration oracle. Because curation is not an identity gate, it is also not the
tool for protecting contents — see the caveats below.

What the 404 says depends on whether a gate is in play. When the model that refused the query
carries no `#(authorize)` and no `#(access_filter)` anywhere, the 404 says the target is off the surface, names the surface
file, and gives the fix. A modeler who saves a new file and queries it would otherwise read a typo,
and there is nothing to hide: `/compile` already answers a hidden file differently from a missing
one. When the model carries a gate, every refusal is the plain `No queryable model "…"`,
`No queryable source "…"` or `Query target is not queryable.`, worded exactly as for a name that
does not exist, so a hidden gated name cannot be told from a missing one. A name that does not exist
always gets the plain form.

## Curating, and what curation is not

`export { … }` is a discovery filter and a query boundary over _what exists_; `#(authorize)` is the
gate over _who is asking_. Those gates are enforced against the complete source set and are never
weakened by curation: a hidden source keeps its gate.

The boundary applies to the **query** surface (`getQueryResults` and the MCP query tool). It does
**not** gate `/compile` (or `compile_model`): compile is the authoring loop, so a curated package
stays authorable. The consequence is that `/compile` can reveal a hidden source's schema, and with
`includeSql` its SQL. That is by design. Use `#(authorize)` when the contents themselves must be
protected rather than merely removed from discovery: a lock is truth-evaluated on `/compile`, so a
refused caller gets a 403 and no SQL. `#(access_filter)` is not,
because it decides rows and `/compile` returns none. A source that is both hidden and locked still
answers `/compile` with the boundary's generic 404, so the exemption cannot be used to enumerate
gated names.

The boundary checks the source a query **runs**. A query over an exported source that joins a
hidden source still runs, and returns the joined fields.

Materialization ignores the surface. A build compiles every model, so a hidden `#@ persist`
intermediate is built, and an exported source that reads it reads the built table. See
[materialization.md](materialization.md).

## Runnable example

[`examples/governed-analytics`](../examples/governed-analytics) publishes two sources from
[`index.malloy`](../examples/governed-analytics/index.malloy) and declares no manifest keys at all:

```malloy
##! experimental.givens

import "orders.malloy"
import "secured.malloy"

export { sales, orders_secured }
```

`orders_base` lives in [`internal.malloy`](../examples/governed-analytics/internal.malloy) and is
exported by nothing, so both public sources still extend it while a direct query is refused:

```bash
API=http://localhost:4000/api/v0/environments/examples/packages/governed-analytics/models
curl -s -X POST $API/internal.malloy/query -H 'content-type: application/json' \
  -d '{"query":"run: orders_base -> { aggregate: c is count() }"}'   # → 404, "not on this package's published surface"
```

## The older form: `explores` and `queryableSources`

Both keys are deprecated and both still work. Each use that has a replacement gets a load-time
warning in the package's `warnings` naming the edit that replaces it. `queryableSources: "all"` has
none, so it loads with no warning: it is the one way to hide a source from listings while it stays
queryable by name.

- **`explores`** — an optional `string[]` of `.malloy` file paths, relative to the package root,
  naming the surface. The surface is then what those files export. An explicit `explores` always
  wins over `index.malloy`. Its warnings:
  - a non-empty list: `"explores" in publisher.json is deprecated.`, then either `Fix: import …
    into index.malloy, export the sources you publish from it, then delete "explores".` (with the
    package's own file names) or, when the list names only `index.malloy`, `index.malloy already
    publishes the same thing. Fix: delete "explores".` Dashboard entries need no replacement, since
    every dashboard is served.
  - a list that leaves out an existing `index.malloy` gets a second warning saying `index.malloy`
    is ignored.
  - `"explores": []` beside an `index.malloy` stops that file from limiting what the package
    publishes. The fix: to publish everything, rename `index.malloy`, point any import of it at the new name, then delete `explores`.
  - `"explores": []` with no `index.malloy` does nothing. The fix: delete it.

- **`queryableSources`** — `"declared"` (the default) or `"all"`. `"declared"` makes queryable ==
  discoverable, which is what the sections above describe, so writing it does nothing and the
  warning says to delete it. Admission is by _declaration_, not by name: a request clears only when
  the model it names resolves the name to the very source a surface file exported, so a same-named
  source in a hidden file is not admitted by the coincidence.

  `"all"` makes the surface decide listings only: every compiled source stays queryable by name,
  and model files off the surface are returned by path. It works beside `index.malloy`. Use it to
  hide an `#(authorize)`-gated source from listings while authorized callers can still query it by
  name. `index.malloy` has no way to say this on its own.

Only a root file named exactly `index.malloy` counts. A root file that differs only in case, such
as `Index.malloy`, gets a warning that it is ignored.

## Validation

Validation is asymmetric by design: **publishing** a package with an `explores` entry that doesn't
resolve to a real model is rejected with a `400`, while at **startup/reload** the package still
serves but hides the unresolved entry (it never falls back to listing everything) and surfaces the
reason in the package's `exploresWarnings` field. A surface derived from `index.malloy` always
resolves, so it never appears there.

`exploresWarnings` is about entries that name nothing. The conditions below ride the package's
general `warnings` field instead, because they are about a surface that resolves and still leaves
the package answering differently than its author expects:

- **The surface disappeared.** A reload that leaves a package with no surface where it had one --
  a deleted or renamed `index.malloy` -- warns once, naming what was published and how to restore
  it. Without it the change is invisible: an uncurated package looks exactly like one that was
  never curated. An explicit `"explores": []` is not this case; it gets its own deprecation
  warning instead.
- **A malformed `explores`** is not a warning at all: it fails the package load (`424`), like an
  invalid `scope`, with `Invalid "explores" in publisher.json: it must be a list of file names, but
  is […]. The package was not loaded. Fix: delete "explores" and add an index.malloy.` Ignoring it
  would resolve to no surface and publish every source the key was meant to withhold, and keeping
  the entries that parse would serve a surface the author did not write.
- **The whole surface failed to compile.** A surface that does not compile exports nothing, so the
  boundary refuses every model in the package — including the ones that compiled — with the 404 a
  hidden model gets (plain where the model is gated, explained where it is not). The warning names the broken files and how many working models they
  took down. Narrow by design: a compile error at first load fails the package (it appears in
  `loadErrors`), and a failed reload keeps the last good model serving and is reported with
  `stale: true`, so the surface empties only on the materialization and manifest rebind paths. It
  stays fail-closed on purpose: falling back to listing everything would expose exactly what the
  author curated away.
