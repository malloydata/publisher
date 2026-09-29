---
name: malloy-source-unreachable
description: Work out why a source is missing from get_context, or why a query against it was refused with a 404 or a 403, and what to do next. Use when a source you expect is absent from discovery, when execute_query answers "No queryable source", when a query that used to work stops working, or when deciding whether a package's curation is hiding something you need.
---
<!-- Copyright (c) Credible Data Inc. SPDX-License-Identifier: MIT -->

# A source you cannot reach

> **Tool names** are written bare here - `get_context`, `execute_query`, `get_status`, `compile_model`. The exact prefixed name depends on the host surface; match each against the tools you actually have.

Most packages publish a curated surface, so a source being absent or refused is usually the package working as its author intended, not a fault. Read the refusal before working around it, because the three things that can refuse you need three different responses and they are told apart by the status code.

## The three answers

| what you got | what it means | what to do |
| --- | --- | --- |
| **404** that says "not on this package's published surface" | the name is real and the package does not publish it. It is curation, not a typo | follow the fix the message names (usually: name it in the `export { ... }` of `index.malloy` and address the query there), or query what IS published |
| **404**, plain "No queryable source", "No queryable model" or "Query target is not queryable" | the name does not exist, or it is hidden in a model that carries an `#(authorize)` or `#(access_filter)` gate. In a gated model the two are deliberately indistinguishable, so a refusal cannot be used to probe for hidden names | check the name against `get_context`, then query something on the surface or ask the package's author to publish it |
| **403**, "Access denied" | the source exists and is on the surface, but an `#(authorize)` gate did not admit you | supply the givens the gate reads, or accept that this caller may not read it |
| **200** with zero rows | you were admitted, and an `#(access_filter)` narrowed the rows to none of them | this is a real answer. Report it as "no matching rows", never as an error |

A 403 names a source, so it tells you the source exists. A plain 404 tells you nothing at all. That asymmetry is deliberate: a source that is both hidden AND gated answers a plain **404**, so you can never use a 403, or the wording of a 404, to discover that a hidden gated name is real.

## Addressing the surface, which is the most common 404

A query names a model file. When a package curates its surface, only files on that surface are valid entry points, **even for a source that file declares itself**:

```
POST .../packages/sales/models/index.malloy/query    {"sourceName": "orders"}   -> 200
POST .../packages/sales/models/orders.malloy/query   {"sourceName": "orders"}   -> 404
```

Same source, same package. `orders.malloy` is off the surface, so it is not an entry point.

**Use the `model_path` that `get_context` gave you, verbatim.** Its `source_info.resource_id` holds `environment`, `package`, `model_path` and `source`, and those are exactly `execute_query`'s `environmentName`, `packageName`, `modelPath` and `sourceName`. On a curated package the `model_path` is the surface file, not the file that declares the source. Substituting the declaring file because it looks more correct is how this 404 happens.

## Other places a hidden source answers 404

The query route is not the only one held to the surface. On a curated package, a source or file off it also shows up as a 404 here:

- **Reading a model.** `GET .../models/{path}` answers 404 for a file off the surface, with the same words as the query route. The file still exists and still compiles; read `index.malloy` instead. A model that is on the surface lists only the names it publishes, not everything it imports.
- **A dashboard tile.** A tile, a single-query dashboard's query, or a filter `suggest` over a hidden source answers 404. The dashboard itself is still listed. The package load warns once per tile, for example `Tile orders_staging -> by_flag on dashboard overview reads orders_staging, which index.malloy doesn't export, so it won't load. Fix: add orders_staging to the export { ... } in index.malloy.` It is in the `warnings` on the package's own response, `GET .../packages/{pkg}`.
- **A notebook cell.** A cell over a hidden source answers 404, even when the notebook imports its file. A source an earlier cell derives from a published one still works.

The fix is the same in each case: add the source to the `export { ... }` in `index.malloy`, or use what is already published.

## When the source is not in `get_context` at all

Work down this list. The first three are far more common than the last.

1. **It is off the surface.** The package publishes a curated set and this source is not in it. Nothing you can do from the query side; it is a package edit.
2. **The package failed to load, or is serving a stale model.** Call `get_status`. A package that never loaded is absent from listings entirely, which reads exactly like "does not exist". A package whose last reload failed to compile is listed and answering, from the model it compiled *before* that save, and carries `stale: true`. Neither is visible from a listing alone.
3. **You are talking to a different server than you think.** Call `list_packages` and check the environment and package names are the ones you expect. A stale `.mcp.json` outlives the server that wrote it, and another Publisher may hold that port.
4. **Your search phrasing missed it.** Retry with a bare target (no `search_text`) to enumerate rather than rank. If it appears there, the source exists and was a ranking miss.

## What curation does not do

- **It does not hide fields.** A published source may `join` an unpublished one, and a query grouping by a joined field returns that field's values normally. Hiding a source removes it as a landing point; it does not redact columns a published source pulls in.
- **It does not gate `/compile`.** `compile_model` is exempt, because compile is the authoring loop. A hidden source can still be compile-checked, and that is intended. The exception is a hidden source that is also gated, which answers 404 at compile too.
- **It is not access control.** Curation answers "what is queryable by name", not "who may query it". Identity is `#(authorize)` and `#(access_filter)`. A source is not protected by being hidden.

## Reaching an unpublished source from your own model

If you are writing Malloy rather than just querying, an unpublished source is still usable, with one condition that trips people.

`export { ... }` decides what an **importing file** can see, which is Malloy's rule and not Publisher's:

- a file that declares **no** `export` hands an importer everything it declares, so import the file that declares the source and join it normally;
- a file that **does** declare one hands over exactly that list. Importing a file whose `export` omits the source and then naming it fails to compile with `Reference to undefined object`.

So an unpublished source is reachable through a file that exports it, or that exports nothing, and not through a file whose `export` leaves it out. Re-export chains are fine at any depth: a source re-exported through several files stays queryable through the surface, because admission follows the declaration rather than the path taken to it.

## Before you report a source as missing

Confirm all three, in this order. Each one has been mistaken for "the data is not there".

1. `get_status` shows the package loaded and not `stale`.
2. `list_packages` names the environment and package you meant.
3. An enumerating `get_context` call (a target with no `search_text`) does not list it.

Only then is it genuinely not published, and the answer to the user is that the package does not expose it, not that the data does not exist.
