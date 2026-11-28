<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# The `#(materialize)` annotation

```malloy
#(materialize) name="my_dataset.my_table"
source: my_rollup is some_source -> { group_by: ...; aggregate: ... }
```

## What can carry it

**Any source.** `#(materialize)` stores the source as written - its rows with its `where:` applied, every dimension it declares or inherits (private and internal ones included) - so a table with an `extend`, a query source and a `conn.sql("...")` all build, and no `##!` flag is needed in the file. What is not stored is what depends on the caller: a dimension that reads a given, the current time or the query's time zone is computed at read from the stored columns it reads. A dimension that reads a given or the current time and is declared on a base refuses the source (`materialize-not-built`, saying to declare it on the materialized source itself), and it answers live.

**A source that inherits `#@ persist` is refused** (`inherited_persist`): it already is its base's table. Move the base to `#(materialize)` first. A source carrying both `#(materialize)` and its own `#@ persist` fails the load (`materialize-invalid`).

**A view or a named query persists its result with `#@ persist`**, not `#(materialize)`, with `PUBLISHER_MATERIALIZE` on. `#@ persist` on a `view:` stores the view's result; it answers a call of the view on the source that annotates it (`run: flights -> by_carrier`, and later stages over it), not a refinement or a nest of it. `#@ persist` on a top-level `query:` builds its result (as `<query>__query`), and the table answers `run: q`, a `queryName` call, a report naming `q`, and `run: q -> { … }`; a refinement (`q + { … }`) and any use of `q` inside another definition are answered live. A query that reaches a row-level gate (its source's own, an inherited one, or a joined source's), or reads a given, is not built; persist a view on a gated source instead. A view or query whose result nests is not built either. Whenever one is not built, the package carries a `warn` with code `persist-view-not-built` or `persist-query-not-built` saying why. With the switch off, either is answered live, its line is still read (an invalid one fails the load), and a valid one warns. A view or query whose result reads the current time (`now`, or a relative time filter string such as `f'last 7 days'`) over an ungated source is built and served **as of its build**, and rebuilt on every run; the package carries a `persist-reads-current-time` warning saying so.

**A mistake on the line fails the load.** A `#(materialize)` line that does not parse (`materialize-unparseable`), a key outside the closed set (`materialize-unknown-key`), a retired key (`partition=`; `materialize-retired-key`), and a value that is wrong as written (an empty or non-text `storage=`, an `@env.` reference, an `inline=` that is not a list of joins of the source, an unsafe or reserved `name=`, an invalid incremental declaration; `materialize-invalid`) each fail the publish and the load, on every path: publish, reload, restart, watch mode. A misspelled route (`#(materialise)`) fails the same way, code `materialization-near-miss`, naming the spelling it meant. The error has one line per mistake, each ending with its code in brackets, and shows in the publish response and `/status` `loadErrors`; a failed reload keeps serving the previous model (`stale: true`). Fix what it names and reload. A _valid_ annotation Publisher cannot build (an unconfigured destination, an inherited given, a dialect that cannot refresh incrementally, a BigQuery database table with no dataset) is different: that declaration alone is refused with a `warn` under its mechanism's not-built code (`materialize-not-built`, `rollup-not-built`, `persist-view-not-built`, `persist-query-not-built`), answered live, and the package loads. The build plan's `refusedSources` holds one record for it, with the reason.

## The name

- **Optional.** Without `name=`, Publisher's own builds give the table a readable name, `<package>__<file>__<source>__p<8 hex>` (`ecommerce__orders__orders__p3f9c2a1b`), the hex a hash of the environment, package, model and source, so packages sharing a schema never collide. On BigQuery a database table needs a dataset, so give it a dataset-qualified `name=` there (`needs_qualified_name` otherwise).
- **Quote it.** `name="my_table"`, or a path `name="dataset.table"` / `name="project.dataset.table"`.
- **Plain identifier segments only**: dot-separated segments of letters, digits, underscores and hyphens, because the name is inlined into DDL. Publisher quotes each segment for the target warehouse, so case is preserved (`name="OrdersDaily"`) and reserved words work (`name="order"`); never add quotes inside the value (`name="\"Quoted Tbl\""` fails the load, `materialize-invalid`).
- **How `name=` is read depends on `scope`.** An unversioned or `scope: "package"` package writes the table at exactly that name, checked and marked as this package's; under `scope: "version"` it is the start of the table's name, lowercased, and the version and a hash follow it (`summary_t__v1_2_0__p<8 hex>`). No view is made at `name=`.
- **One name per table.** Two sources that resolve to the same `name=` in the same destination fail the publish and the load (`materialization-table-collision`). A deployment that opts out with `PERSIST_COLLISION_ENFORCE=false` loads the package instead and refuses both alone (`materialize-not-built`). A `name=` another package holds refuses that source's build (`table-owned-elsewhere` in the run's failures).
- **The target's container must exist.** `name="analytics.daily"` puts the table in `analytics`, which the connection must be able to write and which Publisher does not create. A `storage=` destination takes `name="daily"` (built in the destination's schema: its `ducklakeConnection.destinationSchema`, else `main`) or `name="analytics.daily"`; that schema must already exist in the destination.

## Identity and reuse

A materialized source's identity is its **`sourceEntityId`**: a content address of its connection and its build. Not the name, not the file, not the package version. So:

- **A rebuild with unchanged logic reuses the existing table** (skip-if-unchanged); changing the source builds fresh under a new address. `--force-refresh` and every scheduler fire defeat the skip; an incremental source is exempt from it and advances by its boundary instead (`reference/refresh.md`).
- **A republish by itself builds nothing on a standalone server**; reuse is a property of the next *run*.

## Readers of a materialized source

- **An `extend` that declares no `#(materialize)` reads the stored table** through the served copy: its own `extend` block is your text over the table, and is not a build target of its own. An extension that declares its own `#(materialize)` gets its own table, built over its parent's (`reference/chaining.md`).
- **A query, a view, a join or a source in a file that imports it** reads the table too, with the materialized source's own `where:` re-applied per caller.
- **`#@ -persist`** on an extension opts it out: it recomputes from raw on every query and never reads the table. Reach for it when a reader must not see stale rows, and remember it forgoes exactly the work materialization was there to save. It is Malloy core's own opt-out, and it is not deprecated. Declare it in a model file: written in the query text itself it is not opted out.
- Which queries over a reader route to the table is decided per query - a join to a source not stored with it or a nested column serves live for the queries that touch it (`reference/storage.md`).

## Every key

| On the tag | What it does | Where it is explained |
|---|---|---|
| `name="..."` | the target table (optional) | above |
| `storage="<destination>"` | build into a storage destination and serve from it | `reference/storage.md` |
| `refresh="incremental" watermark="col" [merge_key="a,b"] [lookback="2 days"]` | refresh by bounded delta instead of a full rebuild; `lookback=` re-reads late rows (needs `merge_key=` and a date or timestamp watermark) | `reference/refresh.md` |
| `freshness.window="24h" freshness.fallback="live"` | per-source freshness objective | `reference/refresh.md` |
| `queryMetadata.<key>="v"` | per-source cost-attribution properties | `reference/policy.md` |
| `inline=["j"]` | store a join_one as `j__<field>` columns, or a join_many as a pair table | the main skill |
| `#@ -persist` (on a reader) | opt a reader out of the table | above |
| `#(rollup) grain=["a", "b"]` (on a measure) | store a derived rollup | `reference/storage.md` |

`storage=` and `name=` may be written beside the route's key or inside it: `#(materialize) storage="lake"` and `#(materialize) { storage="lake" }` place the table the same way, on every route (`#(materialize)`, `#(rollup)`, a persisted view or query). When both are written, the one inside the key wins, even when it names nothing. A `storage=` that is empty or only whitespace, or that is a list or a block rather than text, fails the publish and the load (the route's own `*-invalid` code): name the destination, or drop the key to build in the source's own warehouse.

A rollup takes `name=` too, as a prefix: `name="analytics.orders_rollup"` creates `analytics.orders_rollup__p<hash>`, the container placing it and the leaf starting its name, as `name=` does under `scope: "version"`; the hash, and so the identity, stays Publisher's. Without `name=` the table takes the readable name `<pkg>__<file>__<base>_by_<grain>__p<hash>`, in its base's database `name=` container if it has one. The leaf is lowercased in the name; one the name would otherwise rewrite (a space, punctuation, a leading digit, `__`), an empty one, or one ending in `__p<8 hex>`, fails the load (`rollup-invalid`). Changing the leaf renames the table (rebuilt once; the old one is dropped).

Scope and schedule are declared once for the package, never per source (`reference/policy.md`).

**Legacy.** v1 wrote `#@ persist` on a source. It still loads, but new work uses `#(materialize)`; see the main skill's root-first migration note.
