<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# The `#@ persist` annotation

```malloy
#@ persist name="my_dataset.my_table"
source: my_rollup is some_source -> { group_by: ...; aggregate: ... }
```

## What can carry it

**Only `query_source` and `sql_select` sources are persistable** - a source whose definition has a `-> { ... }` pipeline or a `conn.sql("...")`, including one refined by a trailing `extend { ... }`. What is **not** persistable is a *plain* `extend` over a bare `conn.table(...)` (a filtered pass-through). A `#@ persist` on such a source (or on a table base with only added fields, such as `duckdb.table('x') extend { measure: … }`, or on an unbound parameterized source) is **not ignored**: the publish and the load fail, naming the source (code `persist-not-materializable`). Persist a query source instead (`source: x is raw -> { where: ...; select: * }`), or remove the annotation.

**A named `query:` persists its result only with `PUBLISHER_MATERIALIZE` on.** Then `#@ persist` on a top-level `query:` builds its result (as `<query>__query`), and the table answers `run: q`, a `queryName` call, a report naming `q`, and `run: q -> { … }`; a refinement (`q + { … }`) and any use of `q` inside another definition are answered live. A query that reaches a row-level gate (its source's own, an inherited one, or a joined source's), or reads a given or `now`, is not built; persist a view on a gated source instead. Whenever it is not built, the package carries a `warn` with code `persist-query-not-built` saying why, and the workaround that works everywhere: persist a source over it (`#@ persist` on `source: q_result is q`) and query that source.

**A parameterized source must have its argument bound.** `#@ persist` on `high_value(min_amount is 500) -> { ... }` builds one fixed relation; on the free template `high_value -> { ... }` it is refused (`free_parameter`), since there is no single relation to store. A `given` the persisted *query* reads is refused the same way (`reads_given`); a given in the source's own `extend` block is applied per caller at read. `reference/storage.md` says where a given may sit. A persisted query that reads `now` (in its stages, a view it runs, the `where:`, a field or a join of the source it reads, `select: *` included, or the query of a query-shaped source it is built from or joins) is refused too (`current_time`, a `persist-not-built` warning, answered live), since its table would hold the rows as of the build; `now` in the source's own `extend` block is applied at every read and builds.

**A mistake on the line fails the load.** A `#@` line that does not parse (`persist-unparseable`), a key Publisher does not read (`merge_keys=` for `merge_key=`, `persist-unknown-key`), a retired key (`partition=`, `sharing=`, `schedule=`, `realization=`; `persist-retired-key`, and on `#(materialize)` `materialize-retired-key`), and a value that is wrong as written (an empty or non-text `storage=`, an `@env.` reference; `persist-invalid`) each fail the publish and the load, on every path: publish, reload, restart, watch mode. A `#@` line Malloy never routes to persistence (`#@persist` with no space, `# persist`, `#(persist)`, or a misspelled directive such as `#@ persits`) fails the same way, code `materialization-near-miss`, naming the spelling it meant. The error has one line per mistake, each ending with its code in brackets, and shows in the publish response and `/status` `loadErrors`; a failed reload keeps serving the previous model (`stale: true`). Fix what it names and reload. A _valid_ annotation Publisher cannot build (an unconfigured destination, a gate, a dialect that cannot refresh incrementally) is different: that declaration alone is refused with a `warn` under its mechanism's not-built code (`persist-not-built`, formerly `persist-source-refused`; `materialize-not-built`; `persist-view-not-built`; `persist-query-not-built`; `rollup-not-built`), answered live, and the package loads. The build plan's `refusedSources` holds one record for it, with the reason.

## The name

- **Quote it, once.** `name="my_table"`, or a path `name="dataset.table"` / `name="project.dataset.table"`. A **bare** `name=my_table` fails the package **load** - so every publish and every restart - with `persist annotation name must be quoted`; it never silently no-ops.
- **Plain identifier segments only**: dot-separated segments of letters, digits, underscores and hyphens. A quoted name holding a space, quote, semicolon or backtick is rejected with its own message (`persist annotation name must be a plain identifier path`), because the name is inlined into DDL. Publisher quotes each segment for the target warehouse, so case is preserved (`name="OrdersDaily"`) and reserved words work (`name="order"`); never add quotes inside the value.
- **Never quote inside the quotes.** `name="\"Quoted Tbl\""` fails the load (`persist-invalid`): a name must be a plain identifier path (letters, digits, `_`, `-`, dot-separated), and Publisher quotes each segment for the warehouse itself.
- **One name per table.** Two distinct persist sources that resolve to the same `name=` in the same destination clobber each other (the second build overwrites the first, and dropping one drops the other's). The publish and the load fail (`materialization-table-collision`). A deployment that opts out with `PERSIST_COLLISION_ENFORCE=false` loads the package instead, refuses both sources alone (`persist-not-built`), and answers them live rather than from the shared table. A shared *body* is fine (it shares one table by content address); a shared name over different bodies is not.
- **The target's container must exist.** `name="analytics.daily"` puts the table in `analytics`, which the connection must be able to write and which Publisher does not create. The source can *read* anything the connection can read.
- **The name is not the identity.** In a standalone Publisher `name=` is the physical table, rebuilt in place; a hosted deployment may assign the physical name itself. A `storage=` destination takes `name="daily"` (built in the destination's schema: its `ducklakeConnection.destinationSchema`, else `main`) or `name="analytics.daily"`; a second dot fails the build with a DuckDB parser error. That schema must already exist in the destination, since the build does not create it. A hosting platform may refuse a schema there.

## Identity and reuse

A persist source's identity is its **`sourceEntityId`**: a content address of its connection and its canonical SQL. Not the name, not the file, not the package version. So:

- **A rebuild with unchanged persist logic reuses the existing table** (skip-if-unchanged); changing the SQL builds fresh under a new address. `--force-refresh` and every scheduler fire defeat the skip; an incremental source is exempt from it and advances by its boundary instead (`reference/refresh.md`).
- **Two sources whose bodies compile to the same SQL share one table** when they name the same one (the same `name=`, or none), and share one freshness policy and one incremental boundary with it. Different `name=`s are different tables, each built under its own name.
- **A republish by itself builds nothing on a standalone server**; reuse is a property of the next *run*.

## Readers of a persisted source

- **An `extend` of a persisted source reads the stored table** and is not a build target of its own: it inherits the parent's tag and the same address, so the two names are one table. To make a distinct non-persisted intermediate, pipe first: `base -> { select: * } extend { ... }`.
- **A query over a persisted source** (`orders is _orders_fact -> { select: * }`, the private-fact / public-wrapper idiom) reads the table too, with the fact's `extend`-block filters applied per caller, and is not a build target.
- **`#@ -persist`** on an extension opts it out: it recomputes from raw on every query and never reads the table. Reach for it when a reader must not see stale rows, and remember it forgoes exactly the work persistence was there to save. It is Malloy core's own opt-out, not a Publisher extension, and it stays supported: it is not deprecated, and the in-development `#(materialize)` path honours it the same way.
- Which queries over a reader route to the table is decided per query - a join to a non-persisted source or a nested column serves live for the queries that touch it (`reference/storage.md`).

## Every option

| On the tag | What it does | Where it is explained |
|---|---|---|
| `name="..."` | the target table (required) | above |
| `storage=<destination>` | build into a storage destination and serve from it | `reference/storage.md` |
| `refresh="incremental" watermark="col" [merge_key="a,b"] [lookback="2 days"]` | refresh by bounded delta instead of a full rebuild; `lookback=` re-reads late rows (needs `merge_key=` and a date or timestamp watermark) | `reference/refresh.md` |
| `freshness.window="24h" freshness.fallback="live"` | per-source freshness objective (hosted deployments) | `reference/refresh.md` |
| `#@ queryMetadata.<key>="v"` (sibling line) | per-source cost-attribution properties | `reference/policy.md` |
| `#@ -persist` (on a reader) | opt a reader out of the table | above |
| `#@ preaggregate grain="..."` (on a measure) | store a derived rollup | `reference/storage.md` |

`storage=` and `name=` may be written beside the route's key or inside it: `#@ persist storage="lake"` and `#@ persist { storage="lake" }` place the table the same way, on every route (`#@ persist`, a persisted view, `#@ preaggregate`, `#(materialize)`, `#(rollup)`). When both are written, the one inside the key wins, even when it names nothing. A `storage=` that is empty or only whitespace, or that is a list or a block rather than text, fails the publish and the load (`persist-invalid`, or the route's own `*-invalid` code): name the destination, or drop the key to build in the source's own warehouse.

On a server with `PUBLISHER_MATERIALIZE` on, the v2 spelling `#(materialize)` takes the same keys, plus `inline=[...]`, as a closed set (an unknown key fails the publish and the load), and `#(rollup) grain=["a", "b"]` replaces `#@ preaggregate`.

A rollup (`#(rollup)` or `#@ preaggregate`) takes `name=` too, as a prefix: `name="analytics.orders_rollup"` creates `analytics.orders_rollup__p<hash>`, the container placing it and the leaf starting its name, as `name=` does under `scope: "version"`; the hash, and so the identity, stays Publisher's. Without `name=` the table takes the readable name `<pkg>__<file>__<base>_by_<grain>__p<hash>`, in its base's database `name=` container if it has one (`#@ persist` or `#(materialize)`). The leaf is lowercased in the name; one the name would otherwise rewrite (a space, punctuation, a leading digit, `__`), an empty one, or one ending in `__p<8 hex>`, fails the load (`rollup-invalid`). Changing the leaf renames the table (rebuilt once; the old one is dropped). `namespace="analytics"` is the deprecated spelling of the container alone, with a `deprecated-rollup-namespace` warning. The main skill's "v1 or v2" section says when to write which.

An unrecognized key on the tag fails the publish and the load (`persist-unknown-key`). The retired per-source `sharing=` and `schedule=` fail them too (`persist-retired-key`): scope and schedule are declared once for the package (`reference/policy.md`). So does `realization=`, even `"COPY"`: every table is built by CREATE TABLE AS SELECT.
