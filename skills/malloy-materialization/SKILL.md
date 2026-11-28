---
name: malloy-materialization
description: Add, confirm and debug materializations in a Malloy package - `#(materialize)` an expensive source so queries read a pre-built table, in the source's own warehouse or in a storage destination, and `#(rollup)` a measure to a coarse grain. Read this whenever the user wants to materialize a source, speed up a slow source, roll up a measure, build one stored source over another, persist a view or named query, tune what to materialize, or asks why a materialized source isn't building or isn't being read.
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Materialization

Materialize an expensive source once so queries read a **pre-built table** instead of recomputing it every time. You tag a source `#(materialize)`, a materialization run builds it into a physical table, and queries against it are answered from that table through the model's **served copy**: your own files, with the source re-rooted on its table under the same name.

> **The #1 gotcha, up front:** if a materialized source isn't serving, it is almost always one of four things - no build ever ran (a standalone Publisher does not build on publish - see **Building and refreshing**), a build ran and failed or lost that one source (a run can finish ready with `failures`; read them), the source was refused at plan (a `materialize-not-built` warning says why), or the served copy cannot answer from the table (a `served-copy-not-served` warning). Jump to **Debugging a source that serves live**.

This file is the core. The details live beside it, one topic per file, and each is a prompt of its own:

- `reference/annotation.md` - the `#(materialize)` tag: the name rules, identity and reuse, readers of a materialized source, persisted views and named queries, and every key with where it is explained.
- `reference/policy.md` - the `publisher.json` block: `scope`, `queryMetadata`, and where the rules are enforced.
- `reference/storage.md` - building into a **storage destination** (`storage=`), where a given may sit, which sources that tier refuses, how a stored table is served, and rollups.
- `reference/chaining.md` - one stored source built **over another stored source**: what makes it build over the parent's table, why one that cannot is refused rather than recomputed from the warehouse, and how to read what happened.
- `reference/refresh.md` - keeping a table current: `freshness`, `schedule`, and `refresh="incremental"`.
- `reference/access-filter.md` - materializing a `#(access_filter)`-gated source, and the bound on a stale access decision.
- `reference/tuning.md` - reading the materialization history to decide what to materialize, stop materializing, or reschedule (recommendations only).

## Which annotation to write

| To store | Write |
|---|---|
| a source, in its own warehouse | `#(materialize)`, or `#(materialize) name="schema.table"` to name and place the table |
| a source, in a storage destination | `#(materialize) storage="lake"` (needs `PERSIST_STORAGE_MODE=on` too) |
| a measure rolled up to a coarse grain | `#(rollup) grain=["a", "b"]` on the measure, one line per grain |
| a view's result | `#@ persist` on the `view:` |
| a named query's result | `#@ persist` on the top-level `query:` |
| nothing, for one reader of a materialized source | `#@ -persist` on that reader |

```malloy
#(materialize) name="east_orders" storage="lake"
source: east_orders is orders_base extend {
  where: region = 'east'
  measure: order_count is count()
}

source: sales is orders_base extend {
  #(rollup) grain=["category"]
  #(rollup) grain=["category", "region"]
  measure: revenue is amount.sum()
}
```

`#@ persist` belongs on views and named queries only; never put it on a source. `#(materialize)` and `#(rollup)` are read with the server switch `PUBLISHER_MATERIALIZE` on, which is the default. **With the switch set `false`, `#(materialize)` is ignored with no warning at all** (no build, no serve verdict, answered live), and `#(rollup)` is only an `annotation-near-miss` warning, so a v2 annotation on a server with it off looks exactly like a build that never ran. The startup log lists `materialize.enabled=false from PUBLISHER_MATERIALIZE` when someone turned it off; ask whoever runs the server if you cannot tell.

What to know about `#(materialize)`:

- **It stores the source as written** - every dimension (computed, private and internal ones too) with its `where:` applied, not a query's output - so a plain `extend` over a table is fine and needs no `-> { select: * }`, and no `##!` flag is needed in its file. Keys are closed: `name`, `storage`, `refresh`, `watermark`, `merge_key`, `lookback`, `freshness.*`, `queryMetadata.*`, `inline`. Any other key fails the publish and the load (`materialize-unknown-key`; the retired `partition=` is `materialize-retired-key`).
- **A base's measures, views and joins are carried** when the base is named by its own name, in the source's file or a file it imports whole: the served copy writes the base's `extend` block again over the table, and every file that imports the source reads the table. It cannot when the base comes in under another name or by a selective import (`import { o is orders } from ...`), or its block reads a name its own file imports (a join to an imported source): then the source is built but answered live, `served-copy-not-served` ("This source inherits fields ..."), serve verdict `live` / `served_copy`. Import the base's file whole, or build over a base with none (as above).
- **An extension that declares its own `#(materialize)` gets its own table**, built over its parent's table rather than recomputing the chain; one that declares none reads its parent's table (`reference/chaining.md`).
- **Joins are references by default.** `inline=["j"]` stores a join_one as `j__<field>` columns, or a join_many as a pair table; an incremental pair table names a watermark on each side, `watermark=["updated_at", "j.updated_at"]`.
- **A given-reading `where:` must sit on the materialized source itself.** Its own term is left out of the build and applied per caller at read; one inherited from a base refuses the source (`materialize-not-built`) and it answers live (`reference/storage.md`).
- **A source with a row-level gate of its own** (`#(authorize)`, `#(access_filter)`) is built and served past that gate on either tier, database or `storage=`: the table holds every caller's rows and the gate is re-applied on every read. A `storage=` table puts those rows, ungated, in the destination, where anyone with direct access to it can read them; say so when you materialize a gated source there. A row filter it inherits, a gate reached through a join, or a gate over a source whose build reads a given is refused on both tiers (`reference/access-filter.md`).
- **On BigQuery a database table needs a dataset**: a database `#(materialize)` with no dataset-qualified `name=`, or a rollup whose `name=` (or its base's) gives none, is refused at plan (`needs_qualified_name`) and serves live.
- **The table's name.** Without `name=`, a table takes a readable name, `<package>__<file>__<source>__p<8 hex>`. An unversioned or `scope: "package"` package writes `name=` exactly as given; under `scope: "version"` a `name=` sets the start of the table's name, schema or dataset included, and the version and a hash follow it (`analytics.summary_t__v1_2_0__p<hash>`). No view is ever made at `name=`.

**A chain migrates root-first.** Code written with v1 `#@ persist` on a source still loads and serves, but do not extend it with v2: `#(materialize)` on an extension of a `#@ persist` source (one that inherits the tag) is refused, reason `inherited_persist`, and answers live, because the extension already is its base's table. Move the base to `#(materialize)` first, then its extensions. When asked to move such code, follow `docs/materialization-migration.md` in the Publisher repository: a dry-run publish (`POST .../packages?dryRun=true`) to read the warnings and serve verdicts, a build (every migrated source rebuilds once, because its content address moves), then the path report and a served-versus-live comparison. Keep the v1 source's `name=` when you move it, so an unversioned or `scope: "package"` package's table stays at that name. The legacy `#@ preaggregate` on a measure becomes `#(rollup)`, with its comma-separated `grain="a, b"` written as a list.

## The recipe (get this right and it just works)

1. **`#(materialize)` on the source**, with values quoted:
   ```malloy
   #(materialize) name="my_dataset.my_table"
   source: my_rollup is some_source -> { group_by: ...; aggregate: ... }
   ```
   - **Any source shape builds**: a table with an `extend`, a query source, a `conn.sql("...")`.
   - **Quote the name, once.** A name quoted *inside* the quotes (`name="\"My Table\""`) fails the load, as not a plain identifier path. Publisher quotes each segment itself, so mixed case and reserved words need no extra quoting. The target's container must already exist and be writable by the connection.
   - **The name is not the identity.** A materialized source is identified by a content address of its connection and its build, so a rebuild with unchanged logic reuses the table. An `extend` that declares no `#(materialize)` of its own reads the same table and is not a second build target.
   - **A `name=` is one table on its schema, whichever package asks.** A `name=` is used as written and marked as its package's; if another package already holds it, that source's build is refused (`table-owned-elsewhere` in the run's failures) and it answers live. Give it a different `name=`. Two sources of one package on one `name=` fail the load (`materialization-table-collision`).

   The name rules, `#@ -persist`, persisted views and queries, and every key (`storage=`, `refresh=`, `freshness.*`, `queryMetadata.*`, `inline=`) are in `reference/annotation.md`.

2. **Package materialization policy in `publisher.json`** (all optional):
   ```jsonc
   {
     "name": "my-package",
     "queryMetadata": { "team": "finance" },   // root level: tags every statement a build OR a query issues
     "materialization": {
       "scope": "package",                      // default; "version" = each published version owns its own tables
       "freshness": { "window": "24h", "fallback": "live" }
     }
   }
   ```
   - **`scope`** is a package-level contract about sharing tables across versions; on a standalone server it changes nothing except gating `schedule` and how `name=` is read. Its deprecated root-level home still works, but the two homes disagreeing **fails the load**.
   - **`freshness`** bounds how old a served table may be, the same way on every mechanism (rollups included) and in both deployments: a standalone run stamps the window on the tables it builds and rebuilds one past it on its next run; a **hosted control plane** stamps its own manifest and schedules refreshes to meet it. `fail` behaves like `live` today; only `stale_ok` keeps serving a stale table.
   - **`queryMetadata`** tags every build statement and every query for the backend's cost attribution. Root level; the home inside `materialization` is deprecated. Observability only.
   - **`schedule`** is a 5-field UTC cron that **requires `scope: "version"`** and **excludes `freshness`**; it fires only on a standalone server whose scheduler is enabled.

   The coherence rules are rejected at publish and at a policy edit, warned at load, and skipped by the scheduler. `reference/policy.md` has `scope` and `queryMetadata` in full; `reference/refresh.md` has `freshness` and `schedule`.

3. **Preview, build, confirm.** A dry-run publish (`POST .../packages?dryRun=true`) shows each source's serve verdict and any warning before anything is built; then build (below) and confirm (**Confirming it worked**).

## Building and refreshing (standalone vs. hosted)

A `#(materialize)` tag declares *what* to materialize; it does not by itself build anything.

- **Standalone Publisher:** publishing or loading a package only computes its build plan - **no table is built until a materialization run executes.** Trigger one explicitly (`malloy-pub materialize --environment <env> --package <pkg> --wait`, or the materialization API), or turn on the opt-in local scheduler (off unless `PUBLISHER_LOCAL_MATERIALIZATION_SCHEDULER` is set) to fire the package's `schedule` cron. Refresh is a re-run or that cron; `freshness` is not a refresh trigger here, so a freshness-only standalone package builds once and is not auto-refreshed. Two flags to know: `--force-refresh` builds every source even when its content address is unchanged (which is what every scheduler fire does), and `--reseed` is the only thing that rebuilds an incremental source from scratch. `--wait` gives up after **120 s by default** (`--timeout <s>`) and exits non-zero while the build keeps running; `malloy-pub get materialization <id>` then shows where it got to.
- **Hosted (control-plane) deployment:** the build runs automatically on publish, best-effort - a build failure does **not** fail the publish (which is why a broken materialization can look like a silent no-op), and the control plane drives refresh to meet the `freshness` objective.

Two mechanics either way. **One build at a time per package**: a second run requested while one is active is rejected with a 409 conflict rather than queued, and the scheduler skips a fire while a run is active. **A build can be partial**: `sourceNames` on the request builds only the sources it names and leaves the others exactly as they were (live if never built), which is how to add one source to a package without rebuilding the rest.

Either way, a successful publish alone does not prove a table exists - confirm the build separately.

## Confirming it worked

A green run is not proof, and neither is a fast query (on small data a live recompute is fast too). Check what the server says it serves:

1. **The run lists your source, and did not lose it.** A run's detail (`malloy-pub get materialization <id> --environment <env> --package <pkg>`) carries `manifest.entries`, a map keyed by `sourceEntityId` whose values name each built or reused source (`sourceName`) and its physical table. A run can finish `MANIFEST_FILE_READY` having built nothing (every source carried forward unchanged) **or having lost a source**: a source that failed is absent from `entries` and present in `manifest.failures` with its error, and a source the eligibility gate refused is in `metadata.refusedSources`, while the run still reads ready. `metadata.sourcesBuilt` / `sourcesReused` / `sourcesFailed` / `sourcesRefused` are the counts. For a source built over another stored source, the entry's `upstreamReuse` says whether it read the parent's table (`reused`) or recomputed it (`recomputed`, with `upstreamRecomputeReason`) - see `reference/chaining.md`.
2. **The source's serve verdict reads `serves`.** The package's serving state, `GET /api/v0/environments/{env}/packages/{pkg}/status`, lists `serveVerdicts`. `live` with reason `served_copy` means the table is built but no query will read it; its `notServedIn` names each model and its reasons, the same as the package's `served-copy-not-served` warnings. Read `notServedIn` on a `serves` verdict too: it names a model that answers the source live while another serves it.
3. **The query names the table.** Every query result carries a path report, `path`. An answer from a `#(materialize)` table reads `servedBy.kind: "materialized"` with `via: "served_copy"`; a rollup reads `kind: "rollup"`; a persisted view `kind: "view"`; a persisted named query `kind: "query"`. A table that was tried and not used is in `considered` with its `reason`. The full result (the REST query response without `compactJson`) also carries the SQL it ran, whose `FROM` names the physical table.
4. **Compare served with live.** Declare an opted-out reader in a scratch model file (`#@ -persist source: x_live is x extend {}`, in a file, not in the query text, where it is not opted out), run the same query on both, and compare the rows; delete the file afterwards.

A query response's `servedFrom` field names the tier that answered: `database` for a table built in the source's own connection, `storage` for a `storage=` table, `live_fallback` when a `storage=` read failed and live answered, and empty when no table answered. The path report's `servedBy.tier` names the same tier.

## Debugging a source that serves live

Symptom: the source still recomputes on every query. Check, in order:

0. **Is the switch on?** With `PUBLISHER_MATERIALIZE=false` every `#(materialize)` is ignored without a word. The startup log says so.
1. **Did a build actually run?** On a standalone Publisher, publish/load does **not** build - run `malloy-pub materialize` (or enable the scheduler). "Publishes fine, no table" is the *expected* standalone state, not a model bug. On a hosted deployment the build is automatic but best-effort, so a failure is silent - look for a `FAILED` run, and for a ready run with `failures`.
2. **Was the source refused at plan, or did its build fail?** The package's warnings name each source Publisher will not build (`materialize-not-built`, `rollup-not-built`, `persist-view-not-built`, `persist-query-not-built`) and why; `GET …/packages/{pkg}/build-plan` lists the sources a run will build, the ones it will refuse (`refusedSources`, with the reason), and under `omittedSources` any the planner itself failed on. A ready run's `manifest.failures` names each source that did not build.
3. **Is it built but not served?** A `served-copy-not-served` warning, or a serve verdict `live` / `served_copy`, means the table exists but the served copy cannot answer from it; the message names why (a base whose block cannot be carried, fields not carried, a join the destination cannot reach).
4. **Does the query take a path the table cannot answer?** Read the path report's `considered`: a join to a source not stored with it, a nested column, a `timezone:` over a time-zone-dependent stored dimension, a stale table. Each is decided per query; the source's other queries still read the table.
5. **A `storage=` source serves live while the store is switched off.** With `PERSIST_STORAGE_MODE=off` a `storage=` source is skipped by the build and served live, with no error; it is never built into the warehouse instead. See `reference/storage.md`.
6. **Is it an extension of a `#@ persist` source?** Refused `inherited_persist`: migrate the base first.

**Isolation test** - add a trivial, self-contained materialized source in its own file and rebuild:
```malloy
#(materialize) name="scratch_dataset.materialize_smoke_test"
source: materialize_smoke is my_conn.table('some_dataset.some_table') -> { aggregate: n is count() }
```
If **even this** doesn't build after a real run, the problem is package-wide or server-wide: check the switch, read the run's error, then check that the connection can write the target dataset. If the smoke source builds and your real one doesn't, your real source is the problem: its warning, or its entry in `failures` / `refusedSources`. Delete the smoke file, delete its run (`malloy-pub delete materialization <id>`, record-only), and drop its table (`DELETE …/connections/{connectionName}/materializedTables/{physicalTableName}`, which refuses a table still in use).

## Gotchas

- **`#@ persist` is for views and named queries** - materialize a source with `#(materialize)`, roll up a measure with `#(rollup)`.
- **A tag doesn't build** - only an explicit run, the standalone scheduler, or a hosted control plane does.
- **A server with `PUBLISHER_MATERIALIZE=false` ignores `#(materialize)` silently.**
- **Quote the name, once** - a name with quotes inside it stops the load; Publisher quotes each segment for you.
- **A ready run can still have lost a source** - read `manifest.failures` and `metadata.refusedSources`.
- **Reuse is by content address, not by name** - `--force-refresh` and every scheduler fire defeat it.
- **Removing a materialized source does not drop its table** - and neither does `delete materialization`, which is record-only. Drop the table once no remaining ready run, of any version (list runs per version with `versionId`), names a table with that name in any place: `DELETE …/connections/{connectionName}/materializedTables/{physicalTableName}` checks that itself and answers 409 `TABLE_IN_USE` otherwise; serving reverts to live in place.
- **A given in the source's own `where:` is applied per caller; inherited from a base it refuses the source** - `reference/storage.md`.
- **A join to a source not stored with it, or a nested column, serves live for the queries that touch it** - the source's own fields still serve from the table.
- **A `#(access_filter)`-gated source freezes its gating column when materialized** - a `storage=` table holds every caller's rows, ungated, in the destination; `reference/access-filter.md`.
- **A chain migrates root-first** - `#(materialize)` on an extension of a `#@ persist` source is refused `inherited_persist`.
