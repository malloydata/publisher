---
name: malloy-materialization
description: Add, confirm and debug Malloy Persistence materializations in a package - persist an expensive source so queries read a pre-built table, in the source's own warehouse or in a storage destination. Read this whenever the user wants to materialize a source, add a persist annotation, speed up a slow source, build one stored rollup over another, tune what to persist, or asks why a persist source isn't building or isn't being read.
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Materialization (Malloy Persistence)

Materialize an expensive source once so queries read a **pre-built table** instead of recomputing it every time. You tag a source `#@ persist`, a materialization run builds it into a physical table, and queries against it are rewritten to read that table.

> **The #1 gotcha, up front:** if a persist source isn't materializing, it is almost always one of four things - no build ever ran (a standalone Publisher does not build on publish - see **Building and refreshing**), a build ran and failed or lost that one source (a run can finish ready with `failures`; read them), the model declaring the source has no `##! experimental.persistence` flag and nothing flagged imports it (it is skipped without a warning), or the package was reloaded since the build (a reload drops the bindings until the next run). Jump to **Debugging a no-op build**.

This file is the core. The details live beside it, one topic per file, and each is a prompt of its own:

- `reference/annotation.md` - the `#@ persist` tag: what can carry it, the name rules, identity and reuse, readers of a persisted source, and every option with where it is explained.
- `reference/policy.md` - the `publisher.json` block: `scope`, `queryMetadata`, and where the rules are enforced.
- `reference/storage.md` - building into a **storage destination** (`storage=`), `partition=`, where a given may sit, which sources that tier refuses, and how a stored table is served.
- `reference/chaining.md` - one stored source built **over another stored source**: what makes it reuse the parent's table or recompute from the warehouse, and how to read which happened.
- `reference/refresh.md` - keeping a table current: `freshness`, `schedule`, and `refresh="incremental"`.
- `reference/access-filter.md` - persisting a `#(access_filter)`-gated source, and the bound on a stale access decision.
- `reference/tuning.md` - reading the materialization history to decide what to persist, stop persisting, or reschedule (recommendations only).

## v1 or v2: which spelling to write

Publisher has two spellings. **v1** is `#@ persist` on a source and `#@ preaggregate` on a measure, and it is what a server reads by default. **v2** is `#(materialize)` on a source and `#(preaggregate)` on a measure, in development behind the server switch `PUBLISHER_MATERIALIZE` (default off). Since the v1 cutover both are served the same way: a stored table by the served copy (the author's own files with each stored source re-rooted on its table), a rollup by the rollup leg, so a query reads a table only when the path report says so. Everything else in this skill describes v1 unless it says otherwise.

**Pick by what the server runs, then by where the table goes:**

| The server | A table in a storage destination | A table in the source's own warehouse | A rollup on a measure |
|---|---|---|---|
| `PUBLISHER_MATERIALIZE` on | `#(materialize) storage="lake"` (needs `PERSIST_STORAGE_MODE=on` too) | `#(materialize) name="..."` (`name=` is required here) | `#(preaggregate) grain=["a", "b"]` |
| switch off (the default) | `#@ persist name="..." storage="lake"` | `#@ persist name="..."` | `#@ preaggregate grain="a, b"` |

So for new work, write v2 where the server has the switch on, and v1 where it is off. One exception: a source with a row-level gate (`#(authorize)`, `#(access_filter)`) is built as `#(materialize)` only with `PUBLISHER_MATERIALIZE_GATED_SOURCES` on as well; otherwise keep it on `#@ persist`. Ask whoever runs the server if you cannot tell; the startup log lists `materialize.enabled=true from PUBLISHER_MATERIALIZE` when it is on. **With the switch off, `#(materialize)` is ignored with no warning at all** (no build, no serve verdict, answered live), and `#(preaggregate)` is only an `annotation-near-miss` warning, so a v2 annotation on a v1 server looks exactly like a build that never ran.

```malloy
// v2, switch on: no ##! experimental.persistence needed in this file
#(materialize) name="east_orders" storage="lake"
source: east_orders is orders_base extend {
  where: region = 'east'
  measure: order_count is count()
}

source: sales is orders_base extend {
  #(preaggregate) grain=["category"]
  #(preaggregate) grain=["category", "region"]
  measure: revenue is amount.sum()
}
```

What is different about v2, briefly:

- **It stores the source as written** - every dimension (computed, private and internal ones too) with its `where:` applied, not a query's output - so a plain `extend` over a table is fine and needs no `-> { select: * }`. Keys are closed: `name`, `storage`, `refresh`, `watermark`, `merge_key`, `partition`, `freshness.*`, `queryMetadata.*`, `inline`. Any other key refuses the publish (`materialize-unknown-key`).
- **The base must not declare measures, views or joins**, unless the base is materialized too. A `#(materialize)` source that inherits one is built but never read: `served-copy-not-served` ("This source inherits fields ..."), serve verdict `live` / `served_copy`, every query live. Build over a base with none (as above), or keep the query shape (`orders -> { select: * } extend { ... }`) and declare the measures after it.
- **Joins are references by default.** `inline=["j"]` stores a join_one as `j__<field>` columns, or a join_many as a pair table; an incremental pair table names a watermark on each side, `watermark=["updated_at", "j.updated_at"]`.
- **`#@ persist` on a view and `#@ -persist` on a reader are the same in both.** A persisted view is built only with the switch on. `#@ preaggregate` is deprecated (a `deprecated-preaggregate-annotation` warning with the switch on; it still builds).
- **Confirm it the v2 way.** The package's serve verdicts (`GET .../packages/{pkg}/status`, `serveVerdicts`) read `serves`; a query's path report reads `servedBy.kind: "materialized"` with `via: "served_copy"` (a rollup: `kind: "rollup"`; a persisted view: `kind: "view"`). `manifestBindingStatus` does not cover v2 tables.

**Existing v1 code is not wrong**, and moving it is optional. When asked to move it, follow `docs/materialization-migration.md` in the Publisher repository: one v1 spelling at a time, a dry-run publish (`POST .../packages?dryRun=true`) to read the warnings and serve verdicts, a build (every migrated source rebuilds once, because its content address moves), then the path report and a served-versus-live comparison against an opted-out reader (`#@ -persist source: x_live is x extend {}`, declared in a model file, not in the query text). A colocated `#(materialize)` must keep a `name=`; keeping the v1 source's `name=` is fine, since the package's own runs wrote that table.

## The recipe (get this right and it just works)

1. **`##! experimental.persistence` on every `.malloy` file that declares a `#@ persist` source** (a file whose only materializations are v2 `#(materialize)` needs none). Either form enables it:
   - `##! experimental.persistence`, or
   - `##! experimental { access_modifiers, sql_functions, persistence }` (add `persistence` to the existing list).

   **Why it matters:** the build plan walks only the models that carry the flag, and skips every other file **without a warning**. The flag is a property of the model being *walked*, not of the file that declares the annotation: a flagged model builds every persist source its walk reaches, including ones declared in a flagless file it imports, and a flagless file's persist sources build only when some flagged model imports it. So a `#@ persist` in a flagless file that nothing flagged imports is silently never built, while adding a flagged entry model that imports it starts building tables that were inert - neither state warns. Put the flag in the declaring file itself: that is the one placement whose effect does not depend on what else imports it. A file with no persist source of its own does not need the flag; adding it anyway is harmless. (`#@ preaggregate` rollups are the one exception: they build from an unflagged file, because they live in a model Publisher synthesizes.)

2. **`#@ persist name="..."` on a query-based source, with the name quoted:**
   ```malloy
   #@ persist name="my_dataset.my_table"
   source: my_rollup is some_source -> { group_by: ...; aggregate: ... }
   ```
   - **Only `query_source` and `sql_select` sources are persistable** - a `-> { ... }` pipeline or a `conn.sql("...")`, including one refined by a trailing `extend { ... }`. A `#@ persist` on a *plain* `extend` over a bare `conn.table(...)` is **refused, not ignored**: the package warns, and every run that covers it fails naming the source, so a whole-package run builds nothing until you fix it. Persist a query source instead (`source: x is raw -> { where: ...; select: * }`).
   - **Quote the name, once.** A bare `name=my_table` fails the package load with `persist annotation name must be quoted`; a name quoted *inside* the quotes (`name="\"My Table\""`) also fails the load, as not a plain identifier path. Publisher quotes each segment itself, so mixed case and reserved words need no extra quoting. The target's container must already exist and be writable by the connection.
   - **The name is not the identity.** A persist source is identified by a content address of its connection, SQL and partition layout, so a rebuild with unchanged logic reuses the table and two sources with the same body share one. An `extend` of a persisted source reads the same table and is not a second build target.
   - **A `name=` is one table on its schema, whichever package asks.** Publisher's own builds name a table with no `name=` after the source plus a hash of the environment and package (`orders__p3f9c2a1b`), so packages sharing a schema never collide. A `name=` is used as written and marked as its package's; if another package already holds it, that source's build is refused (`table-owned-elsewhere` in the run's failures) and it answers live. Give it a different `name=`.

   The name rules, bound arguments, `#@ -persist`, and every option (`storage=`, `partition=`, `refresh=`, `freshness.*`, `queryMetadata.*`) are in `reference/annotation.md`.

3. **Package persistence policy in `publisher.json`** (all optional):
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
   - **`scope`** is a package-level contract about sharing tables across versions; on a standalone server it changes nothing except gating `schedule`. Its deprecated root-level home still works, but the two homes disagreeing **fails the load**.
   - **`freshness`** is the objective a **hosted control plane** enforces by refreshing the table; a **standalone** Publisher does not act on it at all. `fail` behaves like `live` today; only `stale_ok` keeps serving a stale table.
   - **`queryMetadata`** tags every build statement and every query for the backend's cost attribution. Root level; the home inside `materialization` is deprecated. Observability only.
   - **`schedule`** is a 5-field UTC cron that **requires `scope: "version"`** and **excludes `freshness`**; it fires only on a standalone server whose scheduler is enabled.

   The coherence rules are rejected at publish and at a policy edit, warned at load, and skipped by the scheduler. `reference/policy.md` has `scope` and `queryMetadata` in full; `reference/refresh.md` has `freshness` and `schedule`.

## Building and refreshing (standalone vs. hosted)

A `#@ persist` tag declares *what* to materialize; it does not by itself build anything.

- **Standalone Publisher:** publishing or loading a package only computes its build plan - **no table is built until a materialization run executes.** Trigger one explicitly (`malloy-pub materialize --environment <env> --package <pkg> --wait`, or the materialization API), or turn on the opt-in local scheduler (off unless `PUBLISHER_LOCAL_MATERIALIZATION_SCHEDULER` is set) to fire the package's `schedule` cron. Refresh is a re-run or that cron; `freshness` is not a refresh trigger here, so a freshness-only standalone package builds once and is not auto-refreshed. Two flags to know: `--force-refresh` builds every source even when its content address is unchanged (which is what every scheduler fire does), and `--reseed` is the only thing that rebuilds an incremental source from scratch. `--wait` gives up after **120 s by default** (`--timeout <s>`) and exits non-zero while the build keeps running; `malloy-pub get materialization <id>` then shows where it got to.
- **Hosted (control-plane) deployment:** the build runs automatically on publish, best-effort - a build failure does **not** fail the publish (which is why a broken persist can look like a silent no-op), and the control plane drives refresh to meet the `freshness` objective.

Two mechanics either way. **One build at a time per package**: a second run requested while one is active is rejected with a 409 conflict rather than queued, and the scheduler skips a fire while a run is active. **A build can be partial**: `sourceNames` on the request builds only the sources it names and leaves the others exactly as they were (live if never built), which is how to add one source to a package without rebuilding the rest.

Either way, a successful publish alone does not prove a table exists - confirm the build separately.

## Confirming it worked

A green run is not proof, and neither is a fast query (on small data a live recompute is fast too). Check what the server says it serves:

1. **The run lists your source, and did not lose it.** A run's detail (`malloy-pub get materialization <id> --environment <env> --package <pkg>`) carries `manifest.entries`, a map keyed by `sourceEntityId` whose values name each built or reused source (`sourceName`) and its physical table. A run can finish `MANIFEST_FILE_READY` having built nothing (every source skipped for a missing flag, or carried forward unchanged) **or having lost a source**: a source that failed is absent from `entries` and present in `manifest.failures` with its error, and a source the eligibility gate refused is in `metadata.refusedSources`, while the run still reads ready. `metadata.sourcesBuilt` / `sourcesReused` / `sourcesFailed` / `sourcesRefused` are the counts. For a source built over another stored source, the entry's `upstreamReuse` says whether it read the parent's table (`reused`) or recomputed it (`recomputed`, with `upstreamRecomputeReason`) - see `reference/chaining.md`.
2. **The package is bound to those tables.** On a standalone Publisher the package's serving state, `GET /api/v0/environments/{env}/packages/{pkg}/status`, reports `manifestBindingStatus` and `manifestEntryCount` (an older server: its entry in `GET /api/v0/status`, or the package's details). The count covers tables built in the source's own warehouse only: it reads `bound` with a nonzero count when such a table is bound and `unbound` otherwise. A `storage=` table never counts there - it appears under the package's `storageServeBindings`, so a package whose only persist sources are `storage=` reads `unbound` while serving from the store. `live_fallback` is a hosted-only status: a configured `manifestLocation` could not be fetched or bound, and the previously bound manifest (if any) stays in force.
3. **The query names the table.** A query's full result (the REST query response without `compactJson`) carries the SQL it ran. Served from the table, its `FROM` names the physical table; computed live, it carries the source's own SQL instead. This is the per-query proof.
4. **A reload unbinds them.** Reloading the package - including the automatic reload when a watched file is saved - drops the binding, and queries compute live until the next materialization run, which reuses unchanged tables and binds them again. (A server *restart* re-establishes the bindings from its store; a reload does not.) After editing a model, run a build before you trust a timing.

A query response's `servedFrom` field does not answer this question for a table built in the source's own connection: it reports only the `storage=` tier's outcome (`storage` when that tier answered, `live_fallback` when it degraded to a live recompute) and is empty otherwise, served from a colocated table or not. Do not read `live_fallback` here as a signal about a colocated table.

## Debugging a no-op build

Symptom: no table was built and the source still recomputes on every query. Check, in order:

0. **Did a build actually run?** On a standalone Publisher, publish/load does **not** build - run `malloy-pub materialize` (or enable the scheduler). "Publishes fine, no table" is the *expected* standalone state, not a model bug. On a hosted deployment the build is automatic but best-effort, so a failure is silent - look for a `FAILED` run, and for a ready run with `failures`.
1. **Did the run fail, or lose this source?** A `FAILED` run's `error` names what stopped it. A ready run's `manifest.failures` names each source that did not build, and `metadata.refusedSources` each one the gate refused, with the reason to act on. The package flags some of these before any run: an unbuildable persist source appears in its warnings, and `GET …/packages/{pkg}/build-plan` lists the sources a run will build, the ones it will refuse, and under `omittedSources` any the planner itself failed on (a code per source, or under `omittedFiles` a file whose sources it could not name; the server log has the error). A `#@ persist` on a non-persistable source fails every run that covers it, naming the source.
2. **The declaring model is missing the persistence flag** and nothing flagged imports it: skipped without any warning, the run succeeds, the source is simply absent from what was built. Add the flag to the declaring file.
3. **An unquoted persist name** - a bare `name=foo` **always** stops the load with `persist annotation name must be quoted`. (If you got *no* error at all, it isn't this.)
4. **The package was reloaded** after the build (a saved file under watch mode counts). The tables exist and the run reads ready; the binding is gone. Run a build. And if a watched edit failed to compile, the server keeps serving the **old** model and its binding, and records the error under `staleCompileErrors` on `/status` - so "still bound" after an edit can mean the edit never took.
5. **A `storage=` source serves live while the store is switched off.** With `PERSIST_STORAGE_MODE=off` a `storage=` source is skipped by the build and served live, with no error; it is never built into the warehouse instead. See `reference/storage.md`.

**Isolation test** - add a trivial, self-contained persist source in its own file and rebuild:
```malloy
##! experimental.persistence
source: smoke_raw is my_conn.table('some_dataset.some_table')
#@ persist name="scratch_dataset.persist_smoke_test"
source: persist_smoke is smoke_raw -> { aggregate: n is count() }
```
If **even this** doesn't build after a real run, the problem is package-wide: read the run's error (a non-persistable source anywhere fails the whole run and names it), then check that the connection can write the target dataset. If the smoke source builds and your real one doesn't, your real source is the problem: its file's flag, or its entry in `failures` / `refusedSources`. Delete the smoke file and drop its table afterward (`malloy-pub delete materialization <id> --drop-tables`).

## Gotchas

- **Flag every file that declares a persist source** - a flagless model nothing flagged imports is skipped without a warning.
- **A `#@ persist` on a plain `extend` over `conn.table(...)` fails the run** - every run that covers it, naming the source.
- **A tag doesn't build** - only an explicit run, the standalone scheduler, or a hosted control plane does.
- **Quote the name, once** - bare stops the load, and so does a name with quotes inside it; Publisher quotes each segment for you.
- **A ready run can still have lost a source** - read `manifest.failures` and `metadata.refusedSources`.
- **A reload unbinds; a restart rebinds** - run a build before trusting a timing.
- **Reuse is by content address, not by name** - `--force-refresh` and every scheduler fire defeat it.
- **Removing a persist source does not drop its table** - `delete materialization --drop-tables` drops what no other ready run names, and serving reverts to live in place.
- **A given inside the persisted query is refused; in the source's own `extend` block it is applied per caller** - `reference/storage.md`.
- **A join to a non-persisted source, or a nested column, serves live for the queries that touch it** - the source's own fields still serve from the table.
- **A `#(access_filter)`-gated source freezes its gating column when persisted** - `storage=` and `#@ preaggregate` refuse it outright; `reference/access-filter.md`.
