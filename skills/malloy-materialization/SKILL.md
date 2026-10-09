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

## The recipe (get this right and it just works)

1. **`##! experimental.persistence` on every `.malloy` file that declares a persist source.** Either form enables it:
   - `##! experimental.persistence`, or
   - `##! experimental { access_modifiers, sql_functions, persistence }` (add `persistence` to the existing list).

   **Why it matters:** the build plan walks only the models that carry the flag, and skips every other file **without a warning**. The flag is a property of the model being *walked*, not of the file that declares the annotation: a flagged model builds every persist source its walk reaches, including ones declared in a flagless file it imports, and a flagless file's persist sources build only when some flagged model imports it. So a `#@ persist` in a flagless file that nothing flagged imports is silently never built, while adding a flagged entry model that imports it starts building tables that were inert - neither state warns. Put the flag in the declaring file itself: that is the one placement whose effect does not depend on what else imports it. A file with no persist source of its own does not need the flag; adding it anyway is harmless. (`#@ preaggregate` rollups are the one exception: they build from an unflagged file, because they live in a model Publisher synthesizes.)

2. **`#@ persist name="..."` on a query-based source, with the name quoted:**
   ```malloy
   #@ persist name="my_dataset.my_table"
   source: my_rollup is some_source -> { group_by: ...; aggregate: ... }
   ```
   - **Only `query_source` and `sql_select` sources are persistable** - a `-> { ... }` pipeline or a `conn.sql("...")`, including one refined by a trailing `extend { ... }`. A `#@ persist` on a *plain* `extend` over a bare `conn.table(...)` is **refused, not ignored**: the package warns, and every run that covers it fails naming the source, so a whole-package run builds nothing until you fix it. Persist a query source instead (`source: x is raw -> { where: ...; select: * }`).
   - **Quote the name, once.** A bare `name=my_table` fails the package load with `persist annotation name must be quoted`; a name quoted inside the quotes silently serves live. The target's container must already exist and be writable by the connection.
   - **The name is not the identity.** A persist source is identified by a content address of its connection, SQL and partition layout, so a rebuild with unchanged logic reuses the table and two sources with the same body share one. An `extend` of a persisted source reads the same table and is not a second build target.

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
2. **The package is bound to those tables.** On a standalone Publisher the package's details report `manifestBindingStatus` and `manifestEntryCount`. The count covers tables built in the source's own warehouse only: it reads `bound` with a nonzero count when such a table is bound and `unbound` otherwise. A `storage=` table never counts there - it appears under the package's `storageServeBindings`, so a package whose only persist sources are `storage=` reads `unbound` while serving from the store. `live_fallback` is a hosted-only status: a configured `manifestLocation` could not be fetched or bound, and the previously bound manifest (if any) stays in force.
3. **The query names the table.** A query's full result (the REST query response without `compactJson`) carries the SQL it ran. Served from the table, its `FROM` names the physical table; computed live, it carries the source's own SQL instead. This is the per-query proof.
4. **A reload unbinds them.** Reloading the package - including the automatic reload when a watched file is saved - drops the binding, and queries compute live until the next materialization run, which reuses unchanged tables and binds them again. (A server *restart* re-establishes the bindings from its store; a reload does not.) After editing a model, run a build before you trust a timing.

A query response's `servedFrom` field does not answer this question for a table built in the source's own connection: it reports only the `storage=` tier's outcome (`storage` when that tier answered, `live_fallback` when it degraded to a live recompute) and is empty otherwise, served from a colocated table or not. Do not read `live_fallback` here as a signal about a colocated table.

## Debugging a no-op build

Symptom: no table was built and the source still recomputes on every query. Check, in order:

0. **Did a build actually run?** On a standalone Publisher, publish/load does **not** build - run `malloy-pub materialize` (or enable the scheduler). "Publishes fine, no table" is the *expected* standalone state, not a model bug. On a hosted deployment the build is automatic but best-effort, so a failure is silent - look for a `FAILED` run, and for a ready run with `failures`.
1. **Did the run fail, or lose this source?** A `FAILED` run's `error` names what stopped it. A ready run's `manifest.failures` names each source that did not build, and `metadata.refusedSources` each one the gate refused, with the reason to act on. The package's details flag some of these before any run: an unbuildable persist source appears in its warnings, and its `buildPlan` lists the sources a run will build and the ones it will refuse. A `#@ persist` on a non-persistable source fails every run that covers it, naming the source.
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
- **Quote the name, once** - bare stops the load; double-quoted silently serves live.
- **A ready run can still have lost a source** - read `manifest.failures` and `metadata.refusedSources`.
- **A reload unbinds; a restart rebinds** - run a build before trusting a timing.
- **Reuse is by content address, not by name** - `--force-refresh` and every scheduler fire defeat it.
- **Removing a persist source does not drop its table** - `delete materialization --drop-tables` drops what no other ready run names, and serving reverts to live in place.
- **A given inside the persisted query is refused; in the source's own `extend` block it is applied per caller** - `reference/storage.md`.
- **A join to a non-persisted source, or a nested column, serves live for the queries that touch it** - the source's own fields still serve from the table.
- **A `#(access_filter)`-gated source freezes its gating column when persisted** - `storage=` and `#@ preaggregate` refuse it outright; `reference/access-filter.md`.
