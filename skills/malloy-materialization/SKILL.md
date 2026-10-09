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

> **The #1 gotcha, up front:** if a persist source isn't materializing, it is almost always one of four things - no build ever ran (a standalone Publisher does not build on publish - see **Building and refreshing**), a build ran and failed or lost that one source (a run can finish ready with `failures`; read them), the file declaring the source has no `##! experimental.persistence` flag, of its own or from a file it imports (that file is skipped without a warning), or the package was reloaded since the build (a reload drops the bindings until the next run). Jump to **Debugging a no-op build**.

This file is the core. The details live beside it, one topic per file, and each is a prompt of its own:

- `reference/storage.md` - building into a **storage destination** (`storage=`), `partition=`, which sources that tier refuses, and how a stored table is served.
- `reference/chaining.md` - one stored source built **over another stored source**: what makes it reuse the parent's table or recompute from the warehouse, and how to read which happened.
- `reference/refresh.md` - keeping a table current: `freshness`, `schedule`, and `refresh="incremental"`.
- `reference/access-filter.md` - persisting a `#(access_filter)`-gated source, and the bound on a stale access decision.
- `reference/tuning.md` - reading the materialization history to decide what to persist, stop persisting, or reschedule (recommendations only).

## The recipe (get this right and it just works)

1. **`##! experimental.persistence` on every `.malloy` file that declares a persist source.** Either form enables it:
   - `##! experimental.persistence`, or
   - `##! experimental { access_modifiers, sql_functions, persistence }` (add `persistence` to the existing list).

   **Why it matters:** the build plan walks only the models that carry the flag, and skips every other file **without a warning**. The flag is a property of the model being *walked*, not of the file that declares the annotation: a flagged model builds every persist source its walk reaches, including ones declared in a flagless file it imports, and a flagless file's persist sources build only when some flagged model imports it. So a `#@ persist` in a flagless file that nothing flagged imports is silently never built, while adding a flagged entry model that imports it starts building tables that were inert - neither state warns. Put the flag in the declaring file itself: that is the one placement whose effect does not depend on what else imports it. A file with no persist source of its own (a helper or import file) does not need the flag; adding it anyway is harmless. (`#@ preaggregate` rollups are the one exception: they build from an unflagged file, because they live in a model Publisher synthesizes.)

2. **`#@ persist name="..."` on a query-based source, with the name quoted:**
   ```malloy
   #@ persist name="my_dataset.my_table"
   source: my_rollup is some_source -> { group_by: ...; aggregate: ... }
   ```
   - **Only `query_source` and `sql_select` sources are persistable** - a source whose definition has a `-> { ... }` pipeline or a `conn.sql("...")`. This **includes** one refined by a trailing `extend { ... }`. What is **not** persistable is a *plain* `extend` over a bare `conn.table(...)` (a filtered pass-through). A `#@ persist` on such a source is **refused, not ignored**: the package carries a warning naming the source, and any materialization run that covers it fails with an error naming it (`annotated '#@ persist' but were not recognized as a materializable source`), so a whole-package run builds nothing until you fix it. Persist a query source instead (`source: x is raw -> { where: ...; select: * }`), or remove the annotation.
   - **Quote the name, once.** `name="my_table"` (or a path `name="dataset.table"` / `name="project.dataset.table"`) is required. A **bare** `name=my_table` fails the package **load** - so every publish and every restart - with `persist annotation name must be quoted`; it never silently no-ops. Keep the name to dot-separated segments of letters, digits, underscores and hyphens: a quoted name holding a space, quote, semicolon or backtick is rejected too, with its own message (the name is inlined into DDL). Never quote *inside* the quotes (`name="\"Quoted Tbl\""`): the build creates a table whose name includes the quote characters, the serve side looks for the unquoted one, and the source silently serves live.
   - **One name per table.** Two distinct persist sources that resolve to the same `name=` in the same destination clobber each other (the second build overwrites the first, and dropping one drops the other's). Publisher warns on the package at load and, where `PERSIST_COLLISION_ENFORCE` is set, rejects the publish. Give every persist source its own name; a shared *body* is fine (it shares one table by content address), a shared name over different bodies is not.
   - **A parameterized source must have its argument bound.** `#@ persist` on `high_value(min_amount is 500) -> { ... }` builds one fixed relation; on the free template `high_value -> { ... }` it is refused (`free_parameter`), since there is no single relation to store. A `given` the persisted query reads is refused the same way - see `reference/storage.md` for where a given may sit.
   - `name=` is the target table. In a standalone Publisher it **is** the physical table (rebuilt in place); a hosted deployment may assign the physical name itself. In both, the source's identity for reuse is a content address of its connection, its canonical SQL and its `partition=` layout (its `sourceEntityId`), so **a rebuild with unchanged persist logic reuses the existing table** and changing the logic builds fresh. The name is not part of the identity: two sources whose bodies compile to the same SQL share one table.
   - A source that `extend`s a persisted source reads the stored table too and is not a build target of its own; `#@ -persist` on the extension makes it recompute live instead. A source whose *query* reads a persisted source (`orders is _orders_fact -> { select: * }`) also reads the table and is not a build target. The options beyond `name=` - `storage=`, `partition=`, `refresh=`, `watermark=`, `merge_key=`, `freshness.*` - are in the reference files above.

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
   - **`scope`**: `package` (default) or `version`. It is a package-level contract about whether a table may be shared across published versions; a standalone auto-run keys tables by content address regardless, so there it changes nothing except gating `schedule`. A root-level `scope` is the deprecated home and still works, with a warning. **The two homes holding different values is not a warning: the package fails to load**, disappears from the server, and says so only in `/status` `loadErrors` - delete the root copy rather than editing it.
   - **`materialization.freshness`** (`window` + `fallback` of `live` / `stale_ok` / `fail`) is the objective a **hosted control plane** enforces by refreshing the table to meet it and stamping each entry's `dataAsOf`. A **standalone** Publisher does not act on it at all: it binds what it just built with no `dataAsOf` and no window, so the table never ages out. `fail` behaves like `live` today (a stale table is skipped and the query computes live); only `stale_ok` keeps serving a stale table. Per-source spelling and the model-file layer are in `reference/refresh.md`.
   - **`queryMetadata`** is a bag of up to 20 string properties attached to every statement a build issues **and every query against the source**, for the backend's own cost attribution (Snowflake `QUERY_TAG`, BigQuery job labels, a leading SQL comment elsewhere). Declare it at the **root** of `publisher.json`; the older home inside `materialization` still works but warns on every load. Override per source with a sibling line `#@ queryMetadata.team="risk"` above the source. Observability only: it never changes what gets built, and problems with it are warnings, never rejections. See `docs/query-metadata.md`.
   - **`materialization.schedule`** is a 5-field UTC cron (`min hour dom mon dow`; `L`/`W`/`#`/`?` rejected). It **requires `scope: "version"`** and is **mutually exclusive with `freshness`**, including a per-source `freshness.*`. It is how a standalone Publisher refreshes on a cadence - and only when its scheduler is enabled (next section).

   The scope / schedule / freshness rules are enforced at publish (rejected), at a package edit that touches the policy (rejected), at load (warned, the package still serves), and by the scheduler (an offending package is skipped).

4. **Reads vs writes.** The persist source can *read* any dataset the connection can read; the persist *target* (`name=`'s dataset) must be a dataset the connection can **write**, and it must already exist - Publisher creates the table, not its container.

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
1. **Did the run fail, or lose this source?** A `FAILED` run's `error` names what stopped it. A ready run's `manifest.failures` names each source that did not build, and `metadata.refusedSources` each one the gate refused (with the reason to act on). The package's details flag some of these before any run: an unbuildable persist source appears in its warnings, and its `buildPlan` lists the sources a run will build and the ones it will refuse. A `#@ persist` on a non-persistable source (a plain `extend` over `conn.table(...)`) fails every run that covers it, naming the source - so one bad annotation stops the whole package from building. Tag a `query_source` / `sql_select` instead, or remove the annotation.
2. **The declaring file is missing the persistence flag.** A file with no `persistence` flag, of its own or from a file it imports, is skipped without any warning: the run succeeds and its persist sources are simply absent from what was built. Add the flag to every file that declares a `#@ persist`.
3. **An unquoted persist name** - a bare `name=foo` **always** stops the load with `persist annotation name must be quoted`; use `name="foo"`. (If you got *no* error at all, it isn't this.)
4. **The package was reloaded** after the build (a saved file under watch mode counts). The tables exist and the run reads ready; the binding is gone. Run a build. And if a watched edit failed to compile, the server keeps serving the **old** model and its binding, and records the error under `staleCompileErrors` on `/status` - so "still bound" after an edit can mean the edit never took.
5. **A `storage=` source serves live while the store is switched off.** With `PERSIST_STORAGE_MODE=off` a `storage=` source is skipped by the build and served live, with no error; it is never built into the warehouse instead. See `reference/storage.md`.

**Isolation test** - add a trivial, self-contained persist source in its own file and rebuild:
```malloy
##! experimental.persistence
source: smoke_raw is my_conn.table('some_dataset.some_table')
#@ persist name="scratch_dataset.persist_smoke_test"
source: persist_smoke is smoke_raw -> { aggregate: n is count() }
```
- If **even this** doesn't build (after a real materialization run), the problem is package-wide: read the run's error. A `#@ persist` on a non-persistable source anywhere in the package fails the whole run and names it; otherwise check that the connection can write the target dataset.
- If the smoke source **does** build but your real one doesn't, your real source is the problem - its own file's flag, or the source itself (read its entry in `failures` or `refusedSources`).

Delete the smoke file and drop its table afterward (`malloy-pub delete materialization <id> --drop-tables` drops a run's tables that no other ready run still names).

## Gotchas

- **Flag every file that declares a persist source** - a file with no `##! experimental.persistence`, of its own or from a file it imports, is skipped without a warning, so its persist sources are silently never built.
- **A `#@ persist` on a non-persistable source fails the run** - a plain `extend` over `conn.table(...)` is refused, and every run that covers it fails naming the source, so a whole-package run builds nothing until you fix it.
- **A tag doesn't build** - a standalone Publisher materializes only on an explicit run or its scheduler; only a hosted control plane builds on publish.
- **Quote the name** - a bare `name=` always stops the load.
- **A ready run can still have lost a source** - read `manifest.failures` and `metadata.refusedSources`, not just the status.
- **A reload unbinds; a restart rebinds** - after any package reload, run a build before trusting a timing.
- **Rebuilding unchanged persist logic reuses the table** - reuse is keyed on the content-addressed `sourceEntityId` (connection, SQL, partition layout), not the `name=`; `--force-refresh` and every scheduler fire defeat it.
- **Removing a persist source (or a smoke test) does not drop its table** - physical-table cleanup is the caller's responsibility; `delete materialization --drop-tables` drops the tables a run holds that no other ready run still names, and serving reverts to live in place, with no restart.
- **A given inside the persisted query is refused; a given in the source's own `extend` block is applied per caller** - the table holds every caller's rows and the read filters them. See `reference/storage.md`.
- **A join to a non-persisted source, or a nested column, is served live for the queries that touch it** - the source's own fields still serve from the table. See `reference/storage.md`.
- **A `#(access_filter)`-gated source freezes its gating column when persisted** - the gate still runs live, but a revoked row keeps being served under its old access decision until the next rebuild. `storage=` and `#@ preaggregate` refuse a gated source outright. See `reference/access-filter.md`.
