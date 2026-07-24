---
name: malloy-materialization
description: Add and debug Malloy Persistence materializations in a package - persist an expensive source so queries read a pre-built table. Read this whenever the user wants to materialize a source, add a persist annotation, speed up a slow source, or asks why a persist source isn't building.
---

# Materialization (Malloy Persistence)

Materialize an expensive source once so queries read a **pre-built warehouse table** instead of recomputing it every time. You tag a source `#@ persist`, a materialization run builds it into a physical table, and queries against it are rewritten to read that table.

> **The #1 gotcha, up front:** if a persist source isn't materializing, it is almost always one of two things - a `.malloy` file in the package missing the `##! experimental.persistence` flag (which aborts the *whole* package's build plan), or no build ever ran (a standalone Publisher does not build on publish - see **Building and refreshing**). Jump to **Debugging a no-op build**.

## The recipe (get this right and it just works)

1. **`##! experimental.persistence` on EVERY `.malloy` file in the package** - not only the file that declares the persist source. Either form enables it:
   - `##! experimental.persistence`, or
   - `##! experimental { access_modifiers, sql_functions, persistence }` (add `persistence` to the existing list).

   **Why every file:** the build plan is computed by asking *every* `.malloy` file in the package for its persist sources, and that call **throws on any file whose model lacks the flag** (`Model must have ##! experimental.persistence`). One unflagged helper or import file, even one with no persist source of its own, aborts the whole package's build plan, so *every* persist source in the package drops out. This is the most common cause of a no-op build.

2. **`#@ persist name="..."` on a query-based source, with the name quoted:**
   ```malloy
   #@ persist name="my_dataset.my_table"
   source: my_rollup is some_source -> { group_by: ...; aggregate: ... }
   ```
   - **Only `query_source` and `sql_select` sources are persistable** - a source whose definition has a `-> { ... }` pipeline or a `conn.sql("...")`. This **includes** one refined by a trailing `extend { ... }`. What is **not** persistable is a *plain* `extend` over a bare `conn.table(...)`; a `#@ persist` on such a source is **silently ignored** (its annotation is never read) - that one source just won't materialize, and the rest of the package still builds.
   - **Quote the name.** `name="my_table"` (or a path `name="dataset.table"` / `name="project.dataset.table"`) is required. A **bare** `name=my_table` **always fails the build/publish** with `persist annotation name must be quoted` (a raw-source scan that hard-stops); it never silently no-ops.
   - `name=` is the target table name. In a standalone Publisher this **is** the physical table (rebuilt in place); a hosted (control-plane) deployment builds it under a content-addressed generation name. In both, the source's identity for reuse is a content address of its connection and canonical SQL (its `sourceEntityId`), so **republishing unchanged persist logic reuses the existing table** and changing the logic builds fresh.

3. **Package persistence policy in `publisher.json`** (all optional):
   ```jsonc
   {
     "name": "my-package",
     "scope": "package",  // default; "version" = each published version owns its own tables
     "materialization": { "freshness": { "window": "24h", "fallback": "live" } }
   }
   ```
   Enforced at publish (strict), on edits (strict), at load (warn, still serves), and by the scheduler (an offending package is skipped):
   - **`scope`**: `package` (default; artifacts reused across published versions) or `version` (each artifact owned by one version). Package-level only; there is no per-source scope.
   - **`materialization.freshness`** (`window` + `fallback` of `live`/`stale_ok`/`fail`) is the objective a **hosted control plane** enforces by refreshing the table to meet it (`fallback: "live"` serves live compute while stale/absent). A **standalone** Publisher does **not** act on `freshness` for refresh - see **Building and refreshing**.
   - **`materialization.schedule`** is a 5-field UTC cron (`min hour dom mon dow`; `L`/`W`/`#`/`?` rejected). It **requires `scope: "version"`** and is **mutually exclusive with `freshness`**. This is how a standalone Publisher refreshes on a cadence.

4. **Reads vs writes.** The persist source can *read* any dataset the connection can read; the persist *target* (`name=`'s dataset) must be a dataset the connection can **write** (typically a scratch dataset).

## Building and refreshing (standalone vs. hosted)

A `#@ persist` tag declares *what* to materialize; it does not by itself build anything.

- **Standalone Publisher:** publishing or loading a package only computes its build plan - **no table is built until a materialization run executes.** Trigger one explicitly (`malloy-pub materialize --package <pkg> --wait`, or the materialization API), or turn on the opt-in local scheduler (off unless `PUBLISHER_LOCAL_MATERIALIZATION_SCHEDULER` is set) to fire the package's `schedule` cron. Refresh is a re-run or that cron; `freshness` is not a refresh trigger here, so a freshness-only standalone package builds once and is not auto-refreshed.
- **Hosted (control-plane) deployment:** the build runs automatically on publish, best-effort - a build failure does **not** fail the publish (which is why a broken persist can look like a silent no-op), and the control plane drives refresh to meet the `freshness` objective.

Either way, a successful publish alone does not prove a table exists - confirm the build separately.

## Serve-time routing is `query_source`-only (today)

Both persistable types *build* a table, but only a **`query_source`** (a `-> { ... }` pipeline) is rewritten to *read* it at query time. A raw **`sql_select`** (`conn.sql("...")`, including `conn.sql("...") extend { ... }`) builds its table and then the query path re-inlines its SQL, so the table is built and never read, and queries are no faster. If you have raw SQL you want served from a table, wrap it in a thin `query_source` and persist that:

```malloy
source: x_raw is my_conn.sql("select ...")
#@ persist name="scratch_dataset.x"
source: x is x_raw -> { select: * }
```

## Confirming it worked

After a build runs, re-run one of the source's queries - a persisted `query_source` should return quickly, reading the pre-built table instead of recomputing the upstream. Your host also reports each persisted source as **ready** with its physical table name (a materialization run detail, CLI listing, or materialization view, depending on the host); if nothing is listed, either no build ran (standalone) or the build plan was empty - see **Debugging a no-op build**.

## Debugging a no-op build

Symptom: no table was built and the source still recomputes on every query. Check, in order:

0. **Did a build actually run?** On a standalone Publisher, publish/load does **not** build - run `malloy-pub materialize` (or enable the scheduler). "Publishes fine, no table" is the *expected* standalone state, not a model bug. On a hosted deployment the build is automatic but best-effort, so a failure is silent - look for a `FAILED` run.
1. **A `.malloy` file missing the persistence flag** (the most common real bug). Every model file's `##!` line needs `persistence`, including pure helper/import files with no persist source - one unflagged file aborts the whole package's build plan.
2. **An unquoted persist name** - a bare `name=foo` **always** hard-stops the build/publish with `persist annotation name must be quoted`; use `name="foo"`. (If you got *no* error at all, it isn't this.)
3. **A `#@ persist` on a non-persistable source** - a bare `extend` over `conn.table(...)` is silently ignored, so *that* source won't materialize (the rest of the package is unaffected). Tag a `query_source` / `sql_select` instead.
4. **A persisted raw `sql_select` that builds but is never read** - if the table exists yet queries are no faster, it's the serve-routing gap above; wrap the `sql_select` in a `query_source`.

**Isolation test** - add a trivial, self-contained persist source in its own file and rebuild:
```malloy
##! experimental.persistence
source: smoke_raw is my_conn.table('some_dataset.some_table')
#@ persist name="scratch_dataset.persist_smoke_test"
source: persist_smoke is smoke_raw -> { aggregate: n is count() }
```
- If **even this** doesn't build (after a real materialization run), the whole package's plan is aborting - a sibling `.malloy` file is missing the flag. Fix rule 1 across the package.
- If the smoke source **does** build but your real one doesn't, your real source is the problem - a non-persistable type (a bare `extend`), or its own file's flag.

Delete the smoke file and drop its table afterward.

## Gotchas

- **Every `.malloy` file needs the persistence flag** - one unflagged file aborts the whole package's build plan. (A `#@ persist` on a *non*-persistable source, by contrast, is silently ignored and does not affect other sources.)
- **A tag doesn't build** - a standalone Publisher materializes only on an explicit run or its scheduler; only a hosted control plane builds on publish.
- **Serve-time routing is `query_source`-only** - a raw `sql_select` builds a table the query path doesn't read; wrap it in a `query_source`.
- **Quote the name** - a bare `name=` always hard-stops the build.
- **Republishing unchanged persist logic reuses the table** - reuse is keyed on the content-addressed `sourceEntityId`, not the `name=`.
- **Removing a persist source (or a smoke test) does not drop its table** - physical-table cleanup is the caller's responsibility; drop it yourself.
