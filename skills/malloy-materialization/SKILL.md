---
name: malloy-materialization
description: Add and debug Malloy Persistence materializations in a package - persist an expensive source so queries read a pre-built table. Read this whenever the user wants to materialize a source, add a persist annotation, speed up a slow source, tune what to persist, or asks why a persist source isn't building.
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Materialization (Malloy Persistence)

Materialize an expensive source once so queries read a **pre-built warehouse table** instead of recomputing it every time. You tag a source `#@ persist`, a materialization run builds it into a physical table, and queries against it are rewritten to read that table.

> **The #1 gotcha, up front:** if a persist source isn't materializing, it is almost always one of three things - no build ever ran (a standalone Publisher does not build on publish - see **Building and refreshing**), a build ran and failed (read the run's error: it names the source), or the file declaring the source has no `##! experimental.persistence` flag, of its own or from a file it imports (that file is skipped without a warning). Jump to **Debugging a no-op build**.

**Deciding what to persist, what to stop persisting, and how to schedule it** (making a package cheaper or faster): read `reference/tuning.md`. It reads the materialization history with the `malloy-pub` CLI and proposes changes; it recommends and does not edit.

## The recipe (get this right and it just works)

1. **`##! experimental.persistence` on every `.malloy` file that declares a persist source.** Either form enables it:
   - `##! experimental.persistence`, or
   - `##! experimental { access_modifiers, sql_functions, persistence }` (add `persistence` to the existing list).

   **Why it matters:** the build plan reads persist sources only from files whose model carries the flag, and skips every other file **without a warning**. A file's model carries the flag when the file declares it or imports a file that does. So a `#@ persist` in a file with neither is silently never built, while the rest of the package builds normally. Put the flag in the declaring file itself rather than relying on an import to supply it. A file with no persist source of its own (a helper or import file) does not need the flag; adding it anyway is harmless.

2. **`#@ persist name="..."` on a query-based source, with the name quoted:**
   ```malloy
   #@ persist name="my_dataset.my_table"
   source: my_rollup is some_source -> { group_by: ...; aggregate: ... }
   ```
   - **Only `query_source` and `sql_select` sources are persistable** - a source whose definition has a `-> { ... }` pipeline or a `conn.sql("...")`. This **includes** one refined by a trailing `extend { ... }`. Both build a table, and queries against either read it. What is **not** persistable is a *plain* `extend` over a bare `conn.table(...)` (a filtered pass-through). A `#@ persist` on such a source is **refused, not ignored**: the package carries a warning naming the source, and any materialization run that covers it fails with an error naming it, so a whole-package run builds nothing until you fix it. Persist a query source instead (`source: x is raw -> { where: ...; select: * }`), or remove the annotation.
   - **Quote the name.** `name="my_table"` (or a path `name="dataset.table"` / `name="project.dataset.table"`) is required. A **bare** `name=my_table` **always fails the build/publish** with `persist annotation name must be quoted` (a raw-source scan that hard-stops); it never silently no-ops.
   - `name=` is the target table name. In a standalone Publisher this **is** the physical table (rebuilt in place); a hosted (control-plane) deployment builds it under a content-addressed generation name. In both, the source's identity for reuse is a content address of its connection and canonical SQL (its `sourceEntityId`), so **republishing unchanged persist logic reuses the existing table** and changing the logic builds fresh.

3. **Package persistence policy in `publisher.json`** (all optional):
   ```jsonc
   {
     "name": "my-package",
     "materialization": {
       "scope": "package",  // default; "version" = each published version owns its own tables
       "freshness": { "window": "24h", "fallback": "live" },
       "queryMetadata": { "team": "finance" }  // tags the build's backend statements
     }
   }
   ```
   Enforced at publish (strict), on edits (strict), at load (warn, still serves), and by the scheduler (an offending package is skipped):
   - **`scope`**: `package` (default; artifacts reused across published versions) or `version` (each artifact owned by one version). Package-level only; there is no per-source scope. A root-level `scope` is the deprecated home and still works, with a warning; declaring both homes with different values is rejected.
   - **`materialization.freshness`** (`window` + `fallback` of `live`/`stale_ok`/`fail`) is the objective a **hosted control plane** enforces by refreshing the table to meet it (`fallback: "live"` serves live compute while stale/absent). A **standalone** Publisher does **not** act on `freshness` for refresh - see **Building and refreshing**.
   - **`materialization.queryMetadata`** is a bag of string properties attached to every statement the build issues, for the backend's own cost attribution (Snowflake `QUERY_TAG`, BigQuery job labels, a leading SQL comment elsewhere). Overridable per source with `#@ persist queryMetadata.<name>="<value>"`. Observability only: it never changes what gets built. See `docs/query-metadata.md`.
   - **`materialization.schedule`** is a 5-field UTC cron (`min hour dom mon dow`; `L`/`W`/`#`/`?` rejected). It **requires `scope: "version"`** and is **mutually exclusive with `freshness`**. This is how a standalone Publisher refreshes on a cadence.

4. **Reads vs writes.** The persist source can *read* any dataset the connection can read; the persist *target* (`name=`'s dataset) must be a dataset the connection can **write** (typically a scratch dataset).

## Building and refreshing (standalone vs. hosted)

A `#@ persist` tag declares *what* to materialize; it does not by itself build anything.

- **Standalone Publisher:** publishing or loading a package only computes its build plan - **no table is built until a materialization run executes.** Trigger one explicitly (`malloy-pub materialize --environment <env> --package <pkg> --wait`, or the materialization API), or turn on the opt-in local scheduler (off unless `PUBLISHER_LOCAL_MATERIALIZATION_SCHEDULER` is set) to fire the package's `schedule` cron. Refresh is a re-run or that cron; `freshness` is not a refresh trigger here, so a freshness-only standalone package builds once and is not auto-refreshed.
- **Hosted (control-plane) deployment:** the build runs automatically on publish, best-effort - a build failure does **not** fail the publish (which is why a broken persist can look like a silent no-op), and the control plane drives refresh to meet the `freshness` objective.

Either way, a successful publish alone does not prove a table exists - confirm the build separately.

## Confirming it worked

A green run is not proof, and neither is a fast query (on small data a live recompute is fast too). Check what the server says it serves:

1. **The run lists your source.** A run's detail (`malloy-pub get materialization <id> --environment <env> --package <pkg>`) names each built or reused source with its physical table. A run can finish ready having built nothing - a source missing from its manifest was not built; see **Debugging a no-op build**.
2. **The package is bound to those tables.** On a standalone Publisher the package's details report `manifestBindingStatus: "bound"` with a `manifestEntryCount` counting the built tables bound to its queries. `"unbound"` means every query computes live, whatever tables exist.
3. **The query names the table.** A query's full result (the REST query response without `compactJson`) carries the SQL it ran. Served from the table, its `FROM` names the physical table; computed live, it carries the source's own SQL instead. This is the per-query proof.
4. **A reload unbinds them.** Reloading the package - including the automatic reload when a watched file is saved - drops the binding, and queries compute live until the next materialization run, which reuses unchanged tables and binds them again. After editing a model, run a build before you trust a timing.

A query response's `servedFrom` field does not answer this question for a table built in the source's own connection: it reports only the separate `storage=` tier, and stays empty for everything else, served from a table or not.

## Debugging a no-op build

Symptom: no table was built and the source still recomputes on every query. Check, in order:

0. **Did a build actually run?** On a standalone Publisher, publish/load does **not** build - run `malloy-pub materialize` (or enable the scheduler). "Publishes fine, no table" is the *expected* standalone state, not a model bug. On a hosted deployment the build is automatic but best-effort, so a failure is silent - look for a `FAILED` run.
1. **Did the run fail?** Read its error before anything else. The package's details flag some of these before any run: an unbuildable persist source appears in its warnings, and its build plan lists the sources a run will build. A `#@ persist` on a non-persistable source (a plain `extend` over `conn.table(...)`) fails every run that covers it, naming the source - so one bad annotation stops the whole package from building. Tag a `query_source` / `sql_select` instead, or remove the annotation.
2. **The declaring file is missing the persistence flag.** A file with no `persistence` flag, of its own or from a file it imports, is skipped without any warning: the run succeeds and its persist sources are simply absent from what was built. Add the flag to every file that declares a `#@ persist`.
3. **An unquoted persist name** - a bare `name=foo` **always** hard-stops the build/publish with `persist annotation name must be quoted`; use `name="foo"`. (If you got *no* error at all, it isn't this.)

**Isolation test** - add a trivial, self-contained persist source in its own file and rebuild:
```malloy
##! experimental.persistence
source: smoke_raw is my_conn.table('some_dataset.some_table')
#@ persist name="scratch_dataset.persist_smoke_test"
source: persist_smoke is smoke_raw -> { aggregate: n is count() }
```
- If **even this** doesn't build (after a real materialization run), the problem is package-wide: read the run's error. A `#@ persist` on a non-persistable source anywhere in the package fails the whole run and names it; otherwise check that the connection can write the target dataset.
- If the smoke source **does** build but your real one doesn't, your real source is the problem - its own file's flag, or the source itself (read the run's error for it).

Delete the smoke file and drop its table afterward.

## Persisting a `#(access_filter)`-gated source

A gated source **can** be persisted, but only on one tier and only in one shape, and the thing to be
careful about is not refused by anything - you have to decide it.

- **`storage=` and `#@ preaggregate` always refuse a gated source**, naming it. The build skips a refused
  source, records it on the run (`metadata.refusedSources`), and builds the rest of the package. A run fails
  on a refusal only when every authored source it targeted was refused, or when `sourceNames` named this one;
  a refused rollup never fails it. (`#@ persist storage=<name>` is the tier that materializes into a separate
  registered storage destination and serves from there, rather than building in the source's own connection;
  `#@ preaggregate` stores a rollup Publisher derives from a measure you annotated with a grain, rather than a
  source you wrote.) A rollup also groups *across* the gated column, so it could not be row-filtered afterwards
  even in principle.
- **A colocated `#@ persist` (no `storage=`) is admitted** when the gate is provably the entry point's
  **own row filter**. It is refused when the gate is reached only through a join, inherited from a base
  the compiler cannot attribute cleanly, or does not classify as a row filter at all. The gate is found
  through the import -> rename -> `query_source` chain, so a gate the persisted source did not declare
  itself still counts.

**What to be wary of.** Persisting does not weaken the gate: it changes only where rows are read FROM, and
the gate still runs live on every query as that query's own `WHERE`, so filtered rows come back filtered.
What freezes is the **column the gate filters on**. A row whose access decision changes - it changes
owner, say - keeps being served under its OLD decision until the next rebuild. That is a stale *access
decision*, not merely stale data, and nothing raises an error.

**None of this is needed for the gate to work.** It is enforced live on every query either way; what
needs a bound is how long a *stale* decision can survive. Of the three controls that look like that
bound, only the first is:

- **`materialization.freshness` `{ "window": "24h", "fallback": "live" }` is the bound.** The serve path
  re-checks freshness per query, so once the artifact ages past the window it drops out of the serving set
  and the query recomputes live, correctly filtered - whether or not a rebuild ever lands. Three details
  decide whether you actually get that. **`fallback` must be `live`**: under `stale_ok` a stale artifact
  keeps being served, which voids the bound, and window and fallback resolve *independently* per layer,
  so a package-level `stale_ok` silently defeats a window you set on the source. That is a statement about
  **layers**, which do not combine - not about siblings, below. Prefer the
  **per-source** spelling `#@ persist name="..." freshness.window="24h" freshness.fallback="live"` over
  the package-wide `materialization.freshness` key: the gated source is what needs the bound, and setting
  it package-wide forces every other persisted source to recompute once stale too. And **a
  content-identical sibling shares the artifact, so it shares the window**: reuse is keyed on the
  content-addressed `sourceEntityId`, which folds the connection and the SQL but *not* the source name, so
  two persist sources whose bodies compute the same SQL resolve to one table carrying one freshness
  policy. The tightest window any of them declares governs all of them - a sibling declaring nothing
  cannot loosen yours, and yours pulls that sibling's reads off the table once it lapses. A sibling's
  `stale_ok` cannot void your bound either: the fold keeps whichever fallback bounds staleness, so the
  layer rule above does not carry over here. If two sources need genuinely different windows, give them
  genuinely different SQL.

  Both of those are properties of the **host** that assembles the manifest, not of the annotation. Where
  the host does not fold, which sibling's policy reaches the wire is unspecified; and a host that folds at
  manifest-assembly time typically applies it when a version's manifest is next published rather than
  retroactively to manifests already distributed - so you can declare the window correctly and not have it
  in force yet.
- **A cron alone is not a bound.** A failed build or a stopped scheduler leaves the source serving its old
  decisions indefinitely. `freshness` and `schedule` are mutually exclusive; for a gated source, take the
  window.
- **`refresh="incremental"` does not bound revocation.** The delta only re-reads rows in
  `[covered_through, frontier)`, so a row that changes owner *without its watermark advancing* is never
  re-read - while the entry still reports an advancing `coveredThrough` and reads as healthy. Only a full
  rebuild recomputes the gating column.

**And the window only binds where the serving manifest carries it.** Freshness is enforced from fields a
control plane stamps onto the manifest it distributes; a Publisher that serves what it just built binds the
table with no `dataAsOf` and no window, and an entry carrying no window never ages out. So on a standalone
deployment the declared window is inert and the artifact serves until the next full rebuild - which leaves a
rebuild cadence you actually verify as the only bound, and makes leaving a revocation-sensitive source
unpersisted the safer call.

When recommending `#@ persist` on a gated source, pair it with a freshness window and say out loud what
staleness the author is accepting. A gated source with neither a window nor a full-rebuild cadence has no
bound on how long a revoked row keeps being served.

## Gotchas

- **Flag every file that declares a persist source** - a file with no `##! experimental.persistence`, of its own or from a file it imports, is skipped without a warning, so its persist sources are silently never built.
- **A `#@ persist` on a non-persistable source fails the run** - a plain `extend` over `conn.table(...)` is refused, and every run that covers it fails naming the source, so the rest of the package doesn't build either.
- **A tag doesn't build** - a standalone Publisher materializes only on an explicit run or its scheduler; only a hosted control plane builds on publish.
- **Quote the name** - a bare `name=` always hard-stops the build.
- **Republishing unchanged persist logic reuses the table** - reuse is keyed on the content-addressed `sourceEntityId`, not the `name=`.
- **Removing a persist source (or a smoke test) does not drop its table** - physical-table cleanup is the caller's responsibility; drop it yourself.
- **A `#(access_filter)`-gated source freezes its gating column when persisted** - the gate still runs live, but a revoked row keeps being served under its old access decision until the next rebuild. `storage=` and `#@ preaggregate` refuse a gated source outright. See **Persisting a `#(access_filter)`-gated source**.
