# Materialization (Malloy Persistence)

Materialization pre-builds a Malloy source into a physical table so queries read the table instead of recomputing the source. In Publisher it is driven by **Malloy Persistence**: you annotate a source `#@ persist`, and Publisher builds it, records a manifest, and serves queries from the built table.

This doc covers the open-source Publisher's materialization surface — how to declare it, the rules Publisher enforces, how to build on demand or on a schedule, the `malloy-pub` CLI, and how a self-hosted (standalone) deployment differs from a control-plane-driven (hosted) one.

## Declare what to persist

Annotate a source with `#@ persist` (the persistence experiment must be enabled in the model):

```malloy
##! experimental.persistence

source: raw_orders is duckdb.table('data/orders.csv')

#@ persist name="order_summary"
source: order_summary is raw_orders -> {
  group_by: category
  aggregate:
    total_orders is count()
    total_revenue is amount.sum()
}
```

`name=` is the physical table Publisher writes. Persist the sources that are expensive to compute and reused by many queries; leave cheap or rarely-read sources unpersisted. The [`malloy-materialization-tuning`](../skills/malloy-materialization-tuning/SKILL.md) skill helps decide.

### Opting a reader out: `#@ -persist`

A source that `extend`s a persisted source reads the persisted table too — the extension adds computed fields on top of the stored rows rather than recomputing them. Annotate the extension with `#@ -persist` when you want it recomputed live instead:

```malloy
#@ persist name="order_summary"
source: order_summary is raw_orders -> { … }

// Reads the stored order_summary table, adds a field on top.
source: summary_with_margin is order_summary extend {
  dimension: margin is total_revenue / total_orders
}

// Recomputed from raw on every query — never reads the stored table.
#@ -persist
source: summary_fresh is order_summary extend {
  dimension: margin is total_revenue / total_orders
}
```

Reach for it when a reader must not see stale rows, and remember what it costs: the opted-out source recomputes its whole upstream on every query, so it forgoes exactly the work persistence was there to save. It also keeps the extension from being materialized itself, which matters today because a plain extension of a persisted source is currently treated as a second build target for the same table.

## The persistence policy (the publish gate)

Package-level persistence policy lives at the root of `publisher.json`:

```json
{
  "name": "orders",
  "scope": "version",
  "materialization": { "schedule": "0 6 * * *" }
}
```

Publisher enforces these rules identically at **publish** (strict — rejected), at **PATCH** that edits the policy (strict), at **package load** (warn — the package still serves), and in the **scheduler** (an offending package is skipped):

1. **`scope` is a single package-level mode** — `package` (default) or `version`. There is no per-source scope or per-source schedule; those are declared at the package root, not on individual `#@ persist` sources.
   - `package`: persisted artifacts are reused across the package's published versions while they satisfy freshness.
   - `version`: each artifact is owned by one published version.
2. **A `schedule` cron requires `scope: version`.** A package-scoped lineage is reused across versions, so a single per-version cadence is meaningless.
3. **`schedule` and `freshness` are mutually exclusive.** Declare either a `schedule` (the power tier) or a `freshness` policy (the objective tier), never both.
4. **`schedule` must be a valid 5-field UNIX cron** (`minute hour day-of-month month day-of-week`), evaluated in **UTC**. Extensions (`L`, `W`, `#`, `?`) are rejected, so a garbage cron can't pass publish and then silently never fire.

## Build a materialization

A materialization run compiles the package, builds every `#@ persist` source into its table, writes a manifest, and loads it so queries serve from the built tables. It settles at `MANIFEST_FILE_READY` (success) or `FAILED` / `CANCELLED`.

Each run records a **trigger** in its metadata: `ON_DEMAND` (a manual/API build) or `SCHEDULER` (a scheduled fire). Only one materialization can be active per (environment, package) at a time — a second concurrent build is rejected with HTTP 409, and the scheduler coalesces (skips) rather than stacking a second build.

On demand, via the CLI:

```bash
malloy-pub materialize --environment <env> --package <pkg> --wait
```

## The standalone scheduler

A self-hosted Publisher can rebuild packages on their cron cadence with no control plane. The scheduler is **opt-in and off by default**:

```bash
PUBLISHER_LOCAL_MATERIALIZATION_SCHEDULER=true \
PUBLISHER_MATERIALIZATION_SCHEDULER_INTERVAL_MS=60000 \
  <start Publisher>
```

See [configuration.md](configuration.md) for the env vars. Fire semantics:

- **Sweeps only already-loaded packages.** It never forces a load; a not-yet-loaded package simply isn't scheduled until something else loads it.
- **Arms to the next occurrence.** On first sight it computes the next cron time (strictly future), so a freshly-scheduled package does **not** fire on the arming tick.
- **Recovers one missed occurrence across a restart.** On first arm after a (re)start it re-anchors from the newest recorded `SCHEDULER` run: if an occurrence came due while the process was down, it fires exactly one catch-up and then jumps forward, rather than skipping it. (A schedule set while the scheduler was _disabled_ has no prior run to anchor from, so it is not caught up on first enable.)
- **Skips what it must not fire:** a control-plane-driven package (one with a `manifestLocation`), a package whose policy is invalid, or one with an unparseable cron.
- **Caps fires per tick** (`…_MAX_FIRES_PER_TICK`) so a burst of due packages doesn't stampede; a capped package fires on a later tick.
- **Coalesces** when a build is already active for the package (the in-flight build covers the occurrence).

> **Never set `PUBLISHER_LOCAL_MATERIALIZATION_SCHEDULER` on a control-plane-driven (orchestrated) worker.** A package serving live under a control plane has no `manifestLocation`, so the per-package skip does not cover it — the flag being off is the primary guard against the standalone scheduler double-driving refresh the control plane already owns.

## Manage it with the CLI

```bash
# View / set / clear a package's schedule (set also sets scope: version)
malloy-pub schedule view  --environment <env> --package <pkg>
malloy-pub schedule set "0 6 * * *" --environment <env> --package <pkg>
malloy-pub schedule clear --environment <env> --package <pkg>

# List a package's runs (ID, Status, Trigger, Started, Completed, Error)
malloy-pub list materialization --environment <env> --package <pkg>

# List every package's runs across the environment (adds a leading Package column)
malloy-pub list materialization --environment <env>

# Inspect one run (timings, sourcesBuilt/sourcesReused, manifest entries)
malloy-pub get materialization <id> --environment <env> --package <pkg>

# Drop a materialization record and its physical tables
malloy-pub delete materialization <id> --environment <env> --package <pkg> --drop-tables
```

The `schedule` commands share the server's publish-gate validation: an invalid cron or an illegal scope/freshness combination is rejected, so a rejection means the change was unsafe.

> **`--drop-tables` drops _every_ physical table in that run's manifest**, not just one source's. Auto-run assigns stable table names and carries unchanged sources forward, so an old run's manifest names tables a newer manifest still serves. To remove a persisted source, drop the old run **first, then `materialize --wait`** so every still-persisted source is re-created; dropping a run whose tables the current serving manifest depends on, without rebuilding, breaks queries.

## Standalone vs. hosted (control-plane) deployments

The same package definition behaves differently depending on who drives materialization. Honest divergences to be aware of:

- **Who refreshes.** Standalone: the opt-in scheduler above. Hosted: the control plane drives refresh; the standalone scheduler stays off and skips control-plane-driven packages.
- **Tables-only vs. two-phase.** A standalone fire is a single-phase build that materializes persist sources into **tables**. A hosted deployment runs a two-phase job — tables, then a second pass over indexed dimensions — so index behavior is a hosted-only concern and can't be exercised locally.
- **Per-version tables.** `scope: version` is a policy contract. In the current standalone auto-run, a materialized table's identity is a content address derived from its connection and the source's canonical SQL (the `sourceEntityId`) — not the `#@ persist name` (that is the physical table name) and not the package version; true per-version tables are produced when the control plane assigns versioned build targets. Standalone is the right place to exercise the _policy_ (scope/schedule rules) and single-version builds, not per-version fan-out.
- **Physical-table GC.** Deleting a materialization with `--drop-tables` drops its tables. Deleting an environment or package removes the materialization **records only** — physical tables are intentionally left in place (physical-table GC is the caller's responsibility), so clean up tables you no longer want explicitly.

## Tune for cost and performance

The materialization history (`list` + `get` above) records per-run timings and how many sources were built vs. reused — enough to decide what to persist, what to stop persisting, and how to schedule it. The [`malloy-materialization-tuning`](../skills/malloy-materialization-tuning/SKILL.md) skill walks an agent through reading those signals and proposing (recommendations-only) changes.
