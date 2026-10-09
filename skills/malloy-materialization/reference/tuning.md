<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Tuning materializations for cost and performance

This skill turns the signals the open-source Publisher already records (the materialization history, per-run and per-source timings, which sources were built vs reused, and for the storage tier how queries were routed) into concrete, **recommendations-only** advice: which sources to persist, which to stop persisting, which to chain or partition, and how to schedule them. The Publisher has the raw signals, and you read them with the `malloy-pub` CLI.

> **Recommendations only. Never change a model, schedule, or scope without the user's explicit go-ahead.** Present the findings and the proposed edits, then apply them only when asked. Persisting the wrong source wastes storage and rebuild time; unpersisting a hot one makes queries slow. Let the user decide.

Assumes the `malloy-pub` CLI is on PATH and points at the server (`--url` or `MALLOY_PUBLISHER_URL`). Substitute the real environment and package for `<env>` / `<pkg>`.

## Step 1: Take inventory

Establish what the package persists today and how it is governed.

- **Persist sources:** the sources annotated `#@ persist name="..."` in the package's `.malloy` files, with their tier (`storage=` or not), `partition=`, and `refresh=`. Read the models (or `get_context` the package) to list them, and read the package's `buildPlan` for what a run will build and what it will refuse.
- **Schedule + scope:**

  ```bash
  malloy-pub schedule view --environment <env> --package <pkg>
  ```

  This prints the cron (or `none`, meaning on-demand only), the persist **scope** (`package` = artifacts reused across versions; `version` = per published version), and whether a freshness policy is set. It also says when a control plane manages the package (`manifestLocation` set): leave that package's cadence alone, the control plane refreshes it. And remember the standalone scheduler only runs with `PUBLISHER_LOCAL_MATERIALIZATION_SCHEDULER` set; a cron on a server without it is inert.

## Step 2: Read the materialization history

The history is where cost lives. Each run records its trigger, timing, and per source what it did.

- **For one package** (newest first; `--limit` / `--offset` page it):

  ```bash
  malloy-pub list materialization --environment <env> --package <pkg>
  ```

  Columns: ID, Status, **Trigger** (`SCHEDULER` vs `ON_DEMAND`), Started, Completed, Error.

- **A single run's detail** (the cost signals):

  ```bash
  malloy-pub get materialization <id> --environment <env> --package <pkg>
  ```

  In the JSON, read:
  - `metadata.durationMs`: how long the whole run took.
  - `metadata.sourcesBuilt` / `sourcesReused` / `sourcesFailed`: how much work the run did. A run that reused most sources was cheap. **A `SCHEDULER` run never reuses**: every scheduled fire is forced, so its `sourcesReused` is 0 by construction; judge a scheduled run by its duration and its per-source entries instead. An incremental source is counted as built even when its refresh applied nothing.
  - `metadata.sourcesRefused` and `metadata.refusedSources` (present only when a source was refused, and only on a run the publisher planned itself): sources the eligibility gate skipped. Each one serves live on every query, so a refused source the user expected to be persisted is a finding in its own right; its entry's `message` says what to change.
  - `manifest.entries` - a map keyed by `sourceEntityId`. Each entry carries `sourceName`, `physicalTableName`, `realization`, and the per-source cost signals: `buildDurationMs`; `queryCostBytes` where the warehouse reports it; `refresh: delta | full | none` for an incremental source (a `full` on an unchanged source every run is a source that never advances); `upstreamReuse: reused | recomputed` for a chained source (a `recomputed` with `upstreamRecomputeReason` is a chain that went back to the warehouse).
  - `manifest.failures`: sources the run lost while still finishing ready.

Look across several runs, not one: the pattern over the recent history (how often it rebuilds, how long each source takes, what reuses) is the signal.

- **For `storage=` tables, how queries were routed.** `publisher_storage_serve_routing_total{outcome, origin}` on the metrics endpoint counts queries answered from the store (`storage`) against ones that degraded (`live_fallback`, `runtime_live_fallback`), and each query response's `servedFrom` says it per query. Nothing equivalent exists for a table built in the source's own warehouse: there, "rarely queried" is a judgement from the model and the user's knowledge, not a measured metric. Say so when it matters.

## Step 3: Analyze and recommend

Weigh rebuild cost against query benefit. Common findings:

- **Persist candidate:** an expensive, frequently-queried source that is _not_ persisted (recomputed on every query). Recommend adding `#@ persist name="..."`. Strongest when the source is a heavy aggregate/join reused by many queries and its inputs change slowly. If the source carries a `#(access_filter)` gate, check which case it is first. A gate reached only through a `join_*`, or not classifying as a row filter, means the colocated persist is **refused** - the build skips it and it serves live - so do not recommend persisting it at all. Where the gate is the source's own row filter the persist is admitted, and then only recommend it alongside a freshness window (`freshness.fallback="live"`): the gating column freezes at build time, so without one a revoked row can be served under its old access decision indefinitely (see `access-filter.md`).
- **Removal candidate:** a persisted source that is cheap to compute, rarely queried, or rebuilt far more often than it is read. Recommend dropping the `#@ persist` annotation (and its table): the storage + rebuild cost is not buying anything.
- **Chain candidate:** a `storage=` rollup whose intermediates re-read the warehouse on every build while a stored root already holds the rows. Rewire the intermediates onto the stored root (declaration order, a distinct `-> { select: * }` intermediate, joins onto stored siblings - `chaining.md`), and read `upstreamReuse` on the next run to confirm the chain holds. Keep a self-join or a wide hash aggregate over a large fact **in the warehouse**: chained, it builds in the destination's engine under its memory bound.
- **Partition candidate:** a `storage=` rollup carrying every key at a fine grain that queries filter to one key - read whole per query. Recommend `partition="<key>"` on the filter column (`storage.md`), noting the cardinality.
- **Denominator candidate:** a ratio whose numerator is a rollup and whose denominator counts the fact table per query. Persist the denominator at its own grain.
- **Cadence mismatch:** a `SCHEDULER` cadence out of step with how fast the data changes or how long a build takes. If the build's `durationMs` approaches the interval, the cadence is too aggressive. If queries routinely read stale data, tighten it. For an incremental source, `refresh: delta` with small `buildDurationMs` per fire is the cheap steady state; `refresh: full` every fire means the boundary is being lost (read the reason).
- **Scope mismatch:** `scope: version` re-materializes per published version (right when versions must be isolated, e.g. a schedule); `scope: package` reuses one lineage across versions (cheaper when versions can share). A schedule _requires_ `version`. If a package carries a schedule it does not need, clearing it is step one; the scope itself is edited in `publisher.json` (the CLI does not move it back).

Frame each recommendation with the evidence from Step 2 (the run IDs, timings, built/reused counts, per-entry fields) so the user can judge it.

## Step 4: Apply (only once approved)

- **Add a persist source:** add the `#@ persist` annotation in the `.malloy` file (use the modeling workflow to validate and reload), then rebuild so it is materialized:

  ```bash
  malloy-pub materialize --environment <env> --package <pkg> --wait --timeout 900
  ```

  (`--wait` gives up after 120 s by default and exits non-zero while the build continues; raise `--timeout` for a real build.)

- **Remove a persist source:** delete the `#@ persist` annotation, rebuild, then drop the old run's tables:

  ```bash
  malloy-pub materialize --environment <env> --package <pkg> --wait
  malloy-pub delete materialization <old-id> --environment <env> --package <pkg> --drop-tables
  ```

  > `--drop-tables` drops the physical tables in that run's manifest **except any a still-ready run also names** - with the stable names an auto-run assigns, a table the new run carried forward survives the delete. Deleting a run also re-derives the package's serve bindings from the latest remaining ready run (or clears them to serve live), so a delete does not break queries; it can leave a table behind. A removed source's table is therefore dropped only once no ready run names it - delete the old runs that do, or drop it in the warehouse yourself.

- **Change the schedule / cadence:**

  ```bash
  malloy-pub schedule set "0 6 * * *" --environment <env> --package <pkg>   # 5-field UTC cron
  malloy-pub schedule clear --environment <env> --package <pkg>
  ```

  `set` also sets `scope: version` (a schedule requires it) and **replaces the whole `materialization` block, dropping an existing `freshness` policy**; the server rejects an invalid cron or an illegal scope/freshness combination, so a rejection means the change was unsafe. `clear` leaves `scope: version` in place.

- **Change the scope:** `scope` is declared in the package's `publisher.json` under `materialization` (`"scope": "package"` or `"version"`; the root-level form is deprecated but still read). Edit it there, then reload/republish the package. Remember a schedule pins scope to `version`, so clear the schedule first if moving to `package`.

After applying, re-run Step 2 on the next few builds to confirm the change did what you predicted (more reuse, shorter builds, a chain that reports `reused`, or a table that is actually read).

## What this skill does not do

- It does not decide for the user or apply changes silently; every edit needs an explicit go-ahead.
- It cannot measure reads of a table built in the source's own warehouse (only the storage tier reports routing), so "rarely queried" there is a judgment from the model and the user's knowledge, not a measured metric. Say so when it matters.
