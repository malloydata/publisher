---
name: malloy-materialization-tuning
description: Optimize a package's Malloy Persistence materializations for cost and performance using the malloy-pub CLI and the materialization history. Recommend what to persist, what to stop persisting, and how to schedule/scope it. Use when the user asks to make a package cheaper or faster, tune persistence, decide what to materialize, or review persist/schedule choices.
---

# Tuning materializations for cost and performance

This skill turns the signals the open-source Publisher already records (the materialization history, per-run timings, and which sources were built vs reused) into concrete, **recommendations-only** advice: which sources to persist, which to stop persisting, and how to schedule and scope them. It is the local counterpart to the platform's usage-driven optimization: the Publisher has the raw signals, and you read them with the `malloy-pub` CLI.

> **Recommendations only. Never change a model, schedule, or scope without the user's explicit go-ahead.** Present the findings and the proposed edits, then apply them only when asked. Persisting the wrong source wastes storage and rebuild time; unpersisting a hot one makes queries slow. Let the user decide.

Assumes the `malloy-pub` CLI is on PATH and points at the server (`--url` or `MALLOY_PUBLISHER_URL`, default `http://localhost:4000`). Substitute the real environment and package for `<env>` / `<pkg>`.

## Step 1: Take inventory

Establish what the package persists today and how it is governed.

- **Persist sources:** the sources annotated `#@ persist name="…"` in the package's `.malloy` files. Read the models (or `malloy_getContext` the package) to list them.
- **Schedule + scope:**

  ```bash
  malloy-pub schedule view --environment <env> --package <pkg>
  ```

  This prints the cron (or `none`, meaning publish / on-demand only), the persist **scope** (`package` = artifacts reused across versions; `version` = per published version), and whether a freshness policy is set. A control-plane-managed package (manifestLocation set) is refreshed by the control plane, not the standalone scheduler, so leave its cadence alone.

## Step 2: Read the materialization history

The history is where cost lives. Each run records its trigger, timing, and how many sources were built vs reused.

- **Across the whole environment** (all packages, newest first; the rows are interleaved and labeled by package, not grouped into contiguous per-package blocks):

  ```bash
  malloy-pub list materialization --environment <env>
  ```

  Columns: Package, ID, Status, **Trigger** (`SCHEDULER` vs `ON_DEMAND`), Started, Completed, Error.

- **For one package:**

  ```bash
  malloy-pub list materialization --environment <env> --package <pkg>
  ```

- **A single run's detail** (the cost signals):

  ```bash
  malloy-pub get materialization <id> --environment <env> --package <pkg>
  ```

  In the JSON, read:
  - `metadata.durationMs`: how long the build took.
  - `metadata.sourcesBuilt` vs `metadata.sourcesReused`: how much work each run actually did. A run that is nearly all _reused_ is cheap; one that is nearly all _built_ every time is where cost accumulates.
  - `metadata.trigger`: `SCHEDULER` (a cron fired it) or `ON_DEMAND`.
  - `manifest.entries[*]`: the persisted sources: `sourceName`, `physicalTableName`, `realization`.

Look across several runs, not one: the pattern over the recent history (how often it rebuilds, how much it reuses, how long it takes) is the signal.

## Step 3: Analyze and recommend

Weigh rebuild cost against query benefit. Common findings:

- **Persist candidate:** an expensive, frequently-queried source that is _not_ persisted (recomputed on every query). Recommend adding `#@ persist name="…"`. Strongest when the source is a heavy aggregate/join reused by many queries and its inputs change slowly.
- **Removal candidate:** a persisted source that is cheap to compute, rarely queried, or rebuilt far more often than it is read. Recommend dropping the `#@ persist` annotation (and its table): the storage + rebuild cost is not buying anything.
- **Cadence mismatch:** a `SCHEDULER` cadence out of step with how fast the data changes or how long a build takes. If most scheduled runs are all-reused (nothing changed), the cron is too frequent, so loosen it. If queries routinely read stale data, tighten it. If the build's `durationMs` approaches the interval, the cadence is too aggressive.
- **Scope mismatch:** `scope: version` re-materializes per published version (right when versions must be isolated, e.g. a schedule); `scope: package` reuses one lineage across versions (cheaper when versions can share). A schedule _requires_ `version`. If a package carries a schedule it does not need, clearing the schedule frees it to use the cheaper package scope.

Frame each recommendation with the evidence from Step 2 (the run IDs, timings, built/reused counts) so the user can judge it.

## Step 4: Apply (only once approved)

- **Add a persist source:** add the `#@ persist` annotation in the `.malloy` file (use the modeling workflow to validate and reload), then rebuild so it is materialized:

  ```bash
  malloy-pub materialize --environment <env> --package <pkg> --wait
  ```

- **Remove a persist source:** delete the `#@ persist` annotation, then drop the old run's tables **before** rebuilding what remains:

  ```bash
  malloy-pub delete materialization <id> --environment <env> --package <pkg> --drop-tables
  malloy-pub materialize --environment <env> --package <pkg> --wait
  ```

  > `--drop-tables` drops **every** physical table in that run's manifest, not just the removed source's: auto-run assigns stable table names and carries unchanged sources forward, so an old run's manifest names tables a newer manifest still serves. Do the drop **first, then rebuild**: `materialize --wait` re-creates every source that is still persisted, ending in the desired state. Dropping a run whose tables the current serving manifest depends on, without an immediate rebuild, breaks queries (`Table ... does not exist`).

- **Change the schedule / cadence:**

  ```bash
  malloy-pub schedule set "0 6 * * *" --environment <env> --package <pkg>   # 5-field UTC cron
  malloy-pub schedule clear --environment <env> --package <pkg>
  ```

  `set` also sets `scope: version` (a schedule requires it); the server rejects an invalid cron or an illegal scope/freshness combination, so a rejection means the change was unsafe.

- **Change the scope:** `scope` is declared at the root of the package's `publisher.json` (`"scope": "package"` or `"version"`). Edit it there, then reload/republish the package. Remember a schedule pins scope to `version`, so clear the schedule first if moving to `package`.

After applying, re-run Step 2 on the next few builds to confirm the change did what you predicted (more reuse, shorter builds, or a table that is actually read).

## What this skill does not do

- It does not decide for the user or apply changes silently; every edit needs an explicit go-ahead.
- It cannot see per-query read counts (the open-source Publisher records build history, not query-level table usage), so "rarely queried" is a judgment from the model and the user's knowledge, not a measured metric. Say so when it matters.
