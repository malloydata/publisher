<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Keeping a table current: freshness, schedule, incremental refresh

A persisted table is a snapshot. Three declarations govern when it is rebuilt and what a reader gets in the meantime; which of them does anything depends on who drives the builds.

## `freshness`: the objective tier

```malloy
#@ persist name="daily_orders" freshness.window="24h" freshness.fallback="live"
```

or package-wide in `publisher.json` under `materialization.freshness`, or per model file with `## materialization.freshness.window="24h"`. The three layers resolve **per field** - source, then model file, then package - so a source that declares only `window` takes its `fallback` from the package. That is how a package-level `stale_ok` silently defeats a window set on one source.

- `window` is how old a table may be before it is stale. `fallback` says what a query over a stale table does: `live` and `fail` both skip the table and compute live (`fail` is reserved to error later; today it behaves as `live`); `stale_ok` keeps serving the stale table.
- A **hosted control plane** enforces the objective: it refreshes the table to stay within the window and stamps each manifest entry with `dataAsOf` and the window, and the serve path re-checks freshness **per query**, so a table that ages past its window drops out of the serving set whether or not a rebuild ever lands.
- A **standalone** Publisher does not act on it: it binds what it just built with no `dataAsOf` and no window, and an entry carrying no window never ages out. The declaration is inert there unless something binds the manifest through the API with those fields set. For a standalone server, the only refresh is a run or the scheduler below.
- Two sources whose bodies compile to the same SQL share one table, and a host that folds their policies keeps the tightest window and the bounding fallback for both. If two sources need different windows, give them different SQL.

## `schedule`: the power tier

```jsonc
{ "name": "orders", "materialization": { "scope": "version", "schedule": "0 6 * * *" } }
```

- A 5-field UTC cron; `L`, `W`, `#`, `?` are rejected at publish. It **requires `scope: "version"`** and is **mutually exclusive with `freshness`** - including a per-source `freshness.*`.
- It fires only on a standalone Publisher whose scheduler is enabled with `PUBLISHER_LOCAL_MATERIALIZATION_SCHEDULER`; without that, a schedule is inert. The scheduler sweeps loaded packages, skips one whose policy is invalid or that a control plane manages (`manifestLocation` set), fires at most a bounded number per tick, fires once to catch up after downtime, and skips a fire while a run is already active.
- **Every scheduled fire is a forced run** (`forceRefresh: true`): every source is rebuilt whether or not its content address changed. A scheduled run therefore never reports reused sources, and a cron that is too frequent shows as build time, not as reuse.
- `malloy-pub schedule set "<cron>"` writes the schedule and sets `scope: version` in one PATCH; it **replaces the whole `materialization` block**, dropping an existing `freshness` policy (it says so). `schedule clear` removes the cron and leaves `scope: version` in place; moving back to `package` scope is an edit to `publisher.json`.

## `refresh="incremental"`: bounded deltas

```malloy
#@ persist name="daily_orders" refresh="incremental" watermark="order_date"
source: daily_orders is orders -> { group_by: order_date; aggregate: revenue is amount.sum() }
```

- `watermark=` names one of the source's own output columns - real, orderable, non-aggregate. Publisher records how far the table is materialized (`covered_through`) and each refresh recomputes only `[covered_through, frontier)`, half-open, so the frontier value itself is left for the next run. A `date`/`timestamp` watermark takes the run's start time as the frontier; a numeric or string one reads `max(watermark)` from the source. Two refreshes within the same day of a date-watermarked source produce an empty range and skip - correct, and it looks like nothing happened.
- Add `merge_key="col,..."` when a row can be **restated** with a new watermark value; the delta is then applied as a `MERGE` on that identity instead of a delete-and-reinsert of the range.
- **An invalid declaration fails the package load**, not just the publish: `watermark=` without `refresh="incremental"`, `merge_key=` without a watermark, a watermark that names no materialized column or an aggregate, a `calculate:` field, or an unsupported dialect. Postgres, BigQuery and Snowflake sources only (the dialect that has to express the range); a DuckDB source has to say `refresh="full"`.
- **`forceRefresh` never re-seeds**; it only defeats skip-if-unchanged, and an incremental source is exempt from that anyway. Ask for a full rebuild with `reseed` (`malloy-pub materialize --reseed`, or per source on the build instruction). Keeping them apart is what lets a schedule drive deltas at all, since every scheduled fire is forced.
- Each manifest entry reports what the run **did**: `refresh: delta | full | none`, with `ledger.coveredThrough` as the boundary now in force. Every fallback - no recorded boundary, a boundary measured on a different table or watermark, drifted columns, `MERGE` on a Postgres older than 15 - rebuilds in full and succeeds, so a source quietly rebuilding on every run looks exactly like one advancing unless you read `refresh`. The reason codes (`forced`, `no_boundary`, `lineage_changed`, `table_renamed`, `merge_unsupported`, `table_unreadable`, `table_emptied`, `shape_mismatch`, `ledger_unreadable`, `chained_storage`, `frontier_unreadable`, `not_advanced`) say why.
- Two sources that compile to identical SQL share one table **and one boundary**; if their incremental declarations differ, neither ever advances. Publisher warns and names both.
- `storage=` is supported: the warehouse computes the bounded range and the DML lands in the destination. A **chained** stored source always rebuilds in full (`chained_storage`): see `reference/chaining.md`.
- The boundary lives in the publisher's own per-process store. A host that spreads a package's refreshes over several workers holds the ledger itself: it stores each entry's `ledger` object and sends them back as `buildInstructions.ledger` on the next run. Standalone, the store is the publisher's, always.

## Which refresh a gated source needs

A `#(access_filter)`-gated source that is persisted freezes the column its gate filters on, so its "refresh" is also the bound on how long a revoked row keeps being served. Only a freshness window with `fallback: live` on a hosted deployment bounds that; a cron alone does not, and an incremental delta never re-reads a row whose watermark did not move. See `reference/access-filter.md`.
