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
- A **standalone** run stamps the same fields itself: each table it builds carries the declared window and fallback and a `dataAsOf` of the run's start (or of an older stored table it was built from), so the serve path ages it exactly as it ages a hosted table. Its next run rebuilds a table past its window instead of carrying it forward; nothing rebuilds it the moment the window passes, so trigger runs yourself (the package `schedule` excludes `freshness`). A window is a positive whole number and a unit (`90s`, `30m`, `24h`, `7d`, `2w`) and a fallback is `live`, `stale_ok` or `fail`; anything else fails the load on every mechanism. A table is never fresher than what it read: a rebuilt windowed table rebuilds a carried upstream of unknown age, and takes an older upstream's `dataAsOf`.
- A **rollup** takes the same keys on `#(rollup)` or `#@ preaggregate`, and otherwise its base's own declaration, then the model file's and the package's, so it goes stale with its base. Measures of one rollup share one freshness; two that disagree, an unknown key in the group, or a window that does not read as above fail the load.
- Two sources whose bodies compile to the same SQL, with the same `name=` or none, share one table, and a host that folds their policies keeps the tightest window and the bounding fallback for both. If two sources need different windows, give them different SQL or different `name=`s.

## `schedule`: the power tier

```jsonc
{
  "name": "orders",
  "materialization": { "scope": "version", "schedule": "0 6 * * *" },
}
```

- A 5-field UTC cron; `L`, `W`, `#`, `?` are rejected at publish. It **requires `scope: "version"`** and is **mutually exclusive with `freshness`** - including a per-source `freshness.*`.
- It fires only on a standalone Publisher whose scheduler is enabled with `PUBLISHER_LOCAL_MATERIALIZATION_SCHEDULER`; without that, a schedule is inert. The scheduler sweeps loaded packages, skips one whose policy is invalid or that a control plane manages (`manifestLocation` set), fires at most a bounded number per tick, fires once to catch up after downtime, and skips a fire while a run is already active.
- **Every scheduled fire is a forced run** (`forceRefresh: true`): every source is rebuilt whether or not its content address changed. A scheduled run therefore never reports reused sources, and a cron that is too frequent shows as build time, not as reuse.
- `malloy-pub schedule set "<cron>"` writes the schedule and sets `scope: version` in one PATCH; it **replaces the whole `materialization` block**, dropping an existing `freshness` policy (it says so). `schedule clear` removes the cron and leaves `scope: version` in place; moving back to `package` scope is an edit to `publisher.json`. A published version (by default, any package outside a watched environment) refuses both with 409 `PACKAGE_IS_VERSIONED`: set the schedule in its `publisher.json` and publish a new version.

## `refresh="incremental"`: bounded deltas

```malloy
#@ persist name="daily_orders" refresh="incremental" watermark="order_date"
source: daily_orders is orders -> { group_by: order_date; aggregate: revenue is amount.sum() }
```

- `watermark=` names one of the source's own output columns - real, orderable, non-aggregate. Publisher records how far each refresh reached (`covered_through`) and the next recomputes only `[start, frontier)`, half-open, so the frontier value itself is left for the next run. A `date`/`timestamp` watermark takes the run's start time as the frontier and **starts at the newest watermark the stored table holds** (one `max()` over the table), so a row stamped after the newest stored row is read however late it loads; it never starts at the previous run's start time, which would skip such a row for good. The start is inclusive: a merge or a range replace re-reads the newest stored instant without writing it twice. A numeric or string watermark reads `max(watermark)` from the source as its frontier, and the next range starts there; two refreshes of one with no new rows skip - correct, and it looks like nothing happened.
- **Without `merge_key=`, a refresh replaces the whole range it re-reads**, from the newest stored row (on a `date` watermark, the whole newest stored day): a stored row the source no longer holds there is removed. A source that drops or trims recent rows (a staging table keeping only the latest day) notices; declare `merge_key=` for append-only behaviour, since a merge never deletes.
- **Late rows: `lookback="2 days"`.** There is no lookback unless declared. A row stamped _below_ the newest stored row, loaded after it, is below every later delta. `lookback=` starts each delta that much further back (`[newest stored − lookback, frontier)`), and the merge key matches the rows read again. A positive whole number and one unit, `seconds`/`minutes`/`hours`/`days`/`weeks` spelled out or as `s`/`m`/`h`/`d`/`w` (`"36h"`), at most 365 days; no month or year. It needs a `date` or `timestamp` watermark **and** `merge_key=`; anything else fails the load (`persist-invalid` on `#@ persist`, `materialize-invalid` on `#(materialize)`). Size it to how late rows really arrive: each day of lookback is re-read every refresh.
- **Missed rows are reported.** After each delta, one check counts the source's rows in the window just below where the delta started (as wide as the delta's own range, capped at 7 days or the lookback, whichever is longer) that the table lacks. Any found are on the manifest entry as `lateRows` (`count`, `from`, `to`) and on the package as a `persist-incremental-late-rows` warning: the served table undercounts. Both are carried, with later findings added, until the source is rebuilt in full. Recover with one `reseed`, then add or widen `lookback=`. The count is a lower bound (a slice is checked once, so a row landing in an already-checked slice is not counted), and the report stays until a full rebuild even after a widened lookback has written the rows. A row older than the window is not seen; pair tables, string watermarks and `storage=` sources on BigQuery are not checked; the check gives up after 60 seconds and never fails the run.
- **A zoned timestamp watermark (`timestamptz`) in a session time zone other than UTC** (set by the connection's setupSQL) reads back with that zone's offset, so the source rebuilds in full each refresh, logged at `warn`. A plain `timestamp` or `date` watermark is not affected.
- **An invalid declaration fails the package load**, not just the publish: `watermark=` without `refresh="incremental"`, `merge_key=` without a watermark, a watermark that names no materialized column or an aggregate, or a `lookback=` that is not a duration, is over 365 days, lacks `merge_key=`, or sits on a number or string watermark (`persist-invalid`; `materialize-invalid` on `#(materialize)`). (A `calculate:` field or an unsupported dialect is valid but refuses the source alone.) Postgres, BigQuery and Snowflake sources only (the dialect that has to express the range); a DuckDB source has to say `refresh="full"`.
- **`forceRefresh` never re-seeds**; it only defeats skip-if-unchanged, and an incremental source is exempt from that anyway. Ask for a full rebuild with `reseed` (`malloy-pub materialize --reseed`, or per source on the build instruction). Keeping them apart is what lets a schedule drive deltas at all, since every scheduled fire is forced.
- Each manifest entry reports what the run **did**: `refresh: delta | full | none`, with `ledger.coveredThrough` as the boundary now in force. Every fallback - no recorded boundary, a boundary measured on a different table or watermark, drifted columns, `MERGE` on a Postgres older than 15 - rebuilds in full and succeeds, so a source quietly rebuilding on every run looks exactly like one advancing unless you read `refresh`. The reason codes (`forced`, `no_boundary`, `lineage_changed`, `table_renamed`, `merge_unsupported`, `table_unreadable`, `table_emptied`, `shape_mismatch`, `ledger_unreadable`, `chained_storage`, `merge_key_not_unique`, `upstream_generation_skipped`, `upstream_watermark_differs`, `frontier_unreadable`, `not_advanced`) say why.
- **An upstream column widened after the build** (INTEGER to BIGINT, REAL to DOUBLE PRECISION) is invisible to Malloy's types, so before each delta the delta's column types are compared with the stored ones; a stored column that cannot hold the delta's values exactly rebuilds the source in full (`shape_mismatch`). Read for a Postgres, DuckDB or MotherDuck database table and a `storage=` table with a Postgres source; BigQuery cannot widen (one integer type, one float type); Snowflake is not checked.
- **`merge_key=` must be unique in the source.** A merge cannot store two rows under one key, so a delta that reads a key twice is rolled back and the source rebuilt in full (`merge_key_not_unique`; on Snowflake, found by a probe after the delta, which makes the next refresh rebuild), and a rebuilt table that holds a key twice records no boundary, so it is rebuilt in full on every refresh until the key is unique. Both raise a `persist-incremental-merge-key-not-unique` warning naming the source; fix the key (or drop `merge_key=`) and the next rebuild clears it. The warning is held in memory: after a restart it reappears on the next refresh. A key repeated *across* refreshes is not caught: a later delta's row replaces the stored one, as a restatement, so a source whose key is not a row's identity over time (a log of versions) keeps one row per key where live has several.
- Two sources that compile to identical SQL, with the same `name=` or none, share one table **and one boundary**; if their incremental declarations differ, neither ever advances. Publisher warns and names both.
- `storage=` is supported: the warehouse computes the bounded range and the DML lands in the destination. A **chained** stored source always rebuilds in full (`chained_storage`): see `reference/chaining.md`.
- The boundary lives in the publisher's own per-process store. A host that spreads a package's refreshes over several workers holds the ledger itself: it stores each entry's `ledger` object and sends them back as `buildInstructions.ledger` on the next run. Standalone, the store is the publisher's, always.

## Which refresh a gated source needs

A `#(access_filter)`-gated source that is persisted freezes the column its gate filters on, so its "refresh" is also the bound on how long a revoked row keeps being served. Only a freshness window with `fallback: live` on a hosted deployment bounds that; a cron alone does not, and an incremental delta never re-reads a row whose watermark did not move. See `reference/access-filter.md`.
