<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Moving from `#@ persist` to `#(materialize)`

> **In development.** `#(materialize)` is materialization v2's annotation. It is behind
> `PUBLISHER_MATERIALIZE` (`materialize.enabled`, default off) and is still being built
> ([the plan](materialization-v2-plan.md)). Leave the switch off outside development. Nothing on
> this page is required: every v1 annotation keeps working as [materialization.md](materialization.md)
> describes it, with the switch on or off.

This page maps each v1 materialization form to its v2 form, with before and after Malloy. Each
row says whether the v2 form works today. "Works" means it was checked against `main`, through
the build-file generator (`build_file_generator.ts`) and the served copy, with the switch on. The
rest is marked with the phase that brings it, so keep the v1 form until then.

## What is deprecated, and when

Moving to `#(materialize)` is optional. `#@ persist` itself is not deprecated: a v1 package keeps its
meaning, and `#@ persist` on a view stays the way to persist a view. Two v1 forms have no future in
v2:

| v1 form           | Why                                                                          | v2 form                                      |
| ----------------- | ---------------------------------------------------------------------------- | -------------------------------------------- |
| `#@ -persist`     | A materialized source serves every reader, so there is nothing to opt out of | None. [Rewrite it](#persist-opt-out)         |
| `#@ preaggregate` | Renamed onto the `#(…)` routes Publisher owns                                | [`#(preaggregate)`](#preaggregate) (phase 4) |

Both follow the Publisher v1 plan's rule for a deprecated form: a package warning through 0.x,
refused at 1.0.0, and the reader removed in 1.1. The warnings start in the release that ships v2's
annotations:

- `deprecated-persist-opt-out`, one per source that declares `#@ -persist`. Until that release it
  shows only with `PUBLISHER_MATERIALIZE` on.
- `deprecated-preaggregate-annotation`, one per measure that declares `#@ preaggregate`. **There is
  no warning yet**, even with the switch on: it is held back until `#(preaggregate)` is read
  (phase 4), so it never points at a spelling that does nothing. `#@ preaggregate` keeps working.

Each names the source (`subject`) and the file that declares it (`model`). A source that extends or
imports an annotated one is not reported again. Neither warning changes what the package builds or
serves: an opted-out reader still recomputes live, and a rollup still builds and serves.

## What changes when you move

`#(materialize)` stores the source you put it on, as written: every public dimension, under its own
name, computed or not, with the source's `where:` applied. v1 stores whatever the `#@ persist`
source's own query produces. The served copy then reads that table under the source's own name, so
its extensions, views, joins, named queries and importing files are unchanged
([the served copy](materialization.md#the-served-copy-in-development-publisher_materialize)).

- **No `##! experimental.persistence` in your file.** The flag lives in the build source Publisher
  generates. A file that also has v1 `#@ persist` sources still needs it.
- **One annotation per source.** A source that carries both `#(materialize)` and its own
  `#@ persist` is not built, and is reported as `materialize-not-built`.
- **A declaration Publisher cannot build serves live, with a warning.** It is never refused. The
  warning is `materialize-not-built`, and it names the reason.
- **`#(materialize)` takes only these keys:** `name`, `storage`, `refresh`, `watermark`,
  `merge_key`, `partition`, `freshness.*` and `queryMetadata.*`. Any other key, a misspelled
  `name=` for example, is a `materialize-not-built` warning rather than a default table. v1's
  `realization=` has no v2 form.

## Form by form

### `storage=`: build into a destination and serve from it

**Works**, with `PERSIST_STORAGE_MODE=on`. When a query is answered from the served copy, the path
report's `servedBy` carries `via: "served_copy"`.

```malloy
// v1
##! experimental.persistence
#@ persist name="east_orders" storage="lake"
source: east_orders is orders -> { select: * } extend {
  where: region = 'east'
}

// v2
#(materialize) storage="lake"
source: east_orders is orders extend {
  where: region = 'east'
}
```

`name=` is optional with `storage=`. Without one, the table takes the source's name. The destination
is Publisher's, and the existing checks refuse two sources that land on one table.

### Colocated: build into the source's own warehouse

**Builds today. Serving is coming in phase 3.** The build plan builds the table under your
source's name, but queries on the source are still computed live from its warehouse. Keep the v1
form for anything that needs to be served from its table now.

```malloy
// v1
##! experimental.persistence
#@ persist name="order_summary"
source: order_summary is raw_orders -> {
  group_by: category
  aggregate: total_revenue is amount.sum()
}

// v2: name= is required here
#(materialize) name="order_summary"
source: order_summary is raw_orders -> {
  group_by: category
  aggregate: total_revenue is amount.sum()
}
```

A colocated `#(materialize)` must name its table. Its default would be the source's own name, which
for a source over `conn.table('orders')` is often the very table it reads.

### `name=`

**Works**, under the same rules as v1: a plain identifier path, which may name a container
(`name="analytics.order_summary"`). Keep the same `name=` when you move, and the first build after
the move replaces the old table under the same name.

### A query-shaped source

**Works** with `storage=`. v2 stores the query's output rows, plus any dimension that the `extend`
after the query adds.

```malloy
// v1: stores region and total; big is computed on every read
#@ persist name="daily" storage="lake"
source: daily is ev -> { group_by: region; aggregate: total is amt.sum() } extend {
  dimension: big is total > 20
}

// v2: stores region, total and big
#(materialize) name="daily" storage="lake"
source: daily is ev -> { group_by: region; aggregate: total is amt.sum() } extend {
  dimension: big is total > 20
}
```

You do not have to move a query-shaped v1 source. With the switch on, a query-shaped `#@ persist
storage=` source is also re-rooted by the served copy as written.

A v1 `#@ persist` on a source declared only by refinement (`source: x is orders extend { … }`)
stores its root table's rows. `#(materialize)` on the same source stores the source's own rows, with
its `where:` already applied.

### `refresh="incremental"`, `watermark=`, `merge_key=`

**Accepted and carried; not yet verified end to end.** The keys are copied onto the generated build
source and checked by the [same rules](materialization.md#incremental-refresh), including the
dialect rule: Postgres, BigQuery and Snowflake only, so a DuckDB source must say `refresh="full"`.
Rolling windows (a current-time `where:`) with the incremental delete arrive later in phase 3.

```malloy
// v1
#@ persist name="daily_orders" refresh="incremental" watermark="order_date"
source: daily_orders is orders -> {
  group_by: order_date
  aggregate: revenue is amount.sum()
}

// v2
#(materialize) name="daily_orders" refresh="incremental" watermark="order_date"
source: daily_orders is orders -> {
  group_by: order_date
  aggregate: revenue is amount.sum()
}
```

The first build after the move starts the table again from a full build. The address moved (see
[Addresses](#addresses-what-moves-once)), so the recorded boundary no longer applies.

### Freshness, `partition=` and `queryMetadata`

**Works.** Each is copied onto the generated build source as written. A per-source freshness window
appears in the build plan, and the served copy drops a stale table per query, as v1 does.

```malloy
// v1
#@ persist name="orders_all" freshness.window="24h" partition="org_id" queryMetadata.team="ops"

// v2
#(materialize) name="orders_all" freshness.window="24h" partition="org_id" queryMetadata.team="ops"
```

The package-level `materialization` block in `publisher.json` (`schedule`, `freshness`, `scope`)
applies to `#(materialize)` sources as it does to v1 ones.

<a id="persist-opt-out"></a>

### `#@ -persist`

**Deprecated, and has no v2 form.** In v1 it opts a reader of a persisted source out of the table,
so the reader recomputes. In v2 a materialized source serves every use. To keep a reader live, base
the reader on a source with no annotation, and materialize a separate source that reads the same one.

```malloy
// v1
#@ persist name="order_summary" storage="lake"
source: order_summary is raw_orders -> { group_by: region; aggregate: total is amount.sum() }

#@ -persist
source: summary_fresh is order_summary extend { dimension: r2 is region }

// v2
source: summary_live is raw_orders -> { group_by: region; aggregate: total is amount.sum() }

#(materialize) name="order_summary" storage="lake"
source: order_summary is summary_live

source: summary_fresh is summary_live extend { dimension: r2 is region }
```

**Works**, with `storage=`: `order_summary` is answered from its table, and `summary_fresh` from the
warehouse. Callers keep the same source names.

<a id="preaggregate"></a>

### `#@ preaggregate`

**Deprecated. The v2 form is not available yet (phase 4).** `#@ preaggregate` keeps working, and
there is no deprecation warning for it yet; keep writing it until `#(preaggregate)` ships. The v2 spelling takes the grain as a list:

```malloy
// v1, works today
source: orders is duckdb.table('data/orders.parquet') extend {
  #@ preaggregate grain="category, order_day"
  measure: revenue is amount.sum()
}

// v2, phase 4: not read yet
source: orders is duckdb.table('data/orders.parquet') extend {
  #(preaggregate) grain=["category", "order_day"]
  measure: revenue is amount.sum()
}
```

Phase 4 settles the v2 forms of `namespace=` and `storage=`, rollups over a `storage=` base, and
inherited measures. A `#(preaggregate)` written today is not read.

### Not available yet

Each of these is a later pull request. The warning text names the reason when it applies.

| v2 form                                                                      | Status                                                              |
| ---------------------------------------------------------------------------- | ------------------------------------------------------------------- |
| Serving a colocated `#(materialize)` table                                   | Phase 3. It builds, and queries are computed live                   |
| `inline=[…]`: a join stored in the row                                       | Phase 3 (joins). Not built; `materialize-not-built`                 |
| A `where:` that reads a given (`$ORG_ID`), left out of the build             | Phase 3. Refused by the same gates as v1, so the source serves live |
| A current-time `where:` (a rolling window)                                   | Phase 3 (table behaviours)                                          |
| The `name=` view, and a materialized extension built from its parent's table | Phase 3                                                             |
| A dimension that depends on the query time zone, computed at read            | Phase 3. Today it is stored as the build computed it                |
| `#(preaggregate)`, and routing a persisted view (`#@ persist` on a view)     | Phase 4                                                             |
| Generations, as-of reads, and the control plane's promote and cleanup        | Phase 5                                                             |

## Addresses: what moves once

A table's content address (`sourceEntityId`) is a hash of its connection and the SQL that builds
it. The build plan, the manifest and reuse all key on it.

- **Moving a source to `#(materialize)` gives it a new address.** The build query is now the
  generated one (`src -> { select: … }`), not the v1 source's own SQL, even when the rows are the
  same. The first run after the move misses reuse and builds the table. An incremental source starts
  from a full build. Keep `name=` and the table is replaced in place.
- **P12, the storage destination in the address, is a separate move.** With
  `PUBLISHER_PERSIST_ADDRESS_INCLUDES_DESTINATION` on, every `storage=` table is re-addressed once
  ([configuration](configuration.md)). P2, which leaves a given-reading `where:` out of the build,
  ships with it and moves no existing address. Turn the switch on in the same rollout as your
  migration, so each `storage=` table rebuilds once and not twice. Make the first run after it a full
  one.

## What a control plane must do

- **Build plan and wire names.** A `#(materialize)` source appears under the author's source name
  and model path, and its `sourceID` is the author's. The generated build source (`<src>__mat`) and
  its companion model never reach the plan, a run's `sourceNames`, the manifest, warnings or the path
  report. Anchors keyed on (version, source name, model path) stay put.
- **Expect one rebuild per migrated source.** Its `sourceEntityId` changes, so reuse misses and an
  incremental source re-seeds. A table Publisher names itself (`name=`, or the source's name) is
  rebuilt in place. A table the host instructed under a generational name is the host's to reclaim.
- **Set the switch the same on every worker.** `PUBLISHER_MATERIALIZE` and
  `PUBLISHER_PERSIST_ADDRESS_INCLUDES_DESTINATION` are read at startup. Workers that disagree plan
  different sources or compute different addresses for one source.
- **Read warning codes as open strings.** This page adds `deprecated-persist-opt-out`, and reserves
  `deprecated-preaggregate-annotation` (not emitted until phase 4), beside `materialize-not-built` and
  `served-copy-not-served`. None is an error, and the package loads.
- **Later phases add more to watch.** The `name=` view is a new, comment-marked warehouse object, and
  the sweeper, the table dropper and package delete must recognize it. A rolling window adds a delete
  to the incremental ledger. Both arrive in phase 3.
  Generations and promote arrive in phase 5. See
  [the plan's control-plane contract](materialization-v2-plan.md#ownership).
