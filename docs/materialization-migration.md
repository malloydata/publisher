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
meaning, and `#@ persist` on a view stays the way to persist a view. `#@ -persist` is not deprecated
either: it is Malloy core's own opt-out, which Malloy's persistence documentation teaches, and
Publisher keeps honouring it under `#(materialize)` too ([below](#persist-opt-out)). One v1 form has
no future in v2:

| v1 form           | Why                                           | v2 form                                      |
| ----------------- | --------------------------------------------- | -------------------------------------------- |
| `#@ preaggregate` | Renamed onto the `#(…)` routes Publisher owns | [`#(preaggregate)`](#preaggregate) (phase 4) |

It follows the Publisher v1 plan's rule for a deprecated form: a package warning through 0.x,
refused at 1.0.0, and the reader removed in 1.1. Its warning, `deprecated-preaggregate-annotation`,
comes one per measure that declares `#@ preaggregate`, naming the source (`subject`) and the file
that declares it (`model`). It starts for everyone in the release that ships v2's annotations; until
then it shows only with `PUBLISHER_MATERIALIZE` on, where `#(preaggregate)` is read, built and
served. A source that extends or imports an annotated one is not reported again. The warning does
not change what the package builds or serves: `#@ preaggregate` keeps working, and a rollup still
builds and serves.

## What changes when you move

`#(materialize)` stores the source you put it on, as written: every public dimension, under its own
name, computed or not, with the source's `where:` applied. v1 stores whatever the `#@ persist`
source's own query produces. The served copy then reads that table under the source's own name, so
its extensions, views, joins, named queries and importing files are unchanged
([the served copy](materialization.md#the-served-copy-in-development-publisher_materialize)).

- **No `##! experimental.persistence` in your file** for a `#(materialize)` source. The flag lives
  in the build source Publisher generates. A file that also has v1 `#@ persist` sources still needs
  it: a v1 file without the flag is skipped by the build, as before, until that requirement (D3) is
  removed after colocated tables move onto the served copy.
- **An extension does not inherit `#(materialize)`.** Only the source whose own statement carries it
  is built. A source that extends or renames it is not a second table: it reads the materialized
  source's table through the served copy, as its own text says.
- **One annotation per source.** A source that carries both `#(materialize)` and its own
  `#@ persist` is not built, and is reported as `materialize-not-built`.
- **A declaration Publisher cannot build serves live, with a warning.** It is not refused at
  publish. The warning is `materialize-not-built`, and it names the reason. The exception is a
  declaration that cannot be read as written (below): it is not built either, and a publish is
  refused.
- **`#(materialize)` takes only these keys:** `name`, `storage`, `refresh`, `watermark`,
  `merge_key`, `partition`, `freshness.*`, `queryMetadata.*`, and `inline` (v2 only; [joins](#joins-reference-by-default-inline-to-store-a-join_one)).
  Any other key, a misspelled `name=` for example, is malformed rather than a default table: the
  source is not built, the package warning is `materialize-unknown-key` at severity `error`, naming
  the key it was likely meant to be, and a publish is refused. A package already published keeps
  loading with the warning. A `#(materialize)` line that does not parse (`name="t` with no closing
  quote) is malformed the same way, as `materialize-unparseable`. v1's `realization=` has no v2
  form. A misspelled route, such as `#(materialise)`, is an `annotation-near-miss` warning.

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

**Works.** A `#(materialize)` source without `storage=` is built into its own warehouse, and the
served copy answers it from that table on the source's own connection, with everything it does for
`storage=` (stored computed dimensions, `inline=`, private and internal fields, terms re-applied at
read, extension tables, gates). It needs `PUBLISHER_MATERIALIZE` only, not a storage tier. A column
whose type the served copy cannot declare as stored (a `timestamptz`, a `json` or driver-specific
type, or a nest outside DuckDB) leaves that source answered live.

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
A [rolling window](#a-rolling-window-a-current-time-where) adds a delete below the moving cut-off
to each incremental refresh.

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

### A `where:` that reads a given

**Works**, with `storage=`. The given-reading term is left out of the build and applied at read
with each caller's value, as a v1 `#@ persist storage=` source does when the term is in its
`extend`. v2 needs no query shape for it.

```malloy
// v1
##! experimental { persistence givens }
given: ORG_ID :: number is 1
#@ persist name="scoped" storage="lake"
source: scoped is raw -> { select: * } extend { where: org_id = $ORG_ID }

// v2
##! experimental { givens }
given: ORG_ID :: number is 1
#(materialize) storage="lake"
source: scoped is raw extend { where: org_id = $ORG_ID }
```

The gates v1 applies to a given still apply, because the generated build source is judged like any
`#@ persist` source. A dimension that reads a given is left out of the build and computed at read, and
a given-reading `where:` on a base the source extends in the same file is left out of the build and
re-applied at read too, unlike v1, which refuses a given there
([materialization.md](materialization.md#the-served-copy-in-development-publisher_materialize)).

### A rolling window: a current-time `where:`

**Works**, v2 only. A `where:` that keeps the last stretch of time (`ts >= now - 30 days`) builds as
of the build and is re-applied at every read at the read's own clock. Other shapes that read `now`
leave the source unbuilt, and it serves live. The rules are in
[materialization.md](materialization.md#the-served-copy-in-development-publisher_materialize).

```malloy
#(materialize) storage="lake" refresh="full"
source: recent_orders is orders extend {
  where: order_date >= now - 30 days
}
```

### Joins: reference by default, `inline=` to store a join_one

**Works**, v2 only. A `#(materialize)` table holds the source's own rows, and its joins read the
joined source at query time, from that source's table when it is materialized in the same
destination. `inline=["j"]` on a join_one also stores what reads through `j` as `j__<field>`
columns, after checking the join key is unique. A Malloy query through `j` still reads the join, so
it is answered live unless the joined source is stored in the same destination.

```malloy
#(materialize) storage="lake" inline=["regions"]
source: orders_wide is orders extend {
  join_one: regions with region_id
}
```

### A source with a row-level gate

**Behind its own switch, off by default.** With `PUBLISHER_MATERIALIZE_GATED_SOURCES` on as well, a
`#(materialize) storage=` source whose own `#(authorize)` or `#(access_filter)` compiles to a row
filter is built and served from the served copy with the gate grafted on. Off, it is refused as a
gated `#@ persist storage=` source is, and serves live. It stays off until a security review of
storing gated sources' columns in the managed tier.

<a id="persist-opt-out"></a>

### `#@ -persist`

**Supported, with no rewrite.** `#@ -persist` is Malloy core's opt-out: on a reader of a persisted
source, it makes the reader recompute rather than read the table. Keep it when you move the source
it reads to `#(materialize)`: a materialized source serves every reader except one that opts out
this way. The opted-out reader, and any source built on it, recomputes live, and the served copy
declines it (its path report's skip carries the code `persist-opt-out`).

```malloy
// v1
#@ persist name="order_summary" storage="lake"
source: order_summary is raw_orders -> { group_by: region; aggregate: total is amount.sum() }

#@ -persist
source: summary_fresh is order_summary extend { dimension: r2 is region }

// v2: only the persisted source's annotation changes
#(materialize) name="order_summary" storage="lake"
source: order_summary is raw_orders -> { group_by: region; aggregate: total is amount.sum() }

#@ -persist
source: summary_fresh is order_summary extend { dimension: r2 is region }
```

**Works**, with `storage=`: `order_summary` is answered from its table, and `summary_fresh` from the
warehouse.

<a id="preaggregate"></a>

### `#@ preaggregate`

**Deprecated.** With `PUBLISHER_MATERIALIZE` on, write `#(preaggregate)` on the measure; `#@ preaggregate`
keeps working, with a deprecation warning while the switch is on. The v2 spelling takes the grain as a list:

```malloy
// v1, works today
source: orders is duckdb.table('data/orders.parquet') extend {
  #@ preaggregate grain="category, order_day"
  measure: revenue is amount.sum()
}

// v2, with PUBLISHER_MATERIALIZE on
source: orders is duckdb.table('data/orders.parquet') extend {
  #(preaggregate) grain=["category", "order_day"]
  measure: revenue is amount.sum()
}
```

`#(preaggregate)` keeps `namespace=` and `storage=`, builds a rollup over a `storage=` base, and
belongs to the source that writes it, so an extension or an import does not repeat it. With the
switch off it is not read. [preaggregation.md](preaggregation.md#preaggregate-materialization-v2-behind-publisher_materialize)
lists where it differs from `#@ preaggregate`.

### Not available yet

Each of these is a later pull request. The warning text names the reason when it applies.

| v2 form                                                                        | Status                                                               |
| ------------------------------------------------------------------------------ | -------------------------------------------------------------------- |
| `inline=` on a join_many or join_cross                                         | Phase 4. Not built; `materialize-not-built`                          |
| The `name=` view, and a materialized extension built from its parent's table   | Phase 3                                                              |
| A stored computed dimension under a query's own `timezone:`                    | Answered live today, since the build computed it in the default zone |
| Routing a persisted view (`#@ persist` on a view), or a named view to a rollup | Phase 4                                                              |
| Generations, as-of reads, and the control plane's promote and cleanup          | Phase 5                                                              |

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
- **Read warning codes as open strings.** This page adds
  `deprecated-preaggregate-annotation` (with the switch on), beside `materialize-not-built`,
  `served-copy-not-served`, `materialization-destination-missing` and
  `materialization-connection-cannot-hold-table`. None is an error, and the package loads.
- **A rolling window deletes as well as appends.** An incremental refresh of a source with a
  rolling window deletes the rows below the new cut-off in the same transaction.
- **Later phases add more to watch.** The `name=` view (phase 3) is a new, comment-marked warehouse
  object, and the sweeper, the table dropper and package delete must recognize it. Generations and
  promote arrive in phase 5. See
  [the plan's control-plane contract](materialization-v2-plan.md#ownership).
