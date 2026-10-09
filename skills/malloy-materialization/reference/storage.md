<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# The storage tier: `storage=`, `partition=`, and how a stored table is served

By default a persist source is built **in its own connection** (colocated): the warehouse runs the query and the table lands beside the data. `#@ persist name="..." storage=<destination>` builds it into a **storage destination** the environment declares instead - today a DuckLake catalog over local or object storage - and serves it from there, so a query served from the table never runs in the warehouse at all.

```malloy
#@ persist name="daily_orders" storage=lake
source: daily_orders is orders -> { group_by: order_date; aggregate: total is amount.sum() }
```

- `storage=` names a **storage destination**, never a connection. Destinations are declared on the environment (`storageDestinations: [{ "name": "lake", "type": "ducklake", "ducklakeConnection": { "catalog": {...}, "storage": {...} } }]`, by config or by environment PATCH); a build naming one the environment does not hold fails the run with a `DestinationNotFoundError`. Reads of the environment report a destination's name and type only, never its config.
- The build is a native **query passthrough**: the source warehouse (Postgres, BigQuery or Snowflake) computes the query and only the result crosses into the store, through DuckDB. A source on a DuckDB connection is refused for `storage=` (there is nothing to push down). A `#(access_filter)`-gated source is always refused on this tier (`reference/access-filter.md`).
- The tier is gated by `PERSIST_STORAGE_MODE` (`off` / `write-only` / `on`). Under `off` a `storage=` source is **skipped by the build and served live, with no error** - it is never built colocated instead. Under `write-only` it builds but is still served live, and the package warns. If a `storage=` source "did nothing", check the mode before the model.
- A package may name several destinations; each source routes to its own.
- `partition=` requires `storage=` (refused as `partition_without_storage` otherwise) and every named column must be one of the source's public output columns.

## Where a given may sit

A persisted table is built once, with no caller present, so a `given` has to be somewhere the build never reads it.

- **Inside the persisted query - refused.** `scoped_rollup is orders -> { where: region = $REGION; group_by: ... }` has only the declaration default available at build time, so the table would hold one caller's slice and nothing at read would re-apply the filter. The build refuses it (`given_in_persisted_query`). The same applies when the given is *inherited*: a query over a source whose `extend` block carries the term reads that filter into its SQL, and is refused just the same - what decides is whether the **build** would substitute a value, not which line the author typed it on.
- **In the persist source's own `extend` block - honoured per caller.** `#@ persist ... storage=lake` on `fact is raw -> { select: * } extend { where: org_id = $ORG_ID }` strips the term from the build, stores every caller's rows in one table, and re-applies the term per caller at read. One artifact serves every tenant, and a term a caller writes on an extension of it still scopes what is read. Partition on the scoped column (`partition="org_id"`) so each caller's reads touch one partition.

A `#(access_filter)` gate is the same shape with a different source of truth; see `reference/access-filter.md`.

## `partition=`: laying the table out

`#@ persist ... storage=lake partition="org_id"` writes the stored table as one file set per distinct value, so an equality term on that column (`=`, `in`) reads only the files it names; `partition="org_id,day"` nests them in the order given. It is a **layout and carries no isolation**: every filter is re-applied at read whether or not its column is partitioned, so a partition list that omits the column a query filters by costs a full scan, never a wrong answer.

Use it for the column the reads filter by. A rollup carrying every key at a fine grain (every species, every tenant, every product) that each query filters to one key is read **whole** per query without a partition on that key - that is what makes a pre-aggregated table answer in seconds instead of milliseconds. Partition on the filter key and read cost tracks one partition while build cost tracks the whole table. Nothing checks the column's cardinality for you: a partition per value across very many values is the many-small-files case, and that judgement is yours.

`partition=` is part of the source's content address, so changing it rebuilds the table. A chained downstream of a partitioned parent reads the parent's table like any other.

## Shaping a rollup for its reads

A stored rollup pays at build time so that reads are cheap; the grain and the inputs decide whether they are.

- **Grain to the reads, not to the data.** Group by exactly the columns the queries filter or group by. Every extra dimension multiplies the row count, and a rollup that approaches the fact table's size saves nothing.
- **Bake the filters into the query.** A `where: is_valid = 1` inside the `-> { ... }` shrinks the table; the same clause in a trailing `extend { ... }` is re-applied at read and the table holds every row.
- **Give a ratio both halves.** A percentage whose numerator is a rollup and whose denominator is a count over the fact table still scans the fact per query. Persist the denominator at its own grain too.
- **List the columns a pass-through keeps.** `select: *` on a stored copy of a wide fact carries columns no reader uses; name the ones the downstream rollups read.

## How a stored table is served

A query against a package with bound `storage=` tables is compiled over a transient model in which each stored source is rebound to its table - a *virtual source* - under the `##!` flags of the files that declared the originals, with their `extend`-block refinements re-declared on top. A public wrapper over a stored source (`orders is _orders_fact -> { select: * }`, with or without `include { public: ... }`) is carried onto that shape verbatim and served from the table. A wrapper whose query or joins reach a source that is not materialized there - a warehouse table, or a stored source whose table is stale past its freshness window - is served **live** instead.

Routing is decided per query, not per source, so one source can serve some queries from its table and others live:

- **A join to a non-persisted source** serves live for the queries that traverse it; queries on the fact's own fields still route to the table. A join between two persisted sources routes both legs.
- **A nested column** (`nest:`, a repeated record) is stored, but the serve shape carries it as opaque JSON, so every query that traverses it pays the live cost while the scalar columns serve from storage. The source looks fully materialized and is, for its flat fields.
- **A refinement the shape cannot reproduce is thinned** (a view, then a join, then a dimension or measure reading a join); a **filter is never thinned** - a source whose filter the shape cannot reproduce loses the tier itself, and only itself.
- A **stale** table (past its freshness window, with `fallback` `live` or `fail`) leaves the serving set, and a run-time store failure under `fallback: live` degrades that query to live.

What the server reports:

- `QueryResult.servedFrom`: `storage` when the tier answered, `live_fallback` when the tier was bound but a run-time store failure degraded the query to a live recompute, and empty for everything else (a colocated hit, no binding, or an ineligible query - three different things).
- The package's `storageServeBindings` lists the stored tables the package is currently bound to, with each entry's origin (`persist` or `preaggregate`). `manifestEntryCount` does not count them.
- `publisher_storage_serve_routing_total{outcome=storage|live_fallback|runtime_live_fallback|blocked_by_row_level_gate, origin}` counts routed queries; `publisher_storage_serve_shape_tier_drop_total{tier}` counts refinements the serve shape had to thin.
- A query's full result carries the SQL it ran; served from the store, its `FROM` names the destination and the physical table.

## Pre-aggregation uses the same tier

`#@ preaggregate grain="category, order_day"` on a **measure** stores a rollup Publisher derives at that grain and routes to it when a query groups by a subset of the grain; queries do not change. It builds through the same plan, manifest and scheduler as `#@ persist`, needs no `##!` flag, inherits the base source's freshness, and refuses a gated source. A rollup on a measure the published surface hides is not built; the package loads and warns. See `docs/preaggregation.md` for grain choice.
