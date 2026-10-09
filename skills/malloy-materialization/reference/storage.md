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
- The tier is gated by `PERSIST_STORAGE_MODE` (`off` / `write-only` / `on`). Under `off` a `storage=` source is **skipped by the build and served live, with no error** - it is never built colocated instead. If a `storage=` source "did nothing", check the mode before the model.
- `partition=` requires `storage=` (refused as `partition_without_storage` otherwise) and every named column must be one of the source's public output columns.

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

What the server reports:

- `QueryResult.servedFrom`: `storage` when the tier answered, `live_fallback` when the tier was bound but a run-time store failure degraded the query to a live recompute, and empty for everything else (a colocated hit, no binding, or an ineligible query - three different things).
- The package's `storageServeBindings` lists the stored tables the package is currently bound to, with each entry's origin (`persist` or `preaggregate`). `manifestEntryCount` does not count them.
- `publisher_storage_serve_routing_total{outcome=storage|live_fallback|runtime_live_fallback|blocked_by_row_level_gate, origin}` counts routed queries; `publisher_storage_serve_shape_tier_drop_total{tier}` counts refinements the serve shape had to thin.
- A query's full result carries the SQL it ran; served from the store, its `FROM` names the destination and the physical table.

## Pre-aggregation uses the same tier

`#@ preaggregate grain="category, order_day"` on a **measure** stores a rollup Publisher derives at that grain and routes to it when a query groups by a subset of the grain; queries do not change. It builds through the same plan, manifest and scheduler as `#@ persist`, needs no `##!` flag, inherits the base source's freshness, and refuses a gated source. See `docs/preaggregation.md` for grain choice.
