<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# The storage tier: `storage=`, and how a stored table is served

By default a persist source is built **in its own connection** (database): the warehouse runs the query and the table lands beside the data. `#@ persist name="..." storage=<destination>` builds it into a **storage destination** the environment declares instead - today a DuckLake catalog over local or object storage - and serves it from there, so a query served from the table never runs in the warehouse at all.

```malloy
#@ persist name="daily_orders" storage=lake
source: daily_orders is orders -> { group_by: order_date; aggregate: total is amount.sum() }
```

- `storage=` names a **storage destination**, never a connection. Destinations are declared on the environment (`storageDestinations: [{ "name": "lake", "type": "ducklake", "ducklakeConnection": { "catalog": {...}, "storage": {...} } }]`, by config or by environment PATCH); a build naming one the environment does not hold fails the run with a `DestinationNotFoundError`. Reads of the environment report a destination's name and type only, never its config.
- The build is a native **query passthrough**: the source warehouse (Postgres, BigQuery or Snowflake) computes the query and only the result crosses into the store, through DuckDB. A source on any other connection, the package's DuckDB sandbox (`duckdb.sql(...)`, `duckdb.table(...)`) and an environment DuckDB connection included, is refused at load: it is not built, it is answered live, and the route's `*-not-built` warning (reason `storage_source_unsupported`) names the connection. Drop `storage=` to build it in its own connection, or move the source to a warehouse connection. A DuckLake source builds only into its own catalog. A `#(access_filter)`-gated source is always refused on this tier (`reference/access-filter.md`).
- The tier is gated by `PERSIST_STORAGE_MODE` (`off` / `write-only` / `on`). Under `off` a `storage=` source is **skipped by the build and served live, with no error** - it is never built in the database tier instead. Under `write-only` it builds but is still served live, and the package warns (`materialization-storage-mode`). If a `storage=` source "did nothing", check the mode before the model.
- **Hidden columns are stored.** A `storage=` table holds every column its source declares, `private:` and `internal:` ones included (a renamed one under the name its query gives it; an `except:`-ed one not at all, unless another source with identical SQL shares the table and stores it - a fix is coming), and the served copy hides them again: a measure over a hidden column is answered from the table, and a caller is refused a hidden column in live's words. The values are at rest in the destination, readable by anyone with direct access to it, never through Publisher. To keep a value out of the destination, leave it out of the source's query, do not just hide it. `#(carries)` checks what a caller can reach, not what a table stores.
- A package may name several destinations; each source routes to its own.
- `partition=` is not supported: a stored table is written unpartitioned, and a package whose `#@ persist` or `#(materialize)` carries `partition=` fails the publish and the load (`persist-retired-key`, `materialize-retired-key`). Remove the key; it was a layout only, so the cost is scan time and no answer changes.

## Where a given may sit

A persisted table is built once, with no caller present, so a `given` has to be somewhere the build never reads it.

- **Inside the persisted query - refused.** `scoped_rollup is orders -> { where: region = $REGION; group_by: ... }` has only the declaration default available at build time, so the table would hold one caller's slice and nothing at read would re-apply the filter. The build refuses it (`given_in_persisted_query`). The same applies when the given is *inherited*: a query over a source whose `extend` block carries the term reads that filter into its SQL, and is refused just the same - what decides is whether the **build** would substitute a value, not which line the author typed it on.
- **In the persist source's own `extend` block - honoured per caller.** `#@ persist ... storage=lake` on `fact is raw -> { select: * } extend { where: org_id = $ORG_ID }` strips the term from the build, stores every caller's rows in one table, and re-applies the term per caller at read. One artifact serves every tenant, and a term a caller writes on an extension of it still scopes what is read.
- **Inherited from a base - refused, answered live.** `#@ persist` (or `#(materialize)`) on `r is scoped extend { ... }`, where `scoped is raw extend { where: org_id = $ORG_ID }`, is not built: only the persisted source's own declaration is re-applied over its table at read. It is refused (`inherited_where_with_given` for `#@ persist`, a `materialize-not-built` warning for `#(materialize)`) and answers live, correctly but at live speed. The same holds for a given-reading dimension a base declares. To serve it from a table, move the given-reading `where:` onto the persisted source itself, and let extensions that persist nothing read its table.

A `#(access_filter)` gate is the same shape with a different source of truth; see `reference/access-filter.md`.

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
- **A nested column** (`nest:`, a repeated record) a destination cannot hold as one is left out of the stored source, so every query that traverses it answers live while the scalar columns serve from storage. The source looks fully materialized and is, for its flat fields.
- **The served copy reads the author's own text** over the stored table (its views, joins, measures and filters as written), so nothing is thinned: a source whose re-root does not compile is left as written and answers live, with a `served-copy-not-served` warning, and the model's other stored sources still serve.
- A **stale** table (past its freshness window, with `fallback` `live` or `fail`) leaves the serving set, and a run-time store failure under `fallback: live` degrades that query to live.

What the server reports:

- `QueryResult.servedFrom`: `storage` when the tier answered, `database` when a table in the source's own database did, `live_fallback` when the tier was bound but a run-time store failure degraded the query to a live recompute, and empty when no table answered (no binding, or an ineligible query; the path report says which).
- The package's `storageServeBindings`, in its serving state at `GET /api/v0/environments/{env}/packages/{pkg}/status` (an older server: its entry in `GET /api/v0/status`, or the package read), lists the stored tables the package is currently bound to, with each entry's origin (`persist` or `preaggregate`). `manifestEntryCount` does not count them.
- `publisher_query_route_total{leg, outcome, reason}` counts each leg the router considered (`used`, `skipped` with the path report's reason, `fallback` when a stored run failed and live answered), and each query's path report names the table that answered (`servedBy.via: served_copy`) or why each one was skipped.
- A query's full result carries the SQL it ran; served from the store, its `FROM` names the destination and the physical table.

## Pre-aggregation uses the same tier

`#@ preaggregate grain="category, order_day"` on a **measure** stores a rollup Publisher derives at that grain and routes to it when a query groups by a subset of the grain; queries do not change. It builds through the same plan, manifest and scheduler as `#@ persist`, needs no `##!` flag, inherits the base source's freshness, and refuses a gated source. A rollup on a measure the published surface hides is not built; the package loads and warns. A hidden (`private` or `internal`) grain dimension is stored and stays hidden; a `where:` of the source's own that reads a given over grain dimensions is left out of the build and re-applied per caller at read; and a query that sets its own `timezone:` but reads no date or timestamp is answered from the rollup. See `docs/preaggregation.md` for grain choice. On a server with `PUBLISHER_MATERIALIZE` on, write it `#(preaggregate) grain=["category", "order_day"]` instead; `#@ preaggregate` is deprecated there.
