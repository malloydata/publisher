<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# The storage tier: `storage=`, and how a stored table is served

By default a materialized source is built **in its own connection** (database): the warehouse runs the build and the table lands beside the data. `#(materialize) storage="<destination>"` builds it into a **storage destination** the environment declares instead - today a DuckLake catalog over local or object storage - and serves it from there, so a query served from the table never runs in the warehouse at all.

```malloy
#(materialize) name="daily_orders" storage="lake"
source: daily_orders is orders -> { group_by: order_date; aggregate: total is amount.sum() }
```

- `storage=` names a **storage destination**, never a connection. Destinations are declared on the environment (`storageDestinations: [{ "name": "lake", "type": "ducklake", "ducklakeConnection": { "catalog": {...}, "storage": {...} } }]`, by config or by environment PATCH); a build naming one the environment does not hold fails the run with a `DestinationNotFoundError`. Reads of the environment report a destination's name and type only, never its config.
- The build is a native **query passthrough**: the source warehouse (Postgres, BigQuery or Snowflake) computes the source and only the result crosses into the store, through DuckDB. A source on any other connection, the package's DuckDB sandbox (`duckdb.sql(...)`, `duckdb.table(...)`) and an environment DuckDB connection included, is refused at load: it is not built, it is answered live, and the route's `*-not-built` warning (reason `storage_source_unsupported`) names the connection. Drop `storage=` to build it in its own connection, or move the source to a warehouse connection. A DuckLake source builds only into its own catalog. A `#(access_filter)`-gated source is always refused on this tier (`reference/access-filter.md`).
- **A source already in the destination's own catalog is the exception to the passthrough** (the access-filter refusal still applies): a DuckLake connection whose catalog is the destination's, matched not by name but by the catalog database's `host`, `port` and `databaseName` (a `connectionString` config never matches), its metadata schema, and its data path when both configs state one (`docs/ducklake.md`). Its build runs inside the catalog with no passthrough. That session can write the catalog, so it runs only SQL Malloy generated: a source with `.sql()` or a `sql_*` expression anywhere beneath it is refused, as is a table read from a file or URL: refused at load (`materialize-not-built`, reason `storage_source_unsupported`) and answered live, never left to fail its build. On a connection configured `managed` this holds without `storage=` too, since a managed catalog's tables are always built inside it. It uses the DuckDB dialect, so `refresh="incremental"` is refused on it, as on any DuckDB source.
- The tier is gated by `PERSIST_STORAGE_MODE` (`off` / `write-only` / `on`). Under `off` a `storage=` source is **skipped by the build and served live, with no error** - it is never built in the database tier instead. Under `write-only` it builds but is still served live, and the package warns (`materialization-storage-mode`). If a `storage=` source "did nothing", check the mode before the model.
- **Hidden columns are stored.** A `storage=` table holds every column its source declares, `private:` and `internal:` ones included (an `except:`-ed one not at all), and the served copy hides them again: a measure over a hidden column is answered from the table, and a caller is refused a hidden column in live's words. The values are at rest in the destination, readable by anyone with direct access to it, never through Publisher. To keep a value out of the destination, leave it out of the source (`except:` it, or select around it), do not just hide it. `#(carries)` checks what a caller can reach, not what a table stores.
- A package may name several destinations; each source routes to its own.
- `partition=` is not supported: a stored table is written unpartitioned, and a package whose `#(materialize)` carries `partition=` fails the publish and the load (`materialize-retired-key`). Remove the key; it was a layout only, so the cost is scan time and no answer changes.

## Where a given may sit

A materialized table is built once, with no caller present, so a `given` has to be somewhere the build never reads it.

- **In the materialized source's own `where:` - honoured per caller.** `#(materialize) storage="lake"` on `fact is raw extend { where: org_id = $ORG_ID }` leaves the term out of the build, stores every caller's rows in one table, and re-applies the term per caller at read. One table serves every tenant, and a term a caller writes on an extension of it still scopes what is read. A dimension of its own that reads a given is not stored; the served copy computes it at read.
- **Inherited from a base - refused, answered live.** `#(materialize)` on `r is scoped extend { ... }`, where `scoped is raw extend { where: org_id = $ORG_ID }`, is not built: only the materialized source's own declaration is re-applied over its table at read. It is refused with a `materialize-not-built` warning and answers live, correctly but at live speed. The same holds for a given-reading dimension a base declares. To serve it from a table, move the given-reading `where:` onto the materialized source itself, and let extensions that materialize nothing read its table. An extension that declares its own `#(materialize)` over such a parent is refused too (`upstream_table_unusable`), and its queries are answered from the parent's table.
- **Inside a persisted view's or named query's result - refused.** A view or query whose result reads a given differs per caller, so it is not built (`reads_given`, `persist-view-not-built` / `persist-query-not-built`).

A `#(access_filter)` gate is the same shape with a different source of truth; see `reference/access-filter.md`.

## Shaping a rollup for its reads

A stored rollup pays at build time so that reads are cheap; the grain and the inputs decide whether they are.

- **Grain to the reads, not to the data.** Group by exactly the columns the queries filter or group by. Every extra dimension multiplies the row count, and a rollup that approaches the fact table's size saves nothing.
- **Bake the filters into the source.** A `where: is_valid = 1` on the materialized source shrinks the table: its `where:` is applied by the build. A term that reads a given is left out of the build and re-applied at read, and a rolling time window is re-applied at every read.
- **Give a ratio both halves.** A percentage whose numerator is a rollup and whose denominator is a count over the fact table still scans the fact per query. Materialize the denominator at its own grain too.
- **Keep only the columns readers use.** A stored copy of a wide fact carries every column it declares; `except:` the ones no reader uses.

## How a stored table is served

A query against a package with bound tables is compiled over the **served copy**: your own files, held in memory, with each materialized source's definition replaced by its stored table under the same name. Its `extend` blocks, annotations and every other line are your text, so an extension, a join to it, a view, a named query or a source in a file that imports it reads the stored table with no change to how it is written. A reader whose query or joins reach a source that is not materialized there - a warehouse table, or a stored source whose table is stale past its freshness window - is served **live** instead.

Routing is decided per query, not per source, so one source can serve some queries from its table and others live:

- **A join to a source not stored with it** serves live for the queries that traverse it; queries on the fact's own fields still route to the table. A join between two sources stored in one destination routes both legs.
- **A nested column** (`nest:`, a repeated record) a destination cannot hold as one is left out of the stored source, so every query that traverses it answers live while the scalar columns serve from storage. The source looks fully materialized and is, for its flat fields.
- **The served copy reads the author's own text** over the stored table (its views, joins, measures and filters as written), so nothing is thinned: a source whose re-root does not compile is left as written and answers live, with a `served-copy-not-served` warning, and the model's other stored sources still serve.
- A **stale** table (past its freshness window, with `fallback` `live` or `fail`) leaves the serving set, and a run-time store failure under `fallback: live` degrades that query to live.

What the server reports:

- `QueryResult.servedFrom`: `storage` when the tier answered, `database` when a table in the source's own database did, `live_fallback` when the tier was bound but a run-time store failure degraded the query to a live recompute, and empty when no table answered (no binding, or an ineligible query; the path report says which).
- The package's `storageServeBindings`, in its serving state at `GET /api/v0/environments/{env}/packages/{pkg}/status` (an older server: its entry in `GET /api/v0/status`, or the package read), lists the stored tables the package is currently bound to, with each entry's origin. `manifestEntryCount` does not count them.
- `publisher_query_route_total{leg, outcome, reason}` counts each leg the router considered (`used`, `skipped` with the path report's reason, `fallback` when a stored run failed and live answered), and each query's path report names the table that answered (`servedBy.via: served_copy`) or why each one was skipped.
- A query's full result carries the SQL it ran; served from the store, its `FROM` names the destination and the physical table.

## Rollups use the same tier

`#(rollup) grain=["category", "order_day"]` on a **measure** stores a rollup Publisher derives at that grain and routes to it when a query groups by a subset of the grain; queries do not change. Write one line per grain. It builds through the same plan, manifest and scheduler as `#(materialize)`, inherits the base source's freshness, and refuses a gated source unless its gate holds on the rollup's rows (`reference/access-filter.md`). A rollup on a measure the published surface hides is not built; the package loads and warns. A hidden (`private` or `internal`) grain dimension is stored and stays hidden; a `where:` of the source's own that reads a given over grain dimensions is left out of the build and re-applied per caller at read; and a query that sets its own `timezone:` but reads no date or timestamp is answered from the rollup. See `docs/preaggregation.md` for grain choice.
