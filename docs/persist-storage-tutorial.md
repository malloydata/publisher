# Tutorial: materialize a source into DuckLake and serve it back

Malloy Publisher can materialize a `#@ persist` source into a **store you choose**
— a registered DuckDB or DuckLake connection — instead of the source's own
warehouse, and then serve queries against that source **straight from the
materialized table**, cross-dialect, with no changes to your model. The query
you already run keeps working; behind it, the rows now come from the
materialized table instead of re-scanning the warehouse.

This walkthrough takes you end to end on your own machine: register a warehouse
source and a DuckLake you create, materialize a rollup into it, and query it —
watching each step through the server's status and logs. It also shows the
`PERSIST_STORAGE_MODE` deployment switch and the safety behaviors (fallback,
eligibility refusals) so you can see exactly what the feature does and doesn't
do. Every step here was run against a real server; the outputs shown are real.

> **Terminology.** `storage=` is the authoring keyword on the persist
> annotation. The default (no `storage=`) is a **colocated** materialization: the
> source materializes into and serves from its own warehouse, unchanged. This
> tutorial is about **external** materialization — materialize into a _separate_
> DuckDB/DuckLake store and serve from there.

---

## 0. What you'll build

- A **Postgres** database with one table (`orders`) — your "warehouse" source.
  The build pushes the compiled query to the source warehouse via a native
  passthrough; supported source types are `postgres`, `bigquery`, and
  `snowflake`. Postgres is the easiest to run locally.
- A **DuckLake** connection — a catalog (a Postgres database) plus a local data
  directory — that you create and materialize into. (A cloud deployment would
  point `bucketUrl` at `s3://`/`gs://`; locally a filesystem path is enough and
  no object-storage secret is created. Publisher disables DuckLake's small-table
  inlining on materialization writes, so data always lands as Parquet in the
  data directory, not inside the catalog database.)
- A tiny package with a rollup source `daily_orders` annotated
  `#@ persist name="daily_orders" storage=lake`.

One Postgres container does double duty: it holds the source `orders` table
**and** the DuckLake catalog (two separate databases).

Prerequisites: a clone of this repo, `docker`, `curl`, and `jq`.

```bash
bun install
bun run build          # bakes the DuckDB extensions the build/serve path needs
```

The REST API is at `http://localhost:4000/api/v0`. Everything here also works
over the MCP endpoint (`malloy_executeQuery` / `malloy_reloadPackage`); REST is
used so every step is a copy-pasteable `curl` you can inspect.

---

## 1. Start Postgres and seed the source table

```bash
docker run -d --name publisher-tutorial-pg \
  -e POSTGRES_PASSWORD=tutorial -e POSTGRES_USER=tutorial -e POSTGRES_DB=tutorial \
  -p 5432:5432 postgres:16

# wait for readiness
until docker exec publisher-tutorial-pg pg_isready -U tutorial -d tutorial; do sleep 1; done

docker exec -i publisher-tutorial-pg psql -U tutorial -d tutorial <<'SQL'
CREATE TABLE orders (order_id int, order_date date, region text, amount numeric);
INSERT INTO orders VALUES
  (1, DATE '2026-01-01', 'US', 100), (2, DATE '2026-01-01', 'US', 50),
  (3, DATE '2026-01-02', 'EU', 200), (4, DATE '2026-01-02', 'US', 25);
SQL

# a separate database for the DuckLake catalog metadata
docker exec -i publisher-tutorial-pg psql -U tutorial -d tutorial -c "CREATE DATABASE ducklake_catalog;"

mkdir -p /tmp/publisher-tutorial-lake     # local DuckLake data directory
```

---

## 2. Start the server (feature off — the safe default)

`PERSIST_STORAGE_MODE` is off by default. We'll turn it up in stages; changing
it means restarting the server (it's read at startup).

```bash
PERSIST_STORAGE_MODE=off bun run start        # REST :4000, MCP :4040
curl -s http://localhost:4000/api/v0/status | jq -r .operationalState   # -> "serving"
```

A clone serves the bundled `examples` environment; we'll add our connections and
package to it.

---

## 3. Register the two connections

`POST /environments/{env}/connections/{name}` — the name is in the path, the
body carries the type-specific config. (The response is a success message.)

```bash
# Postgres source
curl -s -X POST http://localhost:4000/api/v0/environments/examples/connections/orders_pg \
  -H 'content-type: application/json' -d '{
    "name":"orders_pg","type":"postgres",
    "postgresConnection":{"host":"localhost","port":5432,"databaseName":"tutorial","userName":"tutorial","password":"tutorial"}
  }'

# DuckLake destination: catalog = the ducklake_catalog DB, storage = a local dir
curl -s -X POST http://localhost:4000/api/v0/environments/examples/connections/lake \
  -H 'content-type: application/json' -d '{
    "name":"lake","type":"ducklake",
    "ducklakeConnection":{
      "catalog":{"postgresConnection":{"host":"localhost","port":5432,"databaseName":"ducklake_catalog","userName":"tutorial","password":"tutorial"}},
      "storage":{"bucketUrl":"/tmp/publisher-tutorial-lake"}
    }
  }'

curl -s http://localhost:4000/api/v0/environments/examples/connections | jq '[.[].name]'
# -> [ ..., "orders_pg", "lake" ]
```

> **Reserved name.** A connection can't be named `source` — that's the reserved
> `storage=source` sentinel ("materialize in the warehouse" — colocated). The server
> rejects it at registration.

---

## 4. Author the package

The `examples` environment serves packages from its on-disk directory
(`packages/server/publisher_data/examples/` in a clone — the server logs the
environment's `location` at startup). Create the package there, then register it.

```bash
ENVDIR=packages/server/publisher_data/examples
mkdir -p "$ENVDIR/persist-tutorial"

cat > "$ENVDIR/persist-tutorial/publisher.json" <<'JSON'
{ "name": "persist-tutorial", "version": "1.0.0", "description": "storage= materialization tutorial" }
JSON

cat > "$ENVDIR/persist-tutorial/orders.malloy" <<'MALLOY'
##! experimental.persistence

source: orders is orders_pg.table('public.orders')

// A daily rollup, materialized into the `lake` DuckLake and served from there.
// `name=` is the logical table name; `storage=` picks the destination.
#@ persist name="daily_orders" storage=lake
source: daily_orders is orders -> {
  group_by: order_date
  aggregate:
    order_count is count()
    total_amount is amount.sum()
}
MALLOY

# Register the package (connections must already exist so the source compiles).
curl -s -X POST http://localhost:4000/api/v0/environments/examples/packages \
  -H 'content-type: application/json' -d '{"name":"persist-tutorial"}' \
  | jq '{name, sources: (.buildPlan.sources|keys|map(split("@")[0])), warnings}'
```

Because the server is in `off` mode, the persist plan compiles but the
`storage=` annotation is reported as ignored:

```json
{
  "name": "persist-tutorial",
  "sources": ["daily_orders"],
  "warnings": [
    {
      "model": "orders.malloy",
      "target": "daily_orders",
      "message": "declares storage=\"lake\" but PERSIST_STORAGE_MODE is off; the annotation is ignored and the source is served live from its own warehouse."
    }
  ]
}
```

That's the kill switch working: with the feature off the package loads and
serves normally — the `storage=` intent is reported, not acted on.

> After editing a model later, re-read it without a restart with
> `GET …/packages/persist-tutorial?reload=true`.

---

## 5. Mode 1 — `write-only`: materialize and inspect the table

Stop the server (Ctrl-C) and restart in `write-only`: the build runs (the rollup
lands in DuckLake) but the serve path still runs live. Connections and the
package persist across the restart.

```bash
PERSIST_STORAGE_MODE=write-only bun run start
```

Trigger an auto-run build (the publisher plans and builds every persist source):

```bash
curl -s -X POST http://localhost:4000/api/v0/environments/examples/packages/persist-tutorial/materializations \
  -H 'content-type: application/json' -d '{}' | jq '{id, status}'
# -> { "id": "...", "status": "PENDING" }
```

Poll until `MANIFEST_FILE_READY` and inspect the manifest entry — note the
destination connection and the **authoritative schema** captured from the built
table:

```bash
MZID=$(curl -s http://localhost:4000/api/v0/environments/examples/packages/persist-tutorial/materializations | jq -r '.[0].id')
curl -s http://localhost:4000/api/v0/environments/examples/packages/persist-tutorial/materializations/$MZID \
  | jq '{status, entry: (.manifest.entries|to_entries[0].value|{sourceName, storageConnectionName, physicalTableName, schema})}'
```

```json
{
  "status": "MANIFEST_FILE_READY",
  "entry": {
    "sourceName": "daily_orders",
    "storageConnectionName": "lake",
    "physicalTableName": "daily_orders",
    "schema": [
      { "name": "order_date", "type": "DATE" },
      { "name": "order_count", "type": "BIGINT" },
      { "name": "total_amount", "type": "DOUBLE" }
    ]
  }
}
```

The `physicalTableName` is your `name=` **verbatim** — `daily_orders` — exactly
as the in-warehouse path names its tables. The auto-run server assigns no
generational or hashed suffix of its own; a rebuild replaces this one table in
place (see [Where your data lands](#where-your-data-lands) below). Publisher
always reads and writes this exact name (it's recorded here in the manifest and
echoed into the serve binding); you query the source by its Malloy name as
always. Assigning distinct physical names per generation — for immutable
generations, safe schema evolution, or rollback — is the responsibility of a
caller that owns physical naming and distributes bindings (the orchestrated
build path, where the caller supplies `physicalTableName` per build and
distributes serve bindings via `manifestLocation`).

### Where your data lands

Because you own this lake, it's worth knowing exactly where the rows go. The
source lands in a table named by your `name=` verbatim: `<schema>.<table>`, where
`schema.table` comes from `name=` (schema defaults to `main`). So
`name="daily_orders" storage=lake` lands at **`lake.main.daily_orders`**.
`name="analytics.daily"` would write to schema `analytics` (which must already
exist — Publisher writes into it but does not create it; provision schemas
yourself, e.g. a one-off `CREATE SCHEMA analytics` over an attached session).

A rebuild rewrites this same table with an atomic `CREATE OR REPLACE` — DuckLake's
catalog swap is transactional, so the replace is atomic and no stale table is
left behind. (There is no separate convenience view and no coexisting
generations; the table _is_ the logical name.)

The rows land as **Parquet** in your data directory (Publisher disables
DuckLake's small-table inlining on writes, so materialized data always goes to
object storage rather than into the catalog database):

```bash
find /tmp/publisher-tutorial-lake -name '*.parquet'
# .../publisher-tutorial-lake/main/daily_orders/ducklake-<uuid>.parquet
```

Query it directly with the DuckDB CLI (`brew install duckdb`, or see
duckdb.org/docs/installation) — attach your lake and list what's there: the
one base table at your `name=`.

```bash
duckdb -c "
  INSTALL ducklake; LOAD ducklake; INSTALL postgres; LOAD postgres;
  ATTACH 'ducklake:postgres:host=localhost port=5432 dbname=ducklake_catalog user=tutorial password=tutorial'
    AS lake (DATA_PATH '/tmp/publisher-tutorial-lake', READ_ONLY);
  SELECT table_name, table_type FROM information_schema.tables WHERE table_catalog='lake';
  -- daily_orders                      BASE TABLE
  SELECT * FROM lake.main.daily_orders ORDER BY order_date;
"
# ┌────────────┬─────────────┬──────────────┐
# │ order_date │ order_count │ total_amount │
# ├────────────┼─────────────┼──────────────┤
# │ 2026-01-01 │           2 │        150.0 │
# │ 2026-01-02 │           2 │        225.0 │
# └────────────┴─────────────┴──────────────┘
```

Now query the source through Publisher. In `write-only` it's still served
**live** from Postgres (`/status` carries a `write-only` warning). Query results
come back on the `result` field; with `compactJson:true` that field is a JSON
string of row objects:

```bash
curl -s -X POST http://localhost:4000/api/v0/environments/examples/packages/persist-tutorial/models/orders.malloy/query \
  -H 'content-type: application/json' \
  -d '{ "query":"run: daily_orders -> { aggregate: t is total_amount.sum() }", "compactJson":true }' \
  | jq -r '.result' | jq '.'
# -> [ { "t": 375 } ]     (computed live in Postgres)
```

---

## 6. Mode 2 — `on`: serve from the materialized table

Restart in `on`, then rebuild so the running server binds the serve path:

```bash
PERSIST_STORAGE_MODE=on bun run start

curl -s -X POST http://localhost:4000/api/v0/environments/examples/packages/persist-tutorial/materializations \
  -H 'content-type: application/json' -d '{"forceRefresh": true}' >/dev/null
# (wait for MANIFEST_FILE_READY as in step 5)
```

Confirm the source is bound for storage serve — the warning is gone and
`storageServeBindings` shows the routing:

```bash
curl -s http://localhost:4000/api/v0/environments/examples/packages/persist-tutorial \
  | jq '{storageServeBindings, warnings}'
```

```json
{
  "storageServeBindings": [
    {
      "sourceName": "daily_orders",
      "storageConnectionName": "lake",
      "tablePath": "lake.daily_orders"
    }
  ],
  "warnings": null
}
```

The `tablePath` is the exact table the manifest recorded (your `name=`,
destination-qualified) — the serve path binds to it directly.

Query it — the answer now comes from the DuckLake table, served cross-dialect:

```bash
curl -s -X POST http://localhost:4000/api/v0/environments/examples/packages/persist-tutorial/models/orders.malloy/query \
  -H 'content-type: application/json' \
  -d '{ "query":"run: daily_orders -> { aggregate: t is total_amount.sum() }", "compactJson":true }' \
  | jq -r '.result' | jq '.'
# -> [ { "t": 375 } ]
```

and the server log confirms the route:

```
info: Serving query from storage tier (virtual-source) { modelPath: "orders.malloy", storageSources: ["daily_orders"] }
```

### Prove it's really the materialized table

Change the underlying Postgres data **without** rebuilding, then query again —
the answer is unchanged, because you're serving the frozen materialized table:

```bash
docker exec -i publisher-tutorial-pg psql -U tutorial -d tutorial \
  -c "INSERT INTO orders VALUES (5, DATE '2026-01-02','US',999);"

curl -s -X POST http://localhost:4000/api/v0/environments/examples/packages/persist-tutorial/models/orders.malloy/query \
  -H 'content-type: application/json' \
  -d '{ "query":"run: daily_orders -> { aggregate: t is total_amount.sum() }", "compactJson":true }' \
  | jq -r '.result' | jq -c '.'
# -> [{"t":375}]     unchanged — served from the materialized table, not live Postgres

# Rebuild, then query — now it reflects the new row (375 + 999 = 1374):
curl -s -X POST http://localhost:4000/api/v0/environments/examples/packages/persist-tutorial/materializations \
  -H 'content-type: application/json' -d '{"forceRefresh": true}' >/dev/null
# (wait for MANIFEST_FILE_READY, then query again) -> [{"t":1374}]
```

That's the point: queries serve from the store you materialized into, and
refresh on your schedule — not per query.

### Rebuilds and cleanup

The auto-run server names the table by your `name=` verbatim, so **every** build
— a same-definition refresh or a change to what the source materializes — rewrites
that one table with an atomic `CREATE OR REPLACE`. Change the rollup to land an
extra column and rebuild:

```bash
# edit orders.malloy: add `avg_amount is amount.avg()` to the aggregate: block
curl -s "http://localhost:4000/api/v0/environments/examples/packages/persist-tutorial?reload=true" >/dev/null
curl -s -X POST http://localhost:4000/api/v0/environments/examples/packages/persist-tutorial/materializations \
  -H 'content-type: application/json' -d '{}' >/dev/null
# (wait for MANIFEST_FILE_READY)
```

The lake still holds a single table at the logical name, now with the new column:

```
-- SELECT table_name, table_type FROM information_schema.tables WHERE table_catalog='lake';
-- daily_orders                      BASE TABLE    (replaced in place)
```

The replace itself is atomic (DuckLake's catalog swap is transactional), so a
query never hits a half-swapped table. But on a **schema-changing** rebuild there
is a brief window where the running server's serve binding still describes the
old columns, until it rebinds on the build's auto-load — and the two directions
differ. A query that reaches a **newly added** field fails the serve-shape
compile and falls back to serving live (safe). A query over a **removed** field
still compiles against the stale binding and then errors at run against the new
table — there is no run-time fallback (see "Compile-time fallback only" in the
release notes). So a column-removing rebuild can surface transient query errors
until the binding refreshes; it does not return wrong data. Explicit generation
management — immutable generations, a staged cutover, rollback — that closes this
window is the job of a caller that assigns physical names per build and
distributes serve bindings (the orchestrated build path), not of the auto-run
server.

A materialization record can be reclaimed by deleting it with `dropTables=true` —
a destination-aware drop (Publisher only ever drops a table name it recorded
building, never a catalog scan):

```bash
# delete a materialization (find its id in the list)
curl -s -X DELETE \
  "http://localhost:4000/api/v0/environments/examples/packages/persist-tutorial/materializations/<ID>?dropTables=true" \
  -o /dev/null -w "HTTP %{http_code}\n"
# -> HTTP 204
```

```
info: Dropped materialized storage table on delete { physicalTableName: "daily_orders", storageConnectionName: "lake" }
```

---

## 7. Safety behaviors worth seeing

### What serves from storage, and the safe fallback

The serve shape carries the materialized table's stored columns **and**
re-declares the source's **dimensions and measures** over them, so anything
computed from the stored columns is served from storage. For example, if
`daily_orders` defines `dimension: avg_order_value is total_amount / order_count`,
a query using `avg_order_value` is served from the lake — the expression is
projected over the materialized table (`SELECT total_amount / order_count …
FROM lake.daily_orders`), not recomputed in the warehouse.

It also re-declares the source's **joins whose joined source is itself
materialized** (the join runs in DuckDB over the two stored tables) and its
**views** built from what's carried, so a query traversing such a join or
invoking such a view by name is served from storage too.

What still **falls back to serving live** (no error; the right answer, computed
in the warehouse): a query that reaches something the serve shape can't
reproduce — a join or view that reaches a **non-materialized source**, a
**window/analytic** field defined on the source, or a query against a source
that isn't materialized. (When a view reaches something not carried, only that
view falls back; the source's other queries still serve from storage.) You'll
see:

```
debug: storage serve-shape ineligible for this query; serving live { modelPath: "orders.malloy", ... }
```

Fallback means turning the feature on can never make a query wrong — at worst it
serves live, exactly as it would with the feature off.

### Joining and chaining sources

**Joining non-persisted sources is the simple case.** A persist source whose
query joins plain (non-persist) sources materializes the _joined result_ — the
join runs once, at build time, in the source warehouse, and only the result
lands in storage. You do **not** persist the joined-in sources:

```malloy
source: orders is orders_pg.table('public.orders')
source: customers is orders_pg.table('public.customers') // NOT persisted

source: orders_with_region is orders extend {
  join_one: c is customers on customer_id = c.customer_id
}
#@ persist name="orders_by_region" storage=lake   // only this is persisted
source: orders_by_region is orders_with_region -> {
  group_by: region is c.region
  aggregate: total_amount is amount.sum()
}
```

`orders_by_region` materializes as a flat `region, total_amount` table and serves
from storage; `orders`/`customers` are just its build-time inputs. (This assumes
the joined sources share one connection — Malloy can't join across two warehouses
in a single query, with or without `storage=`.)

**Chaining persist sources works too.** A persist source can read _another_
persist source:

```malloy
#@ persist name="daily_orders" storage=lake
source: daily_orders is orders -> { group_by: order_date; aggregate: total_amount is amount.sum() }

#@ persist name="monthly_orders" storage=lake
source: monthly_orders is daily_orders -> { group_by: order_month is order_date.month; aggregate: monthly_total is total_amount.sum() }
```

Both materialize, and each serves from its own table. When the whole chain lands
in the same destination, the downstream (`monthly_orders`) is built by **reading
the upstream's materialized table** — `daily_orders`'s stored rows are rolled up
in DuckDB, the upstream is never re-scanned from raw. This reuses the parent's
work and makes the downstream **consistent by construction**: it is a pure
function of the parent's stored rows, so a chain built in one package run cannot
drift between levels.

If the downstream can't be built that way — it reaches a field defined on the
parent that isn't a stored column, joins a live (non-materialized) source in the
same query, or its upstream lives in a _different_ destination — Publisher falls
back to **recomputing the upstream from raw** (inlining it into the downstream's
build query). That still produces a correct table, but two independently-timed
builds can then drift; rebuild the whole package together (`forceRefresh`) to
keep them aligned. Under `strictUpstreams` (orchestrated builds) the fallback is
refused rather than silently recomputing — the build fails loudly instead. The
`publisher_storage_chained_build_total` counter (labeled `parent_reuse` /
`inline_fallback` / `strict_refused`) reports which path each chained build took.

### Eligibility refusals (refused at build time)

Some sources can't be safely materialized into a shared store, and Publisher
refuses them at build time rather than producing a subtly wrong table. In the
**auto-run** flow shown here the refusal surfaces as a **failed materialization**
(`status: FAILED`, reason in `error`); the **orchestrated** build path (a
caller-supplied `buildInstructions`) returns the same refusal synchronously as
**HTTP 422**. Add a given-filtered persist source and materialize it:

```bash
cat > "$ENVDIR/persist-tutorial/givens.malloy" <<'MALLOY'
##! experimental.persistence
##! experimental.givens
given: region_filter :: string is 'US'
source: orders_g is orders_pg.table('public.orders')
#@ persist name="secret_rollup" storage=lake
source: secret_rollup is orders_g -> {
  where: region = $region_filter
  aggregate: total is amount.sum()
}
MALLOY
curl -s "http://localhost:4000/api/v0/environments/examples/packages/persist-tutorial?reload=true" >/dev/null

curl -s -X POST http://localhost:4000/api/v0/environments/examples/packages/persist-tutorial/materializations \
  -H 'content-type: application/json' -d '{"forceRefresh": true}' >/dev/null
MZID=$(curl -s http://localhost:4000/api/v0/environments/examples/packages/persist-tutorial/materializations | jq -r '.[0].id')
curl -s http://localhost:4000/api/v0/environments/examples/packages/persist-tutorial/materializations/$MZID | jq '{status, error}'
```

```json
{
  "status": "FAILED",
  "error": "Source 'secret_rollup' cannot be materialized into a storage destination: it references a given. Givens bind per query and are used for row-level access control, so a materialized-once table served to everyone would leak filtered rows across tenants. This is refused for safety. Serve this source live (drop 'storage=')."
}
```

The build fails with a clear, actionable message — and the package keeps
serving. Remove `givens.malloy` and reload to continue.

The other refusal is an **unbound (free) parameter** — a source with a free
parameter is a template with no single relation to freeze:

```bash
cat > "$ENVDIR/persist-tutorial/paramtest.malloy" <<'MALLOY'
##! experimental.persistence
##! experimental.parameters
source: orders_p is orders_pg.table('public.orders')
#@ persist name="param_rollup" storage=lake
source: param_rollup(threshold::number) is orders_p -> { aggregate: total is amount.sum() }
MALLOY
curl -s "http://localhost:4000/api/v0/environments/examples/packages/persist-tutorial?reload=true" >/dev/null

curl -s -X POST http://localhost:4000/api/v0/environments/examples/packages/persist-tutorial/materializations \
  -H 'content-type: application/json' -d '{"sourceNames":["param_rollup"],"forceRefresh":true}' >/dev/null
MZID=$(curl -s http://localhost:4000/api/v0/environments/examples/packages/persist-tutorial/materializations | jq -r '.[0].id')
curl -s http://localhost:4000/api/v0/environments/examples/packages/persist-tutorial/materializations/$MZID | jq '{status, error}'
```

```json
{
  "status": "FAILED",
  "error": "Source 'param_rollup' cannot be materialized into a storage destination: it has unbound parameter(s) 'threshold'. A source with a free parameter is a template instantiated per query, so there is no single relation to materialize. Bind the parameter to a constant, or drop 'storage=' to serve it live from the source warehouse."
}
```

Remove `paramtest.malloy` and reload to continue.

A third check runs **after** the build, once the table's authoritative schema is
captured: the served shape must **compile in DuckDB** (the served table lives
there, even for a warehouse-authored source). A source whose materialized shape
isn't DuckDB-portable is refused the same way — a serve-time error turned into a
build-time refusal. In practice it rarely fires, because the served shape is
just the stored columns; it's the floor that guarantees the captured schema
forms a valid DuckDB source.

These are the checks derivable from the compiled source and the built schema
alone. One more belongs here and is **not yet enforced**: a source protected by
`#(authorize)` — directly, or transitively through a join or derivation — should
not be materialized into a shared store, because the serve path rebinds it to a
virtual source whose shape carries no `#(authorize)` annotation, so the gate
can't be evaluated on the served table. Until that refusal lands (alongside the
upstream transitive-`#(authorize)` enforcement it reuses), do not materialize an
authorize-gated source; serve it live.

### Field-level hiding and the materialized table

Field visibility is preserved on the **serve** path: the serve shape declares
only the source's publicly visible columns, so a field hidden with `except:` (or
a `private`/`internal` access modifier) is dropped from the virtual source — a
query that references it falls back to serving live, where the source's own
visibility applies, exactly as it would un-materialized.

But be aware of the table **at rest**: the build materializes whatever the
source's compiled SQL projects, so an `except:`-ed column is still physically
written into the destination store (the in-warehouse path does the same, but
there the table lives in your own warehouse; a `storage=` destination may be a
separate, shared store). If a column is genuinely sensitive, **don't rely on
`except:` for a `storage=` source — filter it out in the SQL** so it never lands
in the store. This is the same "sensitive data crossing into the tier's store"
concern as the `#(authorize)` note above.

---

## 8. Observability recap

Everything you need is on the package status and the logs:

- `GET …/packages/{pkg}` →
  - `storageServeBindings`: the sources bound to serve from a `storage=` store,
    with their destination connection and table. Present once a build has bound
    them.
  - `warnings`: a `{model, target, message}` entry for any `storage=` source not
    served from storage — mode `off` (ignored) or `write-only` (built, served
    live). Empty when everything routes.
- `GET …/materializations/{id}` → run status and, on success, the manifest entry
  with `storageConnectionName` and the captured `schema`.
- Server logs → `info` when a query serves from storage; `debug` when a query
  falls back to live.
- Metrics (OpenTelemetry, under the `publisher` meter):
  - `publisher_storage_serve_routing_total{outcome=storage|live_fallback}` — the
    serve hit rate; the headline signal for "is the tier actually serving?"
  - `publisher_storage_chained_build_total{outcome=parent_reuse|inline_fallback|strict_refused}`
    — for a chained source, whether it built by reading its parent's stored table
    (`parent_reuse`) or fell back to recompute-from-raw.
  - `malloy_model_query_duration` tags storage-served queries with
    `served_from=storage` (the attribute is absent otherwise, so an `off`
    deployment's histogram is unchanged), isolating storage-served latency.

`PERSIST_STORAGE_MODE` (`off` default | `write-only` | `on`) is read at startup;
change it by restarting. It's a kill switch: moving it **down** never fails a
loaded package — a `storage=` source just reverts to serving live and shows up
as a warning.

**Two deliberate defaults** for a shared/multitenant lake, both of which can be
made configurable on request: materialized data is written as **Parquet**
(DuckLake small-table inlining is disabled on the write path, so tenant data
doesn't accumulate in the catalog database), and target **schemas must already
exist** (Publisher writes into a schema for a `name="schema.table"` target but
does not create it — provision schemas yourself; an unprovisioned schema fails
the build with a clear "schema not found" error).

---

## 9. Clean up

```bash
docker rm -f publisher-tutorial-pg
rm -rf /tmp/publisher-tutorial-lake packages/server/publisher_data/examples/persist-tutorial
```
