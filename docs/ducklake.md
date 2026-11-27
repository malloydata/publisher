<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# DuckLake connections

> What this is: how Publisher attaches a [DuckLake](https://ducklake.select) catalog, the
> catalog-format compatibility it guarantees, and the DuckDB extension provisioning it relies on —
> including how to run offline / air-gapped. For the connection config file itself, see
> [connections.md](connections.md); for the env vars, see [configuration.md](configuration.md).

A **DuckLake** connection (`type: "ducklake"`) lets Publisher query a DuckLake lakehouse: a
Postgres **catalog** that records table metadata, plus a **data path** in object storage (S3 or GCS)
that holds the Parquet data. Publisher reaches it through DuckDB's `ducklake` extension.

```json
{
  "name": "lakehouse",
  "type": "ducklake",
  "ducklakeConnection": {
    "catalog": {
      "postgresConnection": {
        "host": "catalog.example.com",
        "port": 5432,
        "databaseName": "ducklake_catalog",
        "userName": "publisher",
        "password": "<secret>"
      }
    },
    "storage": {
      "bucketUrl": "s3://my-lakehouse/data",
      "s3Connection": {
        "accessKeyId": "<key>",
        "secretAccessKey": "<secret>"
      }
    }
  }
}
```

- `catalog.postgresConnection` (**required**) — the Postgres catalog database.
- `catalog.metadataSchema` — schema holding this catalog's `ducklake_*` metadata tables, passed to
  `ATTACH` as `METADATA_SCHEMA`. Absent uses DuckLake's default (the catalog connection's default
  schema, typically `public`). See [Several catalogs in one database](#several-catalogs-in-one-database).
- `storage.bucketUrl` — the object-storage data path. Required unless the catalog already exists and
  records one; see [A catalog created elsewhere](#a-catalog-created-elsewhere-served-and-built-into).
- `storage.s3Connection` **or** `storage.gcsConnection` — credentials for the data path.

In the Docker image, a local-path `bucketUrl` must be writable by uid 1000, the user the server runs
as. The image prepares `/publisher/ducklake_data` for it: a new named volume mounted there is seeded
writable, so point `bucketUrl` under that path. A named volume mounted anywhere else starts
root-owned, and the build fails with `No such file or directory` rather than a permission error.
[`packages/server/README.docker.md`](../packages/server/README.docker.md#the-server-runs-as-a-non-root-user)
gives three ways to prepare such a path.

## What the storage credential reaches

The storage credential is installed as a DuckDB secret scoped to the catalog's data path: the configured
`storage.bucketUrl`, or, when the config leaves it out, the path the catalog recorded at its creation, read
from the catalog once it is attached (the attach itself reaches the catalog database and no data file). A
read of any other object, through the catalog or through raw SQL on the connection, is unsigned and fails
(HTTP 403 from the bucket). On S3, DuckDB's fallback to a key seeded from the host's `AWS_*` environment is
turned off on the connection's DuckDB instance, so such a read is never signed by the host's key either.
Two consequences for models:

- A model that read an object outside the data path with the DuckLake connection's credential, such as
  `lake.table('s3://bucket/raw/x.parquet')`, now fails. Put the object under the data path, or read it
  through a `duckdb` connection whose `attachedDatabases` entry holds a credential for it; those secrets
  are not scoped.
- A `managed: true` connection attached read-only is scoped narrower, to the recorded paths of its `main`
  schema and of the schemas its `ducklakeConnection.readableSchemas` lists (bare lowercase names; `main`
  is always readable): its storage secret, or on a local data path its file allowlist. A listed schema
  the catalog does not record, or whose path is not under the data path, is left out and logged. A
  listed schema's directory that also holds an unlisted schema's files (its path begins with the
  listed one's) refuses the attach, naming both, since the scope would reach those files; only a `main` that
  is the data path itself is exempt (a catalog migrated from DuckLake 0.2), and it attaches with a warning
  naming the schemas under it; any other `main` is held like a listed schema. A read
  outside the scope goes out unsigned, so the bucket must not allow anonymous reads. Changing the list
  opens the connection afresh at once; the previous instance finishes its in-flight queries. Create a
  schema in the catalog before listing it: a schema listed before it exists gets no file access until
  the connection is next edited or the server restarts. Removing a schema from the list takes effect
  for already-loaded packages only after they reload. `memory`, `information_schema`, `pg_catalog`,
  `temp`, `system` and any name starting with `__ducklake_metadata_` are refused (`main` is implicit).
  Its SQL is also held to those schemas before it runs, since DuckLake answers columns, row counts, file statistics
  and inlined rows from the catalog without opening a file: a model, or raw SQL, on a managed connection
  may name only `t`, `main.t`, `<connection>.t` or `<connection>.main.t`, or `s.t` or `<connection>.s.t`
  for a listed schema `s`, with a plain name (no path, glob or dot, so nothing a replacement scan reads
  as a file; a qualified name that still reached a replacement scan would be stopped by the file
  allowlist, which opens only the scoped paths), and is refused for any other schema or catalog, the metadata catalog, DuckDB's catalog
  views and functions, `DESCRIBE`, `SHOW`, `SUMMARIZE`, and table functions other than `range`,
  `generate_series` and `unnest` ([security posture](security-posture.md)). Its schema and table
  listings show `main` and the listed schemas only. So a model reads only those schemas' tables, with one
  gap: the check reads the names the query writes, not a view's body, so a DuckLake view in a listed
  schema can read an unlisted one, its catalog-held rows and statistics included (the storage scope
  still stops its files). List only schemas whose views read listed schemas. Tables
  a build wrote to a `destinationSchema` are served through the storage destination's own session, as
  before.

A catalog with nothing to scope to, a config with no `bucketUrl` whose catalog records no data path, is
not attached: the attach fails naming the catalog, and the next lookup retries it. A `publisher` proxy
connection to a DuckLake catalog is scoped by the upstream Publisher, which must run a release with this
behavior.

## Several catalogs in one database

By default a DuckLake catalog keeps its `ducklake_*` metadata tables in the catalog connection's
default schema, so two catalogs pointed at one database would share — and see — each other's
metadata. Give each its own `catalog.metadataSchema` and they stay separate; DuckLake creates the
schema on a read-write attach if it does not exist.

**Give each catalog its own `storage.bucketUrl` prefix too.** Separating the metadata does not
separate the data, and some DuckLake maintenance works from the data path rather than from the
catalog: `ducklake_delete_orphaned_files` lists `DATA_PATH` and removes whatever the attached
catalog does not reference. Run against one of two catalogs sharing a prefix, it treats the other
catalog's Parquet as orphaned. Publisher never deletes by prefix — its own sweeps enumerate through
the attached catalog — but the deletion is irreversible, so a metadata-schema-separated catalog
database is not on its own a reason to share a data path.

**This is an organizational boundary, not an access-control one.** What separates two catalogs is
which schema each configuration names. A configuration whose catalog role can reach a sibling schema
could name it and read that catalog's metadata, and therefore its data-file paths. Where catalogs
must be isolated from one another rather than merely kept tidy, give each its own role with `USAGE`
limited to its own schema; a database per catalog remains the strongest separation.

**It does not migrate an existing catalog.** Adding `metadataSchema` to a catalog whose metadata
already lives elsewhere does not move it. A read-write attach creates a _new, empty_ catalog in the
named schema and materializes into that, while a read-only attach fails because nothing is there
yet — so a typo surfaces as a confusingly empty catalog on the write path, and an attach failure on
the read path.

**Every server reading the catalog database must understand the option.** Configuration validation
ignores keys it does not recognize, so a Publisher predating `metadataSchema` accepts the config and
silently attaches the catalog connection's default schema instead. Where that default holds nothing
the attach fails loudly, which is safe; where it holds another catalog — the arrangement this option
exists to create — the older server reads that catalog's tables and a build materializes into it,
with no error either way. Roll the option out only once every server against the database is new
enough to honor it.

## How Publisher attaches a DuckLake catalog

**Read-only to serve.** Serving a query attaches the catalog `READ_ONLY`: it reads tables and never
writes catalog metadata. The lakehouse's own client owns writes. The one exception is a connection
configured with `ducklakeConnection.readOnly: false`, which attaches read-write and runs everything on
it read-write, user SQL included; set it only where something else decides who may run SQL there.

**Read-write only to build, and only for the build.** A `#@ persist storage=<destination>` source
materializes into the catalog, which needs a read-write attach. That attach is confined to a
transient build-scoped session and is dropped with it, so the serving path stays read-only.
A `storage=` target is declared in `storageDestinations`, not in `connections` — see
[storage destinations](connections.md#storage-destinations) — so everything on this page applies to
a DuckLake declared in either list.
Neither mode ever sets `AUTOMATIC_MIGRATION`: a catalog whose recorded format is outside the
supported range fails the attach rather than being migrated in place by whichever server happened
to reach it first.

**An incremental refresh mutates a stored table in place**, rather than writing a new one: a source
declaring `refresh="incremental"` alongside `storage=` has its delta applied as a `DELETE`+`INSERT`
or a `MERGE` inside one DuckLake transaction, so the table is either at the old snapshot or the new
one and never between them. The read-only serving attach observes the new snapshot on its next query,
with no re-attach and no package reload — the catalog is consulted per transaction rather than pinned
at attach time. Note that this is a property of a **Postgres-backed** catalog, which is the supported
configuration; a DuckDB-file catalog attached read-only does not behave this way.

**Data inlining off on the catalogs Publisher creates.** DuckLake's default keeps a table of up to 10
rows as rows in the catalog database rather than as a file, and a reader confined to the catalog's files
(a managed connection's read-only view, a storage secret scoped to a prefix) still reads those rows. A
build session's `ducklake_default_data_inlining_row_limit=0` does not override a limit the catalog
records. So whenever Publisher creates a catalog, at a destination's first build or through the admin
API's `POST /catalogs`, it records `data_inlining_row_limit` 0 in the catalog
(`CALL <catalog>.set_option('data_inlining_row_limit', 0)`), and every writer's small tables land in
files. A `dataInliningRowLimit` on the `POST` is recorded after it, so an explicit limit still wins.

An existing catalog is the host's, and Publisher never changes its setting. An attach that finds it
recording a limit above 0 logs a warning with code `ducklake-data-inlining-on`, and each package that
stores a table there (its `storage=` destination, or its own DuckLake connection) carries a package
warning of the same code, from the attach on. Record 0 to clear it: `CALL <catalog>.set_option(
'data_inlining_row_limit', 0)`, or `PATCH /catalogs/{catalog}` with `"dataInliningRowLimit": 0`. A
catalog that records no limit is not warned about: Publisher's own builds and loads then write files,
but another writer using DuckLake's default would inline, so record 0 on it as well.

**The catalog's credentials live in a DuckDB secret.** Publisher puts the catalog role's user,
password and any client-key password (`sslpassword`) in a temporary DuckDB secret (never persisted, redacted when listed) and attaches with a
path that carries only the host, port, database and connection options. A `connectionString` in
either libpq form (keyword/value or a `postgres://` URI) is read and split the same way.

**Lazy, and never on the startup path.** The catalog is attached on the _first query_ that uses the
connection — not when the server boots and not when the environment config is built. This is a
deliberate isolation boundary: **a slow or unreachable catalog degrades only that connection's
serving, never worker startup or any other connection.** Building the environment configuration
issues no catalog SQL at all.

**Preflight is non-load-bearing.** Before the real attach, Publisher runs a lightweight
compatibility preflight (below). Any failure of the preflight _itself_ — missing metadata, a
timeout, an unreachable catalog — is logged and falls through to the normal attach, which remains
the source of truth for unrelated errors. The preflight only ever _adds_ a clearer error; it never
introduces a new failure of its own.

## Managing DuckLake catalogs: the admin API

Everything above is for a catalog that comes into being with its first build. The **admin API**
(`admin-api-doc.yaml`) manages catalogs as resources instead: create one deliberately, encrypted and
with the schemas you want; load a file into a table; see what each catalog holds and how many bytes;
drop tables; and reclaim the storage behind old snapshots. It is a separate listener in the same
server, off unless configured:

```bash
PUBLISHER_ADMIN_PORT=4050 \
PUBLISHER_ADMIN_CATALOG_DATABASE="host=localhost port=5432 dbname=ducklake user=lake password=…" \
malloy-publisher --server_root .
```

`PUBLISHER_ADMIN_CATALOG_DATABASE` is the Postgres database your catalogs live in. For files in a cloud
bucket, add `PUBLISHER_ADMIN_STORAGE_PROVIDER` (`s3`, `s3_credential_chain` or `gcs`) and its key
variables ([configuration.md](configuration.md)); for a local directory, nothing more. No request carries
a credential and no response returns one. The listener binds `127.0.0.1` unless `PUBLISHER_ADMIN_HOST`
says otherwise (`PUBLISHER_HOST` does not move it), and it is unauthenticated: it can drop tables, so keep
it on loopback or behind an authenticating proxy.

**Running it on its own.** The catalog database's role reaches every catalog in it, which is fine when one
operator owns the server and every catalog. When the server serves other people's environments, keep the
two apart with two top-level settings, both `true` by default and given in `publisher.config.json`, as a
variable or as a flag ([configuration.md](configuration.md)): the serving deployment sets `"adminApiEnabled": false`, which
refuses to start if `PUBLISHER_ADMIN_PORT` or `PUBLISHER_ADMIN_CATALOG_DATABASE` is set, and the admin
deployment sets `"servingApiEnabled": false`, which runs only the admin listener, loads no environments
(watch mode is then absent) and refuses a config that declares any. An admin-only process serves
`/metrics` on the admin listener, since it has no main API to serve it from.

A catalog is found, not registered: every schema of that database holding DuckLake's
`ducklake_metadata` table is a catalog, named by the schema, and its data path, encryption and settings
are what it recorded when it was created.

```bash
A=http://localhost:4050/api/v0

# Create a catalog: encrypted (the default), its files under /srv/lake/sales.
curl -s -X POST $A/catalogs -H 'Content-Type: application/json' \
  -d '{"name": "sales", "dataPath": "/srv/lake/sales", "fileDeletionGrace": "1 day"}'

# Load a CSV that sits under the data path into main.orders (201 created, 200 replaced;
# add `If-None-Match: *` to refuse a table that exists).
curl -s -X PUT $A/catalogs/sales/schemas/main/tables/orders -H 'Content-Type: application/json' \
  -d '{"source": {"uri": "/srv/lake/sales/uploads/orders.csv", "format": "csv"}}'

# Copy a table from another catalog of the same catalog database (a migration), rewritten
# into this catalog's own files; a missing source answers 404 SOURCE_NOT_FOUND.
curl -s -X PUT $A/catalogs/sales/schemas/main/tables/orders_2025 -H 'Content-Type: application/json' \
  -d '{"source": {"catalog": "sales_2025", "schema": "main", "table": "orders"}}'

# Create a schema (201 created, 200 existed; `If-None-Match: *` refuses one that exists with
# 412 SCHEMA_EXISTS). Lowercase names only; `main`, `memory`, `information_schema`,
# `pg_catalog`, `temp` and `system` answer 400 SCHEMA_RESERVED. A load may not target `main`
# either, and a table PUT or load may not target the schema a storage destination builds into.
curl -s -X PUT $A/catalogs/sales/schemas/staging

# Apply Parquet objects under the data path to staging.charges, upserting by id (201 with the
# load's `id` and a `Location`, and the counts of rows inserted, replaced, unchanged and deleted;
# 200 with the counts alone when nothing changed). `"mode": "replace"` with `maxDeletedRows` makes
# the table equal to the objects instead. A retry with the same `Idempotency-Key` replays the load.
curl -s -X POST $A/catalogs/sales/schemas/staging/tables/charges/loads -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: charges-run_1' \
  -d '{"commitMessage": "charges run_1", "mode": "merge", "key": ["id"], "cursor": "created",
       "objects": ["/srv/lake/sales/uploads/run_1/charges-0.parquet"]}'

# Read a load by the id it answered, or the table's loads, newest first.
curl -s $A/catalogs/sales/schemas/staging/tables/charges/loads/$LOAD_ID
curl -s "$A/catalogs/sales/schemas/staging/tables/charges/loads?limit=20"

# What it holds: the catalog, one schema's tables, or every table of every schema.
curl -s $A/catalogs/sales
curl -s $A/catalogs/sales/schemas/main/tables
curl -s $A/catalogs/sales/tables

# Drop a schema and every table in it (204, also when it does not exist).
curl -s -X DELETE $A/catalogs/sales/schemas/staging

# Drop a table, then reclaim history from before a date, and the files only it held.
curl -s -X DELETE $A/catalogs/sales/schemas/main/tables/orders
curl -s -X DELETE "$A/catalogs/sales/snapshots?olderThan=2026-10-01T00:00:00Z"
```

To read `sales` from a model, give the environment a DuckLake connection to the same catalog, with no
`bucketUrl` (next section): `sales.table('orders')`.

**Files.** A load reads one CSV, Parquet, Excel or JSON object under the catalog's data path, the only
location its session can read, and rewrites the bytes as the catalog's own Parquet in one DuckLake
transaction, so an encrypted catalog stays encrypted and a reader sees the old table or the new one. The
rows never stay in the catalog database, even in a catalog that records a `dataInliningRowLimit`: the
write's inlined rows are flushed to Parquet straight after it.
A copy reads a table of another catalog in the same catalog database, attached read-only as that
catalog's owner, or of the same catalog, and writes it the same way; the configured role must be able to
act as both owners (otherwise `409 CATALOG_OWNER_UNSUPPORTED`). A copy from an encrypted catalog into an
unencrypted one is refused, since it would write the rows unencrypted. A copy keeps the source table's
partition spec (one a since-retired `partition=` build wrote, or any `SET PARTITIONED BY`) and writes
its rows in partition order, on one thread at a low flush threshold, so the copy prunes files the way the
source did; a partitioned table copied onto
itself, under any spelling of its name (DuckDB matches names case-insensitively), is refused, since the
rewrite would empty it before reading it. A table PUT of an object that is missing, is not the format it
claims or holds a value its column cannot is `422 OBJECT_UNREADABLE`, as a load's is: the request is
well-formed but the named data cannot be applied, so correct the object rather than retry.
`GET …/tables/{table}` reports a
table's spec as `partitionedBy`, empty for an unpartitioned table, so a migration can check a copy against
its source.

**Loads.** A table PUT defines a table from one file: whatever the file holds becomes the table. A load
(`POST …/tables/{table}/loads`) applies rows to a table under a contract, for a pipeline that delivers a
stream in batches:

- `merge` upserts by `key`. `replace` makes the table equal to the call's objects, by `key` or, with no
  key, as a whole; a replace that would delete more than `maxDeletedRows` rows writes nothing (`422
  LOAD_DELETE_LIMIT_EXCEEDED`), so an empty or truncated extract cannot empty a table. A keyless replace
  compares whole rows as a multiset: a stored row with no identical row in the objects counts as deleted,
  an object row with no identical stored row as inserted (an edited row is one of each, and
  `rowsReplaced` is 0), and `maxDeletedRows` guards the deleted count.
- A load that changes no row and adds or widens no column, keyed or keyless, writes nothing and takes no
  snapshot, so a load retried after it committed writes nothing. It reports the same `rowCount` and
  `cursorMax` as the first call, with every row counted in `rowsUnchanged` and none inserted, replaced or
  deleted, and it answers `200`, with no `id`. A retry is safe because it finds nothing to change,
  provided no other load of the table committed in between; an `Idempotency-Key` (below) covers a load
  that committed, but not this one. When some row does change,
  a keyed load rewrites only the rows it changes, and a keyless replace rewrites the whole table, so
  there `rowsUnchanged` counts the rows matched, not rows left in place.
  Of two rows with one key, the later object in `objects` wins, then the later row of an object.
- A load that commits records `commitMessage` (optional; `load <schema>.<table>` when absent) as its
  snapshot's message, with the author `publisher-admin`; `GET …/snapshots` lists both as `commitMessage`
  and `author`. A load that changes nothing writes no snapshot, so records nothing.
- A load that commits is a resource: it answers `201` with an `id`, the `snapshotId` and `committedAt` of
  its snapshot, its `mode`, and a `Location` of `loads/{id}`. That includes a load that only adds or
  widens a column. The snapshot carries the load's record, so it is committed with the rows, readable by
  every replica, and kept in no table of its own. `GET …/loads/{id}` answers the load with the snapshot's
  `author` and `commitMessage` (`404 LOAD_NOT_FOUND` when there is none), and `GET …/loads` lists the
  table's loads newest first as `{items, nextCursor}`, paged by `limit` (1 to 1000, default 100) and the
  opaque `after`. Only a missing catalog is a `404` there: a table with no recorded loads, or none yet,
  lists as empty. A table dropped and created again under its name shares one history. A load is readable
  for as long as its snapshot: `DELETE …/snapshots` (and a storage destination's cleanup) expires both.
- `Idempotency-Key` (optional, at most 255 characters) makes the retry of a load that committed safe even
  after other loads committed. A load that repeats a key an earlier committed load of this table was sent
  under, with the same body (`key` and `objects` in the order sent), writes nothing and answers that
  load's `201`, `Location` and body again, after flushing any rows the table holds inlined (so a retry
  still heals a load that failed after its commit). The same key with a different body is `409
  IDEMPOTENCY_KEY_REUSED`. A key belongs to its table and does not outlive a table DELETE or a PUT that
  replaces it; the server keeps only its SHA-256.
  The key replays a load that committed, for as long as its snapshot is kept. A load that changed nothing
  stores no record, and a key whose snapshot an expiry removed has none, so either runs again: a retry
  of such a load, sent after another load committed, can revert it. A table with more than one writer
  therefore needs its loads serialized by the caller. A record is found by a text match on the snapshots'
  commit metadata, with no index, so each keyed load scans the catalog's snapshots since the table was
  created; expiring old snapshots (`DELETE …/snapshots`) bounds that cost, as it bounds the keys.
  The key differs from the v1 API's `Idempotency-Key` in two further ways: a record lasts as long as its
  snapshot, which may be shorter or longer than 24 hours (a storage destination's cleanup expires
  snapshots on the caller's `snapshotsOlderThan`); and a duplicate that arrives while the first is
  running is `503 LOAD_BUSY`, not made to wait, and its retry replays.
- A new column is added, even when no row changes. An existing one keeps its type when the objects type
  it the same or narrower, losslessly, and is widened to the objects' type when that is a lossless widening
  of the table's: a wider integer, a decimal with no fewer integer or fraction digits, an integer into a
  decimal or `DOUBLE` that holds all its digits, `FLOAT` into `DOUBLE`, a longer varying text. The widening
  commits with the rows and rewrites no file already written; a table's type never narrows. Objects that
  type one column differently (`BIGINT` in one, `DOUBLE` in another) must each widen losslessly into the
  type they are read together as. A column the objects lack is written NULL for every row the load
  writes, whatever `DEFAULT` the table gives it (a merge writes NULL for every key it touches), so a
  retry compares equal and changes nothing; a column the table holds `NOT NULL` that the objects lack, or
  hold a NULL in, is refused. A column that every object types NULL, as a Parquet writer writes an
  all-NULL column, is ignored; objects with no other column are refused. As the `cursor`, such a column
  reports `cursorMax: null` (with the table's type as `cursorType`, none on a first load), and as a `key`
  it is a NULL key. Anything else, a column of the table named `__publisher_…`, a NULL key, a `NOT NULL`
  violation, or a cursor that is not an integer, DATE or TIMESTAMP is `422
  LOAD_COLUMN_MISMATCH`, with a `detail` naming the column and the `violation`. An object that is
  missing, is not Parquet, or holds a value its column cannot is `422 OBJECT_UNREADABLE`; a storage
  failure is a `500` to retry.
- One load of a table runs at a time across every replica (a Postgres advisory lock in the catalog
  database), and one load or copy runs at a time per server. Either busy is `503 LOAD_BUSY`, never
  queued: retry. A load whose lock connection is lost before it commits rolls back and answers the same;
  the lock is checked just before the commit, which narrows the window in which a lost lock goes
  unnoticed but does not close it. The lock is a session advisory lock, so the catalog database
  connection must be a session-mode one: a transaction-mode pooler such as PgBouncer does not hold it,
  and two loads of a table could then run together.
- The load reads the table inside the transaction that writes it, so a load that races a table PUT or
  DELETE of the same table (or a schema drop) on another replica is refused when it commits, `409
  LOAD_CONFLICT`: nothing is written, retry. A schema dropped before that transaction begins is a `404
  SCHEMA_NOT_FOUND`.
- A load runs in its own DuckDB session, bounded to `PUBLISHER_ADMIN_MEMORY_LIMIT` of memory (default 1.5 GB,
  or `PUBLISHER_DUCKDB_MEMORY_LIMIT` when that is lower, so a load needs both raised to use more) and
  `PUBLISHER_ADMIN_MAX_TEMP_DIRECTORY_SIZE` of spill (default 6 GiB) under the system temp directory. A server that runs both the admin listener and the serving API shares its CPU and
  memory between a load and user queries; run the admin listener on its own (`"servingApiEnabled":
  false`) when loads are large.
- A keyed load compares a table's columns in narrow passes rather than one join over every column: the
  keys first, then groups of columns sized so both sides of a group's rows fit in half the session's
  memory. A keyed replace or merge of 10 million rows by 21 columns stays within the 6 GiB spill limit
  (measured); spill grows with the stage's size plus the widest single column, so a column whose rows
  times width exceed that budget on its own may still spill past it. A keyless replace has no key to
  narrow by: every one compares whole rows of both sides, a full-width aggregate over the stored and the
  staged rows, split into passes (by a hash of the whole row) sized so each fits in half the session's
  memory; an empty extract, or an empty table, needs no compare. A load or table PUT that outgrows the
  memory or spill limit anyway answers `422 LOAD_TOO_LARGE` and writes nothing, and a retry fails the
  same way: give the load a `key`, split its objects across loads, or raise
  `PUBLISHER_ADMIN_MEMORY_LIMIT` and `PUBLISHER_DUCKDB_MEMORY_LIMIT` (the session uses the lower) or
  `PUBLISHER_ADMIN_MAX_TEMP_DIRECTORY_SIZE`. It is `422` because the work outgrew the session, not because
  the request body was too large. A failure after the load committed, and a full spill disk, are a
  `500`, to retry.

**History.** Dropping or replacing a table only retires its files: they stay readable at earlier
snapshots. `DELETE …/snapshots?olderThan=` expires older snapshots (never the latest) and deletes files
that have been retired longer than the catalog's `fileDeletionGrace` (`1 day` if it records none), so a
reader mid-scan is never cut off; files it retires go on a later call. It deletes only files the catalog
recorded, never an untracked file under the data path.

**Ownership.** When the admin API's configured role is not a catalog schema's owner, every admin-API
session on that catalog acts as the owner (libpq `options=-crole=<owner>`), so a catalog an orchestrator
provisioned with its own role keeps that role's ownership and grants however it is managed. The owner
must be a plain role name (letters, digits, `_`, `-`); any other answers `409 CATALOG_OWNER_UNSUPPORTED`.
The configured connection string must not set `options` itself.

## A catalog created elsewhere, served and built into

Five settings on a DuckLake connection or storage destination serve a catalog the admin API (or any other
tool) created. Each is additive; a configuration that uses none of them behaves as before.

**Leave out the data path.** Without `storage.bucketUrl`, every attach uses the data path the catalog
recorded at its creation, and no attach may create a catalog: a build, a load or a read-write connection to
a catalog that does not exist fails with `Existing DuckLake … does not exist` instead of creating one
unencrypted with its own settings. The storage credential (`s3Connection` or `gcsConnection`) still goes
under `storage`. With a `bucketUrl`, the first build creates the catalog there, as it always has.
Servers before this release require `bucketUrl` and refuse the whole environment's connections without
it, so in a mixed-version deployment upgrade every server before any config leaves it out.

**Keep built tables apart from people's.** `ducklakeConnection.destinationSchema` on a storage
destination puts every table a build writes in that schema (`materialized`, say) instead of `main`, and
the serve binding reads it from there. A table PUT may write people's own tables in `main` (an upload lands
there); a load may not, so loads, and the tables a load will later merge into, go in a schema you create with
`PUT /catalogs/{catalog}/schemas/{schema}`. A destination that shares its catalog with a managed read-only
connection must set `destinationSchema`, since every managed connection reads `main`, and a build without one
writes there. A `#@ persist name=` that already names a schema keeps
it. No build creates the schema: the catalog's owner does, and a build into a destination whose
`destinationSchema` the catalog lacks is refused with a message naming both.

Publisher builds the schema a storage destination builds into (`destinationSchema`, or `main` when it
sets none), so the admin API refuses a table PUT or a load there with `409 SCHEMA_IS_DESTINATION`, naming
the destination: a table a person wrote there would feed the rebuild of an incompatible table and could
be served as the build's. A table DELETE there is still allowed, which is how a build table is dropped,
and a copy may still read its source from it. A destination that builds into `main` therefore makes
`main` read-only to a table PUT, so give a destination that shares a catalog with people's tables a
`destinationSchema`. Only the storage destinations of the environments this server serves count, so an
admin-only listener (`servingApiEnabled: false`), which loads no environments, refuses nothing, and a
connection that carries `destinationSchema` is not a destination. A destination is on the catalog when its `catalog` names the
same host, port and database as `PUBLISHER_ADMIN_CATALOG_DATABASE`, whatever role, credentials or form
(a connection string or fields) it connects with, and has the catalog's name as its `metadataSchema`. A
write that arrives before the environments finish loading is not checked, and the check sees only the
destinations an environment holds, so it misses those of an environment that failed to load or whose stored
destinations could not be read (its list is not yet authoritative, `hasAuthoritativeStorageDestinations()`).
A schema DELETE is refused there too, since it would drop every build table. A load also refuses `main`
itself, with `400 SCHEMA_RESERVED`: every managed connection reads `main`, and a destination with no
`destinationSchema` builds there.

The target is that the check no longer depends on which process answers: a destination records its claim
on its schema in the catalog itself, in a marker the build writes, and every writer of the catalog checks
the marker. Until then the check is only as complete as the memory of the process that receives the write.

A connection's `readableSchemas` may not list a schema a storage destination of its environment builds
into, when both name the same catalog (database and metadata schema; a connection that only shares a data
path with the destination is warned about, not refused): that would make the destination's stored copies, gated sources' rows among them, readable by name
without their gate. Saving such a connection (`POST` or `PUT` of a connection, or an environment create or
update that brings one, a config reload included) is refused with `400`, and the same pair in the
config at startup fails the environment's load. `main` is not refused, since a managed connection always
reads it: a destination that shares a catalog with such a connection must set `destinationSchema`. The
check sees the destinations of the same environment only, so a destination in another environment, and a
schema created later with `PUT /catalogs/{catalog}/schemas/{schema}`, are not seen.

**Serve the catalog to models.** Give the catalog a `connections` entry, with `managed: true`. Publisher
then sets the session's search path to the catalog's `main` after the attach, so `sales.table('orders')`,
which compiles to the bare table name, resolves: DuckDB never searches an attached catalog for an
unqualified name otherwise. `ducklakeConnection.readOnly` stays `true` (the default) unless you have
decided who may write through the connection. Its storage secret reaches only the `main`
schema's files; see [What the storage credential reaches](#what-the-storage-credential-reaches).

**Persist from the catalog into itself.** A `#@ persist storage=<dest>` source on that connection reads
the catalog it is built into. When the connection and the destination are the same catalog — the same
catalog database (given as `host`, `port` and `databaseName`; a `connectionString` config is never
matched) and metadata schema (and the same data path, when both state one), never decided by name — the
build is a plain CTAS on one read-write attach, with no federation and no egress. The attach
takes the connection's name, so the two may be named differently and a table path written for the
connection still resolves. That session can write the catalog, so it runs only SQL Malloy generated: a
source carrying an `.sql()` source or a `sql_*` expression anywhere beneath it is refused, as every
DuckLake source was before, and so is a table read from a file or URL (`lake.table('/x.csv')`),
which that session would read with the destination's storage credential. Such a source is refused when
the package loads (`persist-not-built`, reason `storage_source_unsupported`) and answered live, rather
than failing its build. The build also reads only base tables of the catalog: one whose SQL names a view
there fails, since a view's body is SQL too and would run with the destination's credential. That check
runs in the build's own transaction, so the build reads the catalog as the check saw it. A served copy
that reads a managed connection's tables through the destination, to serve a join across the one catalog,
holds to the same rule, and checks it again before every query it answers: once a table it reads is a view,
queries answer live. On a connection configured `managed` the same holds with no `storage=` too: a
managed catalog's tables are built inside it, so a `#@ persist`, `#(materialize)` or rollup over author
SQL there is refused at load whatever it declares. A chained build (a
downstream stacked on its stored parents in the destination) runs on the same kind of session and follows
the same rule: a downstream carrying author SQL is recomputed from raw instead of stacked on its parents.

**Meter it.** Each statement on a storage destination or a `managed` connection, and each admin-API load
or copy, emits one log record, `statement_usage`, and increments `publisher_statement_usage_total`. The
record names no sink; a log collector maps it. Its fields, each left out when the statement has none:

| Field | What it holds |
|---|---|
| `statement_id` | A UUID of the record's own, so a collector that delivers a record twice can count it once. |
| `environment` | The environment the statement ran in; empty for an admin load or copy. |
| `connection` | The connection or storage destination, by the name the environment gives it; the catalog's name for an admin load or copy. |
| `catalog` | The DuckLake catalog the statement ran on: the connection's `catalog.metadataSchema`, which is also the catalog's name on the admin API. It names the same catalog whatever a connection is called. |
| `surface` | `serve` (a model query or a notebook cell), `sql` (raw SQL on a `managed` connection), `build`, `load` or `copy`. |
| `package` | The package served or built. |
| `duration_ms` | Wall-clock milliseconds the statement ran. |
| `rows` | Rows returned or written, when the result reports a count. |
| `bytes_out` | UTF-8 bytes of the statement's result as Publisher serialized it for the caller, in the shape asked for: a model query's `result` (`serve`, full or compact; MCP's `execute_query` asks for compact), a notebook cell's `result`, raw SQL's `data` (`sql`). The envelope around it (the HTTP response's other fields, an MCP answer's metadata) is not counted. For Publisher's own MCP `execute_query`, it is the compact result the query returned to the tool, before the tool wraps it and trims rows to fit its size budget: the agent can receive fewer bytes than it counts. Absent on a build, a load or a copy, on a failed statement, and when a row or byte cap refused the result. The record of a statement that answered goes out once its result is serialized; `duration_ms` is still the statement's own. |
| `status` | `ok` or `error`. On `serve` or `sql`, `ok` without `bytes_out` means the statement ran but its result was never returned: a row or byte cap refused it (the caller got a 413), or a later step failed. A collector that counts completed answers should not count those. |
| `threads` | The `duckdb.threads` setting: the thread count a DuckDB instance of the process opens with. Left out under `off`, and when the cap could not be applied (a `@malloydata/db-duckdb` release that changed the internals Publisher patches, which Publisher logs at startup); a connection that sets its own `threads` is not reflected. |
| `worker` | The host name of the process that ran it: the pod's name in Kubernetes. |

A same-catalog build and a chained build are recorded; a passthrough build is not, since its time is
mostly the source warehouse's. Raw SQL on a `managed` connection is recorded when it runs; one refused
because it is not a single SELECT never ran and is not. A statement that fails is recorded too, with
`status: "error"`, since it spent the time; a collector that bills only completed work counts
`status: "ok"`.

The main API's storage-destination routes (`GET …/storageDestinations/{dest}/schemas/{schema}/tables`,
`DELETE …/tables/{table}` and `POST …/fileCleanups`) are deprecated: the admin API's catalog, table and
snapshot resources replace them, and they will be removed.

## Catalog-format compatibility

A DuckLake catalog records the on-disk **format version** it was written at. The `ducklake`
extension bundled with a given DuckDB engine attaches a bounded range of formats; a catalog outside
that range fails deep inside DuckDB with an opaque error.

This compatibility is a property of the **catalog format**, not the client that wrote it. Publisher
checks the format recorded at `ducklake_metadata.version` (e.g. `1.0`); the writing client is
recorded separately (`created_by`) and is never checked. A catalog written at a supported format by
a _newer_ DuckLake client still attaches — a client advances the catalog format only when it
genuinely breaks read-compatibility, not for a routine release.

Publisher derives the supported range from the **pinned DuckDB engine version**:

```
1.0  ≤  catalog format  ≤  (max format the pinned engine's ducklake extension attaches)
```

The lower bound is fixed at `1.0` — the 1.x DuckLake line does not attach older `0.x` catalogs
without an explicit in-place migration, which Publisher never performs in either attach mode
(`AUTOMATIC_MIGRATION` is never set). The upper bound moves with the engine.

This is derived from the engine on purpose. An enumerated "supported versions" list drifts silently
as the engine moves and has to be remembered on every bump; deriving the range from the pin keeps it
honest. A CI check fails the build if the pinned engine ever moves to a version whose maximum
catalog format has not been recorded, so the contract can't rot unnoticed.

**On a mismatch**, the runtime preflight produces a clear, actionable error — the format found, the
supported range, and a migration pointer — instead of an opaque engine failure. To attach a catalog
written at an older format, migrate it to a supported format with the DuckLake tooling first (see the
[DuckLake docs](https://ducklake.select/docs)); Publisher will not migrate it for you.

## DuckDB extension provisioning

DuckLake needs several DuckDB extensions (`ducklake`, `postgres`, `aws`/`httpfs` for object storage).
The same provisioning applies to every DuckDB-backed connection (BigQuery, Snowflake, Postgres, and
cloud storage attachments), but it matters most here because DuckLake pulls in the widest set.

Publisher installs the extensions it needs **explicitly**, and controls whether the runtime may fetch
them from the network with the `EXTENSION_FETCH_POLICY` environment variable:

- **`on-demand` (default)** — a missing extension is installed on first use. This is the right mode
  for local/standalone use, where a runtime `INSTALL` _is_ how you provision extensions. Behaviour is
  unchanged from earlier releases. (Publisher uses a plain `INSTALL`, which is local-first: it no-ops
  when the extension is already present, so a pre-provisioned/baked extension is never silently
  re-downloaded.)
- **`local-only`** — the server never runs `INSTALL` and disables DuckDB's implicit auto-install, so
  no code path reaches the network. Extensions already present on disk (however they were
  provisioned — a Docker image bake is one way) still load normally; a genuinely missing extension
  fails with a **loud, actionable error naming the extension and the policy**, rather than a silent
  fetch attempt. This is the mode for **air-gapped / pinned-image deployments**.

**`local-only` needs a pre-populated cache — pick your install route.** The bake runs during `build`
and writes to the DuckDB extension cache under `~/.duckdb/extensions/v<version>/<platform>/`, _not_
into the npm package. So an `npx` / `npm install` consumer starts with an **empty** cache and
genuinely needs a first-use `INSTALL` (i.e. `on-demand`) — raw `npx` cannot be made offline-safe.
`local-only` is offline-safe only where a platform-matched bake has already populated that cache: the
**published Docker image** (which copies the builder's cache into the final image) or a **from-source
`bun run build` on the target platform**. Air-gapped deployments should use one of those two routes.

Regardless of the policy, the DuckLake attach session disables DuckDB's _implicit_ auto-install for
itself — it only ever needs the curated set of extensions Publisher installs explicitly.

### Bundled extensions (what's available under `local-only`)

Publisher's published Docker image **bakes** a curated set of extensions at build time — they are
downloaded into the image's DuckDB extension cache during the build and copied into the final image —
so `local-only` works out of the box on that image with no network at runtime. The baked set is
exactly the extensions the server installs at runtime:

| Extension   | Kind      | Provides                                                                              |
| ----------- | --------- | ------------------------------------------------------------------------------------- |
| `httpfs`    | core      | HTTP(S) and S3/GCS/Azure object-storage access                                        |
| `aws`       | core      | AWS credential-chain resolution for S3                                                |
| `azure`     | core      | Azure Blob Storage access                                                             |
| `postgres`  | core      | Postgres connections, and the DuckLake Postgres catalog                               |
| `ducklake`  | core      | DuckLake catalog attach                                                               |
| `bigquery`  | community | BigQuery connections                                                                  |
| `snowflake` | community | Snowflake connections                                                                 |

In addition, DuckDB's **statically-linked built-ins** — `parquet`, `json`, `icu`, `core_functions`,
and `autocomplete` — are compiled into the engine, so they need no install and are always available
regardless of policy or network.

This baked set is defined in one place — the `EXTENSIONS` array in
[`packages/server/scripts/bake-duckdb-extensions.js`](../packages/server/scripts/bake-duckdb-extensions.js),
which mirrors the runtime install sites in `packages/server/src/service/connection.ts`. The CI smoke
test loads every baked extension with the network disabled (`docker run --network none`), so the set
can't silently drift from what the server actually loads. On the **stock published image**, this is
what's pre-provisioned. Under `local-only` the boundary is not this list but **whatever signed
extensions are present in the DuckDB extensions directory on that host** — autoload stays on, so an
extension you place there out-of-band (e.g. running Publisher outside Docker, or in your own image)
also loads with no network. The network is the only thing `local-only` turns off; if you need an
extension that isn't present and can't pre-provision it, use `on-demand`.

See [configuration.md](configuration.md#environment-variables--cli-flags) for the `EXTENSION_FETCH_POLICY` reference.
