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
(watch mode is then absent) and refuses a config that declares any.

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

# What it holds.
curl -s $A/catalogs/sales
curl -s $A/catalogs/sales/schemas/main/tables

# Drop a table, then reclaim history from before a date, and the files only it held.
curl -s -X DELETE $A/catalogs/sales/schemas/main/tables/orders
curl -s -X DELETE "$A/catalogs/sales/snapshots?olderThan=2026-10-01T00:00:00Z"
```

To read `sales` from a model, give the environment a DuckLake connection to the same catalog, with no
`bucketUrl` (next section): `sales.table('orders')`.

**Files.** A load reads one CSV, Parquet, Excel or JSON object under the catalog's data path, the only
location its session can read, and rewrites the bytes as the catalog's own Parquet in one DuckLake
transaction, so an encrypted catalog stays encrypted and a reader sees the old table or the new one.

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
the serve binding reads it from there. People's own tables stay in `main`. A `#@ persist name=` that
already names a schema keeps it. No build creates the schema: the catalog's owner does, and a build into a
destination whose `destinationSchema` the catalog lacks is refused with a message naming both.

**Serve the catalog to models.** Give the catalog a `connections` entry, with `managed: true`. Publisher
then sets the session's search path to the catalog's `main` after the attach, so `sales.table('orders')`,
which compiles to the bare table name, resolves: DuckDB never searches an attached catalog for an
unqualified name otherwise. `ducklakeConnection.readOnly` stays `true` (the default) unless you have
decided who may write through the connection.

**Persist from the catalog into itself.** A `#@ persist storage=<dest>` source on that connection reads
the catalog it is built into. When the connection and the destination are the same catalog — the same
catalog database (given as `host`, `port` and `databaseName`; a `connectionString` config is never
matched) and metadata schema (and the same data path, when both state one), never decided by name — the
build is a plain CTAS on one read-write attach, with no federation and no egress. The attach
takes the connection's name, so the two may be named differently and a table path written for the
connection still resolves. That session can write the catalog, so it runs only SQL Malloy generated: a
source carrying an `.sql()` source or a `sql_*` expression anywhere beneath it is refused, as every
DuckLake source was before, and so is a table read from a file or URL (`lake.table('/x.csv')`),
which that session would read with the destination's storage credential. A chained build (a
downstream stacked on its stored parents in the destination) runs on the same kind of session and follows
the same rule: a downstream carrying author SQL is recomputed from raw instead of stacked on its parents.

**Meter it.** Each statement on a storage destination or a `managed` connection, and each admin-API load,
emits one log record, `statement_usage`, with the environment (empty for an admin load), connection or
catalog, surface (`serve`, `load`, `build`), package, duration, rows and status, and increments
`publisher_statement_usage_total`. The record names no sink; a log collector maps it. A same-catalog
build and a chained build are recorded; a passthrough build is not, since its time is mostly the source
warehouse's. A statement that fails is recorded too, with `status: "error"`, since it spent the time; a
collector that bills only completed work counts `status: "ok"`.

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
| `httpfs`    | core      | HTTP(S) and S3/GCS/Azure object-storage access (also used by the per-package sandbox) |
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
