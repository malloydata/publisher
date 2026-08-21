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
- `storage.bucketUrl` (**required**) — the object-storage data path.
- `storage.s3Connection` **or** `storage.gcsConnection` — credentials for the data path.

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
writes catalog metadata. The lakehouse's own client owns writes.

**Read-write only to build, and only for the build.** A `#@ persist storage=<destination>` source
materializes into the catalog, which needs a read-write attach. That attach is confined to a
transient build-scoped session and is dropped with it, so the serving path stays read-only.
A `storage=` target is declared in `storageDestinations`, not in `connections` — see
[storage destinations](connections.md#storage-destinations) — so everything on this page applies to
a DuckLake declared in either list.
Neither mode ever sets `AUTOMATIC_MIGRATION`: a catalog whose recorded format is outside the
supported range fails the attach rather than being migrated in place by whichever server happened
to reach it first.

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
