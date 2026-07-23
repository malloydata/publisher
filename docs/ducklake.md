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
- `storage.bucketUrl` (**required**) — the object-storage data path.
- `storage.s3Connection` **or** `storage.gcsConnection` — credentials for the data path.

## How Publisher attaches a DuckLake catalog

**Read-only.** Publisher attaches the catalog `READ_ONLY`: it reads tables and never writes catalog
metadata or migrates the catalog format. The lakehouse's own client owns writes.

**Lazy, and never on the startup path.** The catalog is attached on the *first query* that uses the
connection — not when the server boots and not when the environment config is built. This is a
deliberate isolation boundary: **a slow or unreachable catalog degrades only that connection's
serving, never worker startup or any other connection.** Building the environment configuration
issues no catalog SQL at all.

**Preflight is non-load-bearing.** Before the real attach, Publisher runs a lightweight
compatibility preflight (below). Any failure of the preflight *itself* — missing metadata, a
timeout, an unreachable catalog — is logged and falls through to the normal attach, which remains
the source of truth for unrelated errors. The preflight only ever *adds* a clearer error; it never
introduces a new failure of its own.

## Catalog-format compatibility

A DuckLake catalog records the on-disk **format version** it was written at. The `ducklake`
extension bundled with a given DuckDB engine attaches a bounded range of formats; a catalog outside
that range fails deep inside DuckDB with an opaque error.

This compatibility is a property of the **catalog format**, not the client that wrote it. Publisher
checks the format recorded at `ducklake_metadata.version` (e.g. `1.0`); the writing client is
recorded separately (`created_by`) and is never checked. A catalog written at a supported format by
a *newer* DuckLake client still attaches — a client advances the catalog format only when it
genuinely breaks read-compatibility, not for a routine release.

Publisher derives the supported range from the **pinned DuckDB engine version**:

```
1.0  ≤  catalog format  ≤  (max format the pinned engine's ducklake extension attaches)
```

The lower bound is fixed at `1.0` — the 1.x DuckLake line does not attach older `0.x` catalogs
without an explicit in-place migration, which Publisher never performs (it attaches read-only). The
upper bound moves with the engine.

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
  for local/standalone use, where a runtime `INSTALL` *is* how you provision extensions. Behaviour is
  unchanged from earlier releases. (Publisher uses a plain `INSTALL`, which is local-first: it no-ops
  when the extension is already present, so a pre-provisioned/baked extension is never silently
  re-downloaded.)
- **`local-only`** — the server never runs `INSTALL` and disables DuckDB's implicit auto-install, so
  no code path reaches the network. Extensions already present on disk (however they were
  provisioned — a Docker image bake is one way) still load normally; a genuinely missing extension
  fails with a **loud, actionable error naming the extension and the policy**, rather than a silent
  fetch attempt. This is the mode for **air-gapped / pinned-image deployments**.

**`local-only` needs a pre-populated cache — pick your install route.** The bake runs during `build`
and writes to the DuckDB extension cache under `~/.duckdb/extensions/v<version>/<platform>/`, *not*
into the npm package. So an `npx` / `npm install` consumer starts with an **empty** cache and
genuinely needs a first-use `INSTALL` (i.e. `on-demand`) — raw `npx` cannot be made offline-safe.
`local-only` is offline-safe only where a platform-matched bake has already populated that cache: the
**published Docker image** (which copies the builder's cache into the final image) or a **from-source
`bun run build` on the target platform**. Air-gapped deployments should use one of those two routes.

Regardless of the policy, the DuckLake attach session disables DuckDB's *implicit* auto-install for
itself — it only ever needs the curated set of extensions Publisher installs explicitly.

### Bundled extensions (what's available under `local-only`)

Publisher's published Docker image **bakes** a curated set of extensions at build time — they are
downloaded into the image's DuckDB extension cache during the build and copied into the final image —
so `local-only` works out of the box on that image with no network at runtime. The baked set is
exactly the extensions the server installs at runtime:

| Extension | Kind | Provides |
| --- | --- | --- |
| `httpfs` | core | HTTP(S) and S3/GCS/Azure object-storage access (also used by the per-package sandbox) |
| `aws` | core | AWS credential-chain resolution for S3 |
| `azure` | core | Azure Blob Storage access |
| `postgres` | core | Postgres connections, and the DuckLake Postgres catalog |
| `ducklake` | core | DuckLake catalog attach |
| `bigquery` | community | BigQuery connections |
| `snowflake` | community | Snowflake connections |

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
