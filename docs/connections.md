# Connections

> What this is: how Publisher reaches databases and query engines — per-package DuckDB sandboxes,
> environment-level connections, naming rules, and the SSH-bastion proxy. For the config file itself,
> see [configuration.md](configuration.md).

Publisher uses **connections** to reach databases and query engines. Connections are defined per-environment in `publisher.config.json`; each one has a unique `name` and a `type` (`bigquery`, `snowflake`, `postgres`, `mysql`, `duckdb`, `trino`, etc.) plus type-specific configuration under a matching `*Connection` key.

For full setup details per connection type, see [docs.malloydata.dev/documentation/user_guides/publishing/connections](https://docs.malloydata.dev/documentation/user_guides/publishing/connections).

## Per-package DuckDB sandboxes

Each loaded package automatically gets its own DuckDB connection named `duckdb`. These per-package sandboxes are how the bundled examples (`storefront`, `governed-analytics`, `html-data-app`) query the data files in each package without needing any user-defined connection. DuckDB reads Parquet, CSV, and Excel files in place, so `duckdb.table('data/customers.parquet')`, `duckdb.table('data/regions.csv')`, and `duckdb.table('data/budget.xlsx')` all work with no conversion step (`storefront` uses the first two). To select a sheet or pass other `read_xlsx` options, use a SQL source instead: `duckdb.sql("SELECT * FROM read_xlsx('data/budget.xlsx', sheet = 'Q2')")`.

You do not have to declare these sandboxes; they're created on package load. For the rest of the
package format, see [packages.md](packages.md).

## Environment-level DuckDB connections

You can also declare a top-level DuckDB connection at the environment level. Publisher intentionally exposes only data-source intent for these — database files, working directories, filesystem/network policy, extension loading, temp directories, and resource knobs are all owned by Publisher. The only configuration available is **attached databases**, where you declare foreign databases (BigQuery, Snowflake, Postgres, GCS, S3, Azure) that the DuckDB instance should `ATTACH` so queries can reference them.

An env-level DuckDB connection must declare at least one attached database. If you don't need to attach any foreign databases, you don't need to declare an env-level DuckDB connection at all — each loaded package already gets a per-package `duckdb` sandbox automatically (see above), which covers the plain in-memory use case.

## DuckLake connections (`type: "ducklake"`)

A `ducklake` connection attaches a [DuckLake](https://ducklake.select) lakehouse — a Postgres catalog plus an object-storage (S3/GCS) data path. Publisher attaches it lazily (on first use, never on the startup path), guarantees a derived catalog-format compatibility range, and can run fully offline. See **[ducklake.md](ducklake.md)** for the connection shape, the compatibility contract, and the DuckDB extension-provisioning / air-gapped story.

It also doubles as a materialization **destination** for the storage tier: a `#@ persist storage=<name>` source is built into it and served back from it (see [persist-storage-tutorial.md](persist-storage-tutorial.md)). The live user-facing attach is read-only; a build materializes over a transient, build-scoped read-write session. It pairs a **catalog** — a metadata database, typically Postgres — with **storage** — a `bucketUrl` for the Parquet data (an `s3://`/`gs://` URL in the cloud, or a local directory for dev):

```json
{
  "name": "lake",
  "type": "ducklake",
  "ducklakeConnection": {
    "catalog": {
      "postgresConnection": {
        "host": "…",
        "port": 5432,
        "databaseName": "ducklake_catalog",
        "userName": "…",
        "password": "…"
      }
    },
    "storage": { "bucketUrl": "/path/or/s3://bucket/prefix" }
  }
}
```

Materialized data always lands as **Parquet** in the storage bucket (Publisher disables DuckLake small-table inlining on the write path, so it never accumulates in the catalog database). The materialization tier is gated by `PERSIST_STORAGE_MODE` — see [configuration.md](configuration.md).

## Connection naming rules

A few names are reserved or have special meaning. Picking the wrong name causes a clear error at server startup:

### `duckdb` (reserved)

You cannot define a top-level connection named `duckdb`. The name is claimed by the per-package sandbox described above. Publisher errors at startup:

```
Connection name 'duckdb' is reserved for per-package sandboxes. Choose a different name
for environment-level DuckDB connections (e.g. 'shared_duckdb').
```

Use any other name for an environment-level DuckDB connection.

### Uniqueness within an environment

Connection names must be unique within a single environment. Duplicate names after the first are silently ignored (later definitions don't override earlier ones), so prefer distinct, descriptive names.

## Publisher proxy connections (`type: "publisher"`)

A `publisher` connection does not talk to a warehouse directly. Instead it
**proxies SQL to a remote Publisher dataplane** (e.g. a hosted Credible
environment), which runs the query against its own connection and returns the
rows. This is the local-dev authoring loop: run a local Publisher with
`--watch-env` to serve a package's `public/` app with live-reload, while queries
proxy to your real remote connection — no need to replicate warehouse
credentials locally.

It is the same connection type the Malloy CLI and the VS Code extensions use.

> **Disabled by default.** Because a `publisher` connection makes the server
> issue outbound HTTP requests to a configured `connectionUri`, it is a
> server-side request forgery (SSRF) surface in a hosted, multi-tenant
> deployment. The type is therefore **denied unless explicitly enabled**: set
> the environment variable `PUBLISHER_ALLOW_PROXY_CONNECTIONS=true` on the
> server process. Enable it only for trusted local `--watch-env` authoring —
> never in a shared/hosted deployment. With the flag unset, a `publisher`
> connection fails at config load with an actionable error.

To use it from the server, set `PUBLISHER_ALLOW_PROXY_CONNECTIONS=true` and add a
`publisher` connection block to the environment's `connections` in
`publisher.config.json`:

```json
{
  "name": "analytics",
  "type": "publisher",
  "publisherConnection": {
    "connectionUri": "https://org.data.credibledata.com/api/v0/environments/proj/connections/analytics",
    "accessToken": "<jwt>"
  }
}
```

- `connectionUri` (**required**) — the full URI of the remote connection.
- `accessToken` (optional) — Bearer token for the remote dataplane.

The remote dataplane owns authentication, access control, and read-only
enforcement; the proxy itself does not reject writes. A missing `connectionUri`
fails at startup with an actionable error rather than a generic
`Unsupported connection type`.

**Known limitation:** the `accessToken` is user-scoped and short-lived. The
server uses the token as configured and does not refresh it, so a long-running
`--watch-env` session can outlive the token. Token refresh/expiry is owned by the
CLI/extension today; re-issue the token and restart if queries start failing
auth.

## Reaching a database through an SSH bastion (`proxy`)

Any TCP database connection (postgres today) can carry an optional `proxy` block to
reach a database that is only routable from inside a private network. The server opens
an SSH connection to the bastion, stands up a local `127.0.0.1` forward, and points the
driver at it — the driver is unchanged. A bastion is for **reachability** into a private
VPC; it is not an IP-restriction mechanism (restrict the database directly for that).

> **Authorization & trust.** A proxy makes the server open an outbound SSH tunnel to a
> tenant-configured bastion, so it is authorized by whoever configures the connection. It
> is **not** behind an env-flag gate, and is deliberately kept separate from the
> `publisher` type's `PUBLISHER_ALLOW_PROXY_CONNECTIONS` (that flag is about
> publisher-to-publisher HTTP proxying, a different decision). Optional **host-key
> pinning** (below) adds a fail-closed trust control on the tunnel.

```json
{
  "name": "pg-via-bastion",
  "type": "postgres",
  "postgresConnection": {
    "host": "db.internal.vpc",
    "port": 5432,
    "databaseName": "analytics",
    "userName": "readonly",
    "password": "<secret>"
  },
  "proxy": {
    "type": "ssh",
    "ssh": {
      "host": "bastion.example.com",
      "port": 22,
      "username": "ec2-user",
      "privateKey": "-----BEGIN OPENSSH PRIVATE KEY-----\n…\n-----END OPENSSH PRIVATE KEY-----",
      "hostKey": "bastion.example.com ssh-ed25519 AAAA…"
    }
  }
}
```

- `postgresConnection.host`/`port` — the database address **as reachable from the
  bastion** (the in-VPC host). The connectionString form is not supported with a proxy.
- `proxy.ssh.host`/`port`/`username` — the bastion (jump host). The local forward port is
  chosen automatically; you never specify it.
- `proxy.ssh.privateKey` (+ optional `privateKeyPass`) — the customer generates the
  keypair, authorizes their own public key on the bastion, and provides the private key
  here. Public-key auth only.
- `proxy.ssh.hostKey` — **optional** pinned bastion host public key(s), verified on every
  connect (fail-closed on mismatch). Provide one or more OpenSSH `known_hosts` lines (or
  bare base64 blobs), one per line; a load-balanced/HA bastion presents a different key per
  backend, so list every backend's key and any listed key is accepted. Both plain and
  hashed (`|1|…`, from `ssh-keyscan -H`) lines work — only the key blob is compared, never
  the hostname. **When omitted, the tunnel connects without host-key verification** (the
  self-service default, matching mainstream BI tools); the SSH transport is still
  encrypted, but an unpinned publisher→bastion hop is exposed to MITM — mitigated by the
  customer allowlisting our egress on the bastion's inbound SSH.

A proxy makes the server open an outbound SSH tunnel to a tenant-configured host, so
connection configuration is the authorization boundary; host-key pinning is an optional,
additional trust control on the tunnel itself.

### TLS to the database through the tunnel

A proxied connection sets its TLS mode per-connection via `postgresConnection.sslmode`
(the non-proxied path keeps using the environment's `PGSSLMODE`). The driver connects to the
local forward endpoint (`127.0.0.1`), not the real database host, so the certificate
**hostname** can't be checked. The supported modes:

- `no-verify` (**default** when a proxy is set) — encrypt without verifying. Chosen as the
  default so a force-SSL target (the common RDS case) isn't rejected for plaintext.
- `verify-ca` — validate the server cert **chain** against the trusted CA bundle
  (`NODE_EXTRA_CA_CERTS`, e.g. the baked Amazon RDS roots) while skipping the hostname
  check. Fails if no CA bundle is available.
- `disable` — no TLS.

Full verification (`verify-full`) can't work through the tunnel until per-connection
`servername` override lands (see malloydata/malloy#2960).

## Example: mixed connections

```json
{
  "environments": [
    {
      "name": "my-env",
      "packages": [...],
      "connections": [
        {
          "name": "shared_duckdb",
          "type": "duckdb",
          "duckdbConnection": {
            "attachedDatabases": [
              {
                "name": "warehouse_pg",
                "type": "postgres",
                "postgresConnection": {
                  "host": "warehouse.example.com",
                  "port": 5432,
                  "userName": "publisher",
                  "databaseName": "analytics"
                }
              }
            ]
          }
        },
        {
          "name": "warehouse_bq",
          "type": "bigquery",
          "bigqueryConnection": { "defaultProjectId": "my-gcp-project" }
        }
      ]
    }
  ]
}
```

The package's own DuckDB sandbox (`duckdb`) remains available alongside `shared_duckdb` and `warehouse_bq`.
