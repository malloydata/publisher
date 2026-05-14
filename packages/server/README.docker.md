# Publisher in Docker

The canonical build is the root [`Dockerfile`](../../Dockerfile) and the CI smoke test (`docker_smoke_test` in `.github/workflows/build.yml`) builds and runs that exact image. The two-port REST + MCP server, the Snowflake ADBC driver, the DuckDB CLI, and the production app bundle all ship in it.

A short Docker section in the [repo root README](../../README.md#docker) covers the canonical build + run; this doc goes deeper on runtime layout, environment variables, persistent storage, and credentials.

## Build and run

```bash
docker build -t malloy-publisher .
docker run -d \
  --name malloy-publisher \
  -p 4000:4000 -p 4040:4040 \
  -v $(pwd)/publisher.config.json:/publisher/publisher.config.json:ro \
  malloy-publisher
```

Once `/api/v0/status` reports `operationalState: "serving"`, the REST API is at `http://localhost:4000` and MCP at `http://localhost:4040/mcp`.

If you don't have a config of your own yet, copy [`packages/server/publisher.config.example.duckdb.json`](./publisher.config.example.duckdb.json) (DuckDB-only samples, no credentials required) and mount that. There's also a [`publisher.config.example.bigquery.json`](./publisher.config.example.bigquery.json) sibling for the BigQuery samples.

## Runtime layout

| Path inside container | What's there |
|---|---|
| `/publisher/` | `WORKDIR`. The server reads `<WORKDIR>/publisher.config.json` by default — that's the file you mount. |
| `/publisher/packages/server/dist/` | The bundled server (built by `bun run build` in CI). |
| `/publisher/packages/app/dist/` | The static SPA the server serves. |
| `/publisher/publisher_data/` | Per-environment package clones, DuckDB extension cache, and per-package sandbox DBs. Created at runtime; **persist this as a named volume if you want first-run sample clones to survive a container restart.** |
| `/root/.duckdb/` | DuckDB CLI + extension install dir. Bundled into the image. |

To keep `publisher_data/` across restarts:

```bash
docker run -d \
  --name malloy-publisher \
  -p 4000:4000 -p 4040:4040 \
  -v $(pwd)/publisher.config.json:/publisher/publisher.config.json:ro \
  -v publisher_data:/publisher/publisher_data \
  malloy-publisher
```

The first request after a fresh start clones sample packages from GitHub — a named volume turns that one-time cost into a one-time cost across all container lifecycles.

## Configuration via environment variables

All flags exposed by `bin/malloy-publisher --help` have an equivalent env var, so they're easy to set from `docker run -e` or compose:

| Env var | Equivalent flag | Default | Purpose |
|---|---|---|---|
| `PUBLISHER_PORT` | `--port <n>` | `4000` | REST API port. |
| `PUBLISHER_HOST` | `--host <h>` | `0.0.0.0` | Bind address. |
| `MCP_PORT` | `--mcp_port <n>` | `4040` | MCP API port. |
| `SERVER_ROOT` | `--server_root <path>` | `.` (cwd) at the server level; overridden to `/publisher` by the bundled CMD | Directory the server treats as its working dir. The image's CMD passes `--server_root /publisher` explicitly so the zero-arg `npx` bundled-default trigger doesn't fire inside the container. If you override CMD with your own entrypoint, set `SERVER_ROOT` yourself to keep this behaviour. |
| `PUBLISHER_CONFIG_PATH` | `--config <path>` | unset | Absolute path to a `publisher.config.json`. Wins over `<SERVER_ROOT>/publisher.config.json`. Use this if you want to mount your config somewhere other than `/publisher/`. |
| `INITIALIZE_STORAGE` | `--init` | `false` | Wipes persisted storage state on startup. Useful when `frozenConfig: false` has let `publisher_data/` drift from the on-disk config; destructive otherwise. |
| `SHUTDOWN_DRAIN_DURATION_SECONDS` | — | `0` | On SIGTERM, how long to keep accepting requests while draining before closing server sockets. Set this to your typical request duration to avoid 502s from K8s rolling deploys. |
| `SHUTDOWN_GRACEFUL_CLOSE_TIMEOUT_SECONDS` | — | `0` | Additional grace period after server close before `process.exit`. |
| `GOOGLE_APPLICATION_CREDENTIALS` | — | unset | Path inside the container to a GCP service-account JSON. Required for BigQuery-backed environments. Personal user credentials don't work inside the container — use a service account. |

## BigQuery credentials

To enable BigQuery samples or your own BigQuery connections, mount a service-account key and point `GOOGLE_APPLICATION_CREDENTIALS` at it:

```bash
docker run -d \
  --name malloy-publisher \
  -p 4000:4000 -p 4040:4040 \
  -v $(pwd)/publisher.config.json:/publisher/publisher.config.json:ro \
  -v $(pwd)/gcp-sa.json:/etc/publisher/gcp-sa.json:ro \
  -e GOOGLE_APPLICATION_CREDENTIALS=/etc/publisher/gcp-sa.json \
  malloy-publisher
```

The Dockerfile creates `/etc/publisher/` as an empty directory outside the application tree at `/publisher/`. By the convention this doc establishes, mount credential material there to keep it separated from the app — but any writable path inside the container works.

## The CI Dockerfile (`docker/Dockerfile.ci`)

`docker/Dockerfile.ci` exists for the CI integration-test path (referenced from the repo's `docker-compose.yml`). It is **not** the production image and should not be used for deployment. Production users build the root [`Dockerfile`](../../Dockerfile).

## Deprecated build paths

`docker/production.docker` and `docker/malloy-samples.docker` are leftover from a previous Docker layout. They are not built by CI, are not referenced by any current workflow, and produce a different image than what is deployed. Don't use them. They will be removed in a follow-up cleanup PR; the audit and tracking is in `publisher-audit-docker.md` (finding #1).
