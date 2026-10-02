<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Publisher in Docker

The canonical build is the root [`Dockerfile`](../../Dockerfile) and the CI smoke test (`docker_smoke_test` in `.github/workflows/build.yml`) builds and runs that exact image. The two-port REST + MCP server, the Snowflake ADBC driver, the DuckDB CLI, and the production app bundle all ship in it.

A short Docker section in the [deployment guide](../../docs/deployment.md) covers the canonical build + run; this doc goes deeper on runtime layout, environment variables, persistent storage, and credentials.

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

`serving` says the server is up, not that it loaded anything. If the config never reached `/publisher/publisher.config.json`, the server still starts and serves an empty catalog, which is a supported way to run because environments can be created over the API afterwards. Read the counts rather than the state: the server prints one `PUBLISHER_READY` line to stderr on boot, and a server that was given no config also prints a line naming the path it checked.

```
PUBLISHER_READY url=http://localhost:4000 mcp=http://localhost:4040 environments=0 packages=0 load_errors=0
```

`environments=0` when you expected packages means the config did not arrive. `load_errors=N` means it did and N entries failed to load, with the reasons in `/api/v0/status` under `loadErrors`.

If you don't have a config of your own yet, copy [`packages/server/publisher.config.example.duckdb.json`](./publisher.config.example.duckdb.json) (DuckDB-only samples, no credentials required) and mount that. There's also a [`publisher.config.example.bigquery.json`](./publisher.config.example.bigquery.json) sibling for the BigQuery samples.

## Pre-built image

If you don't want to build the image yourself, the official pre-built image is published to Docker Hub under the **`ms2data/`** namespace (not `malloydata/`):

```bash
docker pull ms2data/malloy-publisher
docker run -d \
  --name malloy-publisher \
  -p 4000:4000 -p 4040:4040 \
  -v $(pwd)/publisher.config.json:/publisher/publisher.config.json:ro \
  ms2data/malloy-publisher
```

See the [Docker Hub tags page](https://hub.docker.com/r/ms2data/malloy-publisher/tags) for available versions. Tag-scheme guidance (`:latest`, `:X.Y.Z`) lives in the [deployment guide](../../docs/deployment.md).

## Runtime layout

| Path inside container | What's there |
|---|---|
| `/publisher/` | `WORKDIR`. The server reads `<WORKDIR>/publisher.config.json` by default — that's the file you mount. |
| `/publisher/packages/server/dist/` | The bundled server (built by `bun run build` in CI). |
| `/publisher/packages/app/dist/` | The static SPA the server serves. |
| `/publisher/publisher_data/` | Per-environment package clones, DuckDB extension cache, and per-package sandbox DBs. Created at runtime; **persist this as a named volume if you want first-run sample clones to survive a container restart.** |
| `/home/bun/.duckdb/` | DuckDB CLI + extension install dir. Bundled into the image. |

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

For the same pattern as a complete Compose file (with a healthcheck against `/api/v0/status` and both ports mapped), see [`docker-compose.example.yml`](../../docker-compose.example.yml) at the repo root.

## The server runs as a non-root user

The image runs the server as `bun`, uid 1000 and gid 1000, the user the `oven/bun` base image ships. Its `USER` is the numeric `1000:1000`, so a Kubernetes pod with `runAsNonRoot: true` starts without also setting `runAsUser`.

Every application file is root-owned, so the server cannot modify one in place. It writes to three places, all owned by uid 1000:

- `/publisher/`, the server root, where it creates `publisher.db`. Because it owns this directory, the server can also rename aside and replace any of its top-level entries: `package.json`, `bun.lock`, and the `packages/` and `node_modules/` directories. Such a change lasts as long as the container. Some storage drivers, Docker's default overlayfs among them, refuse to rename a directory that comes from an image layer, but that is the driver's limit, not the image's.
- `/publisher/publisher_data/`.
- `/home/bun/.duckdb/extensions/`, for an extension the image did not bake.

What you mount has to be writable by uid 1000 too:

- **A new named volume on `/publisher/publisher_data`** works as-is. Docker seeds an empty named volume from the image's directory, ownership included, and that is the only writable mount point the image prepares.
- **A new named volume anywhere else** starts owned by root, because the image has no directory there to copy ownership from. That includes a DuckLake storage destination whose `bucketUrl` is a local path. Prepare it yourself: build a derived image with `RUN mkdir -p /path && chown 1000:1000 /path` (new named volumes there are then seeded correctly), chown the volume once with `--user 0`, or bind-mount a host directory owned by uid 1000. DuckDB reports the unprepared case as `No such file or directory` (for example `Failed to create directory "/data/lake/main/daily_orders"`), not as `EACCES`. The one-time chown names the mount path twice, as the mount target and as chown's argument:

  ```bash
  docker run --rm --user 0 --entrypoint chown \
    -v <volume>:/data/lake \
    ms2data/malloy-publisher -R 1000:1000 /data/lake
  ```

- **A named volume an older, root-run image already wrote to** holds root-owned files, and the server cannot write to them. It still reaches `serving`, but each environment it cannot write is missing from the catalog, and `GET /api/v0/status` lists it under `loadErrors` with an `EACCES: permission denied` message. Chown the volume once, before starting the new image. With `docker run`, name the volume you mount:

  ```bash
  docker run --rm --user 0 --entrypoint chown \
    -v publisher_data:/publisher/publisher_data \
    ms2data/malloy-publisher -R 1000:1000 /publisher/publisher_data
  ```

  With Compose, do not use that command: Compose prefixes the volume with the project name (`<project>_publisher_data`), so `-v publisher_data:` creates a new, empty volume, chowns it, and exits 0 while the real one stays root-owned. Run it through Compose instead, from the directory holding your `docker-compose.yml`, so the service's own volume is mounted:

  ```bash
  docker compose run --rm --no-deps --user 0 --entrypoint chown \
    publisher -R 1000:1000 /publisher/publisher_data
  ```

  `docker volume ls` shows the volume's full name if you would rather use the `docker run` form.

- **A bind mount** keeps the host directory's ownership. On Linux, `chown -R 1000:1000` the host directory. Docker Desktop on macOS and Windows maps ownership for you. Running the container as some other uid to match the host is not a substitute: `/home/bun` is private to uid 1000, so that uid cannot read the baked DuckDB extensions.
- **A Kubernetes PersistentVolume** is not seeded from the image, so even a new one on `/publisher/publisher_data` starts owned by root (an `emptyDir` is world-writable and needs nothing). Set `fsGroup: 1000` in the pod's `securityContext`, which makes the volume group-writable by gid 1000, and `fsGroupChangePolicy: OnRootMismatch` so the kubelet does not re-chown a large volume on every start. `fsGroup` is also what makes a mounted Secret key file, or a projected service-account token, readable by uid 1000 where the platform mounts them `0600`.
- **A read-only mount** only needs to be readable by uid 1000. That covers the config file, a package `location`, a directory of package zips (a `.zip` location is extracted into `publisher_data/`, never beside the archive), and the key file `GOOGLE_APPLICATION_CREDENTIALS` names. A key file bound from a host path that does not exist arrives as a directory, and the server says so. The usual trap is the key file itself: a `gcloud` application-default credentials file is `0600` and owned by you, so uid 1000 cannot read it through a bind on Linux. Bind a copy it can read, or grant the group:

  ```bash
  install -o 1000 -g 1000 -m 0400 ~/.config/gcloud/application_default_credentials.json ./secrets/key.json
  # or, keeping the original in place:
  chgrp 1000 key.json && chmod 0640 key.json
  ```

  For a directory, `chmod -R o+rX <dir>` or the `chgrp 1000` / `g+rX` equivalent.
- **Ownership is right and the server still reports `EACCES`:** on SELinux hosts (Fedora, RHEL) a bind mount needs the `:z` (shared) or `:Z` (private) option, or every access is refused whatever the owner. Under rootless Docker, Podman, or `userns-remap`, uid 1000 in the container is a subordinate uid on the host, so a host `chown 1000` names the wrong owner; use `podman unshare chown -R 1000:1000 <dir>`, or the remapped uid.

To check a mount before starting the server, run the probe as the image's user:

```bash
docker run --rm --entrypoint sh -v <mount> ms2data/malloy-publisher \
  -c 'id; touch <path>/.probe && rm <path>/.probe && echo writable'
```

For a read-only mount, the same with `cat <file> >/dev/null && echo readable`.

When the server cannot make a write it needs, the response names the errno (`EACCES: permission denied, mkdir '…'`) rather than answering a bare `Internal server error.`. A package that cannot be mounted at boot, or added at runtime, is listed under `loadErrors` in `GET /api/v0/status` with that message, until it is added successfully or deleted. If the server cannot start at all, for example on a read-only root filesystem or with a config file it cannot read, `/api/v0/status` stays `initializing` and `initError` says why.

If you cannot change the ownership yet, `--user 0` runs the server as root, as earlier images did. The image sets `HOME=/home/bun`, so a root run still finds the baked DuckDB extensions.

## Configuration via environment variables

All flags exposed by `bin/malloy-publisher --help` have an equivalent env var, so they're easy to set from `docker run -e` or compose:

| Env var | Equivalent flag | Default | Purpose |
|---|---|---|---|
| `PUBLISHER_PORT` | `--port <n>` | `4000` | REST API port. |
| `PUBLISHER_HOST` | `--host <h>` | `0.0.0.0` | REST bind address, and the fallback for MCP. |
| `MCP_HOST` | `--mcp_host <h>` | `127.0.0.1` | MCP bind address. Takes precedence over `PUBLISHER_HOST`. |
| `MCP_CORS_ORIGINS` | | (none) | Comma-separated origins allowed cross-origin access to MCP; `*` allows any. Unset means none. |
| `MCP_PORT` | `--mcp_port <n>` | `4040` | MCP API port. |
| `PUBLISHER_NO_MCP_CONFIG` | `--no-mcp-config` | `1` **in this image** | Suppresses the `.mcp.json` the server otherwise writes into its working directory on startup. That file exists so an AI agent opened in that directory finds the server; nothing starts an agent session inside the container, and the git-working-tree guard that would normally cover `/publisher` cannot fire because `.dockerignore` excludes `.git`. Left on, every boot would create a file there, which matters if you bind-mount a project directory at `/publisher`. Pass `-e PUBLISHER_NO_MCP_CONFIG=` to turn it back on. Note this is the one env var the image sets for you: `docker run -e PUBLISHER_NO_MCP_CONFIG` (no `=`) and a Compose `environment:` entry with no value both *delete* it when the host does not have it set, which re-enables the write. |
| `SERVER_ROOT` | `--server_root <path>` | `.` (cwd) at the server level; overridden to `/publisher` by the bundled CMD | Directory the server treats as its working dir. The image's CMD passes `--server_root /publisher` explicitly so the zero-arg `npx` bundled-default trigger doesn't fire inside the container. If you override CMD with your own entrypoint, set `SERVER_ROOT` yourself to keep this behaviour. |
| `PUBLISHER_CONFIG_PATH` | `--config <path>` | unset | Absolute path to a `publisher.config.json`. Wins over `<SERVER_ROOT>/publisher.config.json`. Use this if you want to mount your config somewhere other than `/publisher/`. |
| `INITIALIZE_STORAGE` | `--init` | `false` | Wipes `publisher_data/` and re-syncs it from the config on boot. A first boot with empty storage loads the config automatically, so set this only to reset state or resync after the on-disk config has drifted from `publisher_data/`. Re-initializing discards any state there that isn't reproducible from the config. See [configuration.md](../../docs/configuration.md#environment-variables--cli-flags). |
| `SHUTDOWN_DRAIN_DURATION_SECONDS` | `--shutdown_drain_duration_seconds <s>` | `0` | On SIGTERM, how long to keep serving requests (readiness flips to not-ready immediately) before closing server sockets. Set this to your typical request duration to avoid 502s from K8s rolling deploys. |
| `SHUTDOWN_GRACEFUL_CLOSE_TIMEOUT_SECONDS` | `--shutdown_graceful_close_timeout_seconds <s>` | `0` | Additional grace period after server close before `process.exit`. |
| `GOOGLE_APPLICATION_CREDENTIALS` | — | unset | Path inside the container to a GCP service-account JSON. Required for BigQuery-backed environments. Personal user credentials don't work inside the container — use a service account. |
| `PUBLISHER_MAX_MEMORY_BYTES` | — | unset (disabled) | Resident-set-size (RSS) cap in bytes. When set, the in-process **memory governor** polls RSS on `PUBLISHER_MEMORY_CHECK_INTERVAL_MS` and rejects new package loads and new queries with **HTTP 503** once RSS crosses the high-water mark. Designed to keep the pod under its k8s `resources.limits.memory` instead of getting OOM-killed. Set this to roughly `0.7 × resources.limits.memory` so the back-pressure band has headroom for traffic spikes and per-request DuckDB scratch. |
| `PUBLISHER_MEMORY_HIGH_WATER_FRACTION` | — | `0.8` | Fraction of `PUBLISHER_MAX_MEMORY_BYTES` at which back-pressure activates. Must be in `(0, 1)` and strictly greater than the low-water fraction. |
| `PUBLISHER_MEMORY_LOW_WATER_FRACTION` | — | `0.7` | Fraction at which back-pressure clears. The gap between low and high gives hysteresis so the governor doesn't flap on every GC cycle. |
| `PUBLISHER_MEMORY_CHECK_INTERVAL_MS` | — | `5000` | How often the governor samples RSS. Minimum `100`. Smaller values catch spikes faster but burn a few extra microseconds per tick. |
| `PUBLISHER_MEMORY_BACKPRESSURE` | — | `true` | When `false`, the governor still samples RSS and emits metrics but never flips the back-pressure flag. Useful for a monitoring-only rollout before enabling the 503 behaviour. |
| `EMBEDDING_API_KEY` | — | _unset_ | Enables semantic ranking for `get_context` question retrieval; sent as a bearer token to the embedding endpoint. Unset keeps lexical retrieval, unchanged. Entity names, annotation text, and query strings are sent to the endpoint when enabled; see "Semantic retrieval" in `docs/configuration.md`. |
| `EMBEDDING_MODEL` | — | `text-embedding-3-small` | Embedding model name. |
| `EMBEDDING_API_BASE` | — | `https://api.openai.com/v1` | Base URL of an OpenAI-compatible embeddings API. |
| `EMBEDDING_DIMENSIONS` | — | _unset_ | Optional `dimensions` request parameter; omitted when unset. |

### Memory governor

When `PUBLISHER_MAX_MEMORY_BYTES` is unset, the governor is **disabled** and the server's behaviour is identical to prior versions. When it's set, the governor:

- Periodically samples `process.memoryUsage().rss`.
- Once RSS crosses the high-water mark, **any code path that would allocate a new package into memory returns HTTP 503**, and new queries are rejected the same way. The package gate sits at the single choke point inside `Environment.getPackage` / `Environment.addPackage`, so it covers every controller that touches a not-yet-loaded package — including lazy loads on cache miss from `ModelController`, `ConnectionController`, `QueryController`, `DatabaseController`, etc. — not just the explicit `POST /packages` and `?reload=true` paths.
- Already-loaded packages remain fully serviceable so dashboards keep rendering under pressure.
- Once RSS drops back to the low-water mark, back-pressure clears automatically.
- Recovery happens naturally as in-flight traffic completes and the kernel reclaims pages — the governor does **not** evict, unload, or interrupt loaded packages.
- A documented `{ allowAdmission: true }` opt-out exists on `Environment.getPackage` / `addPackage` for future internal callers (e.g. warmup / health probes) that genuinely cannot tolerate 503s. No public REST endpoint sets it today.

Metrics exposed on the existing `/metrics` Prometheus endpoint:

| Metric | Type | Notes |
|---|---|---|
| `publisher_process_rss_bytes` | gauge | Sampled RSS. |
| `publisher_memory_backpressure_active` | gauge | `1` when rejecting new loads, `0` otherwise. |
| `publisher_memory_backpressure_activations_total` | counter | Increments on every `false → true` transition; alert on a non-trivial rate to catch flapping pods. |
| `publisher_memory_max_bytes`, `publisher_memory_high_water_bytes`, `publisher_memory_low_water_bytes` | gauges | Static configured thresholds — useful for plotting the band alongside the RSS series. |

#### Recommended k8s sizing

A reasonable starting point (tune for your workload):

```yaml
resources:
  requests:
    memory: 2Gi
  limits:
    memory: 4Gi
env:
  - name: PUBLISHER_MAX_MEMORY_BYTES
    # 2.8Gi — back-pressure activates at ~2.24Gi, clears at ~1.96Gi,
    # leaving ~1.2Gi of headroom under the 4Gi k8s hard limit for
    # in-flight DuckDB scratch and JS heap spikes.
    value: "3006477107"
  - name: PUBLISHER_MEMORY_BACKPRESSURE
    value: "true"
```

If you want a soft-launch where the governor reports but doesn't act, deploy first with `PUBLISHER_MEMORY_BACKPRESSURE=false`, watch the `publisher_process_rss_bytes` series for a week, then enable.

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

`docker/production.docker` and `docker/malloy-samples.docker` are leftover from a previous Docker layout. They are not built by CI, are not referenced by any current workflow, and produce a different image than what is deployed. Don't use them — build the root [`Dockerfile`](../../Dockerfile) instead. They will be removed in a follow-up cleanup PR.
