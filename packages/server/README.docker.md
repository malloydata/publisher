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

See the [Docker Hub tags page](https://hub.docker.com/r/ms2data/malloy-publisher/tags) for available versions. Tag-scheme guidance (`:latest`, `:X.Y.Z`, `:next`) lives in the [deployment guide](../../docs/deployment.md).

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

For the same pattern as a complete Compose file (with a healthcheck against `/api/v0/status` and both ports mapped), see [`docker-compose.example.yml`](../../docker-compose.example.yml) at the repo root.

## Configuration via environment variables

All flags exposed by `bin/malloy-publisher --help` have an equivalent env var, so they're easy to set from `docker run -e` or compose:

| Env var | Equivalent flag | Default | Purpose |
|---|---|---|---|
| `PUBLISHER_PORT` | `--port <n>` | `4000` | REST API port. |
| `PUBLISHER_HOST` | `--host <h>` | `0.0.0.0` | Bind address. |
| `MCP_PORT` | `--mcp_port <n>` | `4040` | MCP API port. |
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
