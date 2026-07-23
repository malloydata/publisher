# Deployment

> What this is: how to run a built Publisher server — via `npx`, Docker, or Docker Compose — and how
> to read its lifecycle state. For the full config/env-var reference, see
> [configuration.md](configuration.md). To build from a clone, see [development.md](development.md).

## Run with npx

```bash
npx @malloy-publisher/server --port 4000
```

Open http://localhost:4000 to explore the bundled example packages. `storefront`,
`governed-analytics`, and `html-data-app` are cloned from GitHub on first launch — expect a short wait
before `operationalState` reports `serving`. No credentials required.

Run one first launch at a time: two `npx` first runs installing concurrently can race in the shared
npx cache and corrupt the install. If the command stops working after an interrupted or doubled-up
first run, delete the cache under `~/.npm/_npx` and run it again.

## Verify it's working

```bash
curl -s http://localhost:4000/api/v0/status | jq .operationalState   # → "serving"
curl -s http://localhost:4000/api/v0/environments | jq '.[].name'    # → list of environments
```

`operationalState` reports the current server lifecycle:

- **`serving`** — ready to handle requests.
- **`initializing`** — loading packages and connections from `publisher.config.json`. Normal on boot,
  and especially noticeable on the first run when sample packages need to be cloned from GitHub. Wait
  for `serving`.
- **`draining`** — graceful shutdown in progress: the server is waiting for in-flight requests to
  finish before closing. Controlled by `SHUTDOWN_DRAIN_DURATION_SECONDS` and
  `SHUTDOWN_GRACEFUL_CLOSE_TIMEOUT_SECONDS` (see [configuration.md](configuration.md)).
- **`throttled`** — the memory governor has hit its back-pressure limit and is rejecting new package
  loads and queries to stay under `PUBLISHER_MAX_MEMORY_BYTES`. Already-loaded packages remain
  serviceable; the control plane should treat the worker as unhealthy for new load until memory
  drops. Only reported when the memory governor is enabled (see
  [configuration.md](configuration.md#operational-tuning-oom-guards)).

### `serving` does not mean everything loaded

A package or environment that fails to load is skipped rather than fatal, so the server still reports
`serving` and simply serves whatever did load. Check `loadErrors` before you treat a `serving` server
as complete:

```bash
curl -s http://localhost:4000/api/v0/status | jq .loadErrors
```

The field is absent when everything loaded. Otherwise it lists what is missing and why:

```json
[
  {
    "environment": "local",
    "package": "sales",
    "message": "Failed to mount local directory: /home/me/my-data/sales-models"
  },
  {
    "environment": "examples",
    "package": "storefront",
    "message": "Package manifest for /publisher_data/examples/storefront does not exist."
  }
]
```

An entry with a `package` names a package that failed to load (a location that would not mount, a
missing `publisher.json`, a compile error) while its sibling packages keep serving. An entry with
no `package` means the environment itself failed to initialize, and its packages are absent with
it. Either way the fix is usually the `location` in `publisher.config.json`, or a missing
`publisher.json` in the package directory.

## Docker

Two ways to run Publisher in Docker: build the image from source, or pull the pre-built image from
Docker Hub. Either way, the container's `WORKDIR` is `/publisher` (mount your `publisher.config.json`
there), REST is on `:4000`, and MCP is on `:4040`.

### Build from source

```bash
docker build -t malloy-publisher .
docker run -d \
  -p 4000:4000 -p 4040:4040 \
  -v $(pwd)/publisher.config.json:/publisher/publisher.config.json:ro \
  malloy-publisher
```

If you don't have a config yet, copy
[`packages/server/publisher.config.example.duckdb.json`](../packages/server/publisher.config.example.duckdb.json)
as a starting point — it loads the same three DuckDB example packages the server ships with
(no credentials needed).

### Pre-built image

The official pre-built image is published to Docker Hub at
[`ms2data/malloy-publisher`](https://hub.docker.com/r/ms2data/malloy-publisher).

```bash
docker pull ms2data/malloy-publisher
docker run -d \
  -p 4000:4000 -p 4040:4040 \
  -v $(pwd)/publisher.config.json:/publisher/publisher.config.json:ro \
  ms2data/malloy-publisher
```

**Tags:**

- `:latest` — most recent stable release.
- `:X.Y.Z` — pinned to a specific release; recommended for production.
- `:next` — pre-release builds; not recommended for production.

`*-dev` tags (e.g. `:0.0.198-dev`) are frozen — no new ones are being published, and `:next` is the
current pre-release channel. Existing `*-dev` tags still resolve in the registry; don't use them for
new deployments.

### Docker Compose

A ready-to-use Compose file lives at
[`docker-compose.example.yml`](../docker-compose.example.yml) — it runs the pre-built image with both
ports mapped, a healthcheck against `/api/v0/status`, and a named volume for `publisher_data/` so
first-boot package clones survive restarts. To use it:

1. Copy it into your project: `cp docker-compose.example.yml docker-compose.yml`.
2. Place a `publisher.config.json` next to it (or change the volume mount). No config of your own yet?
   Copy [`packages/server/publisher.config.example.duckdb.json`](../packages/server/publisher.config.example.duckdb.json) —
   DuckDB-only samples, no credentials needed.
3. `docker compose up -d`

For env-var configuration, persistent `publisher_data/` volumes, and advanced options, see
[`packages/server/README.docker.md`](../packages/server/README.docker.md).
