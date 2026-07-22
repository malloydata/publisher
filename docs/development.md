# Development

> What this is: how to build, run, and hack on Publisher from a clone. For deploying a built server
> (Docker, config, tuning), see [deployment.md](deployment.md) and [configuration.md](configuration.md).

## Prerequisites

| Tool | Version | Required for |
| --- | --- | --- |
| [Bun](https://bun.sh/) | ≥ 1.3.13 | Primary runtime + package manager |
| [Node.js](https://nodejs.org/) | ≥ 20 | DuckDB postinstall scripts and the `npx @malloy-publisher/server` bin shebang |
| [Python](https://www.python.org/) | ≥ 3.12 | Only if you build the Python client (`packages/python-client`) |
| Java | ≥ 21 (Corretto recommended) | Only if you regenerate API clients via `bun run generate-api-types` |

The repo ships a `.tool-versions` file compatible with [mise](https://mise.jdx.dev/) and
[asdf](https://asdf-vm.com/), so `mise install` (or `asdf install`) provisions all four versions at
once.

Sample packages are read from [`publisher.config.json`](../packages/server/publisher.config.json), so
no submodule checkout is needed. From a clone, that config points at the local [`examples/`](../examples)
directories, which are DuckDB-backed (`storefront`, `governed-analytics`, `html-data-app`) and need no
cloud credentials. Nothing is fetched on first boot. (The published `npx` build has no repo to read, so
its bundled default fetches the same packages from GitHub instead.) To enable the BigQuery-required
samples, see [configuration.md](configuration.md#bring-your-own-config).

> **Editing the examples locally.** Because the config points at `examples/`, the server already serves
> your working copy, not the committed versions on `main`. To have edits hot-reload, start in watch mode:
>
> ```bash
> bun run build && bun run start -- --watch-env examples
> ```
>
> (or set `PUBLISHER_WATCH=examples`). Watch mode mounts the packages in place as symlinks, which happens
> when the environment is first loaded: on a fresh server root, or on any boot with `--init`. If you
> already started the env without `--watch-env`, its packages were copied into `publisher_data/` and
> edits to `examples/` will not show up, so run once with both flags together, `--watch-env examples
> --init`, to re-mount them. The `governed-analytics` and `html-data-app` READMEs also have a
> self-contained "Run it standalone" recipe that mounts just that package from a `/tmp` workspace. After
> changing an example's data generator, re-run `bun run generate:example-data` to refresh the Parquet
> and CSV files.

## Makefile shortcuts

A top-level `Makefile` wraps the common workflows so you don't have to remember script names or `cd`
into individual packages. Run `make help` for the full list. The most useful targets:

| Target | What it does |
| --- | --- |
| `make install` | `bun install` at the repo root |
| `make build` | Production build: SDK → app → server bundle |
| `make start` / `make start-init` | Run the built server (`--init` clears persisted storage on boot) |
| `make stop` | Kill anything on ports `:4000` or `:4040` |
| `make dev` | **Express + Vite together** in one terminal with prefixed `[server]`/`[react]` logs (Ctrl+C kills both) |
| `make dev-server` / `make dev-react` | Same dev workflow, split into two terminals |
| `make status` / `make environments` / `make packages` | Quick API smoke checks |
| `make test` / `make lint` / `make typecheck` / `make format` | Quality gates |
| `make regen-api` | Regenerate server + SDK clients from `api-doc.yaml` (needs Java) |

## Production build

One command builds the SDK, app, and server bundle in order:

```bash
make install
make build
make start                # Run the built server (REST on :4000, MCP on :4040)
```

Or run the underlying `bun` scripts directly: `bun install && bun run build && bun run start`
(`bun run build:server-deploy` is the same build with the app bundle produced by the server's
build pipeline — `make build` uses it).

## Dev mode

Express and Vite run as separate processes. Express on `:4000` proxies non-API traffic to Vite on
`:5173` when `NODE_ENV=development`, so visit `http://localhost:4000` for the full app — `:5173`
won't have API access.

**One terminal (recommended):**

```bash
make dev
```

This runs both servers with combined, color-prefixed logs (`[server]` / `[react]`). Ctrl+C stops
both cleanly.

**Two terminals (if you prefer split logs):**

```bash
make dev-server          # Express (REST :4000 + MCP :4040, watch mode)
```

```bash
make dev-react           # Vite dev server (:5173, proxied through :4000)
```

Open http://localhost:4000.

## Tests and quality gates

```bash
make test                # unit + integration server tests
make lint && make format # eslint + prettier
make typecheck           # tsc --noEmit across sdk/app/server
```

`make typecheck` (and the underlying `bun run typecheck`) depends on the SDK's emitted `.d.ts` files,
which in turn depend on the OpenAPI codegen. On a fresh clone, build first — either with `make build`
(full SDK + app + server bundle), or with the targeted minimum:

```bash
bun install
bun run generate-api-types
bun run build:sdk
bun run typecheck
```

After that, `bun run typecheck` works on its own as long as the SDK build artifacts stay current:

- After editing `api-doc.yaml` → re-run `bun run generate-api-types && bun run build:sdk`.
- After editing SDK source → re-run `bun run build:sdk`.

## Contributing

See [CONTRIBUTING.md](../CONTRIBUTING.md) for committers, the Developer Certificate of Origin
sign-off, code-review process, and the Python SDK regeneration workflow.
