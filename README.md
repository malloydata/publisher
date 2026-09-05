<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

<h1 align="center">Malloy Publisher</h1>

<p align="center"><b>The open-source semantic model server for <a href="https://malloydata.dev">Malloy</a></b><br>
Serve governed data models to applications, BI tools, and AI agents — over REST and MCP.<br>
<sub>Created and maintained by <a href="https://www.credibledata.com">Credible</a>, the company behind the AI Analytics Engine.<br>
AI agents: read <a href="AGENTS.md">AGENTS.md</a> first.</sub></p>

<p align="center">
  <a href="https://github.com/malloydata/publisher/actions/workflows/build.yml"><img src="https://github.com/malloydata/publisher/actions/workflows/build.yml/badge.svg" alt="build"></a>
</p>

<p align="center">
  <a href="https://github.com/user-attachments/assets/376a809d-8016-41a7-9464-a5634ea0589d"><img src="docs/malloy-publisher-demo.gif" alt="Malloy Publisher serving the bundled storefront dashboard" width="800"></a>
</p>
<p align="center"><sub>A 60-second walkthrough — model in your IDE with the Malloy skills, serve with Publisher, build a data app, materialize on a schedule, and analyze. <a href="https://github.com/user-attachments/assets/376a809d-8016-41a7-9464-a5634ea0589d">Watch the video</a> for playback controls.</sub></p>

**Problem:** pointed at a raw database, an AI writes SQL from scratch — the wrong join, an invented
column, a fan-out that double-counts but still looks plausible — and the same question tomorrow
yields a different query and different numbers.

**Solution:** put a [Malloy](https://malloydata.dev) model — what the industry calls a semantic
layer — between the AI and your data. Measures, dimensions, and joins are defined once, correctly;
applications, BI tools, and **AI agents** compose queries against the model instead of writing SQL,
so the numbers come back **right by construction**, the same way every time.

Publisher is the open-source server that serves that model, over a REST API and a single MCP endpoint.

- **Any AI, one endpoint** — Claude, Cursor, Codex, VS Code, or an agent you build connect over MCP;
  an agent running unattended uses REST.
- **Tight control** — agents work through the sources the model defines, never your raw tables.
  [Givens](docs/givens.md), [row-level access](docs/row-level-access.md), and
  [`#(authorize)`](docs/authorize.md) decide who sees what; [discovery curation](docs/discovery-and-access.md)
  decides what is even visible.
- **Readable, full-featured queries** — Malloy joins, nests, aggregates, and filters, and stays legible
  enough to review at a glance. Agents already write it as fluently as Python.
- **DuckDB built in** — serve CSV, Parquet, JSON, or Excel files with no warehouse required, or connect
  BigQuery, Snowflake, Postgres, Databricks, MotherDuck, and more.
- **Fast where it counts** — one `#@ persist` annotation materializes an expensive source into a table,
  rebuilt on demand or on a schedule.
- **Ships with the skills** — the open-source Malloy modeling and analysis [skills](skills/) agents use
  to build and query models, auto-discovered by most AI coding hosts.
- **Runs anywhere** — `npx`, Docker, or Compose, in minutes.

## Requirements

Node.js 20 or newer (the server refuses to start on anything older and says so). Building from a clone
also needs [Bun](https://bun.sh/) 1.3.13+. The Docker image carries its own runtime and needs neither.

## Quick start

### Run the examples

```bash
npx @malloy-publisher/server --port 4000
```

Open **http://localhost:4000**. Three example packages are bundled — [`storefront`](examples/storefront)
(a complete ecommerce model), [`governed-analytics`](examples/governed-analytics) (access control), and
[`html-data-app`](examples/html-data-app) (a no-build dashboard) — all DuckDB-backed, no credentials
required. The first run fetches them from GitHub.

### Know when it's ready

The server prints one line to stderr when it is serving, which scripts can wait for instead of polling:

```
PUBLISHER_READY url=http://localhost:4000 mcp=http://localhost:4040 environments=1 packages=3 load_errors=0
```

`load_errors` counts packages and environments that failed to load; `/api/v0/status` names them. The
failure lines, and what each means, are in
[docs/configuration.md](docs/configuration.md#startup-signals).

## Start from your own data

### Scaffold a package

```bash
mkdir my-data && cd my-data
npm create @malloy-publisher/malloy-package@latest sales
npm start
```

This writes a package to `./sales` — plus, in the current directory, a small workspace: start and
reset scripts, an MCP config, agent instructions, and the Malloy skills as files your agent can read.
`npm start` serves the package in watch mode, so edits take effect as you save. Keep the `@latest`:
without it npm may reuse a cached, older scaffolder.

### Bring a file

```bash
npm create @malloy-publisher/malloy-package@latest sales -- --data ./orders.csv
```

CSV, Parquet, JSON, newline-delimited JSON, or Excel `.xlsx` — DuckDB reads all of them in place. The
`--` is required, and the path is relative to where you run the command. A seeded package starts
small: a row count and an overview, which is the moment to point an agent at it.

A package is just Malloy, so it is not limited to a file: point its model at a
[database connection](docs/connections.md) and the same workspace serves a warehouse. The finer points
of the scaffolder — caching, workspace layout, the bare `npx` form — are in
[docs/scaffolding.md](docs/scaffolding.md).

## Point your agent at it

### Connect Claude Code

In one terminal:

```bash
npx @malloy-publisher/server --port 4000 --host 127.0.0.1
```

On startup the server writes a `.mcp.json` into the directory you ran it in, pointing at the MCP port
it bound. In a second terminal, **in that same directory**:

```bash
claude
```

Say yes when the agent asks to trust the folder, use the server it found, and approve the first tool
call — the trust prompt is asked once per directory, and only interactively, so a headless run can't
clear it. Then ask, in plain English:

> _"Use Malloy to explore the storefront sales data and chart revenue by category."_

The agent discovers what exists (`malloy_getContext`), grounds itself in real source, view, and field
names, runs the query (`malloy_executeQuery`), and answers from your model — no schema spelunking, no
hallucinated columns.

### When the config isn't written

The server skips a directory that already has a `.mcp.json`, anything inside a git working tree, and
your home directory, and says so in its startup log along with the one command that connects an agent
anyway. To register the server for yourself rather than one directory:

```bash
claude mcp add --transport http malloy http://127.0.0.1:4040/mcp -s user
```

That is also the fix when an agent reports no `malloy_*` tools. The file the server writes outlives
it and is never corrected, so a stale one can point an agent at whatever later holds that port —
[docs/configuration.md](docs/configuration.md#the-mcpjson-the-server-writes) has the full story.
`--no-mcp-config` turns the whole thing off.

### Other clients, and unattended agents

Cursor, VS Code, Codex, and Claude Desktop take the same endpoint through their own config; see
[docs/ai-agents.md](docs/ai-agents.md). An agent working unattended that started the server itself uses
the same loop over REST:

```bash
curl -s -X POST \
  http://localhost:4000/api/v0/environments/examples/packages/storefront/models/storefront.malloy/query \
  -H 'content-type: application/json' \
  -d '{"query":"run: order_items -> by_category","compactJson":true}' | jq -r .result
```

The running server serves its full OpenAPI spec at `http://localhost:4000/api-doc.yaml`.

### Starting from a database instead of a model

Have a warehouse but no model yet? Add a [connection](docs/connections.md) and ask the agent what is
in it: `malloy_searchDatabaseSchema` ranks a connection's tables against a plain-English description
and hands back the `source:` line for each. Ranking needs no API key; the optional embedding-backed
mode is in [docs/configuration.md](docs/configuration.md#semantic-ranking-for-malloy_searchdatabaseschema).

> **Security.** The server — MCP and REST alike — is stateless and unauthenticated, and it can read any
> data your models connect to. Bind it to loopback (`--host 127.0.0.1`) for local use, and put an
> authenticating gateway in front before exposing it more widely.

## What you can do

- **Explore, no code.** Build and drill into queries visually with [Malloy Explorer](docs/explorer.md);
  every action generates valid Malloy, so metrics stay correct across joins.
- **Answer questions with AI.** Connect an agent over MCP and ask in plain English —
  [docs/ai-agents.md](docs/ai-agents.md).
- **Surface analytics your way.** Explore and share in the [Publisher Console](docs/console.md), or
  ship a no-build [HTML data app](docs/html-data-apps.md) that Publisher hosts inside a package.
- **Build and validate models.** Author with the bundled [skills](skills/), then publish. Agents get the
  same loop over MCP: `malloy_compile` checks an edit without running it; `malloy_reloadPackage`
  recompiles a package from disk, no restart.
- **Govern access.** [Givens](docs/givens.md) power filter widgets,
  [row-level access](docs/row-level-access.md), and [`#(authorize)`](docs/authorize.md) gates; curate
  what is [discoverable and queryable](docs/discovery-and-access.md) separately.
- **Materialize for cost and speed.** `#@ persist` turns an expensive source into a table, rebuilt on
  demand or on a cron with the opt-in scheduler — [docs/materialization.md](docs/materialization.md).

## Documentation

The [`docs/`](docs/) folder is the reference hub — see its [index](docs/README.md). Highlights:

| Topic                                               | Doc                                                                                                                                                                                      |
| --------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Runnable example packages                           | [examples/](examples/) ([storefront](examples/storefront) · [governed-analytics](examples/governed-analytics) · [html-data-app](examples/html-data-app) · [data-app](examples/data-app)) |
| Architecture & how it fits together                 | [docs/architecture.md](docs/architecture.md)                                                                                                                                             |
| REST & MCP API overview                             | [docs/api-overview.md](docs/api-overview.md)                                                                                                                                             |
| The package format (`publisher.json`, models, data) | [docs/packages.md](docs/packages.md)                                                                                                                                                     |
| The Publisher Console (navigation & features)       | [docs/console.md](docs/console.md)                                                                                                                                                       |
| No-code visual query builder                        | [docs/explorer.md](docs/explorer.md)                                                                                                                                                     |
| Connect an AI agent (MCP, or REST when unattended)  | [docs/ai-agents.md](docs/ai-agents.md)                                                                                                                                                   |
| Build a custom UI (no build step)                   | [docs/html-data-apps.md](docs/html-data-apps.md)                                                                                                                                         |
| Runtime parameters & access control                 | [givens](docs/givens.md) (base) · [row-level](docs/row-level-access.md) · [authorize](docs/authorize.md) · [discovery](docs/discovery-and-access.md)                                     |
| Deploy (npx / Docker / Compose)                     | [docs/deployment.md](docs/deployment.md)                                                                                                                                                 |
| Database connections                                | [docs/connections.md](docs/connections.md)                                                                                                                                               |
| Materialization & scheduling                        | [docs/materialization.md](docs/materialization.md)                                                                                                                                       |
| Docker runtime deep-dive (layout, env, tuning)      | [packages/server/README.docker.md](packages/server/README.docker.md)                                                                                                                     |
| Theming (light/dark, palette)                       | [docs/theming.md](docs/theming.md)                                                                                                                                                       |
| Configuration & tuning reference                    | [docs/configuration.md](docs/configuration.md)                                                                                                                                           |
| Build & develop from a clone                        | [docs/development.md](docs/development.md)                                                                                                                                               |

The complete user guide also lives at
**[docs.malloydata.dev](https://docs.malloydata.dev/documentation/user_guides/publishing/publishing)**.

## Publisher and Credible

Publisher is created and maintained by [Credible](https://www.credibledata.com), the company behind
the **AI Analytics Engine**. The two fit together like this:

- **Publisher is the open-source server.** It serves Malloy models over REST and MCP, and everything
  an agent needs from the language and the server — the modeling and analysis skills, the MCP tools —
  ships here in the open. Run it on a laptop, in Docker, or wherever you like.
- **Credible is the hosted, governed engine built around it.** You write down what your data means
  once, in Malloy; the engine owns the how — it materializes and indexes the model in storage it
  brings along, enforces access at one gateway on every query, compresses each model into a concept
  index so an agent gets just the slice a question needs, and serves every surface — agents over MCP,
  dashboards and workspaces, the data apps and APIs in your product — from one model.

Run Publisher yourself, or let Credible run it: the model is the same Malloy either way, and moving
between them is a publish, not a rewrite. Where the server ends and the engine begins:
[credibledata.com/malloy](https://www.credibledata.com/malloy) ·
[Inside the AI Analytics Engine](https://www.credibledata.com/blog/posts/inside-the-ai-analytics-engine).

## Contributing

Build and hack on Publisher from a clone with [docs/development.md](docs/development.md); contribution
process and sign-off are in [CONTRIBUTING.md](CONTRIBUTING.md).

## Community

- Join the [Malloy Slack](https://join.slack.com/t/malloy-community/shared_invite/zt-1kgfwgi5g-CrsdaRqs81QY67QW0~t_uw)
- Report issues on [GitHub](https://github.com/malloydata/publisher/issues)
- Report a security vulnerability privately — see [SECURITY.md](SECURITY.md) for the reporting form
  and what's in scope
