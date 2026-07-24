<h1 align="center">Malloy Publisher</h1>

<p align="center"><b>The open-source semantic model server for <a href="https://malloydata.dev">Malloy</a></b><br>
Serve governed data models to applications, BI tools, and AI agents — over REST and MCP.</p>

<p align="center">
  <a href="https://github.com/malloydata/publisher/actions/workflows/build.yml"><img src="https://github.com/malloydata/publisher/actions/workflows/build.yml/badge.svg" alt="build"></a>
</p>

<p align="center">
  <a href="https://github.com/user-attachments/assets/376a809d-8016-41a7-9464-a5634ea0589d"><img src="docs/malloy-publisher-demo.gif" alt="Malloy Publisher serving the bundled storefront dashboard" width="800"></a>
</p>
<p align="center"><sub>A 60-second walkthrough — model in your IDE with the Malloy skills, serve with Publisher, build a data app, materialize on a schedule, and analyze. <a href="https://github.com/user-attachments/assets/376a809d-8016-41a7-9464-a5634ea0589d">Watch the video</a> for playback controls.</sub></p>

When an AI queries your database directly, it writes its own SQL — and gets it subtly wrong: the wrong
join, an invented column, a fan-out that double-counts but still looks plausible. Publisher puts a
Malloy semantic layer in front of your data, where measures, dimensions, and joins are defined once,
correctly. Applications, BI tools, and **AI agents** compose queries against that model instead of
writing raw SQL, so the numbers come back **right by construction**. Agents work through the sources
the model defines — not your raw tables — and you decide exactly what each caller can see.

Point Publisher at your Malloy models and it serves them over a REST API and a single MCP endpoint.

## Quick start

```bash
npx @malloy-publisher/server --port 4000
```

Open **http://localhost:4000** and explore the bundled example packages —
[`storefront`](examples/storefront) (a complete ecommerce model),
[`governed-analytics`](examples/governed-analytics) (access control), and
[`html-data-app`](examples/html-data-app) (a no-build dashboard) —
all DuckDB-backed, no credentials required. Give the server a moment to report `serving`:

```bash
curl -s http://localhost:4000/api/v0/status | jq .operationalState   # → "serving"
```

A first `npx` run fetches the example packages from GitHub and reports its download progress on
stderr. When the server is ready it prints a single line, also to stderr, that scripts can wait for
instead of polling:

```
PUBLISHER_READY url=http://localhost:4000 mcp=http://localhost:4040 environments=1 packages=3 load_errors=0
```

`load_errors` counts configured packages and environments that failed to load. When it is not 0,
`/api/v0/status` names each one under `.loadErrors`. The `url=` host reads `localhost` when the
server binds every interface (the default); a configured `--host` shows as itself. If initialization
fails, a `PUBLISHER_INIT_FAILED` line is printed in its place; a startup failure outside
initialization, like a port already in use, crashes without either token.

## Point your agent at it

This is the fast path to the "wow." Start the server, then connect any MCP-compatible agent to the
MCP endpoint on port **4040**:

```bash
claude mcp add --transport http malloy http://localhost:4040/mcp
```

Then just ask, in plain English:

> _"Use Malloy to explore the storefront sales data and chart revenue by category."_

The agent discovers what data exists (`malloy_getContext`), grounds itself in the real source, view,
and field names, runs the query (`malloy_executeQuery`), and returns an answer backed by your
semantic model. No schema spelunking, no hallucinated column names.

- **Agents:** this repo ships an [AGENTS.md](AGENTS.md) and a bundled skill library
  ([`skills/`](skills/)) that most AI coding hosts auto-discover. Start there.
- **Any MCP client** (Cursor, VS Code, Codex, Claude Desktop): see
  [docs/ai-agents.md](docs/ai-agents.md) for per-client config and the stdio bridge.

> The server, MCP and REST alike, is stateless and unauthenticated, and it can read any data your
> models connect to. Bind it to loopback (`--host 127.0.0.1`) for local use, and put an
> authenticating gateway in front before exposing it more widely.

No MCP client, or an agent running unattended that started the server itself? The same loop is
available over plain REST:

```bash
curl -s -X POST \
  http://localhost:4000/api/v0/environments/examples/packages/storefront/models/storefront.malloy/query \
  -H 'content-type: application/json' \
  -d '{"query":"run: order_items -> by_category","compactJson":true}' | jq -r .result
```

A package is just a directory with a `publisher.json` and a `.malloy` model;
[docs/packages.md](docs/packages.md) is the format reference. The running server serves its full
OpenAPI spec at `http://localhost:4000/api-doc.yaml`, and [docs/ai-agents.md](docs/ai-agents.md)
covers agents in both modes, MCP and REST.

## What you can do

- **Explore, no code.** Build and drill into queries visually with [Malloy Explorer](docs/explorer.md) —
  every action generates valid Malloy, so metrics stay correct even across joins.
- **Answer questions with AI.** Connect an agent over MCP and ask in plain English — see above and
  [docs/ai-agents.md](docs/ai-agents.md).
- **Surface analytics your way.** Explore and share with zero code in the
  [Publisher App](docs/publisher-app.md), or ship a no-build
  [HTML data app](docs/html-data-apps.md) that Publisher hosts inside a package.
- **Build & validate models.** Author Malloy models guided by the bundled [skills](skills/), then
  publish them for serving. Agents get the same loop over MCP: `malloy_compile` checks an edit and
  returns diagnostics without running it, and `malloy_reloadPackage` recompiles a package from disk
  so a new source or view is queryable by name, no restart.
- **Govern access.** [Givens](docs/givens.md) are one runtime-parameter mechanism that powers filter
  widgets, [row-level access](docs/row-level-access.md) (which rows a caller sees), and
  [`#(authorize)`](docs/authorize.md) source gates (who can query). Separately, curate _what_ is
  [discoverable and queryable](docs/discovery-and-access.md).
- **Materialize for cost & speed.** Persist an expensive source into a table with `#@ persist`, then
  rebuild it on demand or on a cron with the opt-in standalone scheduler — see
  [docs/materialization.md](docs/materialization.md) and the `malloy-pub schedule` /
  `list materialization` CLI.

## Documentation

The [`docs/`](docs/) folder is the reference hub — see its [index](docs/README.md). Highlights:

| Topic                                               | Doc                                                                                                                                                                                      |
| --------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Runnable example packages                           | [examples/](examples/) ([storefront](examples/storefront) · [governed-analytics](examples/governed-analytics) · [html-data-app](examples/html-data-app) · [data-app](examples/data-app)) |
| Architecture & how it fits together                 | [docs/architecture.md](docs/architecture.md)                                                                                                                                             |
| REST & MCP API overview                             | [docs/api-overview.md](docs/api-overview.md)                                                                                                                                             |
| The package format (`publisher.json`, models, data) | [docs/packages.md](docs/packages.md)                                                                                                                                                     |
| The Publisher App (navigation & features)           | [docs/publisher-app.md](docs/publisher-app.md)                                                                                                                                           |
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

## Contributing

Build and hack on Publisher from a clone with [docs/development.md](docs/development.md); contribution
process and sign-off are in [CONTRIBUTING.md](CONTRIBUTING.md).

## Community

- Join the [Malloy Slack](https://join.slack.com/t/malloy-community/shared_invite/zt-1kgfwgi5g-CrsdaRqs81QY67QW0~t_uw)
- Report issues on [GitHub](https://github.com/malloydata/publisher/issues)
