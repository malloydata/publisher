# Publisher

<a href="https://github.com/malloydata/publisher/actions/workflows/build.yml">![build](https://github.com/malloydata/publisher/actions/workflows/build.yml/badge.svg)</a>

**Publisher** is the open-source semantic model server for [Malloy](https://malloydata.dev). It serves Malloy models through REST and MCP APIs, enabling consistent data access for applications, tools, and AI agents.

## Quick Start

```bash
git clone https://github.com/credibledata/malloy-samples.git
cd malloy-samples
npx @malloy-publisher/server --port 4000 --server_root .
```

Open http://localhost:4000 to explore the sample models.

## Documentation

Full documentation is available at **[docs.malloydata.dev/documentation/user_guides/publishing](https://docs.malloydata.dev/documentation/user_guides/publishing/publishing)**:

- [Getting Started](https://docs.malloydata.dev/documentation/user_guides/publishing/publishing) - Setup, deployment options, configuration
- [Database Connections](https://docs.malloydata.dev/documentation/user_guides/publishing/connections) - BigQuery, Snowflake, Postgres, DuckDB, and more
- [Explorer](https://docs.malloydata.dev/documentation/user_guides/publishing/explorer) - No-code visual query builder
- [REST API](https://docs.malloydata.dev/documentation/user_guides/publishing/rest_api) - Build custom applications
- [Publisher SDK](https://docs.malloydata.dev/documentation/user_guides/publishing/publisher_sdk) - Embed analytics in React apps
- [MCP for AI Agents](https://docs.malloydata.dev/documentation/user_guides/publishing/mcp_agents) - Connect Claude and other AI assistants

## How the Pieces Fit Together

### Malloy

The core compiler and query execution engine. Malloy compiles `.malloy` files into SQL, executes queries against databases, and returns structured `Result` objects. Malloy is a pure JavaScript/TypeScript library with no UI or serving capabilities—it's the foundation everything else builds on.

**Repository:** [github.com/malloydata/malloy](https://github.com/malloydata/malloy)

### Malloy Render

A visualization library that transforms Malloy `Result` objects into interactive tables, charts, and dashboards.

When Malloy executes a query, the result includes both **data** and **rendering hints**—tags like `# bar_chart` or `# line_chart` that indicate how the data should be displayed. Malloy Render interprets these tags and produces the appropriate visualization.

**Built with:** SolidJS and Vega/Vega-Lite. Available as both a JavaScript API (`MalloyRenderer`) and a `<malloy-render>` web component.

**Repository:** [github.com/malloydata/malloy/packages/malloy-render](https://github.com/malloydata/malloy/tree/main/packages/malloy-render)

### Publisher

An open-source semantic model server for Malloy. Publisher makes Malloy models accessible over the network and provides a professional UI for data exploration.

- **Server:** REST API for listing content, managing database connections, compiling models, and executing queries. Also provides an MCP API for AI agent integration.
- **App:** Web interface for browsing Malloy content, exploring models with a no-code query builder, and viewing results.

### Publisher SDK

A React component library for building custom data applications powered by Publisher:

- **API communication** — Talks to the Publisher Server via REST
- **Query execution** — Submits queries and retrieves results
- **Result visualization** — Integrates Malloy Render to display results
- **UI components** — Pre-built pages for browsing projects, packages, models, and notebooks

The Publisher App is built entirely with the SDK, but the SDK is a standalone NPM package for building your own applications.

## Architecture

Publisher consists of four packages:

| Package | Description |
|---------|-------------|
| **[packages/server](packages/server/)** | Express.js backend providing REST API (port 4000) and MCP API (port 4040). Loads Malloy packages, compiles queries, executes against databases. |
| **[packages/sdk](packages/sdk/)** | React component library for building UIs that consume Publisher's REST API. |
| **[packages/app](packages/app/)** | Reference implementation and production-ready data exploration tool built with the SDK. |
| **[packages/python-client](packages/python-client/)** | Auto-generated Python SDK for the REST API. |

## Development

This project uses [bun](https://bun.sh/) as the JavaScript runtime.

```bash
git submodule init && git submodule update  # Get malloy-samples
bun install
bun run build:server-deploy
bun run start                               # Production server
```

```bash
bun run start:dev       # Dev server with watch
bun run test            # Run tests
bun run lint && bun run format  # Code quality
```

## Community

- Join the [Malloy Slack](https://join.slack.com/t/malloy-community/shared_invite/zt-1kgfwgi5g-CrsdaRqs81QY67QW0~t_uw)
- Report issues on [GitHub](https://github.com/malloydata/publisher/issues)
