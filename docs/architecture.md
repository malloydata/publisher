# Architecture — how the pieces fit together

> What this is: the mental model for Publisher and the Malloy stack it sits on. Read it when you
> want to know which repo owns what, or how a query flows from a `.malloy` file to a rendered chart.

Publisher is one layer in the Malloy stack. From the bottom up:

## Malloy

The core compiler and query execution engine. Malloy compiles `.malloy` files into SQL, executes
queries against databases, and returns structured `Result` objects. Malloy is a pure
JavaScript/TypeScript library with no UI or serving capabilities — it's the foundation everything
else builds on.

**Repository:** [github.com/malloydata/malloy](https://github.com/malloydata/malloy)

## Malloy Render

A visualization library that transforms Malloy `Result` objects into interactive tables, charts, and
dashboards. When Malloy executes a query, the result includes both **data** and **rendering
hints** — tags like `# bar_chart` or `# line_chart` that indicate how the data should be displayed.
Malloy Render interprets these tags and produces the appropriate visualization.

**Built with:** SolidJS and Vega/Vega-Lite. Available as both a JavaScript API (`MalloyRenderer`) and
a `<malloy-render>` web component.

**Repository:** [github.com/malloydata/malloy/packages/malloy-render](https://github.com/malloydata/malloy/tree/main/packages/malloy-render)

## Publisher

An open-source semantic model server for Malloy. Publisher makes Malloy models accessible over the
network and provides a professional UI for data exploration.

- **Server:** REST API for listing content, managing database connections, compiling models, and
  executing queries. Also provides an MCP API for AI-agent integration, including the
  [agent retrieval tools](ai-agents.md) and the agent skills as MCP prompts. Supports runtime
  [givens](givens.md) for model-driven, server-side query parameters.
- **App:** Web interface for browsing Malloy content, exploring models with a no-code query builder,
  and viewing results.

## Publisher SDK

A React component library — API communication, query execution, Malloy Render integration, and
pre-built pages for browsing environments, packages, models, and notebooks. The **Console is built
entirely from it**, and it's equally the way to bring Malloy's built-in renderer, notebook, and
dashboard types into your own React data app — see [react-data-apps.md](react-data-apps.md)
and the [`examples/data-app`](../examples/data-app) reference. For non-React surfaces, use the
[Publisher Console](console.md), an [HTML data app](html-data-apps.md), or the
[REST/MCP APIs](api-overview.md).

## Packages in this repo

| Package | Description |
| --- | --- |
| **[packages/server](../packages/server/)** | Express.js backend providing REST API (port 4000) and MCP API (port 4040). Loads Malloy packages, compiles queries, executes against databases. |
| **[packages/sdk](../packages/sdk/)** | React component library the Console is built from, and the React path for your own data apps. |
| **[packages/app](../packages/app/)** | Reference implementation and production-ready data exploration tool built with the SDK. |
| **[packages/python-client](../packages/python-client/)** | Auto-generated Python SDK for the REST API. |
