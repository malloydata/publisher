---
name: getting-started
description: First steps for using a Malloy Publisher deployment through its MCP tools. Use when connecting to Publisher for the first time, when you do not yet know the available environments, packages, or models, or when a user asks what data they can explore. Covers verifying the server, discovering data with malloy_getContext, and running a first grounded query.
---

# Getting started with Malloy Publisher

Goal: go from "connected" to a correct, grounded answer without guessing any names.

## 0. Confirm the tools are reachable

You need `malloy_getContext`, `malloy_executeQuery`, and `malloy_searchDocs`. If they are not available, the Publisher server is not running or the MCP client is not connected. Start the server (`npx @malloy-publisher/server --port 4000`, or `bun run build && bun run start` from a clone) and wait until `curl -s http://localhost:4000/api/v0/status` reports `operationalState: serving`, then reconnect.

## 1. Discover what exists (never guess names)

`malloy_getContext` is progressive. Call it with as much as you know:

- No arguments: the available environments, each with its package names.
- `environmentName` only: the packages in that environment.
- `environmentName` + `packageName`: that package's sources.
- `environmentName` + `packageName` + `query` (plain English): the sources, views, named queries, and dimension/measure fields most relevant to the question.

Use the names it returns exactly. Do not invent environments, packages, sources, or fields.

## 2. Run the query

Call `malloy_executeQuery` with the `environmentName`, `packageName`, and `modelPath` from the context results, plus either:

- a named view or query: pass its `name` as `queryName` (with `sourceName` for a view), or
- an ad-hoc query: pass Malloy code as `query`.

The result is JSON. Charts and dashboards defined in the model render in the Publisher UI at http://localhost:4000.

## 3. When you need Malloy syntax

Use `malloy_searchDocs` for language questions (filters, aggregates, joins, nesting, renderers). For deeper work, switch to the `malloy-modeling`, `malloy-analysis`, or `malloy-review` skills.

## Contract

- Ground every query in `malloy_getContext` results. If a name is not in the results, do not use it.
- Start broad and narrow down: environments, then packages, then sources, then query.
- Confirm the environment and package before running a query.
