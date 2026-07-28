---
name: malloy-getting-started
description: First steps for using a Malloy Publisher deployment through its MCP tools. Use when connecting to Publisher for the first time, when you do not yet know the available environments, packages, or models, or when a user asks what data they can explore. Covers verifying the server, discovering data with malloy_getContext, and running a first grounded query.
---

# Getting started with Malloy Publisher

Goal: go from "connected" to a correct, grounded answer without guessing any names.

## 0. Confirm the tools are reachable

At minimum you need `malloy_getContext`, `malloy_executeQuery`, and `malloy_searchDocs`. Authoring a model also needs `malloy_compile` and `malloy_reloadPackage` (see section 4); an older Publisher may not serve those two.

If none of the tools are there, either the server is not running or your client connected before it was. Start the server (`npx @malloy-publisher/server --port 4000`, or `bun run build && bun run start` from a clone) and wait until `curl -s http://localhost:4000/api/v0/status` reports `operationalState: serving`.

If there is no Publisher workspace here at all, and the user wants to work with data of their own rather than the bundled examples, `npm create @malloy-publisher/malloy-package@latest <name>` scaffolds one: the package and a starter model, registered so the server actually serves it, plus the start script, the MCP config and these skills. Keep the `@latest` when you type it: `npm create` resolves through npm's npx cache and an unversioned name is satisfied by any copy already there, so on a machine that has scaffolded before npm never asks the registry and you get an old scaffolder pinning an old server, with nothing to say so. Run bare, it comes with a small sample dataset, so there is something to query straight away. In a fresh directory `npm start` then runs the pinned server against the package in watch mode; if the directory already had a `package.json` the scaffolder leaves it alone and adds no script, printing the equivalent `npx` command to use instead. Where you run it matters: only the package lands in `<name>/`, and the workspace files, the agent instructions and the MCP config among them, are written to the current directory. Run it here if this directory is empty or is meant to become the workspace. If it already holds other work, scaffold into a new directory instead (`mkdir my-data && cd my-data`), because agent config is discovered by walking up, so writing those files here changes what every session beneath this directory inherits. Seed the starter model from a local file with `npm create @malloy-publisher/malloy-package@latest <name> -- --data <path/to/their-file.csv>` (CSV, Parquet, or Excel `.xlsx`), keeping the `--`, which is how `npm create` passes options through. That path is relative to wherever you run the command, so if you scaffolded into a new directory it has to reach back out to their file; the scaffolder copies it into the package and leaves the original alone. A seeded package starts smaller than the sample one, since the scaffolder does not read their columns: expect a row count and an overview, and build the model from there. A package is just Malloy, so it can instead query a database connection the config defines. Because it writes a `.mcp.json` that did not exist when the client connected, the user has to restart or reconnect once before these tools appear, and their client will ask them to approve the new project-scoped server the first time. That only works when the workspace is at the session's own root, so if you scaffolded into a new directory below that root, the user has to open a session there instead: a `.mcp.json` further down is never discovered.

If you started the server yourself in this session, the tools still will not appear: your tool list was fixed when you connected, and you cannot reconnect yourself. Tell the user the tools are missing for that reason and ask them to run `/mcp`, select `malloy`, and choose Reconnect. The panel offers `Authenticate` first and reports `Auth: not authenticated`; that is a red herring, the endpoint has no auth. Restarting Claude Code also works. Continue once the tools are there.

When a user is present, do not route around it by calling the REST API with curl. It appears to work, so the user never learns their session is missing the tools, and you lose what they are for: grounded discovery instead of guessed names, `malloy_compile` instead of throwaway queries, and `malloy_reloadPackage` instead of a restart. Say the tools are missing and let the user fix it in five seconds. Running unattended, with nobody who can reconnect you, is different: there the REST API is the supported interface, not a workaround. Discovery, query, compile, and reload all have REST equivalents (`malloy_searchDocs` and `malloy_getContext`'s plain-English ranking do not; read the bundled skills for syntax and ground from model metadata instead); the running server serves the full spec at `http://localhost:4000/api-doc.yaml`, and AGENTS.md carries the endpoint map.

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

Use `malloy_searchDocs` for language questions (filters, aggregates, joins, nesting, renderers).

## 4. What else you can do here

Answering questions is the start, not the whole surface. When the user asks what is possible, say so rather than offering queries alone. Switch skills for the deeper work:

- `malloy-modeling`: build or change a model. Validate the edit with `malloy_compile`, save it, then `malloy_reloadPackage` so the new sources and views run by name without restarting the server.
- `malloy-analysis`: explore a package and answer data questions.
- `malloy-html-data-apps`: build a data app, a hand-authored HTML page in the package's `public/` directory that Publisher serves, backed by the package's models and needing no build step.
- `malloy-review`: check Malloy for correctness.

## Contract

- Ground every query in `malloy_getContext` results. If a name is not in the results, do not use it.
- Start broad and narrow down: environments, then packages, then sources, then query.
- Confirm the environment and package before running a query.
