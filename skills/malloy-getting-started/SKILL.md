---
name: malloy-getting-started
description: First steps for using a Malloy Publisher deployment through its MCP tools. Use when connecting to Publisher for the first time, when you do not yet know the available environments, packages, or models, or when a user asks what data they can explore. Covers verifying the server, discovering data with malloy_getContext, and running a first grounded query.
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Getting started with Malloy Publisher

Goal: go from "connected" to a correct, grounded answer without guessing any names.

## 0. Confirm the tools are reachable

At minimum you need `malloy_getContext`, `malloy_executeQuery`, and `malloy_searchDocs`. Authoring a model also needs `malloy_compile` and `malloy_reloadPackage` (see section 4); an older Publisher may not serve those two.

If none of the tools are there, either the server is not running or your client connected before it was. Start the server (`npx @malloy-publisher/server --port 4000`, or `bun run build && bun run start` from a clone) and wait until `curl -s http://localhost:4000/api/v0/status` reports `operationalState: serving`. If the point is to author models against a local package, add `--watch-env <env>`: without it Publisher copies local packages at boot and serves the copies, so saved edits are never read.

If there is no Publisher workspace here at all, and the user wants to work with data of their own rather than the bundled examples, `npm create @malloy-publisher/malloy-package@latest <name>` scaffolds one: the package and a starter model, registered so the server actually serves it, plus the start script, the MCP config and these skills. Keep the `@latest` when you type it: `npm create` resolves through npm's npx cache and an unversioned name is satisfied by any copy already there, so on a machine that has scaffolded before npm never asks the registry and you get an old scaffolder pinning an old server, with nothing to say so. Run bare, it comes with a small sample dataset, so there is something to query straight away. In a fresh directory `npm start` then runs the pinned server against the package in watch mode; if the directory already had a `package.json` the scaffolder leaves it alone and adds no script, printing the equivalent `npx` command to use instead. Where you run it matters: only the package lands in `<name>/`, and the workspace files, the agent instructions and the MCP config among them, are written to the current directory. Run it here if this directory is empty or is meant to become the workspace. If it already holds other work, scaffold into a new directory instead (`mkdir my-data && cd my-data`), because agent config is discovered by walking up, so writing those files here changes what every session beneath this directory inherits. Seed the starter model from a local file with `npm create @malloy-publisher/malloy-package@latest <name> -- --data <path/to/their-file.csv>` (CSV, Parquet, or Excel `.xlsx`), keeping the `--`, which is how `npm create` passes options through. That path is relative to wherever you run the command, so if you scaffolded into a new directory it has to reach back out to their file; the scaffolder copies it into the package and leaves the original alone. A seeded package starts smaller than the sample one, since the scaffolder does not read their columns: expect a row count and an overview, and build the model from there. A package is just Malloy, so it can instead query a database connection the config defines. Because it writes a `.mcp.json` that did not exist when the client connected, the user has to restart or reconnect once before these tools appear, and their client will ask them to approve the new project-scoped server the first time. That only works when the workspace is at the session's own root, so if you scaffolded into a new directory below that root, the user has to open a session there instead: a `.mcp.json` further down is never discovered.

If you started the server yourself in this session, the tools still will not appear: your tool list was fixed when you connected, and you cannot reconnect yourself. Tell the user the tools are missing for that reason and ask them to run `/mcp`, select `malloy`, and choose Reconnect. The panel offers `Authenticate` first and reports `Auth: not authenticated`; that is a red herring, the endpoint has no auth. Restarting Claude Code also works. Continue once the tools are there.

Two escape hatches worth knowing:

- **When the session cannot be relaunched from the workspace directory** (a project `.mcp.json` is only discovered by sessions that *start* in its directory), register the server at user scope so the directory stops mattering: `claude mcp add --transport http malloy http://localhost:4040/mcp -s user` (use the MCP port the server actually bound; its startup log prints it). Caveat: for sessions that do start in the workspace, the project `.mcp.json` shadows the user-scoped entry, so prefer the project file when it is discoverable.
- **Do not trust an existing `.mcp.json`'s URL blindly.** The file outlives the server that wrote it, and a boot that failed partway (for example, the REST port was taken) can leave it pointing at a dead port while a live server sits on another. If connecting fails or answers look wrong, confirm identity with `malloy_getContext`, which names the environment and packages you are really talking to; that check works on every platform, which the port check does not (`lsof -iTCP:4040 -sTCP:LISTEN` on macOS and Linux, `netstat -ano | findstr :4040` on Windows).

When a user is present, do not route around it by calling the REST API with curl. It appears to work, so the user never learns their session is missing the tools, and you lose what they are for: grounded discovery instead of guessed names, `malloy_compile` instead of throwaway queries, and `malloy_reloadPackage` instead of a restart. Say the tools are missing and let the user fix it in five seconds. Running unattended, with nobody who can reconnect you, is different: there the REST API is the supported interface, not a workaround. Discovery, query, compile, and reload all have REST equivalents (`malloy_searchDocs` and `malloy_getContext`'s plain-English ranking do not; read the bundled skills for syntax and ground from model metadata instead); the running server serves the full spec at `http://localhost:4000/api-doc.yaml`, and AGENTS.md carries the endpoint map.

## 0.5 Ask whether they also use the CLI or the VS Code extension

Publisher is often not the only thing reading the model. The Malloy CLI (`malloy-cli`) and the VS
Code extension compile the same files, and they do **not** get their connections from
`publisher.config.json` - they read `malloy-config.json`, found by walking up from the file being
compiled. So a package that Publisher serves correctly can fail to compile in the editor, on the
same machine, from the same files.

Ask before the user finds out the hard way:

> Are you also using the Malloy CLI or the VS Code extension on this package?

If yes, and the package reads **local data files**, it needs a `malloy-config.json`. Publisher
resolves a relative `duckdb.table('data/x.csv')` against the package root on its own, so nothing is
configured for it; the other two hosts resolve it against the DuckDB connection's
`workingDirectory`, which has to be set.

**Make that path absolute.** A relative `workingDirectory` is resolved against the process's current
directory, not the config file's directory and not the VS Code workspace root, so the same config
compiles from one directory and fails from another:

```json
// malloy-config.json, beside the model
{
  "connections": {
    "duckdb": {
      "is": "duckdb",
      "workingDirectory": "/abs/path/to/package",
      "securityPolicy": "none"
    }
  }
}
```

Point it at the directory the model's table paths are written relative to - the one holding `data/`.
Leave the model's own paths relative so Publisher still serves it; only the config carries the
absolute path. When this is wrong the editor reports `IO Error: No files found that match the
pattern "data/x.csv"` on the `source:` line, followed by a "not defined" error for every field of
that source; those are cascade, not real. `skill:malloy-gotchas-modeling` § Relative Data-File Paths
has the mechanism.

**Remote Credible connections are a different case.** With the Credible extension running and signed
in, the data is reached through a `publisher` proxy connection - the connection type that forwards
SQL to a remote Publisher dataplane, and the same type the CLI and the VS Code extensions use - and
the extension supplies it, so there is nothing to hand-write and no `workingDirectory` involved.
`workingDirectory` only ever matters for local files that DuckDB opens itself. Worth knowing: those
access tokens are user-scoped and short-lived, and nothing refreshes them mid-session, so queries
that start failing auth after a long session mean the token expired, not that the model broke. See
`docs/connections.md` § Publisher proxy connections.

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

If the data you want is in a connected database but not yet in any package, use `malloy_searchDatabaseSchema` instead of `malloy_getContext`: it walks a connection's schemas and tables and ranks them against a plain-English description, and hands back the `source:` line to start a model from. It returns names and types only, so to see what a column actually contains run `malloy_executeQuery` against a model in a package that uses the same connection, with an ad-hoc query like `run: my_conn.table('sales.orders') -> { group_by: order_status }`. That tool needs an existing model to run against, so a table you have not modelled yet has none of its own.

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
