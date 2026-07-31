<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# AI Agents

## Overview

Publisher speaks the [Model Context Protocol (MCP)](https://modelcontextprotocol.io), so an AI agent can work with your Malloy models over a standard interface. Because a Malloy model already carries the business logic and the relationships between entities, an agent grounds its answers in your definitions instead of guessing at table and column names.

There are two ways in, depending on how the agent runs. An interactive session connects over [MCP](#mcp-server-port-4040). An agent working unattended that started the server itself cannot connect to MCP mid-session (its tool list was fixed before the server existed), so it uses the [REST API](#unattended-and-one-shot-agents-the-rest-loop) instead, which covers the whole workflow: discovery, query, compile, and reload.

Publisher exposes a single MCP server (port 4040) with the tools an agent needs: `malloy_getContext` to discover what the deployment exposes (environments, packages, sources, and the fields relevant to a question) and ground answers in real names, `malloy_searchDocs` to search the Malloy documentation, `malloy_executeQuery` to run Malloy queries, `malloy_searchDatabaseSchema` to find the right tables in a database you have not modelled yet, `malloy_getStatus` to see what failed to load or is serving a stale model, and, for authoring, `malloy_compile` to validate a model change without running it and `malloy_reloadPackage` to make a saved change queryable. It also serves the bundled agent skills as MCP prompts.

Any MCP-compatible client can connect: a desktop chat app, an IDE assistant, or your own script.

## MCP server (port 4040)

The server listens at `http://localhost:4040/mcp` (set the port with `--mcp_port` or `MCP_PORT`). Clients interact with it through tool calls.

### Discovery and grounding

- `malloy_getContext`: the entry point when you do not yet know the environment, package, or model names. It is progressive: call it with no arguments to list the environments (each with its package names), with an environment to list its packages, with a package to list its sources, and with a package plus a plain-English question to return the sources, views, named queries, and dimension and measure fields most relevant to it. This lets an agent discover what a deployment exposes and ground a query in names the model actually defines before writing it. Question-level retrieval is lexical (lunr/BM25) over the model's own text by default, so it matches the terms your model uses. A field named in `snake_case` (say `dep_delay`) indexes as one token, so a search for "delay" will not surface it; when a first pass comes up empty, list the package's sources or narrow with `sourceName` rather than forwarding the user's exact words. Servers started with `EMBEDDING_API_KEY` rank by embedding similarity instead, which closes that gap (the response then carries a `retrieval` field and per-entity `score`); see the "Semantic retrieval" section in [configuration.md](configuration.md).
- `malloy_searchDocs`: keyword search over a bundled index of the Malloy documentation, returning matching titles, URLs, and excerpts.
- `malloy_searchDatabaseSchema`: discovery for the other direction, a database you have not modelled yet. `malloy_getContext` searches models that already exist; this searches a configured connection's schema, so an agent pointed at a warehouse can find the right tables and start a model from them. It is progressive the same way: no arguments lists the environments and their connections, a connection lists its schemas, a schema lists its tables, a schema plus a plain-English `searchQuery` ranks those tables by relevance, and adding `tableName` returns that one table with every column (the way past the per-table column cap). Each table comes back with its columns and a ready-to-use `source:` line. Ranking is lexical by default; set `EMBEDDING_API_KEY` **and** `EMBEDDING_INDEX_CONNECTION_SCHEMA=true` for embedding similarity, and see [Semantic ranking for malloy_searchDatabaseSchema](configuration.md#semantic-ranking-for-malloy_searchdatabaseschema) for exactly what that sends. The tool returns names and types only, never row values, so to see what a column actually contains run `malloy_executeQuery` against a model in a package that uses the same connection, with an ad-hoc query like `run: my_conn.table('sales.orders') -> { group_by: order_status }`. It needs an existing model to run against, because a table you have not modelled yet has none of its own.

### Query tool

- `malloy_executeQuery`: run a Malloy query and return the results as JSON. Accepts `givens` for supplying values to model-declared [runtime parameters](givens.md).

### Authoring tools

- `malloy_compile`: compile Malloy source against a package and return structured diagnostics (`severity`, `message`, `model`, `line`, `character`) without running a query, so an agent can validate a change while authoring instead of firing a throwaway query. Positions are 0-based; `model` is the package-relative file a diagnostic points at. A `scope` parameter says what the source means, and picking the right one is most of using this tool well:

  - `"append"` (the default): the source is appended to `modelPath` and compiled in its namespace. Right for **new** definitions and queries. Positions are relative to the model file with the source appended, so a line in your text lands after the model's own line count.
  - `"file"`: the source is compiled **as** `modelPath`, replacing its on-disk content for the check. This is the way to validate an **edit** before saving — the append form collides with the model's own copy (`Cannot redefine '<name>'`) — and diagnostics land at the true coordinates of your text.
  - `"package"`: runs the same worker compiler and file selection as reload over every `.malloy` and `.malloynb` file, without swapping the result into the served package. An optional `source` acts as a what-if replacement for `modelPath`, so a change that breaks an *importer* surfaces before you save it; a path that does not exactly match an existing file is warned and treated as new. Diagnostics may name files hidden from discovery. No rows or SQL are returned, caller text remains subject to `#(authorize)`, and a clean dry-run still needs `malloy_reloadPackage` after saving for the edit to serve.

  At `"append"`, the source has to stand on its own as top-level Malloy, and the two most natural ways to check an edit both fail for reasons that have nothing to do with the edit. A bare `view:` / `dimension:` / `measure:` is not a top-level statement (`no viable alternative at input 'view:'`), and resubmitting the source you are editing collides with the model's own copy (use `scope: "file"` for that). To check part of a source at append scope, either send the view body as a top-level query:

  ```malloy
  query: check is orders -> { group_by: status, aggregate: revenue }
  ```

  or wrap the fragment in a throwaway extension, which puts it in the namespace the real edit will live in:

  ```malloy
  source: check is orders extend { measure: aov is revenue / nullif(order_count, 0) }
  ```

  Both compile against the real source, so inherited measures resolve and `private:` fields stay hidden exactly as they would in place. An extension *adds* to a source's namespace rather than overriding it, so a fragment reusing an existing view name reports `Cannot redefine` too; rename it for the check.

  One thing you cannot compile-check: submitted source may not contain an `#(authorize)` / `##(authorize)` annotation, and one is rejected with a 400 rather than compiled. A source's own gate replaces the gate it would otherwise inherit from its base, so accepting one from a request would let a caller relax the author's access gate — see [authorize.md](authorize.md#security-model). There is no way to tell an author checking their own gate from a caller forging one, because the compile door has no caller identity to check. So a gate is validated by saving it and reloading: model load compiles every `#(authorize)` annotation in the package and reports a malformed one as a 424 naming the source. Strip the annotation if you only want to check the rest of the edit.
- `malloy_reloadPackage`: recompile a package from its on-disk content so a source or view added after boot becomes queryable by name, without restarting the server. This is the other half of the authoring loop: validate with `malloy_compile`, save, reload, then query. A reload that fails to compile leaves the package's files alone and keeps serving the previously compiled model, returning the compile errors — which is also the loop for an edit carrying an `#(authorize)` gate, since those cannot go through `malloy_compile`.

### Health

- `malloy_getStatus`: the MCP analog of `GET /api/v0/status`, reduced to what an agent needs to judge health: `operationalState`, each environment's loaded package names, and `loadErrors`. It carries no connection attributes, locations, or row data. `loadErrors` is present only when something failed, and holds two different things. An entry without `stale` is a package (or, with no `package` key, a whole environment) that did not load and is therefore missing from `environments` entirely. An entry with `stale: true` is a package that IS loaded and IS answering queries, whose most recent reload failed to compile: it answers from the model compiled before that save, and its `message` is the compile error. Nothing else reports that second case, because the package reads as serving everywhere else, so call this before concluding a package is empty or missing, and after any model edit whose reload you did not run yourself (watch mode, or another process). Fixing the model and reloading clears the entry.

  `malloy_getContext` surfaces the same two facts inline, so ordinary discovery does not have to remember to ask: a failed package appears in the package listing with an `error` instead of being silently omitted, a stale one with `error` and `stale: true`, and anything returned for a stale package carries a top-level `note` saying the names predate the last save.

### Skills as MCP prompts

The server also serves the bundled agent [skills](../skills/) as MCP prompts. A host that ingests MCP but does not read skill files from disk (for example Codex, ChatGPT, or Cursor) can pull the same guidance through this channel. MCP prompts are on-demand: a client lists them and the user or host selects one, so guidance that is always-on for skill-aware hosts becomes opt-in here. For authoring or contributing skills, see [docs/agent-skills](agent-skills/).

MCP also defines resources (for example links to a data dictionary). These are a newer part of the standard and many clients do not use them yet; a tool like the MCP Inspector lets you explore them.

The server does not require authentication, and `malloy_executeQuery` runs Malloy against the databases your models connect to, so anyone who can reach this port can read that data. The surface is not read-only either: `malloy_reloadPackage` mutates server state, and for a package that carries an install location a reload re-fetches it, overwriting on-disk edits. The same effects are already reachable through the equivalent REST endpoints, so this is a reason to gate the deployment rather than a reason to avoid the tools. The server binds `0.0.0.0` by default, which also exposes it on your network. Bind it to loopback with `--host 127.0.0.1` for local-only use, and put an authenticating gateway in front before exposing it more widely.

## Connecting a client

These examples assume Publisher is already running (`npx @malloy-publisher/server --port 4000` needs only Node.js on your PATH). See the [README](https://github.com/malloydata/publisher) for install and run options.

### Over HTTP

For Claude Code you may not have to write anything: in a directory that has no `.mcp.json`, the server usually writes one itself naming the port it bound. It skips several cases, including git working trees and your home directory; the full list is in [configuration.md](configuration.md#the-mcpjson-the-server-writes), and the startup log says which one applied, unless you turned the feature off, which is silent. Everything below is for the other clients, and for the cases where it does not write one.

Clients such as Cursor and VS Code connect straight to the HTTP endpoint. The exact config shape varies by client (key names differ, for example VS Code uses `servers` rather than `mcpServers`), but each entry points an MCP server at a URL:

```json
{
  "mcpServers": {
    "malloy": { "type": "http", "url": "http://localhost:4040/mcp" }
  }
}
```

Add or drop the `"type": "http"` field to match your client. Clients that speak only stdio (for example older Claude Desktop builds) connect through `mcp-remote`, below.

If a client cannot reach `localhost:4040`, another local process may be holding that loopback port (some editor and MCP extensions bind it). Move Publisher's MCP server to another port with `--mcp_port`, or point the client at the machine's network address. Note that a client which *can* reach the port is not proof it reached Publisher: if the wrong process holds it, the client connects to that instead. `malloy_getContext` names the environment and packages it is actually talking to, which is the quickest way to tell.

### With a stdio-only client through mcp-remote

Some clients (for example older Claude Desktop builds) speak only stdio MCP, not HTTP. Bridge them to the HTTP endpoint with [`mcp-remote`](https://www.npmjs.com/package/mcp-remote), which needs no extra script. In the client's MCP config (for Claude Desktop, Settings > Developer > Edit Config) add:

```json
{
  "mcpServers": {
    "malloy": {
      "command": "npx",
      "args": ["-y", "mcp-remote", "http://localhost:4040/mcp", "--allow-http"]
    }
  }
}
```

`--allow-http` is required because the endpoint is plain HTTP on localhost. Save the config and start a conversation; the agent discovers your models through the tools and answers questions about them.

Example prompts against the bundled samples:

- "Use Malloy to explore the storefront sales data and chart revenue by category."
- "Use Malloy to find the top products and top brands in the storefront package."
- "Use Malloy to break down storefront sales by customer state."

## Unattended and one-shot agents: the REST loop

When no user can reconnect your MCP client, use the REST API on port 4000: there it is the supported interface, not a fallback. This is your situation whenever you started the server yourself mid-session (an MCP client's tool list is fixed when it connects, so the `malloy_*` tools never appear) or you run one-shot in a cloud sandbox or CI job. Discovery, query, compile, and reload are all reachable over REST. Two conveniences stay MCP-side: `malloy_searchDocs` (its docs-search index lives inside the MCP server; for Malloy syntax read the bundled [`skills/`](../skills/) markdown or [docs.malloydata.dev](https://docs.malloydata.dev) instead) and `malloy_getContext`'s plain-English relevance ranking (over REST, ground from the model metadata as in step 3 below). Like MCP, this API is unauthenticated and the server binds `0.0.0.0` by default; keep it on localhost (`--host 127.0.0.1`) in a sandbox. The examples below use port 4000, but the real port is whatever `--port` or `PUBLISHER_PORT` the server was started with; `/api/v0/status` answering is the confirmation you found it.

Two references are available without cloning anything. The running server serves its complete OpenAPI spec at `http://localhost:4000/api-doc.yaml`, dependable even offline (see [api-overview.md](api-overview.md#live-api-explorer) for why the Swagger UI page can come up blank in a sandbox while the YAML never does). And every file in this repo resolves at `https://raw.githubusercontent.com/malloydata/publisher/main/<path>`, starting with [AGENTS.md](https://raw.githubusercontent.com/malloydata/publisher/main/AGENTS.md).

The loop:

```bash
API=http://localhost:4000/api/v0

# 1. Wait for the server, then check what failed to load (absent when clean)
curl -s $API/status | jq .operationalState        # repeat until "serving"
curl -s $API/status | jq .loadErrors

# 2. Discover: environments, then packages, then models
curl -s $API/environments | jq '.[].name'                                  # bundled env: "examples"
curl -s $API/environments/examples/packages | jq '.[].name'
curl -s $API/environments/examples/packages/storefront/models | jq '.[].path'

# 3. Ground: a model's sources, views, named queries, and givens are what you can run
curl -s $API/environments/examples/packages/storefront/models/storefront.malloy \
  | jq '{sources: [.sources[]? | {name, views: [.views[]?.name]}], queries: [.queries[]?.name]}'

# 4. Run a named view...
curl -s -X POST $API/environments/examples/packages/storefront/models/storefront.malloy/query \
  -H 'content-type: application/json' \
  -d '{"sourceName":"order_items","queryName":"by_category","compactJson":true}' | jq -r .result

# ...or ad-hoc Malloy
curl -s -X POST $API/environments/examples/packages/storefront/models/storefront.malloy/query \
  -H 'content-type: application/json' \
  -d '{"query":"run: order_items -> by_category","compactJson":true}' | jq -r .result
```

The query body takes one of two shapes: `query` alone (ad-hoc Malloy), or `queryName` without `query` (a named view when `sourceName` is set, a model-level named query when it is not); anything else is a 400. Parse the `result` string; `"compactJson": true` makes it plain row objects, and `givens` rides on either shape ([givens.md](givens.md)). The full statement of the rules is in [api-overview.md](api-overview.md#query-request-shapes).

Authoring works without MCP too:

- `POST …/models/{path}/compile` with `{"source": "…"}` returns `{"status": "success" | "error", "problems": […]}` without running anything, plus the generated `sql` when the body sets `"includeSql": true`. The body also takes `"scope"` (`"append"` default / `"file"` / `"package"`), with the same meanings as the MCP tool's parameter above — `"file"` is the one that validates an edit, and `"package"` (where `source` is optional) is the whole-package dry-run. The wire shape differs from MCP: each REST problem carries `severity`, `message`, `code`, `model` (the package-relative file it points at), and a position nested at `at.range.start/end.{line, character}` (the MCP tool flattens these to `line`/`character`). Positions are 0-based; at `"append"` they count the model's own lines first, since your source is appended to it, and `at.url` names a temporary compile-check overlay rather than the file itself.
- After editing a package's files, `GET …/packages/{pkg}?reload=true` recompiles it, the REST form of `malloy_reloadPackage`. A successful reload returns the package metadata; re-fetch the model to confirm the edit took. A reload that fails to compile leaves the files alone and keeps serving the previous model.
  - **The package name is not optional.** `GET …/packages?reload=true` — the collection, with no package name — cannot reload anything, because reload is per-package. It used to answer `200` with the package list, which reads as a reload that worked; it now answers `400` naming the per-package route. If you are on an older server and a reload seems to succeed while your edit never takes effect, check that the path has the package name in it.

If a `malloy_*` MCP call ever comes back as a bare `"Unknown error"` with no diagnostic, that is not what a compile failure looks like on a current server: `malloy_compile` and `malloy_reloadPackage` both state their diagnostics in plain text, with line and character positions. A bare `"Unknown error"` means the server predates that (before `@malloy-publisher/server` 0.0.233). Reach the same diagnostic over REST with `GET …/packages/{pkg}?reload=true` — with the package name — and upgrade: `npx` resolves through a shared cache that can serve a stale build, so pin `@latest` (`npx @malloy-publisher/server@latest`) rather than trusting the cached copy.

### Serving your own data

A package is a directory with a `publisher.json` and a `.malloy` model ([packages.md](packages.md) is the format reference). Put a config next to it ([configuration.md](configuration.md#bring-your-own-config) is the config reference) and start the server on that:

```
my-data/
  publisher.config.json
  sales/
    publisher.json
    sales.malloy
    data/sales.csv
```

```json
{
  "environments": [
    { "name": "local", "packages": [{ "name": "sales", "location": "./sales" }] }
  ]
}
```

```bash
npx @malloy-publisher/server --port 4000 \
  --config /absolute/path/to/my-data/publisher.config.json --watch-env local
```

`--config` boots only your environment, so there is no example download to wait for, and `--watch-env local` mounts the package in place so a saved model edit recompiles on its own. A save that fails to compile is skipped, and the package keeps serving what it compiled last, so re-check `/status` after each save: the package comes back as a `loadErrors` entry with `stale: true` and the compile error until a save compiles. Compile-check first anyway; it is faster feedback. `GET /api/v0/watch-mode/status` reports whether watching is on (`enabled`) and for which environment. Then poll `/status` and query as above.

On a server that is already running, register the package instead: `POST /api/v0/environments/{env}/packages` with `{"name": "sales", "location": "/absolute/path/to/sales"}`. The tree is copied at registration into the server's own storage at `<server root>/publisher_data/{env}/{pkg}/` (the server root is the directory the server was launched from, unless `--server_root` set another), so afterwards iterate against that copy with `?reload=true`. Either way, re-check `loadErrors` after the package appears; `serving` alone does not mean it loaded.

### Traps worth knowing

- Verifying a data app in a headless browser: wait on `load` plus a content selector, never `networkidle`. The page's `publisher.js` holds the live-reload SSE stream open, so network idle never arrives. See [html-data-apps.md](html-data-apps.md#live-reload).
- Do not run two first-run `npx @malloy-publisher/server` commands concurrently: they can race in the shared npx cache and corrupt the install. See [deployment.md](deployment.md#run-with-npx).
- The bundled skills are plain markdown under [`skills/`](../skills/) and read fine without MCP; `malloy-modeling` is the authoring guide.

## Troubleshooting

Connection errors:

- Confirm the server is running and listening on port 4040.
- Check the URL or file path in your client configuration.
- For `mcp-remote`, confirm Node.js is installed and on your PATH.
- If `localhost:4040` does not respond but the machine's network address does, another local process is holding the loopback port (some editor and MCP extensions bind it). See the HTTP section above.

Model or query errors:

- Confirm your model files are under the directory you pointed the server at.
- Check the model syntax.

Claude Desktop keeps its own MCP log under Developer > Open MCP Log file, and `mcp-remote` prints connection errors to the client's MCP log.

## Further reading

- [Publisher README](https://github.com/malloydata/publisher): build and run instructions and the product overview.
- [configuration.md](configuration.md): the full environment-variable and CLI-flag reference (including `MCP_PORT`).
- [docs/agent-skills](agent-skills/): the agent skills and how to author them.
- [givens.md](givens.md): runtime parameters.
