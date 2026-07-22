# AI Agents

## Overview

Publisher speaks the [Model Context Protocol (MCP)](https://modelcontextprotocol.io), so an AI agent can work with your Malloy models over a standard interface. Because a Malloy model already carries the business logic and the relationships between entities, an agent grounds its answers in your definitions instead of guessing at table and column names.

There are two ways in, depending on how the agent runs. An interactive session connects over [MCP](#mcp-server-port-4040). An agent working unattended that started the server itself cannot connect to MCP mid-session (its tool list was fixed before the server existed), so it uses the [REST API](#unattended-and-one-shot-agents-the-rest-loop) instead, which covers the whole workflow: discovery, query, compile, and reload.

Publisher exposes a single MCP server (port 4040) with the tools an agent needs: `malloy_getContext` to discover what the deployment exposes (environments, packages, sources, and the fields relevant to a question) and ground answers in real names, `malloy_searchDocs` to search the Malloy documentation, `malloy_executeQuery` to run Malloy queries, and, for authoring, `malloy_compile` to validate a model change without running it and `malloy_reloadPackage` to make a saved change queryable. It also serves the bundled agent skills as MCP prompts.

Any MCP-compatible client can connect: a desktop chat app, an IDE assistant, or your own script.

## MCP server (port 4040)

The server listens at `http://localhost:4040/mcp` (set the port with `--mcp_port` or `MCP_PORT`). Clients interact with it through tool calls.

### Discovery and grounding

- `malloy_getContext`: the entry point when you do not yet know the environment, package, or model names. It is progressive: call it with no arguments to list the environments (each with its package names), with an environment to list its packages, with a package to list its sources, and with a package plus a plain-English question to return the sources, views, named queries, and dimension and measure fields most relevant to it. This lets an agent discover what a deployment exposes and ground a query in names the model actually defines before writing it. Question-level retrieval is lexical (lunr/BM25) over the model's own text by default, so it matches the terms your model uses. A field named in `snake_case` (say `dep_delay`) indexes as one token, so a search for "delay" will not surface it; when a first pass comes up empty, list the package's sources or narrow with `sourceName` rather than forwarding the user's exact words. Servers started with `EMBEDDING_API_KEY` rank by embedding similarity instead, which closes that gap (the response then carries a `retrieval` field and per-entity `score`); see the "Semantic retrieval" section in [configuration.md](configuration.md).
- `malloy_searchDocs`: keyword search over a bundled index of the Malloy documentation, returning matching titles, URLs, and excerpts.

### Query tool

- `malloy_executeQuery`: run a Malloy query and return the results as JSON. Accepts `givens` for supplying values to model-declared [runtime parameters](givens.md).

### Authoring tools

- `malloy_compile`: compile Malloy source against a model and return structured diagnostics (`severity`, `message`, `line`, `character`) without running a query, so an agent can validate a change while authoring instead of firing a throwaway query. Positions are 0-based and relative to the model file with the submitted source appended to it.
- `malloy_reloadPackage`: recompile a package from its on-disk content so a source or view added after boot becomes queryable by name, without restarting the server. This is the other half of the authoring loop: validate with `malloy_compile`, save, reload, then query. A reload that fails to compile leaves the package's files alone and keeps serving the previously compiled model, returning the compile errors.

### Skills as MCP prompts

The server also serves the bundled agent [skills](../skills/) as MCP prompts. A host that ingests MCP but does not read skill files from disk (for example Codex, ChatGPT, or Cursor) can pull the same guidance through this channel. MCP prompts are on-demand: a client lists them and the user or host selects one, so guidance that is always-on for skill-aware hosts becomes opt-in here. For authoring or contributing skills, see [docs/agent-skills](agent-skills/).

MCP also defines resources (for example links to a data dictionary). These are a newer part of the standard and many clients do not use them yet; a tool like the MCP Inspector lets you explore them.

The server does not require authentication, and `malloy_executeQuery` runs Malloy against the databases your models connect to, so anyone who can reach this port can read that data. The surface is not read-only either: `malloy_reloadPackage` mutates server state, and for a package that carries an install location a reload re-fetches it, overwriting on-disk edits. The same effects are already reachable through the equivalent REST endpoints, so this is a reason to gate the deployment rather than a reason to avoid the tools. The server binds `0.0.0.0` by default, which also exposes it on your network. Bind it to loopback with `--host 127.0.0.1` for local-only use, and put an authenticating gateway in front before exposing it more widely.

## Connecting a client

These examples assume Publisher is already running (`npx @malloy-publisher/server --port 4000` needs only Node.js on your PATH). See the [README](https://github.com/malloydata/publisher) for install and run options.

### Over HTTP

Clients such as Cursor and VS Code connect straight to the HTTP endpoint. The exact config shape varies by client (key names differ, for example VS Code uses `servers` rather than `mcpServers`), but each entry points an MCP server at a URL:

```json
{
  "mcpServers": {
    "malloy": { "type": "http", "url": "http://localhost:4040/mcp" }
  }
}
```

Add or drop the `"type": "http"` field to match your client. Clients that speak only stdio (for example older Claude Desktop builds) connect through `mcp-remote`, below.

If a client cannot reach `localhost:4040`, another local process may be holding that loopback port (some editor and MCP extensions bind it). Point the client at the machine's network address instead, or move Publisher's MCP server to another port with `--mcp_port`.

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

- `POST …/models/{path}/compile` with `{"source": "…"}` returns `{"status": "success" | "error", "problems": […]}` without running anything, plus the generated `sql` when the body sets `"includeSql": true`. Same semantics as `malloy_compile`, but the wire shape differs: each REST problem carries `severity`, `message`, `code`, and a position nested at `at.range.start/end.{line, character}` (the MCP tool flattens these to `line`/`character`), and `at.url` names a temporary compile-check overlay of the model, not the model file itself. Positions are 0-based and count the model's own lines first, since your source is appended to it.
- After editing a package's files, `GET …/packages/{pkg}?reload=true` recompiles it, the REST form of `malloy_reloadPackage`. A successful reload returns the package metadata; re-fetch the model to confirm the edit took. A reload that fails to compile leaves the files alone and keeps serving the previous model.

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

`--config` boots only your environment, so there is no example download to wait for, and `--watch-env local` mounts the package in place so a saved model edit recompiles on its own. A save that fails to compile is skipped without a signal, so compile-check first. `GET /api/v0/watch-mode/status` reports whether watching is on (`enabled`) and for which environment. Then poll `/status` and query as above.

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
