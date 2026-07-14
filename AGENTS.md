# Working with Malloy Publisher

Publisher is the open-source semantic model server for [Malloy](https://malloydata.dev). It serves one or more Malloy model packages over a REST API and a single MCP endpoint. If you are an AI agent working in this repo, here is what you can do with it and how to start.

## What you can do

- Discover what data exists: environments, packages, models, sources, and fields, without knowing any names in advance.
- Answer plain-English questions by running Malloy queries, which Publisher compiles to SQL and runs against the connected database.
- Build and validate Malloy models, guided by the bundled skills.

All of it runs against a local server you start in step 1 and reach over MCP in step 2.

## 1. Start the server first

The MCP tools talk to a running server, so nothing works until it is up.

From a clone:

```bash
bun install
bun run build && bun run start        # REST on :4000, MCP on :4040
```

To re-initialize the sample storage on a later run, build first and then start with `--init`: `bun run build && bun run start:init`. Without cloning, `npx @malloy-publisher/server --port 4000` runs the published build.

First boot clones the DuckDB sample packages, so wait for the server to report `serving` (about 30 to 60 seconds):

```bash
curl -s http://localhost:4000/api/v0/status | jq .operationalState   # -> "serving"
```

## 2. Connect your agent

Publisher exposes one MCP endpoint: `http://localhost:4040/mcp` (streamable HTTP, stateless, unauthenticated; put it behind a gateway if you expose it beyond localhost).

Claude Code: this repo ships a project `.mcp.json`, so from a clone Claude Code offers to connect on first run. Approve it once. To add it elsewhere:

```bash
claude mcp add --transport http malloy http://localhost:4040/mcp
```

Cursor: add to `.cursor/mcp.json` or global settings:

```json
{ "mcpServers": { "malloy": { "url": "http://localhost:4040/mcp" } } }
```

Codex: add to `~/.codex/config.toml`:

```toml
[mcp_servers.malloy]
url = "http://localhost:4040/mcp"
```

stdio-only clients (older Claude Desktop) bridge through mcp-remote:

```json
{ "mcpServers": { "malloy": { "command": "npx", "args": ["-y", "mcp-remote", "http://localhost:4040/mcp", "--allow-http"] } } }
```

## 3. The MCP tools

- `malloy_getContext`: discovery and grounding. Call it with as much as you know and omit the rest. No arguments lists the environments, an environment lists its packages, a package lists its sources, and a plain-English query returns the most relevant sources, views, and fields. Use the names it returns verbatim.
- `malloy_executeQuery`: run a Malloy query (a named view or query, or ad-hoc code) against a model and get JSON back.
- `malloy_compile`: compile-check Malloy source against a model and get structured diagnostics back (severity, message, line and column) without running a query. Use it to validate a model or a change while authoring, instead of firing a throwaway query.
- `malloy_searchDocs`: search the Malloy language documentation when you need syntax.

## 4. A first run, end to end

Ask "what can I explore here?" A good sequence is:

1. `malloy_getContext` with no arguments, then pick an environment (the bundled one is `malloy-samples`).
2. `malloy_getContext` with that environment, then pick a package (the samples include `ecommerce`, `imdb`, and `faa`).
3. `malloy_getContext` with the package and your question, to get the source, view, and field names.
4. `malloy_executeQuery` with those names, to get the answer. Charts and dashboards defined in the model render in the UI at http://localhost:4000.

## 5. Skills

The [`skills/`](skills/) directory holds task-specific guides. They are symlinked into `.claude/skills/`, so Claude Code auto-discovers them, and other hosts can pull the same content as MCP prompts from the endpoint above. Start with `getting-started`. Use `malloy-modeling` to build or change a model, `malloy-analysis` to explore and answer questions, and `malloy-review` to check Malloy for correctness.
