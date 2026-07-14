# Working with Malloy Publisher

Publisher is the open-source semantic model server for [Malloy](https://malloydata.dev). It serves one or more Malloy model packages over a REST API and a single MCP endpoint. If you are an AI agent working in this repo, here is what you can do with it and how to start.

## What you can do

- Discover what data exists: environments, packages, models, sources, and fields, without knowing any names in advance.
- Answer plain-English questions by running Malloy queries, which Publisher compiles to SQL and runs against the connected database.
- Build and change Malloy models: validate an edit with `malloy_compile`, save it, then `malloy_reloadPackage` to run it by name. The `malloy-modeling` skill covers the workflow.
- Build a data app: a hand-authored HTML page in a package's `public/` directory, backed by that package's models and served by Publisher with no build step. The `html-data-apps` skill covers it.
- Review Malloy for correctness with the `malloy-review` skill.

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

Connect the client after the server is up. An MCP client discovers a server's tools when it connects, so if the client was already running when you started the server (for example you asked the agent to start it), its `malloy_*` tools stay missing until it reconnects. In Claude Code, reconnect with `/mcp` or restart Claude. The simplest path is to start the server first, then launch the agent.

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
- `malloy_reloadPackage`: recompile a package from its on-disk model files so a source or view you added or changed after boot becomes queryable by name, without restarting the server. Use it to close the edit-and-run loop: validate with `malloy_compile`, save, `malloy_reloadPackage`, then `malloy_executeQuery` the new view.
- `malloy_searchDocs`: search the Malloy language documentation when you need syntax.

## 4. A first run, end to end

Ask "what can I explore here?" A good sequence is:

1. `malloy_getContext` with no arguments, then pick an environment (the bundled one is `malloy-samples`).
2. `malloy_getContext` with that environment, then pick a package (the samples include `ecommerce`, `imdb`, and `faa`).
3. `malloy_getContext` with the package and your question, to get the source, view, and field names.
4. `malloy_executeQuery` with those names, to get the answer. Charts and dashboards defined in the model render in the UI at http://localhost:4000.

## 5. Skills

The [`skills/`](skills/) directory holds task-specific guides. They are symlinked into `.claude/skills/`, so Claude Code auto-discovers them, and other hosts can pull the same content as MCP prompts from the endpoint above. Start with `getting-started`. Use `malloy-modeling` to build or change a model, `malloy-analysis` to explore and answer questions, and `malloy-review` to check Malloy for correctness.

## 6. Iterating on a model (watch mode)

For your own fast checks while authoring, use `malloy_compile`; it validates a change and returns diagnostics with no server restart. When you save a model edit and want its new sources and views queryable by name, call `malloy_reloadPackage` (or `GET /api/v0/environments/<env>/packages/<pkg>?reload=true`), which recompiles just that package from disk in place, with no restart. Publisher compiles each configured package at boot and serves that cached model, so a source you add afterwards is not resolvable by name until the package is reloaded. That closes the edit-and-run loop without watch mode.

Compile-check with `malloy_compile` before you reload. A reload whose models do not compile removes the package's on-disk copy under `publisher_data/` and drops the cached package, and a second reload cannot bring it back. Restarting with `--init` restores the package as configured, but it wipes all of `publisher_data/` first, so it does not recover your edits and it discards every other in-place edit too. Watch-mode symlink mounts are exempt from the removal.

Both tools read the copy under `publisher_data/<env>/<pkg>/`. For an env that is not in watch mode, that is a copy Publisher made of the configured source, so editing the original source directory does nothing until you re-copy it. Editing the `publisher_data/` copy is fine for a quick iteration, but keep the source of truth outside it: nothing there is version-controlled and `--init` wipes the whole tree. Watch mode mounts your own source directory in place, which is the durable way to iterate.

Watch mode is a separate, optional thing for a human: it is how someone launches the server so that they and any open browser tab see model edits live. It is a launch-time choice for whoever starts the server, not something to turn on by restarting a server that is already running. To use it, start the server with `--watch-env <env>` (or `PUBLISHER_WATCH=<env>`), which names an environment whose packages Publisher mounts in place (as symlinks) and watches, so edits to the source recompile. Requirements:

- The environment's packages must be LOCAL directories, not `github`, `gcs`, or `s3` URLs. The bundled `malloy-samples` env is remote, so it is not watch-eligible; point `--watch-env` at a local env of your own.
- The in-place mount is set up when the environment is first loaded from config: the first boot on a fresh server root (empty `publisher_data/` storage), or any boot with `--init`. If you previously started the env WITHOUT `--watch-env`, its packages were copied into `publisher_data/` and edits to your source do nothing; run once with both flags together, `--watch-env <env> --init`, to re-mount them in place (`--init` alone re-copies, it does not symlink). You do NOT need `--init` on every boot: once a package is mounted as a symlink it stays one, and later boots keep watching.
- Only the first environment in the watch list auto-reloads.

A save that fails to compile is skipped without a signal, so if a change does not appear, compile-check it first with `malloy_compile`.
