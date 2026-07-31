# Working with Malloy Publisher

Publisher is the open-source semantic model server for [Malloy](https://malloydata.dev). It serves one or more Malloy model packages over a REST API and a single MCP endpoint. If you are an AI agent working in this repo, here is what you can do with it and how to start.

## What you can do

- Discover what data exists: environments, packages, models, sources, and fields, without knowing any names in advance.
- Answer plain-English questions by running Malloy queries, which Publisher compiles to SQL and runs against the connected database.
- Build and change Malloy models: validate an edit with `malloy_compile`, save it, then `malloy_reloadPackage` to run it by name. The `malloy-modeling` skill covers the workflow.
- Build a data app: a hand-authored HTML page in a package's `public/` directory, backed by that package's models and served by Publisher with no build step. The `malloy-html-data-apps` skill covers it.
- Review Malloy for correctness with the `malloy-review` skill.

All of it runs against a local server you start in step 1 and reach over MCP in step 2, or over REST when you work unattended (section 7).

## 1. Start the server first

The MCP tools talk to a running server, so nothing works until it is up.

From a clone:

```bash
bun install
bun run build && bun run start        # REST on :4000, MCP on :4040
```

To re-initialize the sample storage on a later run, build first and then start with `--init`: `bun run build && bun run start:init`. Without cloning, `npx @malloy-publisher/server --port 4000` runs the published build. Start one npx server at a time: concurrent first runs can race in the shared npx cache and corrupt the install ([docs/deployment.md](docs/deployment.md#run-with-npx) has the recovery step).

On startup the server creates a `.mcp.json` in the directory it was run in, naming the MCP port it bound, which is why a session started in that directory finds the `malloy_*` tools with no registration step. It does not always create one, so do not promise a user the file exists without checking: it skips a directory that already has a `.mcp.json` (left exactly as it is, unread, because it may hold other servers and their credentials), any directory inside a git working tree (a clone included, which ships its own at the repo root, but also the user's own project), the home directory, the filesystem root, and any run where the MCP port it bound is not the one it asked for (`--mcp_port 0`, or a non-numeric `MCP_PORT` under Bun; Node refuses to bind at all), because that port changes every run. A failed write is another way to end up with no file. Every one of these logs the `claude mcp add` command that connects an agent anyway, so read the startup output rather than assuming, and `ls -a` if you need certainty. Use that command as printed: it is deliberately local scope, because a project `.mcp.json` outranks a user-scoped entry, so `-s user` would be silently shadowed by the very file that caused the message. The file also outlives the server and is never corrected. Do not treat a stale one as harmless: if another process holds that port now, an agent started there connects to it and answers from the wrong data, so check the URL in the file against the endpoint in this boot's log before trusting it. Never delete a `.mcp.json` you did not create; it may hold other servers and their credentials. `--no-mcp-config` turns it off, and the Docker image sets `PUBLISHER_NO_MCP_CONFIG=1`.

Poll until the server reports `serving` rather than assuming a fixed wait. From a clone the sample packages are read straight from `examples/`, so this is usually seconds. A first `npx` run has to download the published package and then fetch the samples from GitHub, which is network-bound and can push it to a minute or two:

```bash
curl -s http://localhost:4000/api/v0/status | jq .operationalState   # -> "serving"
```

The server also prints one `PUBLISHER_READY` line to stderr at the moment it reaches `serving`,
carrying environment, package, and load-error counts, so a script can watch for that line instead
of polling; if initialization fails, `PUBLISHER_INIT_FAILED` is printed in its place. A first-run
download reports its clone progress on stderr too.

`serving` does not mean everything loaded. A package that fails to load is skipped, not fatal, so the
server serves whatever did load and the package is simply absent. If data you expect is missing, check
`curl -s http://localhost:4000/api/v0/status | jq .loadErrors`, which is absent when everything loaded
and otherwise names each environment or package that did not load, and why.

## 2. Connect your agent

Publisher exposes one MCP endpoint: `http://localhost:4040/mcp` (streamable HTTP, stateless, unauthenticated; put it behind a gateway if you expose it beyond localhost).

Connect the client after the server is up. An MCP client discovers a server's tools when it connects, so if the client was already running when you started the server (for example you asked the agent to start it), its `malloy_*` tools stay missing until it reconnects. In Claude Code: run `/mcp`, select `malloy`, then choose **Reconnect**. That panel reports `Auth: not authenticated` and offers `Authenticate` as its first option, which does not apply here because the endpoint has no auth; Reconnect is the one that works. Restarting Claude also works. The simplest path is to start the server first, then launch the agent.

If you are the agent and you started the server during this session, your `malloy_*` tools will not show up however long you wait: your tool list was fixed when you connected. You cannot reconnect yourself. When a user is present, say so and ask them to run `/mcp`, select `malloy`, and choose Reconnect (the panel offers `Authenticate` first, which is not it), or to restart Claude. Do not quietly fall back to calling the REST API with curl instead: it hides a fixable problem the user can clear in seconds, and it gives up the grounded discovery, compile checks, and reload that the tools exist to provide. Running unattended, with nobody to reconnect you, is the other case: there the REST API is the supported interface, not a workaround. See section 7.

There is a third case, and it is the one that costs the most time because it looks exactly like the first: **a project's `.mcp.json` is only discovered from the directory the agent session *started* in.** A session launched from a parent directory, or from anywhere else, never sees the server, however long it waits and however many times it reconnects. Reconnecting cannot fix it, because the server was never in the client's list to reconnect to.

Skills are the near miss here, and they behave differently: `.claude/skills/` is rescanned as the working directory changes, so a session started further up picks them up on its own once work moves into this directory. That asymmetry is worth knowing precisely because the two symptoms look identical from the outside: same "the agent has nothing", different cause, different fix.

Tell the three apart by what is missing:

| Symptom | Cause | Fix |
| --- | --- | --- |
| `malloy` is listed in `/mcp` but disconnected | client connected before the server existed | Reconnect (or relaunch the agent) |
| `malloy` is not listed in `/mcp` at all, and `.mcp.json` is here | session started outside this directory | relaunch the agent from here. A session that starts here reads this file, so a user-scoped entry would be shadowed by it; user scope is for sessions started elsewhere |
| `malloy` is not listed in `/mcp` at all, and there is no `.mcp.json` here | the server skipped writing one, or the write failed; its startup log says which | run the `claude mcp add` line the server printed, from the directory you start the agent in. Relaunching alone cannot help when there is no config to find, and note a config may exist at the repository root instead of here |
| tools present, no skills auto-invoked | same as above | relaunch the agent from here; skills are rescanned as the working directory changes |

The registration that stops the directory mattering, because it is stored per user rather than per project. Use the port this server actually bound, which its startup log prints; `4040` below is only the default:

```bash
claude mcp add --transport http malloy http://127.0.0.1:4040/mcp -s user
```

This one is persistent and global: it outlives the session and the server. Undo it with `claude mcp remove malloy -s user`. Prefer the command the server printed, at its default local scope, unless the point is specifically to stop the directory mattering.

Note the two symptoms of that one cause do **not** share a fix: the MCP server list is fixed at session boot, so it needs either a relaunch or the user-scoped registration above, while skills need the session actually rooted in the directory. There is no user-scope equivalent for project skills short of symlinking them into `~/.claude/skills/`.

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
{
  "mcpServers": {
    "malloy": {
      "command": "npx",
      "args": ["-y", "mcp-remote", "http://localhost:4040/mcp", "--allow-http"]
    }
  }
}
```

## 3. The MCP tools

- `malloy_getContext`: discovery and grounding. Call it with as much as you know and omit the rest. No arguments lists the environments, an environment lists its packages, a package lists its sources, and a plain-English query returns the most relevant sources, views, and fields. Use the names it returns verbatim.
- `malloy_executeQuery`: run a Malloy query (a named view or query, or ad-hoc code) against a model and get JSON back.
- `malloy_compile`: compile-check Malloy source against a model and get structured diagnostics back (severity, message, line and column) without running a query. Use it to validate a model or a change while authoring, instead of firing a throwaway query.
- `malloy_reloadPackage`: recompile a package from its on-disk model files so a source or view you added or changed after boot becomes queryable by name, without restarting the server. Use it to close the edit-and-run loop: validate with `malloy_compile`, save, `malloy_reloadPackage`, then `malloy_executeQuery` the new view.
- `malloy_searchDocs`: search the Malloy language documentation when you need syntax.

## 4. A first run, end to end

Ask "what can I explore here?" A good sequence is:

1. `malloy_getContext` with no arguments, then pick an environment (the bundled one is `examples`).
2. `malloy_getContext` with that environment, then pick a package (the bundled packages are `storefront`, `governed-analytics`, and `html-data-app`).
3. `malloy_getContext` with the package and your question, to get the source, view, and field names.
4. `malloy_executeQuery` with those names, to get the answer. Charts and dashboards defined in the model render in the UI at http://localhost:4000.

## 5. Skills

The [`skills/`](skills/) directory holds task-specific guides. They are symlinked into `.claude/skills/`, so Claude Code auto-discovers them, and other hosts can pull the same content as MCP prompts from the endpoint above. Start with `malloy-getting-started`. Use `malloy-modeling` to build or change a model, `malloy-analysis` to explore and answer questions, and `malloy-review` to check Malloy for correctness. Most of these are shared, open-source Malloy skills kept in sync with an upstream repo; [`skills/README.md`](skills/README.md) explains what is shared, why `credible-*` skills never appear here, and how the bare tool names in shared skills map to this server's `malloy_*` tools.

## 6. Iterating on a model (watch mode)

For your own fast checks while authoring, use `malloy_compile`; it validates a change and returns diagnostics with no server restart. When you save a model edit and want its new sources and views queryable by name, call `malloy_reloadPackage` (or `GET /api/v0/environments/<env>/packages/<pkg>?reload=true`), which recompiles just that package from disk in place, with no restart. Publisher compiles each configured package at boot and serves that cached model, so a source you add afterwards is not resolvable by name until the package is reloaded. That closes the edit-and-run loop without watch mode.

A reload that fails to compile is safe: your files are left alone, the previously compiled model keeps serving, and the compile errors come back in the response. Compile-check with `malloy_compile` first anyway; it is faster feedback and keeps a broken model from reaching the reload at all.

Both tools read the copy under `publisher_data/<env>/<pkg>/`, which sits in the server root: the directory the server was launched from, unless `--server_root` set another. For an env that is not in watch mode, that is a copy Publisher made of the configured source, so editing the original source directory does nothing until you re-copy it. To tell which mode you are in, `GET /api/v0/watch-mode/status` reports whether watching is enabled and for which environment; a watch-mounted package also shows as a symlink in `publisher_data/`. Editing the `publisher_data/` copy is fine for a quick iteration, but keep the source of truth outside it: nothing there is version-controlled and `--init` wipes the whole tree. Watch mode mounts your own source directory in place, which is the durable way to iterate.

Watch mode is a separate, optional thing for a human: it is how someone launches the server so that they and any open browser tab see model edits live. It is a launch-time choice for whoever starts the server, not something to turn on by restarting a server that is already running. To use it, start the server with `--watch-env <env>` (or `PUBLISHER_WATCH=<env>`), which names an environment whose packages Publisher mounts in place (as symlinks) and watches, so edits to the source recompile. Requirements:

- The environment's packages must be LOCAL directories, not `github`, `gcs`, or `s3` URLs. From a clone, the bundled `examples` env is local and therefore watch-eligible. Under `npx` it is not: the published server has no repo to read, so its bundled default fetches the same packages from GitHub.
- The in-place mount is set up when the environment is first loaded from config: the first boot on a fresh server root (empty `publisher_data/` storage), or any boot with `--init`. If you previously started the env WITHOUT `--watch-env`, its packages were copied into `publisher_data/` and edits to your source do nothing; run once with both flags together, `--watch-env <env> --init`, to re-mount them in place (`--init` alone re-copies, it does not symlink). You do NOT need `--init` on every boot: once a package is mounted as a symlink it stays one, and later boots keep watching.
- Only the first environment in the watch list auto-reloads.

A save that fails to compile is skipped without a signal, so if a change does not appear, compile-check it first with `malloy_compile`.

## 7. Working unattended: the REST API

If you started the server yourself and there is no user to reconnect your MCP client (a one-shot task, a cloud sandbox), MCP is out of reach for the whole session. Use the REST API on port 4000 instead; discovery, query, compile, and reload are all there (`malloy_searchDocs` and `malloy_getContext`'s plain-English ranking stay MCP-only; for syntax, read the bundled [`skills/`](skills/) markdown). Like MCP it is unauthenticated, so keep it on localhost. The playbook with worked examples is [docs/ai-agents.md](docs/ai-agents.md), and the running server serves its complete OpenAPI spec at http://localhost:4000/api-doc.yaml. The map:

- `GET /api/v0/status`: poll until `operationalState` is `"serving"`, then check `loadErrors` (absent when everything loaded).
- `GET /api/v0/environments`: the environment names every other path needs (the bundled one is `examples`).
- `GET /api/v0/environments/{env}/packages`, then `…/packages/{pkg}/models`: what exists.
- `GET …/models/{path}`: the discovery step. The response's `sources` (each with its `views`), `queries`, and `givens` are the names you can run. Use them verbatim; never guess.
- `POST …/models/{path}/query`: run a query. The body is either `{"query": "run: …"}` (ad-hoc) or `{"queryName": "…", "sourceName": "…"}` (a named view; `queryName` alone runs a model-level named query). Add `"compactJson": true` and parse the `result` string to get plain row objects.
- `POST …/models/{path}/compile`: body `{"source": "…"}`; structured diagnostics without running anything.
- `GET …/packages/{pkg}?reload=true`: recompile a package after editing its files (the REST form of `malloy_reloadPackage`).
- `POST /api/v0/environments/{env}/packages` with `{"name": "…", "location": "/absolute/path"}`: serve a package of your own on a running server. [docs/packages.md](docs/packages.md) is the package format.

Reading this file without a clone? Every doc referenced here resolves at `https://raw.githubusercontent.com/malloydata/publisher/main/<path>`, for example `docs/ai-agents.md`.

## 8. Going deeper

- [`docs/`](docs/) is the reference hub, see its [index](docs/README.md). Start with [docs/ai-agents.md](docs/ai-agents.md) for per-client MCP config and the MCP tool reference.
- [`examples/`](examples/) holds the three served packages: [`storefront`](examples/storefront) (ecommerce model + dashboards), [`governed-analytics`](examples/governed-analytics) (givens, authorize, row-level access), and [`html-data-app`](examples/html-data-app) (a no-build HTML dashboard). [`data-app`](examples/data-app) is a standalone React SDK app, not a served package.
