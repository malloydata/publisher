---
name: malloy-publisher-setup
description: Diagnose missing Malloy Publisher MCP tools and get from nothing to a running server. Use when the tools are missing, when asking how to start the server, or when setting up Publisher for the first time.
---

# Setting up Malloy Publisher

Goal: get from nothing to a client that can see the `malloy_*` tools. Once they are there, switch to `skill:malloy-getting-started` for discovery and a first query.

## 0. Confirm the tools are reachable

At minimum you need `malloy_getContext`, `malloy_executeQuery`, and `malloy_searchDocs`. Authoring a model also needs `malloy_compile` and `malloy_reloadPackage` (the edit-and-run loop below); an older Publisher may not serve those two.

| Tool | Purpose |
|------|---------|
| `malloy_getContext` | Ground yourself in a package: its sources, views, and fields |
| `malloy_executeQuery` | Run ad-hoc queries for validation |
| `malloy_compile` | Compile-check a change and get diagnostics back without running a query |
| `malloy_reloadPackage` | Recompile a package from disk so a saved edit becomes queryable by name |
| `malloy_searchDocs` | Search Malloy docs (call BEFORE unfamiliar patterns) |
| `malloy_searchDatabaseSchema` | Find the tables in a database connection by plain-English description, when modelling data that is not in a package yet. Returns each table's columns and the `source:` line to start from. Names and types only: no row value is returned |

Never guess field names. Ground yourself with `malloy_getContext` to see the sources and fields a package defines.

### The edit-and-run loop

Publisher compiles each configured package at boot and serves that cached model, so a source or view you add afterwards is not queryable by name until you reload the package. The loop is:

1. **Validate** the change with `malloy_compile`, picking the scope that matches what you are doing:
   - Adding a new definition or query: the default (`scope: "append"`) compiles your text in the model's namespace. Note its diagnostic positions land in the model-plus-your-text concatenation.
   - **Editing an existing definition: `scope: "file"`**, with the whole edited file as `source`. It compiles your text AS the file (append would collide with "Cannot redefine"), and diagnostics land at the true line numbers of your text.
   - Before saving a change other files import: `scope: "package"` with the edited file as `source` runs reload's worker compiler over every `.malloy` and `.malloynb` file against your edit, so a rename that breaks an importer surfaces now instead of at reload. Each diagnostic carries `model`, the file it points at; files hidden from discovery can appear. If `modelPath` does not exactly match an existing file, a warning says the source was treated as new.
2. **Save** it to the package's model file.
3. **Reload** with `malloy_reloadPackage`.
4. **Run** the new view with `malloy_executeQuery`.

A reload that fails to compile is safe: your files are left alone and the previously compiled model keeps serving, with the compile errors returned to you. Compile first anyway for faster feedback, and a `scope: "package"` dry-run with no `source` uses reload's compiler and file selection (imports across files, every `.malloy` and `.malloynb` file as saved) without touching the served model. Keep the source of truth outside `publisher_data/`, which is not version-controlled and is wiped by a `--init` restart. If these tools are missing, the Publisher you are connected to predates them; fall back to validating with a throwaway `malloy_executeQuery`. An older Publisher that has `malloy_compile` but rejects `scope` supports only the append behavior.

If none of the tools are there, either the server is not running or your client connected before it was. Start the server (`npx @malloy-publisher/server --port 4000`, or `bun run build && bun run start` from a clone) and wait until `curl -s http://localhost:4000/api/v0/status` reports `operationalState: serving`. If the point is to author models against a local package, add `--watch-env <env>`: without it Publisher copies local packages at boot and serves the copies, so saved edits are never read.

If there is no Publisher workspace here at all, and the user wants to work with data of their own rather than the bundled examples, `npm create @malloy-publisher/malloy-package@latest <name>` scaffolds one: the package and a starter model, registered so the server actually serves it, plus the start script, the MCP config and these skills. Keep the `@latest` when you type it: `npm create` resolves through npm's npx cache and an unversioned name is satisfied by any copy already there, so on a machine that has scaffolded before npm never asks the registry and you get an old scaffolder pinning an old server, with nothing to say so. Run bare, it comes with a small sample dataset, so there is something to query straight away. In a fresh directory `npm start` then runs the pinned server against the package in watch mode; if the directory already had a `package.json` the scaffolder leaves it alone and adds no script, printing the equivalent `npx` command to use instead. Where you run it matters: only the package lands in `<name>/`, and the workspace files, the agent instructions and the MCP config among them, are written to the current directory. Run it here if this directory is empty or is meant to become the workspace. If it already holds other work, scaffold into a new directory instead (`mkdir my-data && cd my-data`), because agent config is discovered by walking up, so writing those files here changes what every session beneath this directory inherits. Seed the starter model from a local file with `npm create @malloy-publisher/malloy-package@latest <name> -- --data <path/to/their-file.csv>` (CSV, Parquet, or Excel `.xlsx`), keeping the `--`, which is how `npm create` passes options through. That path is relative to wherever you run the command, so if you scaffolded into a new directory it has to reach back out to their file; the scaffolder copies it into the package and leaves the original alone. A seeded package starts smaller than the sample one, since the scaffolder does not read their columns: expect a row count and an overview, and build the model from there. A package is just Malloy, so it can instead query a database connection the config defines. Because it writes a `.mcp.json` that did not exist when the client connected, the user has to restart or reconnect once before these tools appear, and their client will ask them to approve the new project-scoped server the first time. That only works when the workspace is at the session's own root, so if you scaffolded into a new directory below that root, the user has to open a session there instead: a `.mcp.json` further down is never discovered.

If you started the server yourself in this session, the tools still will not appear: your tool list was fixed when you connected, and you cannot reconnect yourself. Tell the user the tools are missing for that reason and ask them to run `/mcp`, select `malloy`, and choose Reconnect. The panel offers `Authenticate` first and reports `Auth: not authenticated`; that is a red herring, the endpoint has no auth. Restarting Claude Code also works. Continue once the tools are there.

Two escape hatches worth knowing:

- **When the session cannot be relaunched from the workspace directory** (a project `.mcp.json` is only discovered by sessions that *start* in its directory), register the server at user scope so the directory stops mattering: `claude mcp add --transport http malloy http://localhost:4040/mcp -s user` (use the MCP port the server actually bound; its startup log prints it). Caveat: for sessions that do start in the workspace, the project `.mcp.json` shadows the user-scoped entry, so prefer the project file when it is discoverable.
- **Do not trust an existing `.mcp.json`'s URL blindly.** The file outlives the server that wrote it, and a boot that failed partway (for example, the REST port was taken) can leave it pointing at a dead port while a live server sits on another. If connecting fails or answers look wrong, confirm identity with `malloy_getContext`, which names the environment and packages you are really talking to; that check works on every platform, which the port check does not (`lsof -iTCP:4040 -sTCP:LISTEN` on macOS and Linux, `netstat -ano | findstr :4040` on Windows).

When a user is present, do not route around it by calling the REST API with curl. It appears to work, so the user never learns their session is missing the tools, and you lose what they are for: grounded discovery instead of guessed names, `malloy_compile` instead of throwaway queries, and `malloy_reloadPackage` instead of a restart. Say the tools are missing and let the user fix it in five seconds. Running unattended, with nobody who can reconnect you, is different: there the REST API is the supported interface, not a workaround. Discovery, query, compile, and reload all have REST equivalents (`malloy_searchDocs` and `malloy_getContext`'s plain-English ranking do not; read the bundled skills for syntax and ground from model metadata instead); the running server serves the full spec at `http://localhost:4000/api-doc.yaml`, and AGENTS.md carries the endpoint map.

### Why the tools are missing

Tell these apart by what is missing. The first three look similar from the outside and do not share a fix:

| Symptom | Cause | Fix |
| --- | --- | --- |
| `malloy` is listed in `/mcp` but disconnected | client connected before the server existed | Reconnect (or relaunch the agent) |
| `malloy` is not listed in `/mcp` at all, and `.mcp.json` is here | session started outside this directory | relaunch the agent from here. A session that starts here reads this file, so a user-scoped entry would be shadowed by it; user scope is for sessions started elsewhere |
| `malloy` is not listed in `/mcp` at all, and there is no `.mcp.json` here | the server skipped writing one, or the write failed; its startup log says which | run the `claude mcp add` line the server printed, from the directory you start the agent in. Relaunching alone cannot help when there is no config to find, and note a config may exist at the repository root instead of here |
| tools present, no skills auto-invoked | session started outside this directory, the same cause as the second row | relaunch the agent from here; skills are rescanned as the working directory changes |

## 0.5 Connect a client (CLI or extension)

Publisher exposes one MCP endpoint: `http://localhost:4040/mcp` (streamable HTTP, stateless, unauthenticated; put it behind a gateway if you expose it beyond localhost). Connect the client after the server is up.

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

