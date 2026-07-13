# AI Agents with the Model Context Protocol (MCP)

## Overview

Publisher speaks the [Model Context Protocol (MCP)](https://modelcontextprotocol.io), so an AI agent can work with your Malloy models over a standard interface. Because a Malloy model already carries the business logic and the relationships between entities, an agent grounds its answers in your definitions instead of guessing at table and column names.

Publisher exposes a single MCP server (port 4040) with the tools an agent needs: `malloy_getContext` to discover what the deployment exposes (environments, packages, sources, and the fields relevant to a question) and ground answers in real names, `malloy_searchDocs` to search the Malloy documentation, and `malloy_executeQuery` to run Malloy queries. It also serves the bundled agent skills as MCP prompts.

Any MCP-compatible client can connect: a desktop chat app, an IDE assistant, or your own script.

## MCP server (port 4040)

The server listens at `http://localhost:4040/mcp` (set the port with `--mcp_port` or `MCP_PORT`). Clients interact with it through tool calls.

### Discovery and grounding

- `malloy_getContext`: the entry point when you do not yet know the environment, package, or model names. It is progressive: call it with no arguments to list the environments (each with its package names), with an environment to list its packages, with a package to list its sources, and with a package plus a plain-English question to return the sources, views, named queries, and dimension and measure fields most relevant to it. This lets an agent discover what a deployment exposes and ground a query in names the model actually defines before writing it. Question-level retrieval is lexical (lunr/BM25) over the model's own text, so it matches the terms your model uses. A field named in `snake_case` (say `dep_delay`) indexes as one token, so a search for "delay" will not surface it; when a first pass comes up empty, list the package's sources or narrow with `sourceName` rather than forwarding the user's exact words.
- `malloy_searchDocs`: keyword search over a bundled index of the Malloy documentation, returning matching titles, URLs, and excerpts.

### Query tool

- `malloy_executeQuery`: run a Malloy query and return the results as JSON. Accepts `givens` for supplying values to model-declared [runtime parameters](givens.md), and the deprecated `filterParams` argument for the legacy [`#(filter)` path](filters.md).

### Skills as MCP prompts

The server also serves the bundled agent [skills](../skills/) as MCP prompts. A host that ingests MCP but does not read skill files from disk (for example Codex, ChatGPT, or Cursor) can pull the same guidance through this channel. MCP prompts are on-demand: a client lists them and the user or host selects one, so guidance that is always-on for skill-aware hosts becomes opt-in here. For authoring or contributing skills, see [docs/agent-skills](agent-skills/).

MCP also defines resources (for example links to a data dictionary). These are a newer part of the standard and many clients do not use them yet; a tool like the MCP Inspector lets you explore them.

The server does not require authentication, and `malloy_executeQuery` runs Malloy against the databases your models connect to, so anyone who can reach this port can read that data. The server binds `0.0.0.0` by default, which also exposes it on your network. Bind it to loopback with `--host 127.0.0.1` for local-only use, and put an authenticating gateway in front before exposing it more widely.

## Connecting a client

These examples assume Publisher is already running. Running it needs Node.js on your PATH (the quick start below uses `npx`). See the [README](https://github.com/malloydata/publisher) for install and run options.

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

- "Use Malloy to run an exploratory analysis on the FAA flight data."
- "Use Malloy to help me understand the ecommerce data, and chart the results."
- "Use Malloy to check how many movies Tom Hanks has been in."

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

- [Publisher README](https://github.com/malloydata/publisher): build and run instructions, configuration, and the full environment-variable reference (including `MCP_PORT`).
- [docs/agent-skills](agent-skills/): the agent skills and how to author them.
- [givens.md](givens.md) and [filters.md](filters.md): runtime parameters and source filters.
