# AI Agents with the Model Context Protocol (MCP)

## Overview

Publisher speaks the [Model Context Protocol (MCP)](https://modelcontextprotocol.io), so an AI agent can work with your Malloy models over a standard interface. Because a Malloy model already carries the business logic and the relationships between entities, an agent grounds its answers in your definitions instead of guessing at table and column names.

Publisher runs two MCP servers side by side:

- **Core MCP server** (port 4040): discovery and query tools. An agent lists environments, packages, and models, reads model source, and runs Malloy queries.
- **Agent MCP server** (port 4041): retrieval tools plus the bundled agent skills. An agent finds the model entities relevant to a question, searches the Malloy documentation, and pulls guidance skills.

Any MCP-compatible client can connect: a desktop chat app, an IDE assistant, or your own script.

## Core MCP server (port 4040)

The core server listens at `http://localhost:4040/mcp` (set the port with `--mcp_port` or `MCP_PORT`). Clients interact with it through tool calls.

### Discovery tools

- `malloy_environmentList`: list the available environments.
- `malloy_packageList`: list the packages in an environment.
- `malloy_packageGet`: list the models in a package.
- `malloy_modelGetText`: read the source text of a model file.

### Query tool

- `malloy_executeQuery`: run a Malloy query and return the results as JSON. Accepts `givens` for supplying values to model-declared [runtime parameters](givens.md), and the deprecated `filterParams` argument for the legacy [`#(filter)` path](filters.md).

### Prompts and resources

MCP also defines prompts and resources (for example suggested questions or links to a data dictionary). These are newer parts of the standard and many clients do not use them yet. A tool like the MCP Inspector lets you explore them.

## Agent MCP server (port 4041)

Alongside the core server, Publisher runs a second MCP server on port 4041 (set by `AGENT_MCP_PORT`, bound to `PUBLISHER_HOST`) aimed at AI agents. Point a client at `http://localhost:4041/mcp`. It adds two read-only retrieval tools:

- `malloy_getContext`: given a package and a plain-English question, return the model entities most relevant to it (sources, views, named queries, and dimension and measure fields). This lets an agent ground a query in names the model actually defines before writing it. Retrieval is lexical (lunr/BM25) over the model's own text, so it matches the terms your model uses. A field named in `snake_case` (say `dep_delay`) indexes as one token, so a search for "delay" will not surface it; when a first pass comes up empty, query around the source to enumerate its fields rather than forwarding the user's exact words.
- `malloy_searchDocs`: keyword search over a bundled index of the Malloy documentation, returning matching titles, URLs, and excerpts.

### Skills as MCP prompts

The agent server also serves the bundled agent [skills](../skills/) as MCP prompts. A host that ingests MCP but does not read skill files from disk (for example Codex, ChatGPT, or Cursor) can pull the same guidance through this channel. MCP prompts are on-demand: a client lists them and the user or host selects one, so guidance that is always-on for skill-aware hosts becomes opt-in here. For authoring or contributing skills, see [docs/agent-skills](agent-skills/).

Neither server requires authentication, and the core server's `malloy_executeQuery` runs Malloy against the databases your models connect to, so anyone who can reach these ports can read that data. The server binds `0.0.0.0` by default, which also exposes it on your network. Bind it to loopback with `--host 127.0.0.1` for local-only use, and put an authenticating gateway in front before exposing it more widely.

## Connecting a client

These examples assume Publisher is already running. Running it needs Node.js on your PATH (the quick start below uses `npx`), and the Claude Desktop bridge additionally needs Python 3. See the [README](https://github.com/malloydata/publisher) for install and run options.

### Over HTTP

Clients such as Cursor and VS Code connect straight to the HTTP endpoints. The exact config shape varies by client (key names differ, for example VS Code uses `servers` rather than `mcpServers`), but each entry points an MCP server at a URL:

```json
{
  "mcpServers": {
    "malloy": { "type": "http", "url": "http://localhost:4040/mcp" },
    "malloy-agent": { "type": "http", "url": "http://localhost:4041/mcp" }
  }
}
```

Add or drop the `"type": "http"` field to match your client. Current Claude Desktop does not accept an HTTP entry here; connect it through the Python bridge below or its Connectors UI.

If a client cannot reach `localhost:4040`, another local process may be holding that loopback port (some editor and MCP extensions bind it). Point the client at the machine's network address instead, or move Publisher's core server to another port with `--mcp_port`. The agent server on 4041 is usually unaffected.

### With Claude Desktop through the Python bridge (older versions)

Older Claude Desktop builds (around 0.11.4) do not speak HTTP MCP directly and need a small bridge script that translates between the client and the server.

1. Start the server. Run from an empty directory and Publisher serves its default DuckDB sample packages (`ecommerce`, `imdb`, `faa`), which it downloads from the malloy-samples repository on first run, so the first start needs network access and can take a minute:
   ```bash
   npx @malloy-publisher/server
   ```
   You should see `MCP server listening at http://0.0.0.0:4040` (reach it at `http://localhost:4040`). To serve your own models instead, point the server at their directory with `--server_root <path>`.
2. Download the bridge script: [malloy_bridge.py](https://raw.githubusercontent.com/malloydata/publisher/main/packages/server/dxt/malloy_bridge.py).
3. In Claude Desktop, open Settings > Developer > Edit Config and add an entry that runs the script:
   ```json
   {
     "mcpServers": {
       "malloy": {
         "command": "python3",
         "args": ["/path/to/malloy_bridge.py"],
         "env": {}
       }
     }
   }
   ```
4. Save the file and start a conversation. Claude discovers your models through the tools and answers questions about them. [Watch a short demo.](https://www.loom.com/share/fcc5112ac1ca4bf78bee0985f1cd31be)

Example prompts against the bundled samples:

- "Use Malloy to run an exploratory analysis on the FAA flight data."
- "Use Malloy to help me understand the ecommerce data, and chart the results."
- "Use Malloy to check how many movies Tom Hanks has been in."

## Troubleshooting

Connection errors:

- Confirm the server is running and listening on port 4040 (and 4041 for the agent server).
- Check the URL or file path in your client configuration.
- For the bridge, confirm Python 3 is installed and on your PATH.
- If `localhost:4040` does not respond but the machine's network address does, another local process is holding the loopback port (some editor and MCP extensions bind it). See the HTTP section above.

Model or query errors:

- Confirm your model files are under the directory you pointed the server at.
- Check the model syntax.

For the bridge, the script writes a detailed log to `/tmp/malloy_bridge.log`, and Claude Desktop keeps its own MCP log under Developer > Open MCP Log file.

## Further reading

- [Publisher README](https://github.com/malloydata/publisher): build and run instructions, configuration, and the full environment-variable reference (including `MCP_PORT` and `AGENT_MCP_PORT`).
- [docs/agent-skills](agent-skills/): the agent skills and how to author them.
- [givens.md](givens.md) and [filters.md](filters.md): runtime parameters and source filters.
