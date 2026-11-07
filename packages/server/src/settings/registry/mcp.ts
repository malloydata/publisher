// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { setting } from "../setting";
import { boolean, integer, list, oneOf, string } from "../types";

/** The configuration design's `mcp` table. */
export const mcp = [
   setting({
      key: "mcp.port",
      type: integer({ min: 0, max: 65535 }),
      default: 4040,
      description: "MCP HTTP port.",
      notes: "MCP HTTP port. Serves the MCP tools (`list_packages`, `get_context`, `execute_query`, `compile_model`, `reload_package`, `get_status`, `search_malloy_docs`, `search_database_schema`, `get_skill`, `get_agent`, `list_reports`, `show_report`, and the panel-only `report_api`) and the agent skills as MCP prompts. `list_packages` is listed first because it is where an agent starts: `get_context` requires an environment and package in its `scopes`, and those names come from there.",
      aliases: {
         env: ["MCP_PORT"],
         flags: ["--mcp_port"],
      },
   }),
   setting({
      key: "mcp.host",
      type: string(),
      // The endpoint is unauthenticated, so it stays on loopback unless the
      // operator moves the REST API too (`--host`) or names a host for it.
      default: (resolving) =>
         resolving.isSet("host")
            ? (resolving.get("host") as string)
            : "127.0.0.1",
      description: "Host the MCP server binds to.",
      notes: "Host binding for the MCP server. Takes precedence over `PUBLISHER_HOST`. Set `0.0.0.0` to expose MCP on the network, behind an authenticating gateway.",
      defaultText: "{host} when given, else `127.0.0.1`",
      aliases: { env: ["MCP_HOST"], flags: ["--mcp_host"] },
   }),
   setting({
      key: "mcp.corsOrigins",
      type: list(),
      default: [],
      description:
         "Origins that may read MCP responses cross-origin; * allows any, none denies.",
      notes: "Comma-separated origins allowed to read a cross-origin response from the MCP endpoint; `*` allows any. Unset or empty sends no `Access-Control-Allow-Origin` at all.",
      aliases: { env: ["MCP_CORS_ORIGINS"] },
   }),
   setting({
      key: "mcp.configFileEnabled",
      type: boolean(),
      default: true,
      description:
         "Do not write .mcp.json into the working directory. Without it the file is written, so an agent opened there finds this server, unless the directory already has one, is your home directory or the filesystem root, is inside a git working tree, or the MCP port bound is not the one requested.",
      notes: "Stops the server writing a `.mcp.json` into its working directory on startup. Accepts `1`/`true`/`yes`/`on` to disable and `0`/`false`/`no`/`off`/empty to leave on; anything else, including a value an env file left quotes around, is a startup error rather than a disable. See [The `.mcp.json` the server writes](#the-mcpjson-the-server-writes).",
      aliases: {
         env: ["PUBLISHER_NO_MCP_CONFIG"],
         flags: ["--no-mcp-config"],
         negated: true,
      },
   }),
   setting({
      key: "mcp.includeHiddenFilesAndSources",
      type: boolean(),
      default: false,
      description:
         "Offer includeHiddenFilesAndSources on MCP execute_query, for authors testing curation.",
      aliases: {
         file: ["mcp.includeHiddenFilesAndSources"],
         today: { file: "json-boolean", invalid: "warn" },
      },
   }),
   setting({
      key: "mcp.appOrigin",
      type: string(),
      description:
         "The REST server's address as an MCP host's browser reaches it; the show_report panel loads its script from there.",
      notes: "The origin (`scheme://host[:port]`, no path) the `show_report` panel, an MCP App that hosts such as Claude Desktop draw inside the conversation, loads `/mcp-app/report.js` and `/mcp-app/report.css` from, and the address the tool's \"Open in Publisher\" link points at. It is declared as the panel's only resource domain. The panel's data never comes from here: every request it makes goes back through the MCP host as a `report_api` tool call. Set it when the REST server is reached at another address, such as behind a gateway (`https://publisher.example.com`).",
      defaultText: "`http://localhost:{port}`",
      aliases: { env: ["MCP_APP_ORIGIN"] },
   }),
   setting({
      key: "mcp.trace",
      type: oneOf(["retrieval"]),
      description:
         "retrieval writes a trace summary of each ranked get_context call to the log.",
      notes: "`retrieval` writes a one-line trace summary of each ranked `get_context` call to the server log, and adds nothing to any response. Any other value fails startup.",
      aliases: { env: ["PUBLISHER_MCP_TRACE"] },
   }),
];
