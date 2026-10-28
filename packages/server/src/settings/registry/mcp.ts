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
      notes: "MCP HTTP port. Serves the ten MCP tools (`list_packages`, `get_context`, `execute_query`, `compile_model`, `reload_package`, `get_status`, `search_malloy_docs`, `search_database_schema`, `get_skill`, `get_agent`) and the agent skills as MCP prompts. `list_packages` is listed first because it is where an agent starts: `get_context` requires an environment and package in its `scopes`, and those names come from there.",
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
      defaultText: "`host` when given, else `127.0.0.1`",
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
         "Write .mcp.json into the working directory, so an agent opened there finds this server.",
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
      key: "mcp.trace",
      type: oneOf(["retrieval"]),
      description:
         "retrieval writes a trace summary of each ranked get_context call to the log.",
      notes: "`retrieval` writes a one-line trace summary of each ranked `get_context` call to the server log, and adds nothing to any response. Any other value fails startup.",
      aliases: { env: ["PUBLISHER_MCP_TRACE"] },
   }),
];
