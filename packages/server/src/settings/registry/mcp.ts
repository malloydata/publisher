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
      aliases: {
         env: ["MCP_PORT"],
         flags: ["--mcp_port"],
         today: { reads: "unchecked-Number" },
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
      description:
         "Host MCP binds to; default host when that is given, else 127.0.0.1.",
      aliases: { env: ["MCP_HOST"], flags: ["--mcp_host"] },
   }),
   setting({
      key: "mcp.corsOrigins",
      type: list(),
      default: [],
      description:
         "Origins that may read MCP responses cross-origin; * allows any, none denies.",
      aliases: { env: ["MCP_CORS_ORIGINS"] },
   }),
   setting({
      key: "mcp.configFileEnabled",
      type: boolean(),
      default: true,
      description:
         "Write .mcp.json into the working directory, so an agent opened there finds this server.",
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
      aliases: { env: ["PUBLISHER_MCP_TRACE"], today: { invalid: "default" } },
   }),
];
