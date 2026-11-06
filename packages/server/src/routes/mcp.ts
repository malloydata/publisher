// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import cors from "cors";
import express from "express";
import { getMcpCorsOrigins } from "../config";
import { registerHealthEndpoints } from "../health";
import { logger } from "../logger";
import { initializeMcpServer } from "../mcp/server";
import type { EnvironmentStore } from "../service/environment_store";

export const MCP_ENDPOINT = "/mcp";

/**
 * The MCP listener's app: health probes and one stateless streamable-HTTP
 * endpoint. Each POST gets its own transport and server, which is how the SDK
 * runs without sessions.
 */
export function createMcpApp(
   environmentStore: EnvironmentStore,
): express.Express {
   const mcpApp = express();
   registerHealthEndpoints(mcpApp);

   mcpApp.use(MCP_ENDPOINT, express.json());
   // Cross-origin access is opt-in via MCP_CORS_ORIGINS (origins, or `*`). By
   // default no Allow-Origin is reflected, so a page on another origin cannot
   // read this unauthenticated endpoint's responses. Non-browser MCP clients
   // send no Origin and are unaffected. The permissive value CodeQL looks for
   // is reachable only when an operator sets MCP_CORS_ORIGINS=* on purpose,
   // the documented escape hatch.
   // codeql[js/cors-permissive-configuration]
   mcpApp.use(MCP_ENDPOINT, cors({ origin: getMcpCorsOrigins() }));

   mcpApp.all(MCP_ENDPOINT, async (req, res) => {
      logger.debug(`[MCP] Handling ${req.method} (stateless)`);

      try {
         if (req.method === "POST") {
            const transport = new StreamableHTTPServerTransport({
               sessionIdGenerator: undefined,
            });

            transport.onclose = () => {
               logger.debug("[MCP] Stateless transport closed");
            };
            transport.onerror = (err: Error) => {
               logger.error(
                  `[MCP Transport Error] Stateless transport error:`,
                  {
                     error: err,
                  },
               );
            };

            const requestMcpServer = initializeMcpServer(environmentStore);
            await requestMcpServer.connect(transport);

            res.on("close", () => {
               logger.debug("[MCP] Response closed; closing transport");
               transport.close().catch((err) => {
                  logger.error(
                     "[MCP Transport Error] Error closing stateless transport on response close:",
                     { error: err },
                  );
               });
            });

            await transport.handleRequest(req, res, req.body);
         } else if (req.method === "GET" || req.method === "DELETE") {
            logger.warn(
               `[MCP Transport Warn] Method Not Allowed in Stateless Mode: ${req.method}`,
            );
            res.setHeader("Allow", "POST");
            res.status(405).json({
               jsonrpc: "2.0",
               error: {
                  code: -32601,
                  message: "Method Not Allowed in Stateless Mode",
               },
               id: null,
            });
            return;
         } else {
            logger.warn(
               `[MCP Transport Warn] Method Not Allowed: ${req.method}`,
            );
            res.setHeader("Allow", "POST");
            res.status(405).json({
               jsonrpc: "2.0",
               error: { code: -32601, message: "Method Not Allowed" },
               id: null,
            });
            return;
         }
      } catch (error) {
         logger.error(
            `[MCP Transport Error] Unhandled error in ${req.method} handler (Stateless):`,
            { error },
         );
         if (!res.headersSent) {
            res.status(500).json({
               jsonrpc: "2.0",
               error: { code: -32603, message: "Internal server error" },
               id:
                  typeof req.body === "object" &&
                  req.body !== null &&
                  "id" in req.body
                     ? req.body.id
                     : null,
            });
         }
      }
   });
   return mcpApp;
}
