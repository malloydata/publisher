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
 * The MCP endpoint is unauthenticated and exposes every tool, so it binds
 * loopback by default while the REST port keeps its own default. MCP_HOST wins,
 * then an explicit PUBLISHER_HOST (so `--host` moves both listeners together),
 * then loopback. Widening it belongs behind an authenticating gateway.
 */
export function resolveMcpHost(env: NodeJS.ProcessEnv): string {
   return env.MCP_HOST || env.PUBLISHER_HOST || "127.0.0.1";
}

/**
 * The MCP listener's app: health probes and one stateless streamable-HTTP
 * endpoint. Each POST gets its own transport and server, which is how the SDK
 * runs without sessions.
 */
export function createMcpApp(
   environmentStore: EnvironmentStore,
): express.Express {
   const mcpApp = express();
   // Register health endpoints on mcpApp (for E2E tests)
   registerHealthEndpoints(mcpApp);

   mcpApp.use(MCP_ENDPOINT, express.json());
   // Cross-origin access is OPT-IN via MCP_CORS_ORIGINS (comma-separated origins,
   // or `*` to allow any). Default is no allowlist, which reflects no
   // `Access-Control-Allow-Origin` back, so a browser page on another origin cannot
   // read a response from this unauthenticated endpoint. A non-browser client (an
   // MCP agent over HTTP) sends no Origin and is unaffected either way: CORS
   // governs what a browser hands to script, not who may connect.
   // codeql[js/cors-permissive-configuration]: the permissive value this rule
   // looks for is reachable only when an operator sets MCP_CORS_ORIGINS=* on
   // purpose, which is the documented escape hatch; every other input, including
   // the default, resolves to an allowlist or to false. The line this replaced was
   // a bare `cors()` -- permissive unconditionally, and unflagged.
   mcpApp.use(MCP_ENDPOINT, cors({ origin: getMcpCorsOrigins() }));

   mcpApp.all(MCP_ENDPOINT, async (req, res) => {
      logger.info(`[MCP Debug] Handling ${req.method} (Stateless)`);

      try {
         if (req.method === "POST") {
            const transport = new StreamableHTTPServerTransport({
               sessionIdGenerator: undefined,
            });

            transport.onclose = () => {
               logger.info(
                  `[MCP Transport Info] Stateless transport closed for a request.`,
               );
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
               logger.info(
                  "[MCP Transport Info] Response closed, cleaning up stateless transport.",
               );
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
