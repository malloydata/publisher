import http from "http";
import { AddressInfo } from "net";
import { URL } from "url";
import path from "path";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import {
   Request,
   Notification,
   Result,
} from "@modelcontextprotocol/sdk/types.js";

// --- Real Server Import ---
// Import the actual configured Express app instance

// --- E2E Test Environment Setup ---

// Store the original SERVER_ROOT to restore it later.
let originalServerRoot: string | undefined;

export interface McpE2ETestEnvironment {
   httpServer: http.Server;
   serverUrl: string;
   mcpClient: Client<Request, Notification, Result>;
}

/**
 * Starts the real application server and connects a real MCP client.
 */
export async function setupE2ETestEnvironment(): Promise<McpE2ETestEnvironment> {
   // --- Store and Set SERVER_ROOT Env Var ---
   // The ProjectStore relies on SERVER_ROOT to find publisher.config.json.
   originalServerRoot = process.env.SERVER_ROOT; // Store original value
   // Resolve the path to 'packages/server' based on the location of this file (__dirname)
   const serverPackageDir = path.resolve(__dirname, "../../"); // Go up two levels from .../packages/server/src/__tests__
   process.env.SERVER_ROOT = serverPackageDir;
   console.log(
      `[E2E Test Setup] Temporarily set SERVER_ROOT=${process.env.SERVER_ROOT}`,
   );

   // --- IMPORTANT: Import server *after* setting env var ---
   // Dynamically import the actual app instance
   const { mcpApp } = await import("../../src/server");

   let serverInstance: http.Server;
   let serverUrl: string;

   // Define an explicit port for the test server to avoid conflicts
   const TEST_MCP_PORT = Number(process.env.MCP_PORT || 4040) + 2; // e.g., 4042

   await new Promise<void>((resolve, reject) => {
      const server = http
         .createServer(mcpApp)
         .listen(TEST_MCP_PORT, "127.0.0.1", () => {
            // Use explicit TEST_MCP_PORT
            try {
               const address = server.address() as AddressInfo;
               if (!address)
                  throw new Error("Server address is null after listen");
               serverInstance = server;
               serverUrl = `http://127.0.0.1:${TEST_MCP_PORT}`;
               console.log(
                  `[E2E Test Setup] Real server (using mcpApp) listening on ${serverUrl} (Port: ${TEST_MCP_PORT})`,
               );
               resolve();
            } catch (err) {
               reject(err);
            }
         });
      server.on("error", (err: NodeJS.ErrnoException) => {
         // Provide more specific error info if available (like EADDRINUSE)
         console.error(
            `[E2E Test Setup] Server failed to start on port ${TEST_MCP_PORT}: ${err.code} - ${err.message}`,
         );
         reject(err);
      });
   });

   // Ensure serverInstance and serverUrl are assigned
   const listeningServerInstance = serverInstance!;
   const listeningServerUrl = serverUrl!;

   // --- Client Setup ---
   const mcpClient = new Client<Request, Notification, Result>({
      name: "mcp-e2e-test-client",
      version: "1.0",
   });

   // Use StreamableHTTPClientTransport, connecting to the base URL + /mcp endpoint
   const mcpEndpointPath = "/mcp"; // Matches the endpoint used in server.ts
   const clientTransport = new StreamableHTTPClientTransport(
      new URL(`${listeningServerUrl}${mcpEndpointPath}`),
   );

   // Connect client (with timeout)
   console.log(
      `[E2E Test Setup] Connecting MCP client to ${listeningServerUrl}${mcpEndpointPath} ...`,
   );
   let connectTimeout: NodeJS.Timeout | null = null;
   try {
      const connectPromise = mcpClient.connect(clientTransport);
      const timeoutPromise = new Promise((_, reject) => {
         connectTimeout = setTimeout(() => {
            reject(
               new Error("[E2E Test Setup] Client connection timeout (5s)"),
            );
         }, 5000);
      });
      await Promise.race([connectPromise, timeoutPromise]);
      console.log("[E2E Test Setup] MCP client connected.");
   } catch (error) {
      console.error("[E2E Test Setup] MCP Client connection failed:", error);
      // Attempt cleanup even if connection failed
      await mcpClient
         ?.close()
         .catch(() =>
            console.error("[E2E Test Setup] Error closing failed client:"),
         );
      if (listeningServerInstance?.listening) {
         listeningServerInstance.closeAllConnections?.(); // Force close connections
         await new Promise<void>((res) =>
            listeningServerInstance.close(() => res()),
         );
      }
      throw error;
   } finally {
      if (connectTimeout) clearTimeout(connectTimeout);
   }

   // --- Return Environment ---
   return {
      httpServer: listeningServerInstance,
      serverUrl: listeningServerUrl,
      mcpClient,
   };
}

/**
 * Cleans up the E2E test environment by closing the client and server.
 */
export async function cleanupE2ETestEnvironment(
   env: McpE2ETestEnvironment | null,
): Promise<void> {
   // --- Restore Original SERVER_ROOT ---
   // Restore the original SERVER_ROOT value after tests are complete.
   if (originalServerRoot === undefined) {
      delete process.env.SERVER_ROOT;
      console.log("[E2E Test Cleanup] Restored SERVER_ROOT (deleted)");
   } else {
      process.env.SERVER_ROOT = originalServerRoot;
      console.log(
         `[E2E Test Cleanup] Restored SERVER_ROOT=${process.env.SERVER_ROOT}`,
      );
   }

   if (!env) {
      // Attempt cleanup even if env is null (e.g., setup failed after config creation)
      return;
   }

   const { mcpClient, httpServer } = env;

   // 1. Close client first
   if (mcpClient) {
      try {
         await mcpClient.close();
      } catch {
         // Ignore client close errors during cleanup potentially
         console.warn(
            "[E2E Test Cleanup] Error closing MCP client (ignoring):",
         );
      }
   }

   // 2. Close HTTP server connections and then the server itself
   if (httpServer) {
      // Force close any remaining connections immediately
      httpServer.closeAllConnections?.();

      if (httpServer.listening) {
         await new Promise<void>((resolve) => {
            httpServer.close((err: NodeJS.ErrnoException | undefined) => {
               if (err && err.code !== "ERR_SERVER_NOT_RUNNING") {
                  console.error(
                     "[E2E Test Cleanup] Error closing HTTP server (after closing connections):",
                     err,
                  );
               } else if (!err) {
                  console.log("[E2E Test Cleanup] HTTP server closed.");
               }
               resolve();
            });
         });
      } else {
         console.log(
            "[E2E Test Cleanup] HTTP server was not listening or already closed.",
         );
      }
   }
}
