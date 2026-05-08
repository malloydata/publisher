import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import {
   Notification,
   Request,
   Result,
} from "@modelcontextprotocol/sdk/types.js";
import { Mutex } from "async-mutex";
import http from "http";
import { AddressInfo } from "net";
import path from "path";
import { fileURLToPath } from "url";
import { URL } from "url";

// --- Real Server Import ---
// Import the actual configured Express app instance

// --- E2E Test Environment Setup ---

export interface McpE2ETestEnvironment {
   httpServer: http.Server;
   serverUrl: string;
   mcpClient: Client<Request, Notification, Result>;
   originalServerRoot: string | undefined;
   originalInitializeStorage: string | undefined;
}

// Counter for unique port assignment per test suite (incremented only under e2eSetupMutex)
let portCounter = 0;

// True mutex: the previous Promise-based lock had a TOCTOU race where two beforeAll hooks
// could both pass the `if (initializationLock)` check and run setup concurrently, sharing
// one publisher.db / server singleton and causing flaky listResources and port collisions.
const e2eSetupMutex = new Mutex();

/**
 * Starts the real application server and connects a real MCP client.
 */
export async function setupE2ETestEnvironment(): Promise<McpE2ETestEnvironment> {
   return e2eSetupMutex.runExclusive(async () => {
      console.log(
         "[E2E Test Setup] Acquired setup mutex; starting environment...",
      );
      return setupE2ETestEnvironmentInternal();
   });
}

async function setupE2ETestEnvironmentInternal(): Promise<McpE2ETestEnvironment> {
   // --- Store and Set SERVER_ROOT Env Var ---
   // The EnvironmentStore relies on SERVER_ROOT to find publisher.config.json.
   const originalServerRoot = process.env.SERVER_ROOT; // Store original value
   // Resolve the path to 'packages/server' based on the location of this file
   // Use import.meta.url for cross-platform compatibility (works on Windows)
   const __filename = fileURLToPath(import.meta.url);
   const __dirname = path.dirname(__filename);
   const serverPackageDir = path.resolve(__dirname, "../../"); // Go up two levels from .../packages/server/tests/harness
   process.env.SERVER_ROOT = serverPackageDir;
   console.log(
      `[E2E Test Setup] Temporarily set SERVER_ROOT=${process.env.SERVER_ROOT}`,
   );

   // --- Set INITIALIZE_STORAGE to ensure packages are downloaded ---
   // This ensures packages from GitHub are cloned/downloaded during initialization
   const originalInitializeStorage = process.env.INITIALIZE_STORAGE; // Store original value
   process.env.INITIALIZE_STORAGE = "true";
   console.log(
      `[E2E Test Setup] Temporarily set INITIALIZE_STORAGE=${process.env.INITIALIZE_STORAGE}`,
   );

   // --- IMPORTANT: Import server *after* setting env var ---
   // Dynamically import the actual app instance
   const { mcpApp } = await import("../../src/server");

   let serverInstance: http.Server;
   let serverUrl: string;

   // Use unique port per test suite to avoid conflicts when running in parallel
   // Increment port counter and use it to create unique ports: 4042, 4043, 4044, etc.
   const portOffset = portCounter++;
   const TEST_MCP_PORT = Number(process.env.MCP_PORT || 4040) + 2 + portOffset;

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

   // --- Wait a moment for server to start accepting connections ---
   console.log("[E2E Test Setup] Waiting for server to accept connections...");
   await new Promise((resolve) => setTimeout(resolve, 1000));

   // --- Wait for server to be ready (packages downloaded) ---
   // Poll the readiness endpoint to ensure initialization completes before tests run
   // Allow >90s: cold CI can spend most of the budget on clone + package load before markReady().
   console.log("[E2E Test Setup] Waiting for server to be ready...");
   const maxWaitTime = 150000; // 150 seconds (INITIALIZE_STORAGE + large samples can be slow)
   const pollInterval = 1000; // Check every second
   const startTime = Date.now();
   let isReady = false;

   while (!isReady && Date.now() - startTime < maxWaitTime) {
      try {
         // Use Promise.race to add timeout to fetch
         const fetchPromise = fetch(`${listeningServerUrl}/health/readiness`);
         const timeoutPromise = new Promise<never>((_, reject) =>
            setTimeout(() => reject(new Error("Fetch timeout")), 5000),
         );
         const response = await Promise.race([fetchPromise, timeoutPromise]);

         if (response.ok) {
            const data = await response.json();
            if (data.status === "UP") {
               isReady = true;
               console.log("[E2E Test Setup] Server is ready.");
               break;
            } else {
               console.log(
                  `[E2E Test Setup] Server not ready yet (status: ${data.status}), waiting... (${Math.round((Date.now() - startTime) / 1000)}s)`,
               );
            }
         } else {
            console.log(
               `[E2E Test Setup] Readiness check returned ${response.status}, waiting... (${Math.round((Date.now() - startTime) / 1000)}s)`,
            );
         }
      } catch (error) {
         // Server might not be ready yet, continue polling
         const errorMsg =
            error instanceof Error ? error.message : String(error);
         // Only log every 5 seconds to reduce noise
         const elapsed = Math.round((Date.now() - startTime) / 1000);
         if (elapsed % 5 === 0) {
            console.log(
               `[E2E Test Setup] Readiness check failed (${errorMsg}), waiting... (${elapsed}s)`,
            );
         }
      }
      await new Promise((resolve) => setTimeout(resolve, pollInterval));
   }

   if (!isReady) {
      // Cleanup before throwing
      if (listeningServerInstance?.listening) {
         listeningServerInstance.closeAllConnections?.();
         await new Promise<void>((res) =>
            listeningServerInstance.close(() => res()),
         );
      }
      throw new Error(
         `Server did not become ready within ${maxWaitTime / 1000} seconds. Package downloads may have failed.`,
      );
   }

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
      originalServerRoot,
      originalInitializeStorage,
   };
}

/**
 * Cleans up the E2E test environment by closing the client and server.
 */
export async function cleanupE2ETestEnvironment(
   env: McpE2ETestEnvironment | null,
): Promise<void> {
   if (!env) {
      // Attempt cleanup even if env is null (e.g., setup failed after config creation)
      return;
   }

   // --- Restore Original SERVER_ROOT and INITIALIZE_STORAGE ---
   // Restore the original SERVER_ROOT value after tests are complete.
   const { originalServerRoot, originalInitializeStorage } = env;
   if (originalServerRoot === undefined) {
      delete process.env.SERVER_ROOT;
      console.log("[E2E Test Cleanup] Restored SERVER_ROOT (deleted)");
   } else {
      process.env.SERVER_ROOT = originalServerRoot;
      console.log(
         `[E2E Test Cleanup] Restored SERVER_ROOT=${process.env.SERVER_ROOT}`,
      );
   }

   // Restore the original INITIALIZE_STORAGE value after tests are complete.
   if (originalInitializeStorage === undefined) {
      delete process.env.INITIALIZE_STORAGE;
      console.log("[E2E Test Cleanup] Restored INITIALIZE_STORAGE (deleted)");
   } else {
      process.env.INITIALIZE_STORAGE = originalInitializeStorage;
      console.log(
         `[E2E Test Cleanup] Restored INITIALIZE_STORAGE=${process.env.INITIALIZE_STORAGE}`,
      );
   }

   const { mcpClient, httpServer } = env;

   // 1. Close client first (with timeout)
   if (mcpClient) {
      try {
         const closePromise = mcpClient.close();
         const timeoutPromise = new Promise<void>((_, reject) => {
            setTimeout(() => reject(new Error("Client close timeout")), 5000);
         });
         await Promise.race([closePromise, timeoutPromise]);
      } catch (error) {
         // Ignore client close errors during cleanup potentially
         console.warn(
            "[E2E Test Cleanup] Error closing MCP client (ignoring):",
            error instanceof Error ? error.message : String(error),
         );
      }
   }

   // 2. Close HTTP server connections and then the server itself (with timeout)
   if (httpServer) {
      // Force close any remaining connections immediately
      httpServer.closeAllConnections?.();

      if (httpServer.listening) {
         try {
            const closePromise = new Promise<void>((resolve, reject) => {
               const timeout = setTimeout(() => {
                  reject(new Error("Server close timeout"));
               }, 5000);
               httpServer.close((err: NodeJS.ErrnoException | undefined) => {
                  clearTimeout(timeout);
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
            await closePromise;
         } catch (error) {
            console.warn(
               "[E2E Test Cleanup] Server close timeout or error (forcing close):",
               error instanceof Error ? error.message : String(error),
            );
            // Force destroy if close times out
            if (httpServer.listening) {
               httpServer.closeAllConnections?.();
            }
         }
      } else {
         console.log(
            "[E2E Test Cleanup] HTTP server was not listening or already closed.",
         );
      }
   }
}
