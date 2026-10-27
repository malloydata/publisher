// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

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
import { pollUntil } from "./spawn_server";

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

// True mutex: the previous Promise-based lock had a TOCTOU race where two beforeAll hooks
// could both pass the `if (initializationLock)` check and run setup concurrently, sharing
// one publisher.db / server singleton and causing flaky tool calls and port collisions.
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
   const { mcpApp } = await import("../../src/main_server");

   // An OS-assigned port: suites set up one after another under the mutex,
   // and a fixed port could be held by anything else on the machine.
   const listeningServerInstance = await new Promise<http.Server>(
      (resolve, reject) => {
         const server = http
            .createServer(mcpApp)
            .listen(0, "127.0.0.1", () => resolve(server));
         server.on("error", (err: NodeJS.ErrnoException) => {
            console.error(
               `[E2E Test Setup] Server failed to start: ${err.code} - ${err.message}`,
            );
            reject(err);
         });
      },
   );
   const { port } = listeningServerInstance.address() as AddressInfo;
   const listeningServerUrl = `http://127.0.0.1:${port}`;
   console.log(
      `[E2E Test Setup] Real server (using mcpApp) listening on ${listeningServerUrl}`,
   );

   // --- Wait for server to be ready (packages downloaded) ---
   // Allow >90s: cold CI can spend most of the budget on clone + package load
   // before markReady().
   const maxWaitTime = 150_000;
   const isReady = await pollUntil(
      async () => {
         try {
            const response = await fetch(
               `${listeningServerUrl}/health/readiness`,
               { signal: AbortSignal.timeout(5_000) },
            );
            return (
               response.ok &&
               ((await response.json()) as { status?: string }).status === "UP"
            );
         } catch {
            return false;
         }
      },
      maxWaitTime,
      250,
   );

   if (!isReady) {
      listeningServerInstance.closeAllConnections?.();
      await new Promise<void>((res) =>
         listeningServerInstance.close(() => res()),
      );
      throw new Error(
         `Server did not become ready within ${maxWaitTime / 1000} seconds. Package downloads may have failed.`,
      );
   }
   console.log("[E2E Test Setup] Server is ready.");

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
