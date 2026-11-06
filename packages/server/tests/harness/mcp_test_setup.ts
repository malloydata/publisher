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
import { installSettings } from "../../src/test_helpers/settings_env";
import { pollUntil } from "./spawn_server";

// --- Real Server Import ---
// Import the actual configured Express app instance

// --- E2E Test Environment Setup ---

export interface McpE2ETestEnvironment {
   httpServer: http.Server;
   serverUrl: string;
   mcpClient: Client<Request, Notification, Result>;
   /** Puts back the settings that were current before setup. */
   restoreSettings: () => void;
}

/** This process's variables, as a server process started here would read them. */
function processEnv(): Record<string, string> {
   return Object.fromEntries(
      Object.entries(process.env).filter(
         (entry): entry is [string, string] => entry[1] !== undefined,
      ),
   );
}

// True mutex: the previous Promise-based lock had a TOCTOU race where two beforeAll hooks
// could both pass the `if (initializationLock)` check and run setup concurrently, sharing
// one publisher.db / server singleton and causing flaky tool calls and port collisions.
const e2eSetupMutex = new Mutex();

/**
 * Starts the real application server and connects a real MCP client.
 */
/**
 * `env` holds the variables a suite starts its server with, over this
 * process's own; a key set to undefined is removed.
 */
export async function setupE2ETestEnvironment(
   options: { env?: Record<string, string | undefined> } = {},
): Promise<McpE2ETestEnvironment> {
   return e2eSetupMutex.runExclusive(async () => {
      console.log(
         "[E2E Test Setup] Acquired setup mutex; starting environment...",
      );
      return setupE2ETestEnvironmentInternal(options.env ?? {});
   });
}

async function setupE2ETestEnvironmentInternal(
   suiteEnv: Record<string, string | undefined>,
): Promise<McpE2ETestEnvironment> {
   // Resolve the path to 'packages/server' based on the location of this file
   // Use import.meta.url for cross-platform compatibility (works on Windows)
   const __filename = fileURLToPath(import.meta.url);
   const __dirname = path.dirname(__filename);
   const serverPackageDir = path.resolve(__dirname, "../../"); // Go up two levels from .../packages/server/tests/harness

   // --- Install the settings the server starts with ---
   // The server runs in this process, so it starts with this process's
   // variables, as a server process started here would, and then the suite's.
   // On top of them, the server root is packages/server, where
   // publisher.config.json is, and INITIALIZE_STORAGE makes initialization
   // clone/download the packages from GitHub. Kept until cleanup, since the
   // server reads settings as it runs.
   const env: Record<string, string> = processEnv();
   for (const [key, value] of Object.entries(suiteEnv)) {
      if (value === undefined) delete env[key];
      else env[key] = value;
   }
   const { restore: restoreSettings } = installSettings({
      env: {
         ...env,
         SERVER_ROOT: serverPackageDir,
         INITIALIZE_STORAGE: "true",
      },
      cwd: process.cwd(),
   });
   console.log(
      `[E2E Test Setup] Installed settings with SERVER_ROOT=${serverPackageDir} INITIALIZE_STORAGE=true`,
   );
   try {
      return await startServerAndClient(restoreSettings);
   } catch (error) {
      restoreSettings();
      throw error;
   }
}

async function startServerAndClient(
   restoreSettings: () => void,
): Promise<McpE2ETestEnvironment> {
   // --- IMPORTANT: Import server *after* installing the settings ---
   // It reads them at import. Dynamically import the actual app instance
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
      restoreSettings,
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

   try {
      await closeClientAndServer(env);
   } finally {
      // After the server is closed, since it reads settings until then.
      env.restoreSettings();
      console.log("[E2E Test Cleanup] Restored the settings.");
   }
}

async function closeClientAndServer(env: McpE2ETestEnvironment): Promise<void> {
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
