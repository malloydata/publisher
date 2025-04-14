// @ts-ignore - bun:test types (test runner handles this)
import { afterAll, beforeAll, describe, expect, it } from "bun:test"; 
import http from 'http';
import { AddressInfo } from 'net';
import { URL } from 'url';

// MCP SDK Imports
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { SSEClientTransport } from '@modelcontextprotocol/sdk/client/sse.js';
import { Request, Notification, Result } from "@modelcontextprotocol/sdk/types.js";

// --- Real Server Import ---
// Import the actual configured Express app instance
import { mcpApp } from "../server"; // Assuming the express app is exported as mcpApp

// --- E2E Test Environment Setup ---

export interface McpE2ETestEnvironment {
    httpServer: http.Server;
    serverUrl: string;
    mcpClient: Client<Request, Notification, Result>; // Use appropriate generics if needed
}

/**
 * Starts the real application server and connects a real MCP client.
 */
export async function setupE2ETestEnvironment(): Promise<McpE2ETestEnvironment> {
    let serverInstance: http.Server;
    let serverUrl: string;

    // Define an explicit port for the test server to avoid conflicts
    const TEST_MCP_PORT = (Number(process.env.MCP_PORT || 4001)) + 2; // e.g., 4003

    // Start the REAL HTTP server using the REAL Express app on the specific test port
    await new Promise<void>((resolve, reject) => {
        // Use the imported real app instance
        const server = http.createServer(mcpApp).listen(TEST_MCP_PORT, '127.0.0.1', () => { // Use explicit TEST_MCP_PORT
            try {
                const address = server.address() as AddressInfo;
                if (!address) throw new Error("Server address is null after listen");
                serverInstance = server; // Assign the instance
                serverUrl = `http://127.0.0.1:${address.port}`; // Still use address.port in case it differs somehow, though should match TEST_MCP_PORT
                console.log(`[E2E Test Setup] Real server (using mcpApp) listening on ${serverUrl} (Port: ${TEST_MCP_PORT})`);
                resolve();
            } catch (err) {
                reject(err);
            }
        });
        server.on('error', (err: NodeJS.ErrnoException) => {
            // Provide more specific error info if available (like EADDRINUSE)
             console.error(`[E2E Test Setup] Server failed to start on port ${TEST_MCP_PORT}: ${err.code} - ${err.message}`);
             reject(err)
            }); // Handle server startup errors
    });

    // Ensure serverInstance and serverUrl are assigned
    const listeningServerInstance = serverInstance!;
    const listeningServerUrl = serverUrl!;

    // --- Client Setup ---
    const mcpClient = new Client<Request, Notification, Result>({ name: "mcp-e2e-test-client", version: "1.0" });
    // The SSE endpoint path is defined within the mcpApp routes, use that path directly.
    const ssePath = '/'; // Corrected path based on server.ts mcpApp.get('/') route
    const clientTransport = new SSEClientTransport(new URL(`${listeningServerUrl}${ssePath}`)); 

    // Connect client (with timeout)
    console.log(`[E2E Test Setup] Connecting MCP client to ${listeningServerUrl}${ssePath} ...`);
    let connectTimeout: NodeJS.Timeout | null = null;
    try {
        const connectPromise = mcpClient.connect(clientTransport);
        const timeoutPromise = new Promise((_, reject) => {
            connectTimeout = setTimeout(() => {
                reject(new Error("[E2E Test Setup] Client connection timeout (5s)"));
            }, 5000);
        });
        await Promise.race([connectPromise, timeoutPromise]);
        console.log("[E2E Test Setup] MCP client connected.");
    } catch (error) {
        console.error("[E2E Test Setup] MCP Client connection failed:", error);
        // Attempt cleanup even if connection failed
        await mcpClient?.close().catch(e => console.error("[E2E Test Setup] Error closing failed client:", e));
        if (listeningServerInstance?.listening) {
            listeningServerInstance.closeAllConnections?.(); // Force close connections
            await new Promise<void>(res => listeningServerInstance.close(() => res()));
        }
        throw error; // Re-throw the connection error
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
export async function cleanupE2ETestEnvironment(env: McpE2ETestEnvironment | null): Promise<void> {
    if (!env) return;

    const { mcpClient, httpServer } = env;

    // 1. Close client first
    if (mcpClient) {
        // console.log("[E2E Test Cleanup] Closing MCP client...");
        try {
            await mcpClient.close();
             // console.log("[E2E Test Cleanup] MCP client closed.");
        } catch (e) {
            // Ignore client close errors during cleanup potentially
            console.warn("[E2E Test Cleanup] Error closing MCP client (ignoring):"); //, e);
        }
    }

    // 2. Close HTTP server connections and then the server itself
    if (httpServer) {
        // console.log("[E2E Test Cleanup] Closing HTTP server connections and server...");
        // Force close any remaining connections immediately
        httpServer.closeAllConnections?.(); 

        // Now attempt to close the server - should be faster now
        if (httpServer.listening) {
            await new Promise<void>((resolve) => {
                httpServer.close((err) => {
                    if (err) {
                        console.error("[E2E Test Cleanup] Error closing HTTP server (after closing connections):", err);
                    } else {
                        // console.log("[E2E Test Cleanup] HTTP server closed.");
                    }
                    resolve(); // Resolve regardless of error after attempting close
                });
            });
        } else {
             // console.log("[E2E Test Cleanup] HTTP server was not listening or already closed.");
        }
    }
} 