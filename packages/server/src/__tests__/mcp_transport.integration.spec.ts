// @ts-ignore - bun:test types
import { describe, it, expect, beforeAll, afterAll } from "bun:test";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { SSEClientTransport } from "@modelcontextprotocol/sdk/client/sse.js";
import { ErrorCode, Request, Notification, Result } from "@modelcontextprotocol/sdk/types.js";
import { URL } from 'url';
import type { Server } from "http";
import type { AddressInfo } from "net";

// --- Import E2E Test Setup ---
import { McpE2ETestEnvironment, setupE2ETestEnvironment, cleanupE2ETestEnvironment } from "./mcp_test_setup";

// --- Test Suite --- 
describe("MCP Transport Tests (E2E Integration)", () => {
  let env: McpE2ETestEnvironment | null = null;
  let mcpClient: Client<Request, Notification, Result>; // Convenience variable

  beforeAll(async () => {
    env = await setupE2ETestEnvironment();
    mcpClient = env.mcpClient;
  });

  afterAll(async () => {
    await cleanupE2ETestEnvironment(env);
    env = null;
  });

  describe("Basic Protocol", () => {
    it("should handle a simple listResources request", async () => {
      if (!env) throw new Error("Test environment not initialized");
      const result = await mcpClient.listResources();
      // Assert based on actual successful response structure
      expect(result).toHaveProperty('resources');
      expect(Array.isArray(result.resources)).toBe(true);
    });

    it("should receive MethodNotFound error for an unknown method", async () => {
       if (!env) throw new Error("Test environment not initialized");
      expect.assertions(3); // Keep assertion count if specific
      try {
        // Use the client from the test environment
        await (mcpClient as any).call('unknown/method', { param: 'value' });
      } catch (error) {
        expect(error).toBeInstanceOf(Error); 
        // Check error code and message for MethodNotFound
        expect((error as any).code).toBe(ErrorCode.MethodNotFound);
        expect((error as Error).message).toMatch(/Method not found/i);
      }
    });

    it("should receive InvalidParams error when calling a known method with invalid params", async () => {
       if (!env) throw new Error("Test environment not initialized");
      expect.assertions(3);
      try {
        // Use the client from the test environment
        await mcpClient.readResource({} as any); // Force invalid params
      } catch (error) {
        expect(error).toBeInstanceOf(Error);
        // Check error code and message for InvalidParams
        expect((error as any).code).toBe(ErrorCode.InvalidParams);
        expect((error as Error).message).toMatch(/Invalid params/i); 
      }
    });
  });

  describe("Connection Management", () => {
    it("should handle multiple concurrent requests", async () => {
      if (!env) throw new Error("Test environment not initialized");
      // Use the client from the test environment
      const requests = [
        mcpClient.listResources(),
        mcpClient.readResource({ uri: "malloy://project/home" }), // Assuming this URI is valid
        mcpClient.listResources()
      ];
      
      const results = await Promise.all(requests);
      expect(results.length).toBe(3);
      // Basic checks on results structure
      expect(results[0]).toHaveProperty('resources');
      expect(results[1]).toHaveProperty('contents');
      expect(results[2]).toHaveProperty('resources');
    });

    it("should handle reconnection after close", async () => {
       if (!env) throw new Error("Test environment not initialized");
      // Close the existing client connection
      await mcpClient.close();

      // Create a new transport to the SAME server URL
      const newTransport = new SSEClientTransport(
        new URL(`${env.serverUrl}/`) // Connect to the root SSE endpoint
      );
      // Reconnect the SAME client instance with the new transport
      await mcpClient.connect(newTransport);

      // Perform a request to verify reconnection
      const result = await mcpClient.listResources(); // Simple request
      expect(result).toBeDefined();
      expect(result).toHaveProperty('resources');
    });
  });
}); 