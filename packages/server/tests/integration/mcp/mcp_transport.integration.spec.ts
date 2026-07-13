import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import {
   ErrorCode,
   Notification,
   Request,
   Result,
} from "@modelcontextprotocol/sdk/types.js";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { URL } from "url";

// --- Import E2E Test Setup ---
import {
   cleanupE2ETestEnvironment,
   McpE2ETestEnvironment,
   setupE2ETestEnvironment,
} from "../../harness/mcp_test_setup";

// --- Test Suite ---
// Note: These tests assume interaction via a standard MCP client.
// Tests for raw HTTP edge cases (non-JSON, non-RPC) are omitted based on this assumption.
describe.serial("MCP Transport Tests (E2E Integration)", () => {
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
      it(
         "should handle a simple listTools request",
         async () => {
            if (!env) throw new Error("Test environment not initialized");
            const result = await mcpClient.listTools();
            // Assert based on actual successful response structure
            expect(result).toHaveProperty("tools");
            expect(Array.isArray(result.tools)).toBe(true);
            expect(result.tools.some((t) => t.name === "malloy_executeQuery")).toBe(
               true,
            );
         },
         { timeout: 30000 },
      );

      it("should receive MethodNotFound error for unknown methods", async () => {
         if (!env) throw new Error("Test environment not initialized");
         expect.assertions(2); // Expecting error instance and specific code
         try {
            // Attempt to call a method that doesn't exist
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            await (mcpClient as any).request({
               method: "nonexistent/method",
               params: {},
            });
            // Should not reach here
            throw new Error("Request should have failed");
         } catch (error) {
            expect(error).toBeInstanceOf(Error);
            // Check for the specific MethodNotFound error code
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            expect((error as { code?: number }).code).toBe(
               ErrorCode.MethodNotFound,
            );
         }
      });
   });

   describe("Connection Management & Bridge Logic", () => {
      it("should return 405 Method Not Allowed for GET requests", async () => {
         if (!env) throw new Error("Test environment not initialized");
         const getUrl = new URL(`${env.serverUrl}/mcp`); // No session ID needed
         const response = await fetch(getUrl.toString(), {
            method: "GET",
            headers: { Accept: "text/event-stream" },
         });
         expect(response.status).toBe(405); // Expect Method Not Allowed
         const errorBody = await response.json();
         expect(errorBody.error.message).toContain("Method Not Allowed");
      });

      it("should return 405 Method Not Allowed for DELETE requests", async () => {
         if (!env) throw new Error("Test environment not initialized");
         const deleteUrl = new URL(`${env.serverUrl}/mcp`); // No session ID needed
         const response = await fetch(deleteUrl.toString(), {
            method: "DELETE",
         });
         expect(response.status).toBe(405); // Expect Method Not Allowed
         const errorBody = await response.json();
         expect(errorBody.error.message).toContain("Method Not Allowed");
      });
   });
});
