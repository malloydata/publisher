import { describe, it, expect, beforeAll, afterAll, beforeEach, afterEach } from "bun:test";
import { Server } from "http";
import { app, httpServer } from "../server";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { SSEClientTransport } from "@modelcontextprotocol/sdk/client/sse.js";
import { ErrorCode, Request, Notification, Result } from "@modelcontextprotocol/sdk/types.js";

let serverInstance: Server;
let serverUrl: string;
let client: Client<Request, Notification, Result>;
let transport: SSEClientTransport;

beforeAll((done) => {
  serverInstance = httpServer.listen(0, () => {
    const address = serverInstance.address();
    if (typeof address === 'string' || address === null) {
      throw new Error("Failed to get server address");
    }
    serverUrl = `http://localhost:${address.port}`;
    console.log(`Test server running on ${serverUrl}`);
    done();
  });
});

afterAll((done) => {
  serverInstance.close(done);
});

// Setup client and transport before each test
beforeEach(async () => {
  client = new Client<Request, Notification, Result>({ name: "test-client", version: "0.0.1" });
  transport = new SSEClientTransport(new URL(`${serverUrl}/api/v0/mcp/sse`));
  // Connect the client using the transport
  await client.connect(transport);
});

// Close the client after each test (client.close handles transport)
afterEach(async () => {
  if (client) {
    // Use client.close() based on example script
    await client.close();
  }
});

describe("MCP Transport Client SDK Integration Tests", () => {

  // Connection is implicitly tested by beforeEach setup succeeding.

  it("should receive MethodNotFound error for an unknown method", async () => {
    expect.assertions(3); // Ensure the catch block is reached
    try {
      // Use client.call() with type assertion due to persistent linter issue
      await (client as any).call('unknown/method', { param: 'value' });
    } catch (error) {
      // Expect a generic Error
      expect(error).toBeInstanceOf(Error);
      // Check for attached code property (using 'as any' as type isn't certain)
      expect((error as any).code).toBe(ErrorCode.MethodNotFound);
      // Check the error message
      expect((error as Error).message).toMatch(/Method not found/i);
    }
  });

  it("should receive InvalidParams error when calling a known method helper with invalid params", async () => {
    expect.assertions(3); // Ensure the catch block is reached
    try {
      // Use client.readResource() helper which requires { uri: string }
      await client.readResource({} as any); // Pass invalid params (empty object)
    } catch (error) {
      // Expect a generic Error
      expect(error).toBeInstanceOf(Error);
      // Check for attached code property
      expect((error as any).code).toBe(ErrorCode.InvalidParams);
      // Check the error message - this might come from Zod in the client or server
      expect((error as Error).message).toMatch(/Invalid parameter|Invalid params|required/i); // More flexible check
    }
  });

  // Note: ParseError testing remains difficult with correct client usage.
}); 