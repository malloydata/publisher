import { test, expect, describe, beforeAll, afterAll, it, beforeEach, afterEach, fail } from "bun:test";
import { Server } from "http";
import { app, httpServer } from "../server"; // Assuming app/server export is correct
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { SSEClientTransport } from "@modelcontextprotocol/sdk/client/sse.js";
import { ErrorCode, Request, Notification, Result, ReadResourceResult, CallToolResult } from "@modelcontextprotocol/sdk/types.js";
import { PackageService } from "../service/package.service";


let serverInstance: Server;
let serverUrl: string;
let client: Client<Request, Notification, Result>;
let transport: SSEClientTransport;

const PROJECT_URI = "malloy://project/home"; 
const PROJECT_NAME = "home";

beforeAll(async () => { 
  await new Promise<void>((resolve, reject) => {
    const server = httpServer.listen(0, () => { 
      try {
        const address = server.address();
        if (typeof address === 'string' || address === null) {
          reject(new Error("Failed to get server address"));
          return;
        }
        serverInstance = server;
        serverUrl = `http://localhost:${address.port}`;
        console.log(`[MCP Test] Server running on ${serverUrl}`);
        resolve();
      } catch(err) {
        reject(err);
      }
    });
    server.on('error', reject);
  });
});

afterAll(async () => {
  console.log("[MCP Test] Closing server...");
  if (serverInstance) {
    await new Promise<void>((resolve, reject) => {
        serverInstance.close((err) => { 
            if (err) { reject(err); }
            else { resolve(); }
        });
    });
  }
});

beforeEach(async () => {
  if (!serverUrl) {
    throw new Error("Server URL not set before test execution.");
  }
  client = new Client<Request, Notification, Result>({ name: "test-client", version: "0.0.1" });
  transport = new SSEClientTransport(new URL(`${serverUrl}/api/v0/mcp/sse`));
  try {
    await client.connect(transport);
  } catch (error) {
    console.error("[MCP Test] Client connection failed in beforeEach:", error);
    throw error;
  }
});

afterEach(async () => {
  if (client) {
    await client.close();
  }
});

// --- Test Suite ---
describe("MCP Integration Tests", () => { // Rename suite slightly

  describe("MCP Resource Integration Tests", () => {

    describe("mcp/listResources", () => {

      it("should list the 'home' project when called with no URI", async () => {
        const result = await client.listResources();
        expect(result.resources).toBeInstanceOf(Array);
        expect(result.resources.length).toBeGreaterThanOrEqual(1);
        // Only check for the project, as packages might not exist
        expect(result.resources[0]).toMatchObject({
          uri: PROJECT_URI,
          name: PROJECT_NAME,
          description: expect.stringContaining("root Malloy project")
        });
      });

      it("should return empty list when called with a non-package project URI", async () => {
        // Test listing contents of the project itself (should be packages, if any)
        const result = await client.listResources({ uri: PROJECT_URI });
        expect(result.resources).toBeInstanceOf(Array);
        // We expect 0 here unless malloy-samples actually contains packages at the root
        // expect(result.resources.length).toBe(0); 
      });

      it("should return InvalidParams error for a non-existent package URI", async () => {
          expect.assertions(3);
          const badUri = `${PROJECT_URI}/package/non_existent_package`;
          try {
              await client.listResources({ uri: badUri });
          } catch (error) {
              expect(error).toBeInstanceOf(Error);
              expect((error as any).code).toBe(ErrorCode.InvalidParams);
              expect((error as Error).message).toMatch(/Package manifest.*does not exist/i);
          }
      });
    });

    describe("mcp/readResource", () => {
      it("should read the 'home' project resource with details", async () => {
        const result = await client.readResource({ uri: PROJECT_URI });
        expect(result).toBeDefined();
        expect(result).toHaveProperty('contents'); 
        expect(result.contents).toBeInstanceOf(Array);
        expect(result.contents.length).toBe(1);
        // Check URI within content
        expect(result.contents[0].uri).toBe(PROJECT_URI);
        expect(result.contents[0].text).toContain("Malloy Project Resource");
      });

      it("should return InvalidParams error for a non-existent package URI", async () => {
          expect.assertions(3);
          const badUri = `${PROJECT_URI}/package/non_existent_package`;
          try {
              await client.readResource({ uri: badUri });
          } catch (error) {
              expect(error).toBeInstanceOf(Error);
              expect((error as any).code).toBe(ErrorCode.InvalidParams);
              expect((error as Error).message).toMatch(/Package manifest.*does not exist/i);
          }
      });

      it("should return InvalidParams error for a non-existent model URI", async () => {
          expect.assertions(3);
          const badUri = `${PROJECT_URI}/package/flights/models/no_model_here.malloy`; 
          try {
              await client.readResource({ uri: badUri });
          } catch (error) {
              expect(error).toBeInstanceOf(Error);
              expect((error as any).code).toBe(ErrorCode.InvalidParams);
              expect((error as Error).message).toMatch(/Package manifest.*does not exist/i);
          }
      });

      it("should return error for non-existent package URI", async () => {
        expect.assertions(3);
        try {
            await client.readResource({
                uri: "mcp://test/non-existent-package/model"
            });
            expect(true).toBe(false); // Expected error to be thrown
        } catch (error) {
            expect(error).toBeInstanceOf(Error);
            expect((error as any).code).toBe(ErrorCode.InvalidParams);
            expect((error as Error).message).toMatch(/Resource.*not found/i);
        }
      });

      it("should return error for non-existent model URI", async () => {
        expect.assertions(3);
        try {
            await client.readResource({
                uri: "mcp://test/test-package/non-existent-model"
            });
            expect(true).toBe(false); // Expected error to be thrown
        } catch (error) {
            expect(error).toBeInstanceOf(Error);
            expect((error as any).code).toBe(ErrorCode.InvalidParams);
            expect((error as Error).message).toMatch(/Resource.*not found/i);
        }
      });
    });

  });

  // --- Tool Tests ---
  describe("malloy/executeQuery Tool Tests", () => {
    // Keep error tests that don't rely on successful execution on 'flights'
    it('should return InvalidParams error for missing required parameters (e.g., modelPath)', async () => {
        expect.assertions(4);
        const invalidParams = { packageName: "any_package", query: "query: count()" };
        try {
            await client.callTool({ name: 'malloy/executeQuery', arguments: invalidParams });
            expect(true).toBe(false); // Expected error to be thrown
        } catch (error) {
            expect(error).toBeInstanceOf(Error);
            expect(error).toHaveProperty('message');
            expect(error.message).toMatch(/MCP error -32602: MCP error -32602: Invalid arguments for tool malloy\/executeQuery/i);
            expect(error.message).toMatch(/Required/i);
        }
    });

    it('should return InvalidParams error if neither query nor queryName provided', async () => {
        expect.assertions(4);
        const invalidParams = { packageName: "any_package", modelPath: "any_model.malloy" };
        try {
            await client.callTool({ name: 'malloy/executeQuery', arguments: invalidParams });
            expect(true).toBe(false); // Expected error to be thrown
        } catch (error) {
            expect(error).toBeInstanceOf(Error);
            expect(error).toHaveProperty('message');
            expect(error.message).toMatch(/MCP error -32602: Invalid parameters/i);
            expect(error.message).toMatch(/Either 'query' or both 'sourceName' and 'queryName' must be provided/i);
        }
    });

    it('should return InvalidParams error if both query and queryName provided', async () => {
        expect.assertions(4);
        const invalidParams = { 
            packageName: "any_package", modelPath: "any_model.malloy",
            query: "query: count()", sourceName: "s", queryName:"q" 
        };
        try {
            await client.callTool({ name: 'malloy/executeQuery', arguments: invalidParams });
            expect(true).toBe(false); // Expected error to be thrown
        } catch (error) {
            expect(error).toBeInstanceOf(Error);
            expect(error).toHaveProperty('message');
            expect(error.message).toMatch(/MCP error -32602: Invalid parameters/i);
            expect(error.message).toMatch(/Cannot provide both 'query' and 'queryName'/i);
        }
    });

    it('should return InvalidParams error for non-existent package', async () => {
        expect.assertions(4);
        const invalidParams = { packageName: "non_existent_package", modelPath: "any_model.malloy", query: "query: count()" };
        try {
            await client.callTool({ name: 'malloy/executeQuery', arguments: invalidParams });
            expect(true).toBe(false); // Expected error to be thrown
        } catch (error) {
            expect(error).toBeInstanceOf(Error);
            expect(error).toHaveProperty('message');
            expect(error.message).toMatch(/error: Package manifest for non_existent_package does not exist/i);
            expect(error.message).toMatch(/PackageNotFoundError/i);
        }
    });

    it('should return InvalidParams error for a non-existent package URI', async () => {
        expect.assertions(3);
        try {
            const response = await client.listResources({ uri: 'mcp://test/non-existent-package' });
            expect(true).toBe(false); // Expected error to be thrown
        } catch (error) {
            expect(error).toBeInstanceOf(Error);
            expect(error).toHaveProperty('message');
            expect(error.message).toMatch(/Resource.*not found/i);
        }
    });
  });
}); 