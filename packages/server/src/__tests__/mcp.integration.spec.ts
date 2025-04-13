import { describe, it, expect, beforeAll, afterAll, beforeEach, afterEach } from "bun:test";
import { Server } from "http";
import { app, httpServer } from "../server"; // Assuming app/server export is correct
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { SSEClientTransport } from "@modelcontextprotocol/sdk/client/sse.js";
import { ErrorCode, Request, Notification, Result, ReadResourceResult } from "@modelcontextprotocol/sdk/types.js";
import { PackageService } from "../service/package.service"; // Needed for potential mocking later

// --- Test Setup ---
let serverInstance: Server;
let serverUrl: string;
let client: Client<Request, Notification, Result>;
let transport: SSEClientTransport;

const PROJECT_URI = "malloy://project/home"; // Define constant for clarity

beforeAll((done) => {
  serverInstance = httpServer.listen(0, () => {
    const address = serverInstance.address();
    if (typeof address === 'string' || address === null) {
      throw new Error("Failed to get server address");
    }
    serverUrl = `http://localhost:${address.port}`;
    console.log(`[MCP Test] Server running on ${serverUrl}`);
    done();
  });
});

afterAll((done) => {
  console.log("[MCP Test] Closing server...");
  serverInstance.close(done);
});

beforeEach(async () => {
  client = new Client<Request, Notification, Result>({ name: "test-client", version: "0.0.1" });
  transport = new SSEClientTransport(new URL(`${serverUrl}/api/v0/mcp/sse`));
  try {
    await client.connect(transport);
  } catch (error) {
    console.error("[MCP Test] Client connection failed in beforeEach:", error);
    throw error; // Fail fast if connection fails
  }
});

afterEach(async () => {
  if (client) {
    await client.close();
  }
});

// --- Test Suite ---
describe("MCP Resource Integration Tests", () => {

  describe("mcp/listResources", () => {
    it("should list the 'home' project when called with no URI", async () => {
      const result = await client.listResources();
      // Check if the result contains the project URI
      expect(result.resources).toBeInstanceOf(Array);
      expect(result.resources.length).toBeGreaterThanOrEqual(1);
      expect(result.resources).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            uri: PROJECT_URI,
            // name: "home" // Name might not be set by default handler yet
          }),
        ])
      );
      // TODO: Refine expectation once actual handler provides name/description
    });

    it("should return InvalidParams error for an invalid URI", async () => {
        expect.assertions(2);
        try {
            await client.listResources({ uri: "invalid-uri-format" });
        } catch (error) {
            expect(error).toBeInstanceOf(Error);
            expect((error as any).code).toBe(ErrorCode.InvalidParams);
            // TODO: Add message check once error handling is refined
        }
    });

    // More tests needed for listing packages and models (requires data/mocking)
  });

  describe("mcp/readResource", () => {
    it("should read the 'home' project resource", async () => {
      const result = await client.readResource({ uri: PROJECT_URI });
      expect(result).toBeDefined();
      expect(result).toHaveProperty('resource');
      const resource = (result as any).resource;
      expect(resource.uri).toBe(PROJECT_URI);
      expect(resource.contents).toBeInstanceOf(Array);
      expect(resource.contents.length).toBeGreaterThanOrEqual(1);
      expect(resource.contents[0].text).toContain("Malloy Project: home");
      // TODO: Refine expectation based on verbose handler implementation
    });

    it("should return InvalidParams error for an invalid URI format", async () => {
        expect.assertions(2);
        try {
            await client.readResource({ uri: "invalid-uri-format" });
        } catch (error) {
            expect(error).toBeInstanceOf(Error);
            expect((error as any).code).toBe(ErrorCode.InvalidParams);
             // TODO: Add message check once error handling is refined
        }
    });

    it("should return InvalidParams error for a syntactically valid but non-existent resource URI", async () => {
        expect.assertions(2);
        try {
            // Assumes this specific package/model doesn't exist by default
            await client.readResource({ uri: "malloy://project/home/package/no_such_package/models/no_model.malloy" });
        } catch (error) {
            expect(error).toBeInstanceOf(Error);
            // Expect InvalidParams because the resource was not found
            expect((error as any).code).toBe(ErrorCode.InvalidParams);
             // TODO: Add message check once error handling is refined
        }
    });

    // More tests needed for reading packages and models (requires data/mocking)
  });

}); 