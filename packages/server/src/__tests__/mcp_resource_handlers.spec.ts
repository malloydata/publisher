import { test, expect, describe, beforeAll, afterAll, it, beforeEach, afterEach } from "bun:test";
import { Server } from "http";
import { app, httpServer } from "../server";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { SSEClientTransport } from "@modelcontextprotocol/sdk/client/sse.js";
import { ErrorCode, Request, Notification, Result } from "@modelcontextprotocol/sdk/types.js";

let serverInstance: Server;
let serverUrl: string;
let client: Client<Request, Notification, Result>;
let transport: SSEClientTransport;

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
describe("MCP Resource Tests", () => {
  it('should list resources for a valid project', async () => {
    const result = await client.listResources({ uri: "malloy://project/home" });
    expect(Array.isArray(result.resources)).toBe(true);
  });

  it('should return error for non-existent project', async () => {
    expect.assertions(2);
    try {
      await client.listResources({ uri: "malloy://project/non_existent" });
    } catch (error) {
      expect(error).toBeInstanceOf(Error);
      expect((error as Error).message).toMatch(/Project.*not found/i);
    }
  });

  it('should read resource metadata for a valid model', async () => {
    const result = await client.readResource({ uri: "malloy://project/home/package/flights/model/flights.malloy" });
    expect(result).toBeDefined();
    expect(typeof result === 'object' && result !== null).toBe(true);
    const metadata = (result as { metadata?: { type?: string } }).metadata;
    expect(metadata).toBeDefined();
    expect(metadata?.type).toBe("model");
  });

  it('should return error for non-existent resource', async () => {
    expect.assertions(2);
    try {
      await client.readResource({ uri: "malloy://project/home/package/flights/model/non_existent.malloy" });
    } catch (error) {
      expect(error).toBeInstanceOf(Error);
      expect((error as Error).message).toMatch(/Resource.*not found/i);
    }
  });

  it('should return error for invalid URI format', async () => {
    expect.assertions(2);
    try {
      await client.readResource({ uri: "invalid://uri" });
    } catch (error) {
      expect(error).toBeInstanceOf(Error);
      expect((error as Error).message).toMatch(/Invalid URI format/i);
    }
  });
}); 