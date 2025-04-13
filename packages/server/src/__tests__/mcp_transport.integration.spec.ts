import { describe, it, expect, beforeAll, afterAll, beforeEach, afterEach } from "bun:test";
import { Server } from "http";
import { app, httpServer } from "../server";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { SSEClientTransport } from "@modelcontextprotocol/sdk/client/sse.js";
import { ErrorCode, Request, Notification, Result } from "@modelcontextprotocol/sdk/types.js";
import { AddressInfo } from "net";

let serverInstance: Server;
let serverUrl: string;
let client: Client<Request, Notification, Result>;
let transport: SSEClientTransport;

beforeAll(async () => {
  await new Promise<void>((resolve, reject) => {
    serverInstance = httpServer.listen(0, () => {
      try {
        const address = serverInstance.address();
        if (typeof address === 'string' || address === null) {
          reject(new Error("Failed to get server address"));
          return;
        }
        serverUrl = `http://localhost:${address.port}`;
        console.log(`Test server running on ${serverUrl}`);
        resolve();
      } catch (err) {
        reject(err);
      }
    });
    serverInstance.on('error', reject);
  });
});

afterAll(async () => {
  await new Promise<void>((resolve) => {
    serverInstance.close(() => resolve());
  });
});

beforeEach(async () => {
  client = new Client<Request, Notification, Result>({ name: "test-client", version: "0.0.1" });
  transport = new SSEClientTransport(new URL(`${serverUrl}/api/v0/mcp/sse`));
  await client.connect(transport);
});

afterEach(async () => {
  if (client) {
    await client.close();
  }
});

describe("MCP Transport Tests", () => {
  describe("Basic Protocol", () => {
    it("should handle a simple listResources request", async () => {
      const result = await client.listResources();
      expect(result).toHaveProperty('resources');
      expect(Array.isArray(result.resources)).toBe(true);
    });

    it("should receive MethodNotFound error for an unknown method", async () => {
      expect.assertions(3);
      try {
        await (client as any).call('unknown/method', { param: 'value' });
      } catch (error) {
        expect(error).toBeInstanceOf(Error);
        expect((error as any).code).toBe(-32601);
        expect((error as Error).message).toMatch(/Method not found/i);
      }
    });

    it("should receive InvalidParams error when calling a known method with invalid params", async () => {
      expect.assertions(3);
      try {
        await client.readResource({} as any);
      } catch (error) {
        expect(error).toBeInstanceOf(Error);
        expect((error as any).code).toBe(-32602);
        expect((error as Error).message).toMatch(/Invalid params/i);
      }
    });
  });

  describe("Connection Management", () => {
    it("should handle multiple concurrent requests", async () => {
      const requests = [
        client.listResources(),
        client.readResource({ uri: "malloy://project/home" }),
        client.listResources()
      ];
      
      const results = await Promise.all(requests);
      expect(results.length).toBe(3);
      expect(results[0]).toHaveProperty('resources');
      expect(results[1]).toHaveProperty('contents');
      expect(results[2]).toHaveProperty('resources');
    });

    it("should handle reconnection after close", async () => {
      await client.close();
      const address = serverInstance.address();
      if (!address || typeof address === 'string') {
        throw new Error("Invalid server address");
      }
      const newTransport = new SSEClientTransport(
        new URL(`http://localhost:${address.port}/api/v0/mcp/sse`)
      );
      await client.connect(newTransport);
      const result = await client.listResources({ projectName: 'malloy://project/home' });
      expect(result).toBeDefined();
    });

    it("should fail gracefully with invalid connection", async () => {
      const badClient = new Client<Request, Notification, Result>({
        name: 'test-client',
        version: '0.0.1'
      });
      const address = serverInstance.address();
      if (!address || typeof address === 'string') {
        throw new Error("Invalid server address");
      }
      const badTransport = new SSEClientTransport(
        new URL(`http://localhost:${address.port + 1}/api/v0/mcp/sse`)
      );

      await expect(badClient.connect(badTransport)).rejects.toThrow();
    });
  });
}); 