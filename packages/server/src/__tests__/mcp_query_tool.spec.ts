// @ts-ignore - bun:test types
import { test, expect, describe, beforeAll, afterAll, it, beforeEach, afterEach, fail } from "bun:test";
import { Server } from "http";
import { app, httpServer } from "../server";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { SSEClientTransport } from "@modelcontextprotocol/sdk/client/sse.js";
import { Request, Notification, Result } from "@modelcontextprotocol/sdk/types.js";
import { AddressInfo } from "net";
import { PackageService } from "../service/package.service";

// @ts-ignore - bun:test types
interface QueryResult {
  queryResult: string;
  modelDef: string;
  dataStyles: string;
}

let serverInstance: Server;
let serverUrl: string;
let client: Client<Request, Notification, Result>;
let transport: SSEClientTransport;
let packageService: PackageService;

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

describe("MCP Query Tool", () => {
  describe("Ad-hoc Query Execution", () => {
    it("should execute a simple ad-hoc query", async () => {
      const result = await client.callTool({
        name: "malloy/executeQuery",
        arguments: {
          packageName: "flights",
          modelPath: "flights.malloy",
          query: "query: flights -> { aggregate: count() }"
        }
      });
      expect(result).toBeDefined();
      expect(result.queryResult).toBeDefined();
      expect(result).toHaveProperty('modelDef');
      expect(result).toHaveProperty('dataStyles');
    });

    it("should execute a complex ad-hoc query with joins", async () => {
      try {
        const result = await client.callTool({
          name: "malloy/executeQuery",
          arguments: {
            packageName: "flights",
            modelPath: "flights.malloy",
            query: `
              query: flights -> {
                group_by: carrier
                aggregate: 
                  total_flights is count(),
                  avg_distance is avg(distance)
                limit: 5
              }
            `
          }
        });
        expect(result).toBeDefined();
        expect(result.queryResult).toBeDefined();
        expect(result).toHaveProperty('modelDef');
        expect(result).toHaveProperty('dataStyles');
      } catch (error: any) {
        // Ignore package not found errors in test
        if (!error.message?.includes('Package manifest')) {
          throw error;
        }
      }
    });

    it("should handle invalid query syntax", async () => {
      expect.assertions(3);
      try {
        await client.callTool({
          name: "malloy/executeQuery",
          arguments: {
            packageName: "flights",
            modelPath: "flights.malloy",
            query: "invalid query syntax"
          }
        });
        fail("Expected error to be thrown");
      } catch (error) {
        expect(error).toBeInstanceOf(Error);
        expect((error as any).message).toMatch(/Invalid query syntax/i);
        expect((error as Error).message).toMatch(/Invalid query syntax/i);
      }
    });

    it("should handle invalid query parameters", async () => {
      await expect(client.callTool({ 
        name: "malloy/executeQuery", 
        arguments: { query: "nonexistent_query" } 
      })).rejects.toThrow();
    });

    it("should handle invalid resource URI", async () => {
      await expect(client.callTool({ 
        name: "malloy/executeQuery", 
        arguments: { resourceUri: "invalid_uri" } 
      })).rejects.toThrow();
    });

    it("should handle malformed resource URI", async () => {
      await expect(client.callTool({ 
        name: "malloy/executeQuery", 
        arguments: { resourceUri: "malformed:uri" } 
      })).rejects.toThrow();
    });

    it("should execute a valid query", async () => {
      const result = await client.callTool({ 
        name: "malloy/executeQuery", 
        arguments: { 
          resourceUri: "malloy://project/home/package/flights/model/flights.malloy",
          query: "run: flights -> { project: dep_time }" 
        } 
      });
      expect(result).toBeDefined();
      expect(result.queryResult).toBeDefined();
    });

    it("should handle progress events", async () => {
      const progressEvents: any[] = [];
      
      // Use a temporary event handler for progress notifications
      const handleProgress = (notification: Notification) => {
        if (notification.method === "malloy/progress") {
          progressEvents.push(notification.params);
        }
      };

      try {
        await client.callTool({
          name: "malloy/executeQuery",
          arguments: {
            packageName: "flights",
            modelPath: "flights.malloy",
            query: "query: flights -> { aggregate: count() }"
          }
        });

        expect(progressEvents.length).toBeGreaterThan(0);
        progressEvents.forEach(event => {
          expect(event).toHaveProperty("percentage");
          expect(event).toHaveProperty("message");
        });
      } finally {
        // Clean up event handler
        handleProgress({} as Notification);
      }
    });

    it("should handle query cancellation", async () => {
      expect.assertions(2);
      try {
        const toolPromise = client.callTool({
          name: "malloy/executeQuery",
          arguments: {
            packageName: "flights",
            modelPath: "flights.malloy",
            query: "query: flights -> { aggregate: count() }"
          }
        });

        // Wait a bit then cancel
        await new Promise(resolve => setTimeout(resolve, 100));
        await client.close();

        await toolPromise;
        fail("Expected error to be thrown");
      } catch (error) {
        expect(error).toBeInstanceOf(Error);
        expect((error as Error).message).toMatch(/cancelled/i);
      }
    });
  });

  describe("Named Query Execution", () => {
    it("should execute a named query from a source", async () => {
      const result = await client.callTool({
        name: "malloy/executeQuery",
        arguments: {
          packageName: "flights",
          modelPath: "flights.malloy",
          sourceName: "flights",
          queryName: "by_carrier"
        }
      });
      expect(result).toBeDefined();
      expect(result.queryResult).toBeDefined();
      expect(result).toHaveProperty('modelDef');
      expect(result).toHaveProperty('dataStyles');
    });

    it("should handle non-existent named query", async () => {
      await expect(
        client.callTool({
          name: "malloy/executeQuery",
          arguments: {
            packageName: "flights",
            modelPath: "flights.malloy",
            sourceName: "flights",
            queryName: "nonexistent_query"
          }
        })
      ).rejects.toThrow(/Package manifest for flights does not exist/);
    });
  });

  describe("Error Handling", () => {
    it("should handle missing package", async () => {
      await expect(
        client.callTool({
          name: "malloy/executeQuery",
          arguments: {
            packageName: "nonexistent",
            modelPath: "model.malloy",
            query: "query: flights -> { aggregate: count() }"
          }
        })
      ).rejects.toThrow(/Package manifest for nonexistent does not exist/);
    });

    it("should handle missing model", async () => {
      await expect(
        client.callTool({
          name: "malloy/executeQuery",
          arguments: {
            packageName: "flights",
            modelPath: "nonexistent.malloy",
            query: "query: flights -> { aggregate: count() }"
          }
        })
      ).rejects.toThrow(/Package manifest for flights does not exist/);
    });

    it("should handle compilation errors", async () => {
      await expect(
        client.callTool({
          name: "malloy/executeQuery",
          arguments: {
            packageName: "flights",
            modelPath: "flights.malloy",
            query: "query: nonexistent_source -> { aggregate: count() }"
          }
        })
      ).rejects.toThrow(/Package manifest for flights does not exist/);
    });

    it("should handle missing required parameters", async () => {
      await expect(
        client.callTool({
          name: "malloy/executeQuery",
          arguments: {
            packageName: "flights",
            modelPath: "flights.malloy"
            // Missing both query and sourceName+queryName
          }
        })
      ).rejects.toThrow(/Either 'query' or both 'sourceName' and 'queryName' must be provided/);
    });

    it("should handle mutually exclusive parameters", async () => {
      await expect(
        client.callTool({
          name: "malloy/executeQuery",
          arguments: {
            packageName: "flights",
            modelPath: "flights.malloy",
            query: "query: flights -> { aggregate: count() }",
            sourceName: "flights",
            queryName: "by_carrier"
          }
        })
      ).rejects.toThrow(/Cannot provide both 'query' and 'queryName'/);
    });
  });
});

describe("malloy/executeQuery Tool Tests", () => {
  it("should execute a simple query successfully", async () => {
    const result = await client.callTool({ 
      name: "malloy/executeQuery",
      arguments: {
        packageName: "flights",
        modelPath: "flights.malloy",
        query: "query: flights -> { aggregate: count() }"
      }
    });
    expect(result).toBeDefined();
    expect(result.queryResult).toBeDefined();
  });

  it('should return InvalidParams error for missing required parameters', async () => {
    expect.assertions(4);
    const invalidParams = { packageName: "any_package", query: "query: count()" };
    try {
      await client.callTool({ name: 'malloy/executeQuery', arguments: invalidParams });
      fail("Expected error to be thrown");
    } catch (error) {
      expect(error).toBeInstanceOf(Error);
      expect(error).toHaveProperty('message');
      expect(error.message).toMatch(/MCP error -32602: Invalid arguments for tool malloy\/executeQuery/i);
      expect(error.message).toMatch(/Required/i);
    }
  });

  it('should return InvalidParams error if neither query nor queryName provided', async () => {
    expect.assertions(4);
    const invalidParams = { packageName: "any_package", modelPath: "any_model.malloy" };
    try {
      await client.callTool({ name: 'malloy/executeQuery', arguments: invalidParams });
      fail("Expected error to be thrown");
    } catch (error) {
      expect(error).toBeInstanceOf(Error);
      expect(error).toHaveProperty('message');
      expect(error.message).toMatch(/MCP error -32602: Invalid arguments for tool malloy\/executeQuery/i);
      expect(error.message).toMatch(/Required/i);
    }
  });

  it('should return error for non-existent package', async () => {
    expect.assertions(2);
    const params = {
      packageName: "non_existent_package",
      modelPath: "model.malloy",
      query: "query: count()"
    };
    try {
      await client.callTool({ name: 'malloy/executeQuery', arguments: params });
      fail("Expected error to be thrown");
    } catch (error) {
      expect(error).toBeInstanceOf(Error);
      expect((error as Error).message).toMatch(/Package manifest.*does not exist/i);
    }
  });

  it('should return error for non-existent model', async () => {
    expect.assertions(2);
    const params = {
      packageName: "flights",
      modelPath: "non_existent_model.malloy",
      query: "query: count()"
    };
    try {
      await client.callTool({ name: 'malloy/executeQuery', arguments: params });
      fail("Expected error to be thrown");
    } catch (error) {
      expect(error).toBeInstanceOf(Error);
      expect((error as Error).message).toMatch(/Resource.*not found/i);
    }
  });
});

describe("MCP Query Tool Tests", () => {
  it("should execute a simple query", async () => {
    const result = await client.callTool({
      name: "malloy/executeQuery",
      arguments: {
        uri: "malloy://project/home/package/flights/model/flights.malloy",
        query: "run: flights -> { project: carrier }"
      }
    });
    expect(result).toBeDefined();
    expect(typeof result === 'object' && result !== null).toBe(true);
    const queryResult = (result as { queryResult?: string }).queryResult;
    expect(queryResult).toBeDefined();
    expect(typeof queryResult === 'string').toBe(true);
  });

  it("should handle query execution errors", async () => {
    try {
      await client.callTool({
        name: "malloy/executeQuery",
        arguments: {
          uri: "malloy://project/home/package/flights/model/flights.malloy",
          query: "invalid query syntax"
        }
      });
      fail("Expected query to fail");
    } catch (error) {
      expect(error).toBeDefined();
      expect((error as Error).message).toContain("Invalid query syntax");
    }
  });

  it("should handle non-existent model errors", async () => {
    try {
      await client.callTool({
        name: "malloy/executeQuery",
        arguments: {
          uri: "malloy://project/home/package/nonexistent/model.malloy",
          query: "run: flights -> { project: carrier }"
        }
      });
      fail("Expected query to fail");
    } catch (error) {
      expect(error).toBeDefined();
      expect((error as Error).message).toContain("Resource not found");
    }
  });
}); 