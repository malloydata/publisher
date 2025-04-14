// @ts-ignore - bun:test types
import { test, expect, describe, beforeAll, afterAll, it, beforeEach, afterEach, fail } from "bun:test";
import { Server } from "http";
import { app, httpServer } from "../server";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { SSEClientTransport } from "@modelcontextprotocol/sdk/client/sse.js";
import { Request, Notification, Result, ProgressNotificationSchema, CallToolResult } from "@modelcontextprotocol/sdk/types.js";
import { AddressInfo } from "net";
import { PackageService } from "../service/package.service";
import { MCP_ERROR_MESSAGES } from '../mcp/mcp_constants';
import { ErrorCode } from "@modelcontextprotocol/sdk/types.js";

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
      const validQuery = "run: flights -> { aggregate: flight_count is count() }";
      const result = await client.callTool({
        name: "malloy/executeQuery",
        arguments: {
          packageName: "faa",
          modelPath: "flights.malloy",
          query: validQuery
        }
      });
      expect(result.isError).not.toBe(true);
      expect(result.content).toBeDefined();
    });

    it("should handle invalid query syntax", async () => {
      const result = await client.callTool({
        name: "malloy/executeQuery",
        arguments: {
          projectName: "home",
          packageName: "faa",
          modelPath: "flights.malloy",
          query: "invalid query syntax"
        }
      });
      // Application Error: Expect resolution with isError: true and specific message
      expect(result).toEqual({
        isError: true,
        content: [
          { 
            type: 'text',
            text: expect.stringContaining(MCP_ERROR_MESSAGES.ERROR_EXECUTING_QUERY("faa/flights.malloy")) &&
                  expect.stringContaining("no viable alternative at input 'invalid'")
          }
          ]
      });
    });

    it("should handle invalid query parameters", async () => {
      // Protocol Error: Expect rejection with specific code and wrapped message
      await expect(client.callTool({
        name: "malloy/executeQuery",
        arguments: {
          query: "run: flights -> { select: carrier }"  // Missing required packageName and modelPath
        }
      })).rejects.toMatchObject({
        code: ErrorCode.InvalidParams, 
        message: expect.stringContaining("Invalid arguments for tool malloy/executeQuery:") && 
                 expect.stringContaining("packageName") &&
                 expect.stringContaining("modelPath")
      });
    });

    it("should handle invalid resource URI", async () => {
      await expect(client.callTool({
        name: "malloy/executeQuery",
        arguments: { resourceUri: "invalid_uri" }
      })).rejects.toMatchObject({
        code: -32602,
        message: expect.stringMatching(/Invalid arguments/i)
      });
    });

    it("should handle malformed resource URI", async () => {
      await expect(client.callTool({
        name: "malloy/executeQuery",
        arguments: { resourceUri: "malformed:uri" }
      })).rejects.toMatchObject({
        code: -32602,
        message: expect.stringMatching(/Invalid arguments/i)
      });
    });

    it("should execute a valid query", async () => {
      const validQuery = "run: flights -> { select: carrier }";
      const result = await client.callTool({
        name: "malloy/executeQuery",
        arguments: {
          projectName: "home",
          packageName: "faa",
          modelPath: "flights.malloy",
          query: validQuery
        }
      });
      expect(result.isError).not.toBe(true);
      expect(result.content).toBeDefined();
    });

    it("should handle progress events", async () => {
      const result = await client.callTool({
        name: "malloy/executeQuery",
        arguments: {
          projectName: "home",
          packageName: "faa",
          modelPath: "flights.malloy",
          query: "run: flights -> { aggregate: flight_count is count() }"
        }
      });
      expect(result.isError).toBe(false);
      expect(result.content).toBeDefined();
    });

    it("should handle query cancellation", async () => {
      expect.assertions(2);
      let toolPromise;
      try {
         toolPromise = client.callTool({
          name: "malloy/executeQuery",
          arguments: {
            packageName: "faa",
            modelPath: "flights.malloy",
            query: "query: faa -> { aggregate: flight_count is count() }"
          }
        });

        await new Promise(resolve => setTimeout(resolve, 100));
        await client.close(); 
        await toolPromise; 
        fail("Promise should have rejected due to cancellation");
      } catch (error) {
        expect(error).toBeInstanceOf(Error);
        expect((error as Error).message).toMatch(/cancel|closed/i);
      } finally {
          if (client) { 
              // Avoid closing again if already closed
              // await client.close(); 
          }
      }
    });
  });

  describe("Named Query Execution", () => {
    it("should execute a named query from a source", async () => {
      const result = await client.callTool({
        name: "malloy/executeQuery",
        arguments: {
          projectName: "home",
          packageName: "faa",
          modelPath: "flights.malloy",
          sourceName: "flights",
          queryName: "by_carrier"
        }
      });
      expect(result.isError).toBe(false);
      expect(result.content).toBeDefined();
    });

    it("should handle non-existent named query", async () => {
      const result = await client.callTool({
        name: "malloy/executeQuery",
        arguments: {
          projectName: "home",
          packageName: "faa",
          modelPath: "flights.malloy",
          sourceName: "flights",
          queryName: "nonexistent_query"
        }
      });
      // Application Error (non-existent named query): Expect resolution with isError: true
      expect(result.isError).toBe(true);
      expect(result.content[0].type).toBe('text');
      expect(result.content[0].text).toEqual(
        `${MCP_ERROR_MESSAGES.ERROR_EXECUTING_QUERY("faa/flights.malloy")} Error(s) compiling model:\nFILE: internal://internal.malloy\nline 2: 'nonexistent_query' is not defined`
      );
    });
  });

  describe("Error Handling", () => {
    it("should handle missing package", async () => {
      const packageName = "nonexistent";
      const modelPath = "model.malloy";
      const result = await client.callTool({
        name: "malloy/executeQuery",
        arguments: {
          projectName: "home",
          packageName,
          modelPath,
          query: "query: faa -> { aggregate: count() }"
        }
      });
      // Application Error: Expect resolution with isError: true and direct message
      expect(result).toEqual({
        isError: true,
        content: [
          { 
            type: 'text',
            text: MCP_ERROR_MESSAGES.PACKAGE_NOT_FOUND(packageName)
          }
        ]
      });
    });

    it("should handle missing model", async () => {
      const packageName = "faa";
      const modelPath = "nonexistent.malloy";
      const result = await client.callTool({
        name: "malloy/executeQuery",
        arguments: {
          projectName: "home",
          packageName,
          modelPath,
          query: "query: faa -> { aggregate: count() }"
        }
      });
      // Application Error: Expect resolution with isError: true and direct message
      expect(result).toEqual({
        isError: true,
        content: [
          { 
            type: 'text',
            text: MCP_ERROR_MESSAGES.MODEL_NOT_FOUND(packageName, modelPath)
          }
        ]
      });
    });

    it("should handle non-existent named query", async () => {
      const packageName = "faa";
      const modelPath = "flights.malloy";
      const queryName = "nonexistent_query";
      const result = await client.callTool({
        name: "malloy/executeQuery",
        arguments: {
          projectName: "home",
          packageName,
          modelPath,
          sourceName: "flights",
          queryName
        }
      });
      // Application Error (non-existent named query): Expect resolution with isError: true
      expect(result.isError).toBe(true);
      expect(result.content[0].type).toBe('text');
      expect(result.content[0].text).toEqual(
        `${MCP_ERROR_MESSAGES.ERROR_EXECUTING_QUERY("faa/flights.malloy")} Error(s) compiling model:\nFILE: internal://internal.malloy\nline 2: 'nonexistent_query' is not defined`
      );
    });

    it("should handle query execution errors", async () => {
      const result = await client.callTool({
        name: "malloy/executeQuery",
        arguments: {
          projectName: "home",
          packageName: "faa",
          modelPath: "flights.malloy",
          query: "invalid query syntax"
        }
      });
      // Application Error: Expect resolution with isError: true and specific message
      expect(result).toEqual({
        isError: true,
        content: [
          { 
            type: 'text',
            text: expect.stringContaining(MCP_ERROR_MESSAGES.ERROR_EXECUTING_QUERY("faa/flights.malloy")) &&
                  expect.stringContaining("no viable alternative at input 'invalid'")
          }
        ]
      });
    });

    it("should handle compilation errors", async () => {
      const result = await client.callTool({
        name: "malloy/executeQuery",
        arguments: {
          projectName: "home",
          packageName: "faa",
          modelPath: "flights.malloy",
          query: "run: flights -> { aggregate: count() }"  // Missing name for aggregate
        }
      });
      // Application Error: Expect resolution with isError: true and specific message
      expect(result).toEqual({
        isError: true,
        content: [
          { 
            type: 'text',
            text: expect.stringContaining(MCP_ERROR_MESSAGES.ERROR_EXECUTING_QUERY("faa/flights.malloy")) &&
                  expect.stringContaining("no viable alternative at input 'invalid'")
          }
        ]
      });
    });

    it("should handle mutually exclusive parameters", async () => {
      // Protocol Error: Expect REJECTION with specific code and wrapped message
      await expect(client.callTool({
        name: "malloy/executeQuery",
        arguments: {
          packageName: "faa",
          modelPath: "flights.malloy",
          query: "query: faa -> { aggregate: flight_count is count() }",
          sourceName: "faa",
          queryName: "by_carrier"
        }
      })).rejects.toMatchObject({
        code: ErrorCode.InvalidParams, 
        message: `Invalid arguments for tool malloy/executeQuery:  (${MCP_ERROR_MESSAGES.MUTUALLY_EXCLUSIVE_PARAMS})`
      });
    });

    it("should handle missing required parameters", async () => {
      // Protocol Error: Expect REJECTION with specific code and wrapped message
      await expect(client.callTool({
        name: "malloy/executeQuery",
        arguments: {
          projectName: "home",
          packageName: "faa",
          modelPath: "flights.malloy"
        }
      })).rejects.toMatchObject({
        code: ErrorCode.InvalidParams, 
        message: `Invalid arguments for tool malloy/executeQuery:  (${MCP_ERROR_MESSAGES.MISSING_REQUIRED_PARAMS})`
      });
    });
  });
}); 