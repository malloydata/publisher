// @ts-ignore - bun:test types
import { test, expect, describe, beforeAll, afterAll, it, fail } from "bun:test";

// --- Removed Server, app, httpServer, AddressInfo, PackageService imports ---
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { Request, Notification, Result, CallToolResult } from "@modelcontextprotocol/sdk/types.js";
import { MCP_ERROR_MESSAGES } from '../mcp/mcp_constants';
import { ErrorCode } from "@modelcontextprotocol/sdk/types.js";

// --- Import E2E Test Setup ---
import { McpE2ETestEnvironment, setupE2ETestEnvironment, cleanupE2ETestEnvironment } from "./mcp_test_setup";
// Needed for cancellation test
import { SSEClientTransport } from "@modelcontextprotocol/sdk/client/sse.js"; 
import { URL } from 'url';

// --- Test Suite --- 
describe("MCP Query Tool (E2E Integration)", () => {
  let env: McpE2ETestEnvironment | null = null;
  let mcpClient: Client<Request, Notification, Result>; // Convenience variable

  // Define constants in the outer scope
  const FAA_PACKAGE = "faa";
  const FLIGHTS_MODEL = "flights.malloy";

  // Setup/Teardown using E2E Scaffolding
  beforeAll(async () => {
    env = await setupE2ETestEnvironment();
    mcpClient = env.mcpClient;
  });

  afterAll(async () => {
    await cleanupE2ETestEnvironment(env);
    env = null;
  });

  // --- Removed beforeEach and afterEach --- 

  describe("Ad-hoc Query Execution", () => {
    // Constants moved to outer scope

    it("should execute a simple ad-hoc query", async () => {
      if (!env) throw new Error("Test environment not initialized");
      const validQuery = "run: flights -> { aggregate: flight_count is count() }";
      const result: any = await mcpClient.callTool({ // Use any for result type
        name: "malloy/executeQuery",
        arguments: {
          packageName: FAA_PACKAGE,
          modelPath: FLIGHTS_MODEL,
          query: validQuery
        }
      });
      // Assert success based on content
      expect(result).toBeDefined();
      expect(result.isError).toBe(false);
      expect(result.content).toBeDefined();
      expect(Array.isArray(result.content)).toBe(true);
      expect(result.content.length).toBeGreaterThan(0);
      expect(result.content[0].type).toBe('text');
      expect(() => JSON.parse(result.content![0].text as string)).not.toThrow();
    });

    it("should handle invalid query syntax", async () => {
      if (!env) throw new Error("Test environment not initialized");
      const result: any = await mcpClient.callTool({
        name: "malloy/executeQuery",
        arguments: {
          // projectName: "home", // projectName is optional now
          packageName: FAA_PACKAGE,
          modelPath: FLIGHTS_MODEL,
          query: "invalid query syntax"
        }
      });
      // Application Error: Expect resolution with isError: true and specific message
      expect(result).toBeDefined();
      expect(result.isError).toBe(true);
      expect(result.content).toBeDefined();
      expect(result.content![0].type).toBe('text');
      // Check for Malloy compilation error message
      expect(result.content![0].text as string).toMatch(/Error compiling model/i); 
      expect(result.content![0].text as string).toMatch(/no viable alternative/i);
    });

    it("should handle invalid query parameters (missing required)", async () => {
      if (!env) throw new Error("Test environment not initialized");
      // Protocol Error: Expect rejection
      await expect(mcpClient.callTool({
        name: "malloy/executeQuery",
        arguments: {
          query: "run: flights -> { select: carrier }"  // Missing packageName and modelPath
        }
      })).rejects.toMatchObject({
        code: ErrorCode.InvalidParams, 
        message: expect.stringContaining("modelPath") // Zod error message
      });
    });

    // Removed invalid/malformed resourceUri tests as executeQuery doesn't use resourceUri

    it("should execute a valid query", async () => {
       if (!env) throw new Error("Test environment not initialized");
      const validQuery = "run: flights -> { select: carrier }";
      const result: any = await mcpClient.callTool({
        name: "malloy/executeQuery",
        arguments: {
          // projectName: "home",
          packageName: FAA_PACKAGE,
          modelPath: FLIGHTS_MODEL,
          query: validQuery
        }
      });
      expect(result).toBeDefined();
      expect(result.isError).toBe(false);
      expect(result.content).toBeDefined();
    });

    // Skipping progress events test for now - requires specific server implementation
    // it("should handle progress events", async () => { ... });

    it("should handle query cancellation via client close", async () => {
      if (!env) throw new Error("Test environment not initialized");
      
      // Create a new client *specifically* for this test so we can close it
      // without affecting other tests running concurrently (if any).
      const cancelClient = new Client<Request, Notification, Result>({ name: "cancel-test-client", version: "1.0" });
      // Correct: Use SSEClientTransport with the server URL
      const cancelTransport = new SSEClientTransport(new URL(env.serverUrl + '/')); 
      await cancelClient.connect(cancelTransport);

      expect.assertions(2); // Expecting two assertions: instanceof Error and message match
      let toolPromise;
      try {
         toolPromise = cancelClient.callTool({
          name: "malloy/executeQuery",
          arguments: {
            packageName: FAA_PACKAGE,
            modelPath: FLIGHTS_MODEL,
            // Use a query known to take a little time if possible, otherwise a simple one
            query: "run: flights->{aggregate: c is count() for 100}"
          }
        });

        // Give the request a moment to start on the server
        await new Promise(resolve => setTimeout(resolve, 100)); 
        
        // Close the client to trigger cancellation
        await cancelClient.close(); 
        
        // Await the promise - it should reject due to the closure
        await toolPromise; 
        
        fail("Promise should have rejected due to cancellation");
      } catch (error) {
        // Check that the error is an Error instance and the message indicates closure/cancellation
        expect(error).toBeInstanceOf(Error);
        expect((error as Error).message).toMatch(/cancel|closed/i);
      } finally {
          // Ensure the temporary client is closed even if the test failed unexpectedly
          await cancelClient.close().catch(() => {}); // Ignore errors on final cleanup
      }
    });
  });

  describe("Named Query Execution", () => {
    it("should execute a named query from a source", async () => {
      if (!env) throw new Error("Test environment not initialized");
      const result: any = await mcpClient.callTool({
        name: "malloy/executeQuery",
        arguments: {
          // projectName: "home",
          packageName: FAA_PACKAGE,
          modelPath: FLIGHTS_MODEL,
          sourceName: "flights",
          queryName: "by_carrier"
        }
      });
      expect(result).toBeDefined();
      expect(result.isError).toBe(false);
      expect(result.content).toBeDefined();
    });

    it("should handle non-existent named query", async () => {
      if (!env) throw new Error("Test environment not initialized");
      const result: any = await mcpClient.callTool({
        name: "malloy/executeQuery",
        arguments: {
          // projectName: "home",
          packageName: FAA_PACKAGE,
          modelPath: FLIGHTS_MODEL,
          sourceName: "flights",
          queryName: "nonexistent_query"
        }
      });
      // Application Error (non-existent named query): Expect resolution with isError: true
      expect(result).toBeDefined();
      expect(result.isError).toBe(true);
      expect(result.content).toBeDefined();
      expect(result.content![0].type).toBe('text');
      // Check for specific Malloy error message
      expect(result.content![0].text as string).toMatch(/Error.*compiling model.*nonexistent_query.*is not defined/s);
    });
  });

  describe("Error Handling", () => {
    it("should handle missing package", async () => {
       if (!env) throw new Error("Test environment not initialized");
      const packageName = "nonexistent";
      const modelPath = "model.malloy";
      const result: any = await mcpClient.callTool({
        name: "malloy/executeQuery",
        arguments: {
          // projectName: "home",
          packageName,
          modelPath,
          query: "run: test -> { aggregate: count() }" // Query doesn't matter much here
        }
      });
      // Application Error: Expect resolution with isError: true and direct message
      expect(result).toBeDefined();
      expect(result.isError).toBe(true);
      expect(result.content).toBeDefined();
      expect(result.content![0].type).toBe('text');
      expect(result.content![0].text as string).toEqual(MCP_ERROR_MESSAGES.PACKAGE_NOT_FOUND(packageName));
    });

    it("should handle missing model", async () => {
      if (!env) throw new Error("Test environment not initialized");
      const packageName = FAA_PACKAGE;
      const modelPath = "nonexistent.malloy";
      const result: any = await mcpClient.callTool({
        name: "malloy/executeQuery",
        arguments: {
         // projectName: "home",
          packageName,
          modelPath,
          query: "run: test -> { aggregate: count() }"
        }
      });
      // Application Error: Expect resolution with isError: true and direct message
      expect(result).toBeDefined();
      expect(result.isError).toBe(true);
      expect(result.content).toBeDefined();
      expect(result.content![0].type).toBe('text');
      expect(result.content![0].text as string).toEqual(MCP_ERROR_MESSAGES.MODEL_NOT_FOUND(packageName, modelPath));
    });

    it("should handle non-existent named query", async () => {
      if (!env) throw new Error("Test environment not initialized");
      const packageName = FAA_PACKAGE;
      const modelPath = FLIGHTS_MODEL;
      const queryName = "nonexistent_query";
      const result: any = await mcpClient.callTool({
        name: "malloy/executeQuery",
        arguments: {
         // projectName: "home",
          packageName,
          modelPath,
          sourceName: "flights",
          queryName
        }
      });
      // Application Error (non-existent named query): Expect resolution with isError: true
      expect(result).toBeDefined();
      expect(result.isError).toBe(true);
      expect(result.content).toBeDefined();
      expect(result.content![0].type).toBe('text');
      expect(result.content![0].text as string).toMatch(/Error.*compiling model.*nonexistent_query.*is not defined/s);
    });

    // Merged invalid query syntax tests earlier
    // it("should handle query execution errors", async () => { ... });

    // Merged invalid query syntax tests earlier
    // it("should handle compilation errors", async () => { ... });

    it("should handle mutually exclusive parameters", async () => {
       if (!env) throw new Error("Test environment not initialized");
      // Protocol Error: Expect REJECTION
      await expect(mcpClient.callTool({
        name: "malloy/executeQuery",
        arguments: {
          packageName: FAA_PACKAGE,
          modelPath: FLIGHTS_MODEL,
          query: "run: faa -> { aggregate: flight_count is count() }",
          sourceName: "flights", // Provide sourceName too
          queryName: "by_carrier"
        }
      })).rejects.toMatchObject({
        code: ErrorCode.InvalidParams, 
        message: expect.stringContaining(MCP_ERROR_MESSAGES.MUTUALLY_EXCLUSIVE_PARAMS)
      });
    });

    it("should handle missing required parameters", async () => {
       if (!env) throw new Error("Test environment not initialized");
      // Protocol Error: Expect REJECTION
      await expect(mcpClient.callTool({
        name: "malloy/executeQuery",
        arguments: {
         // projectName: "home",
          packageName: FAA_PACKAGE,
          modelPath: FLIGHTS_MODEL
          // Missing query or queryName
        }
      })).rejects.toMatchObject({
        code: ErrorCode.InvalidParams, 
        message: expect.stringContaining(MCP_ERROR_MESSAGES.MISSING_REQUIRED_PARAMS)
      });
    });
  });
}); 