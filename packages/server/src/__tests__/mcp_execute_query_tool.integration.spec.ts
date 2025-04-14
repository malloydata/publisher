// @ts-ignore - bun:test types
import { describe, it, expect, beforeAll, afterAll, fail } from "bun:test";
import { ErrorCode } from "@modelcontextprotocol/sdk/types.js";
import { MCP_ERROR_MESSAGES } from "../mcp/mcp_constants"; // Keep for error message checks
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import type { Request, Notification, Result } from "@modelcontextprotocol/sdk/types.js"; // Keep these base types
import { SSEClientTransport } from "@modelcontextprotocol/sdk/client/sse.js"; 
import { URL } from 'url';

// --- Import E2E Test Setup ---
import { McpE2ETestEnvironment, setupE2ETestEnvironment, cleanupE2ETestEnvironment } from "./mcp_test_setup";

// --- Test Helper ---
// Keep this helper if used, or remove if not needed for E2E checks
interface ToolResultWithContent {
    content: { type: string, text: string }[];
    [key: string]: any;
}
function isToolResultWithContent(result: any): result is ToolResultWithContent {
    return result && typeof result === 'object' && Array.isArray(result.content) && result.content.length > 0 && result.content[0].type === 'text';
}

// --- Test Suite --- 
describe("MCP Tool Handlers (E2E Integration)", () => {
  let env: McpE2ETestEnvironment | null = null;
  let mcpClient: Client;

  beforeAll(async () => {
      // Setup the E2E environment (starts server, connects client)
      env = await setupE2ETestEnvironment();
      mcpClient = env.mcpClient; // Assign client from setup
  });

  afterAll(async () => {
      await cleanupE2ETestEnvironment(env);
      env = null;
  });

  describe("malloy/executeQuery Tool", () => {
    const FAA_PACKAGE = "faa";
    const FLIGHTS_MODEL = "flights.malloy";

    it("should execute a valid ad-hoc query successfully", async () => {
      if (!env) throw new Error("Test environment not initialized");
      const params = {
        packageName: FAA_PACKAGE,
        modelPath: FLIGHTS_MODEL,
        // Use a simple, valid query against the real faa/flights model
        query: "run: flights->{aggregate: flight_count is count()}" 
      };
      
      // Expect successful RESOLUTION for valid query against real data
      // Cast to any to bypass strict type check for now
      const result: any = await mcpClient.callTool({ name: "malloy/executeQuery", arguments: params }); 

      expect(result).toBeDefined();
       // --- Assert success based on presence of content --- 
      expect(result.content).toBeDefined(); 
      expect(Array.isArray(result.content)).toBe(true);
      expect(result.content.length).toBeGreaterThan(0);
      expect(result.content[0].type).toBe('text');
      // Check if the result text is valid JSON (contains query results)
      expect(() => JSON.parse(result.content![0].text as string)).not.toThrow(); 
      const parsedResult = JSON.parse(result.content![0].text as string);
      expect(parsedResult).toHaveProperty('result');
      expect(parsedResult).toHaveProperty('sql');
      expect(Array.isArray(parsedResult.result)).toBe(true);
    });

    it("should execute a valid named query successfully", async () => {
      if (!env) throw new Error("Test environment not initialized");
      const params = {
        packageName: FAA_PACKAGE,
        modelPath: FLIGHTS_MODEL,
        sourceName: "flights", 
        queryName: "by_carrier"
      };
      
      // Expect successful RESOLUTION for valid named query
      const result: any = await mcpClient.callTool({ name: "malloy/executeQuery", arguments: params });

      expect(result).toBeDefined();
      // --- Assert success based on presence of content --- 
      expect(result.content).toBeDefined();
      expect(result.content![0].type).toBe('text');
      expect(() => JSON.parse(result.content![0].text as string)).not.toThrow(); 
      const parsedResult = JSON.parse(result.content![0].text as string); 
      expect(parsedResult).toHaveProperty('result');
    });

    it("should return application error for invalid Malloy query syntax", async () => {
      if (!env) throw new Error("Test environment not initialized");
       const params = {
        packageName: FAA_PACKAGE,
        modelPath: FLIGHTS_MODEL,
        query: "run: flights->{BAD SYNTAX aggregate: flight_count is count()}" 
      };
      
      // Application Error (Malloy Compilation): Expect RESOLUTION with isError: true
       const result: any = await mcpClient.callTool({ name: "malloy/executeQuery", arguments: params });

       expect(result.isError).toBe(true);
       expect(result.content).toBeDefined();
       expect(result.content![0].type).toBe('text'); // Added non-null assertion
       // Check for a compilation error message from Malloy
       expect(result.content![0].text as string).toMatch(/Error compiling model/i); 
    });

    it("should reject with InvalidParams for conflicting parameters (query and queryName)", async () => {
      if (!env) throw new Error("Test environment not initialized");
       const params = {
        packageName: FAA_PACKAGE,
        modelPath: FLIGHTS_MODEL,
        query: "run: flights->{aggregate: c is count()}",
        sourceName: "flights",
        queryName: "by_carrier"
      };
      
       // Protocol Error (Caught by Zod/MCP): Expect REJECTION
       await expect(mcpClient.callTool({ name: "malloy/executeQuery", arguments: params }))
          .rejects.toMatchObject({ 
              code: ErrorCode.InvalidParams,
              // Expect the specific message about mutually exclusive params
              message: expect.stringContaining(MCP_ERROR_MESSAGES.MUTUALLY_EXCLUSIVE_PARAMS) 
       });
    });

    it("should reject with InvalidParams if required params are missing (e.g., query or queryName)", async () => {
      if (!env) throw new Error("Test environment not initialized");
       const params = {
        packageName: FAA_PACKAGE,
        modelPath: FLIGHTS_MODEL
      };
      
       // Protocol Error (Caught by Zod/MCP): Expect REJECTION
       await expect(mcpClient.callTool({ name: "malloy/executeQuery", arguments: params }))
          .rejects.toMatchObject({ 
              code: ErrorCode.InvalidParams,
              // Expect the specific message about missing required params
              message: expect.stringContaining(MCP_ERROR_MESSAGES.MISSING_REQUIRED_PARAMS)
       });
    });

    it("should reject with InvalidParams if required top-level params are missing (e.g., modelPath)", async () => {
      if (!env) throw new Error("Test environment not initialized");
       const params = { // Missing modelPath
        packageName: FAA_PACKAGE,
        query: "run: flights->{aggregate: flight_count is count()}" 
      };
      
       // Protocol Error (Caught by Zod/MCP): Expect REJECTION
       await expect(mcpClient.callTool({ name: "malloy/executeQuery", arguments: params }))
          .rejects.toMatchObject({ 
              code: ErrorCode.InvalidParams,
              // Zod error message will likely mention the missing field 'modelPath'
              message: expect.stringContaining("modelPath") 
       });
    });

    it("should return application error if package not found", async () => {
      if (!env) throw new Error("Test environment not initialized");
      const params = {
        packageName: "nonexistent_package", // Use a package that doesn't exist
        modelPath: "any_model.malloy",
        query: "run: flights->{aggregate: c is count()}"
      };
      
      // Application Error (Service Layer): Expect RESOLUTION with isError: true
      // Cast to any
      const result: any = await mcpClient.callTool({ name: "malloy/executeQuery", arguments: params });

      expect(result.isError).toBe(true);
      expect(result.content).toBeDefined();
      expect(result.content![0].type).toBe('text'); // Added non-null assertion
       // Cast text to string
      expect(result.content![0].text as string).toEqual(MCP_ERROR_MESSAGES.PACKAGE_NOT_FOUND(params.packageName)); 
    });

    it("should return application error if model not found within package", async () => {
        if (!env) throw new Error("Test environment not initialized");
        const params = {
          packageName: FAA_PACKAGE,
          modelPath: "models/nonexistent_model.malloy", // Use a model that doesn't exist
          query: "run: flights->{aggregate: c is count()}"
        };

        // Application Error (Service Layer): Expect RESOLUTION with isError: true
        // Cast to any
        const result: any = await mcpClient.callTool({ name: "malloy/executeQuery", arguments: params });

        expect(result.isError).toBe(true);
        expect(result.content).toBeDefined();
        expect(result.content![0].type).toBe('text'); // Added non-null assertion
        // Cast text to string
        expect(result.content![0].text as string).toEqual(MCP_ERROR_MESSAGES.MODEL_NOT_FOUND(params.packageName, params.modelPath)); 
    });

    // Added from mcp_query_tool.spec.ts
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
}); 