// @ts-ignore - bun:test types
import { describe, it, expect, beforeAll, afterAll } from "bun:test";

// --- Removed imports for mocking (express, http, etc.) ---
import { ErrorCode } from "@modelcontextprotocol/sdk/types.js";
import { MCP_ERROR_MESSAGES } from "../mcp/mcp_constants"; // Keep for error message checks

// --- Import MCP Client SDK (Only Client needed directly) ---
import type { Client } from "@modelcontextprotocol/sdk/client/index.js";
import type { CallToolResult } from "@modelcontextprotocol/sdk/types.js"; // Needed for result type check

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
  let mcpClient: Client; // Convenience variable

  beforeAll(async () => {
      // Setup the E2E environment (starts server, connects client)
      env = await setupE2ETestEnvironment();
      mcpClient = env.mcpClient; // Assign client from setup
  }, 30000); // Increase timeout for server/client setup

  afterAll(async () => {
      // Cleanup the E2E environment (closes client, stops server)
      await cleanupE2ETestEnvironment(env);
      env = null;
  }, 10000); // Increase timeout for cleanup

  describe("malloy/executeQuery Tool", () => {
    const FAA_PACKAGE = "faa"; // Use constant for package name
    const FLIGHTS_MODEL = "flights.malloy"; // Use constant for model path (relative to package)

    it("should execute a valid ad-hoc query successfully", async () => {
      if (!env) throw new Error("Test environment not initialized");
      const params = {
        packageName: FAA_PACKAGE,
        modelPath: FLIGHTS_MODEL,
        // Use a simple, valid query against the real faa/flights model
        query: "run: flights->{aggregate: flight_count is count()}" 
      };
      
      // Expect successful RESOLUTION for valid query against real data
      const result: CallToolResult = await mcpClient.callTool({ name: "malloy/executeQuery", arguments: params });

      expect(result).toBeDefined();
      expect(result.isError).toBe(false); // Should not be an error
      expect(result.content).toBeDefined();
      expect(Array.isArray(result.content)).toBe(true);
      expect(result.content.length).toBeGreaterThan(0);
      expect(result.content[0].type).toBe('text');
      // Check if the result text is valid JSON (contains query results)
      expect(() => JSON.parse(result.content[0].text)).not.toThrow(); 
      // Optionally, parse and check specific aspects of the *real* result data if stable
      const parsedResult = JSON.parse(result.content[0].text);
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
        queryName: "by_carrier" // Assuming this named query exists in the real model
      };
      
      // Expect successful RESOLUTION for valid named query
      const result = await mcpClient.callTool({ name: "malloy/executeQuery", arguments: params });

      expect(result).toBeDefined();
      expect(result.isError).toBe(false);
      expect(result.content).toBeDefined();
      expect(result.content[0].type).toBe('text');
      expect(() => JSON.parse(result.content[0].text)).not.toThrow();
      const parsedResult = JSON.parse(result.content[0].text);
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
       const result = await mcpClient.callTool({ name: "malloy/executeQuery", arguments: params });

       expect(result.isError).toBe(true);
       expect(result.content).toBeDefined();
       expect(result.content[0].type).toBe('text');
       // Check for a compilation error message from Malloy
       expect(result.content[0].text).toMatch(/Error.*compiling model/i);
       expect(result.content[0].text).toMatch(/no viable alternative/i); // Or similar Malloy error detail
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
       const params = { // Missing query, sourceName, and queryName
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
      const result = await mcpClient.callTool({ name: "malloy/executeQuery", arguments: params });

      expect(result.isError).toBe(true);
      expect(result.content).toBeDefined();
      expect(result.content[0].type).toBe('text');
      expect(result.content[0].text).toEqual(MCP_ERROR_MESSAGES.PACKAGE_NOT_FOUND(params.packageName));
    });

    it("should return application error if model not found within package", async () => {
        if (!env) throw new Error("Test environment not initialized");
        const params = {
          packageName: FAA_PACKAGE,
          modelPath: "models/nonexistent_model.malloy", // Use a model that doesn't exist
          query: "run: flights->{aggregate: c is count()}"
        };

        // Application Error (Service Layer): Expect RESOLUTION with isError: true
        const result = await mcpClient.callTool({ name: "malloy/executeQuery", arguments: params });

        expect(result.isError).toBe(true);
        expect(result.content).toBeDefined();
        expect(result.content[0].type).toBe('text');
        expect(result.content[0].text).toEqual(MCP_ERROR_MESSAGES.MODEL_NOT_FOUND(params.packageName, params.modelPath));
    });

  });
}); 