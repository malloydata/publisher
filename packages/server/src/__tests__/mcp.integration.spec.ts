// @ts-ignore - bun:test types
import { test, expect, describe, beforeAll, afterAll, it, fail } from "bun:test";

// --- Removed Server, app, httpServer, PackageService imports ---
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { ErrorCode, Request, Notification, Result } from "@modelcontextprotocol/sdk/types.js";
import { MCP_ERROR_MESSAGES } from '../mcp/mcp_constants'; // Keep for tool error tests

// --- Import E2E Test Setup ---
import { McpE2ETestEnvironment, setupE2ETestEnvironment, cleanupE2ETestEnvironment } from "./mcp_test_setup";

// --- Test Suite ---
describe("MCP Integration Tests (E2E)", () => { 
  let env: McpE2ETestEnvironment | null = null;
  let mcpClient: Client<Request, Notification, Result>; // Convenience variable

  // Constants needed for tests
  const PROJECT_URI = "malloy://project/home"; 
  const PROJECT_NAME = "home";
  const FAA_PACKAGE = "faa"; // For tool tests
  const FLIGHTS_MODEL = "flights.malloy"; // For tool tests
  const NON_EXISTENT_PACKAGE_URI = `${PROJECT_URI}/package/non_existent_package`;
  const NON_EXISTENT_MODEL_URI = `${PROJECT_URI}/package/${FAA_PACKAGE}/${FLIGHTS_MODEL}_non_existent`; // Use valid package for this test
  const INVALID_URI = "invalid://uri";

  // Setup/Teardown using E2E Scaffolding
  beforeAll(async () => {
    env = await setupE2ETestEnvironment();
    mcpClient = env.mcpClient;
  });

  afterAll(async () => {
    await cleanupE2ETestEnvironment(env);
    env = null;
  });

  describe("MCP Resource Integration Tests", () => {

    describe("mcp/listResources", () => {

      it("should list the 'home' project and packages when called with no URI", async () => {
        if (!env) throw new Error("Test environment not initialized");
        const result = await mcpClient.listResources();
        expect(result).toBeDefined();
        expect(result.resources).toBeDefined();
        expect(result.resources).toBeInstanceOf(Array);
        expect(result.resources.length).toBeGreaterThanOrEqual(1); // At least the project
        // Check for home project and faa package
        expect(result.resources).toEqual(expect.arrayContaining([
            expect.objectContaining({ uri: PROJECT_URI, name: PROJECT_NAME }),
            expect.objectContaining({ name: FAA_PACKAGE }) // Check at least faa exists
        ]));
      });

      // Updated based on mcp_resource_handlers test findings
      it("should list packages when called with project URI", async () => {
        if (!env) throw new Error("Test environment not initialized");
        const result = await mcpClient.listResources({ uri: PROJECT_URI });
        expect(result).toBeDefined();
        expect(result.resources).toBeDefined();
        expect(result.resources).toBeInstanceOf(Array);
         // Should list packages within the project
         expect(result.resources).toEqual(expect.arrayContaining([
             expect.objectContaining({ name: FAA_PACKAGE })
         ]));
      });

      // Updated based on mcp_resource_handlers test findings
      it("should resolve with error for a non-existent package URI", async () => {
          if (!env) throw new Error("Test environment not initialized");
          const result = await mcpClient.listResources({ uri: NON_EXISTENT_PACKAGE_URI });
          expect(result).toBeDefined();
          expect(result.isError).toBe(true);
          expect(result.error).toBeDefined();
          expect((result.error as any)?.message).toMatch(/Package manifest.*does not exist/i);
      });
    });

    describe("mcp/readResource", () => {
      it("should read the 'home' project resource with details", async () => {
        if (!env) throw new Error("Test environment not initialized");
        const result: any = await mcpClient.readResource({ uri: PROJECT_URI });
        expect(result).toBeDefined();
        expect(result.contents).toBeDefined(); 
        expect(result.contents).toBeInstanceOf(Array);
        expect(result.contents.length).toBe(1);
        // Check URI within content
        expect(result.contents[0].uri).toBe(PROJECT_URI);
        // Updated expectation based on previous tests
        expect(result.contents[0].text).toBe(`Project: ${PROJECT_NAME}`); 
      });

      // Updated based on mcp_resource_handlers test findings
      it("should resolve with error for a non-existent package URI", async () => {
          if (!env) throw new Error("Test environment not initialized");
          const result = await mcpClient.readResource({ uri: NON_EXISTENT_PACKAGE_URI });
          expect(result).toBeDefined();
          expect(result.isError).toBe(true);
          expect(result.error).toBeDefined();
          expect((result.error as any)?.message).toMatch(/Package manifest.*does not exist/i);
      });

      // Updated based on mcp_resource_handlers test findings
       it("should reject for non-existent model URI", async () => {
           if (!env) throw new Error("Test environment not initialized");
           await expect(mcpClient.readResource({ uri: NON_EXISTENT_MODEL_URI }))
               .rejects.toMatchObject({
                   code: ErrorCode.InvalidParams, 
                   message: expect.stringContaining("not found")
               });
       });

       // Updated based on mcp_resource_handlers test findings
        it("should reject for invalid URI format", async () => {
            if (!env) throw new Error("Test environment not initialized");
            await expect(mcpClient.readResource({ uri: INVALID_URI }))
                .rejects.toMatchObject({
                    code: ErrorCode.InvalidParams,
                    message: expect.stringContaining("Invalid URI format")
                });
        });

        // Removed redundant/conflicting tests for non-existent URIs from original file
    });

  });

  // --- Tool Tests (Copied and adapted from mcp_query_tool.spec.ts structure) ---
  describe("malloy/executeQuery Tool Tests", () => {
    // Reusing constants defined above
    
    it('should reject with InvalidParams for missing required parameters (e.g., modelPath)', async () => {
       if (!env) throw new Error("Test environment not initialized");
        const invalidParams = { packageName: FAA_PACKAGE, query: "run: count()" }; // Missing modelPath
        await expect(mcpClient.callTool({ name: 'malloy/executeQuery', arguments: invalidParams }))
            .rejects.toMatchObject({ 
                code: ErrorCode.InvalidParams,
                message: expect.stringContaining("modelPath") // Zod error
            });
    });

    it('should reject with InvalidParams if neither query nor queryName provided', async () => {
        if (!env) throw new Error("Test environment not initialized");
        const invalidParams = { packageName: FAA_PACKAGE, modelPath: FLIGHTS_MODEL }; // Missing query/queryName
        await expect(mcpClient.callTool({ name: 'malloy/executeQuery', arguments: invalidParams }))
            .rejects.toMatchObject({ 
                code: ErrorCode.InvalidParams,
                message: expect.stringContaining(MCP_ERROR_MESSAGES.MISSING_REQUIRED_PARAMS)
            });
    });

    it('should reject with InvalidParams if both query and queryName provided', async () => {
       if (!env) throw new Error("Test environment not initialized");
        const invalidParams = { 
            packageName: FAA_PACKAGE, modelPath: FLIGHTS_MODEL,
            query: "run: count()", sourceName: "flights", queryName:"by_carrier" 
        };
         await expect(mcpClient.callTool({ name: 'malloy/executeQuery', arguments: invalidParams }))
            .rejects.toMatchObject({ 
                code: ErrorCode.InvalidParams,
                message: expect.stringContaining(MCP_ERROR_MESSAGES.MUTUALLY_EXCLUSIVE_PARAMS)
            });
    });

    it('should resolve with application error for non-existent package', async () => {
        if (!env) throw new Error("Test environment not initialized");
        const invalidParams = { packageName: "non_existent_package", modelPath: "any_model.malloy", query: "run: count()" };
        const result: any = await mcpClient.callTool({ name: 'malloy/executeQuery', arguments: invalidParams });
        expect(result.isError).toBe(true);
        expect(result.content).toBeDefined();
        expect(result.content![0].text as string).toEqual(MCP_ERROR_MESSAGES.PACKAGE_NOT_FOUND(invalidParams.packageName));
    });

    // Removed duplicate test for non-existent package URI using listResources
  });
}); 