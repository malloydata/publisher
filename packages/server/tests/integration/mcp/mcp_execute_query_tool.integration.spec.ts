// @ts-expect-error Bun test types are not recognized by ESLint
import { describe, it, expect, beforeAll, afterAll, fail } from "bun:test";
import { ErrorCode } from "@modelcontextprotocol/sdk/types.js";
import { MCP_ERROR_MESSAGES } from "../../../src/mcp/mcp_constants"; // Keep for error message checks
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import type {
   Request,
   Notification,
   Result,
} from "@modelcontextprotocol/sdk/types.js"; // Keep these base types
import { URL } from "url";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";

// --- Import E2E Test Setup ---
import {
   McpE2ETestEnvironment,
   setupE2ETestEnvironment,
   cleanupE2ETestEnvironment,
} from "../../harness/mcp_test_setup";

// --- Test Suite ---
describe("MCP Tool Handlers (E2E Integration)", () => {
   let env: McpE2ETestEnvironment | null = null;
   let mcpClient: Client;

   const PROJECT_NAME = "home";
   const PACKAGE_NAME = "faa";

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
      // Constants for test parameters

      it("should execute a valid ad-hoc query successfully", async () => {
         if (!env) throw new Error("Test environment not initialized");
         const result = await mcpClient.callTool({
            name: "malloy/executeQuery",
            arguments: {
               projectName: "home",
               packageName: PACKAGE_NAME,
               modelPath: "flights.malloy",
               query: "run: flights->{ aggregate: c is count() }",
            },
         });

         expect(result).toBeDefined();
         expect(result.isError).not.toBe(true); // Should be success
         expect(result.content).toBeDefined();

         const content = result.content as Array<{
            type: string;
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            [key: string]: any;
         }>;
         expect(Array.isArray(content)).toBe(true);
         // Expect 1 content block (result)
         expect(content.length).toBe(1);

         // Check structure of each block
         for (const block of content) {
            expect(block.type).toBe("resource");
            expect(block.resource).toBeDefined();
            expect(block.resource.type).toBe("application/json");
            expect(block.resource.text).toBeDefined();
            expect(typeof block.resource.text).toBe("string");
         }

         // Basic check on the first block (result)

         const queryResultBlock = content[0].resource;
         expect(queryResultBlock.uri).toContain("#result");
         const queryResultData = JSON.parse(queryResultBlock.text);
         expect(queryResultData).toBeDefined();
         // Check properties directly on the parsed Result object
         expect(queryResultData.data).toBeDefined();
         expect(Array.isArray(queryResultData.data.array_value)).toBe(true);
         // Could add more specific checks on data if needed
      });

      it("should successfully execute a nested view using sourceName and queryName", async () => {
         if (!env) throw new Error("Test environment not initialized");
         const params = {
            projectName: PROJECT_NAME,
            packageName: PACKAGE_NAME,
            modelPath: "flights.malloy",
            sourceName: "flights", // Added sourceName
            queryName: "top_carriers",
         };

         // Expect RESOLUTION with success
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         const result: any = await mcpClient.callTool({
            name: "malloy/executeQuery",
            arguments: params,
         });

         expect(result).toBeDefined();
         expect(result.isError).toBe(false); // Expecting success
         expect(result.content).toBeDefined();
         expect(Array.isArray(result.content)).toBe(true);
         // Expect 1 content block (result)
         expect(result.content.length).toBeGreaterThan(0);

         // Check the structure of the first content block (result)
         const queryResultBlock = result.content![0];
         expect(queryResultBlock.type).toBe("resource");
         expect(queryResultBlock.resource).toBeDefined();
         expect(queryResultBlock.resource.type).toBe("application/json");
         expect(queryResultBlock.resource.uri).toMatch(/result/); // Check URI contains queryResult
         expect(queryResultBlock.resource.text).toBeDefined();

         // Optionally, parse and check the actual data
         const queryResultPayload = JSON.parse(queryResultBlock.resource.text);
         expect(queryResultPayload).toBeDefined();
         // Add more specific data checks if needed, e.g., check for specific columns or row count > 0
         // Example: expect(queryResultPayload._queryResult.data.rows.length).toBeGreaterThan(0);
      });

      it("should return application error for invalid Malloy query syntax", async () => {
         if (!env) throw new Error("Test environment not initialized");
         const params = {
            projectName: PROJECT_NAME,
            packageName: PACKAGE_NAME,
            modelPath: "flights.malloy",
            query: "run: flights->{BAD SYNTAX aggregate: flight_count is count()}",
         };

         // Application Error (Malloy Compilation): Expect RESOLUTION with isError: true
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         const result: any = await mcpClient.callTool({
            name: "malloy/executeQuery",
            arguments: params,
         });

         expect(result.isError).toBe(true);
         expect(result.content).toBeDefined();
         // Check the new structure: resource -> application/json
         const errorBlockSyntax = result.content![0];
         expect(errorBlockSyntax.type).toBe("resource");
         expect(errorBlockSyntax.resource).toBeDefined();
         expect(errorBlockSyntax.resource.type).toBe("application/json");

         // Check for Malloy compilation error message from getMalloyErrorDetails
         const errorJsonTextSyntax = errorBlockSyntax.resource.text as string;
         const errorPayloadSyntax = JSON.parse(errorJsonTextSyntax);
         expect(errorPayloadSyntax.error).toMatch(
            /syntax error|no viable alternative/i,
         );
         expect(Array.isArray(errorPayloadSyntax.suggestions)).toBe(true);
      });

      // --- Parameter Validation Error Tests ---
      // These should RESOLVE with isError: true because the error is thrown
      // inside the handler and caught by the transport bridge, which returns 200 OK + error payload.
      it("should RESOLVE with InvalidParams for conflicting parameters (query and queryName)", async () => {
         if (!env) throw new Error("Test environment not initialized");
         const params = {
            projectName: PROJECT_NAME,
            packageName: PACKAGE_NAME,
            modelPath: "flights.malloy",
            query: "run: flights->{aggregate: c is count()}",
            queryName: "top_carriers",
         };

         // Expect RESOLUTION because the error is thrown *inside* the handler
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         const result: any = await mcpClient.callTool({
            name: "malloy/executeQuery",
            arguments: params,
         });

         expect(result.isError).toBe(true);
         expect(result.content).toBeDefined();
         // Revert to expecting type: "text" for McpError caught by SDK
         expect(result.content?.[0]?.type).toBe("text");
         // Check the error message in the content
         expect(result.content?.[0]?.text).toBe(
            // Match the message including the prefix added by the client/framework
            `MCP error ${ErrorCode.InvalidParams}: ${MCP_ERROR_MESSAGES.MUTUALLY_EXCLUSIVE_PARAMS}`,
         );
      });

      it("should RESOLVE with InvalidParams if required params are missing (e.g., query or queryName)", async () => {
         if (!env) throw new Error("Test environment not initialized");
         const params = {
            projectName: PROJECT_NAME,
            packageName: PACKAGE_NAME,
            modelPath: "flights.malloy",
            // Missing query AND queryName
         };

         // Expect RESOLUTION because the error is thrown *inside* the handler
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         const result: any = await mcpClient.callTool({
            name: "malloy/executeQuery",
            arguments: params,
         });

         expect(result.isError).toBe(true);
         expect(result.content).toBeDefined();
         // Revert to expecting type: "text" for McpError caught by SDK
         expect(result.content?.[0]?.type).toBe("text");
         // Check the error message in the content
         expect(result.content?.[0]?.text).toBe(
            // Match the message including the prefix added by the client/framework
            `MCP error ${ErrorCode.InvalidParams}: ${MCP_ERROR_MESSAGES.MISSING_REQUIRED_PARAMS}`,
         );
      });

      it("should reject with InvalidParams if required top-level params are missing (e.g., modelPath)", async () => {
         if (!env) throw new Error("Test environment not initialized");
         const params = {
            // Missing modelPath
            projectName: PROJECT_NAME,
            packageName: PACKAGE_NAME,
            query: "run: flights->{aggregate: flight_count is count()}",
         };

         // Protocol Error (Caught by Zod/MCP): Expect REJECTION
         await expect(
            mcpClient.callTool({
               name: "malloy/executeQuery",
               arguments: params,
            }),
         ).rejects.toMatchObject({
            code: ErrorCode.InvalidParams,
            // Zod error message will likely mention the missing field 'modelPath'
            message: expect.stringContaining("modelPath"),
         });
      });
      // --- End Parameter Validation Error Tests ---

      it("should return application error if package not found", async () => {
         if (!env) throw new Error("Test environment not initialized");
         const params = {
            projectName: PROJECT_NAME,
            packageName: "nonexistent_package", // Use a package that doesn't exist
            modelPath: "flights.malloy",
            query: "run: flights->{aggregate: c is count()}",
         };

         // Application Error (Service Layer): Expect RESOLUTION with isError: true
         // Cast to any
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         const result: any = await mcpClient.callTool({
            name: "malloy/executeQuery",
            arguments: params,
         });

         expect(result.isError).toBe(true);
         expect(result.content).toBeDefined();
         // Check the new structure: resource -> application/json
         const errorBlockPkgNotFound = result.content![0];
         expect(errorBlockPkgNotFound.type).toBe("resource");
         expect(errorBlockPkgNotFound.resource).toBeDefined();
         expect(errorBlockPkgNotFound.resource.type).toBe("application/json");

         // Parse the JSON string from the resource text content
         const errorJsonTextPkgNotFound = errorBlockPkgNotFound.resource
            .text as string;
         const errorPayloadPkgNotFound = JSON.parse(errorJsonTextPkgNotFound);

         // Check the parsed error object
         expect(errorPayloadPkgNotFound).toBeDefined();
         expect(errorPayloadPkgNotFound.error).toBeDefined();
         expect(errorPayloadPkgNotFound.suggestions).toBeDefined();
         expect(Array.isArray(errorPayloadPkgNotFound.suggestions)).toBe(true);
         expect(errorPayloadPkgNotFound.suggestions.length).toBeGreaterThan(0);

         // Check the specific error message within the parsed object
         const expectedErrorMessageNotFound = `Resource not found: package '${params.packageName}' in project '${params.projectName}'`;
         expect(errorPayloadPkgNotFound.error).toEqual(
            expectedErrorMessageNotFound,
         );
      });

      it("should return application error if model not found within package", async () => {
         if (!env) throw new Error("Test environment not initialized");
         const params = {
            projectName: PROJECT_NAME,
            packageName: PACKAGE_NAME,
            modelPath: "nonexistent_model.malloy", // Use a model that doesn't exist
            query: "run: flights->{aggregate: c is count()}",
         };

         // Application Error (Service Layer): Expect RESOLUTION with isError: true
         // Cast to any
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         const result: any = await mcpClient.callTool({
            name: "malloy/executeQuery",
            arguments: params,
         });

         expect(result.isError).toBe(true);
         expect(result.content).toBeDefined();
         // Check the new structure: resource -> application/json
         const errorBlockModel = result.content![0];
         expect(errorBlockModel.type).toBe("resource");
         expect(errorBlockModel.resource).toBeDefined();
         expect(errorBlockModel.resource.type).toBe("application/json");

         // Parse the JSON string from the resource text content
         const errorJsonTextModel = errorBlockModel.resource.text as string;
         const errorPayloadModel = JSON.parse(errorJsonTextModel);

         // Check the parsed error object
         expect(errorPayloadModel).toBeDefined();
         expect(errorPayloadModel.error).toBeDefined();
         expect(errorPayloadModel.suggestions).toBeDefined();
         expect(Array.isArray(errorPayloadModel.suggestions)).toBe(true);
         expect(errorPayloadModel.suggestions.length).toBeGreaterThan(0);

         // Check the specific error message within the parsed object
         const expectedErrorMessageModel = `Resource not found: model '${params.modelPath}' in package '${params.packageName}' for project '${params.projectName}'`;
         expect(errorPayloadModel.error).toEqual(expectedErrorMessageModel);

         // Check for the specific model name and context in the message
         expect(errorPayloadModel.error).toMatch(/Resource not found/i);
      });

      // Added from mcp_query_tool.spec.ts
      it("should handle query cancellation via client close", async () => {
         if (!env) throw new Error("Test environment not initialized");

         // Create a new client *specifically* for this test so we can close it
         // without affecting other tests running concurrently (if any).
         const cancelClient = new Client<Request, Notification, Result>({
            name: "cancel-test-client",
            version: "1.0",
         });
         // Corrected: Use StreamableHTTPClientTransport with the server URL + /mcp endpoint
         const cancelTransport = new StreamableHTTPClientTransport(
            new URL(env.serverUrl + "/mcp"),
         );
         await cancelClient.connect(cancelTransport);

         expect.assertions(2); // Expecting two assertions: instanceof Error and message match
         let toolPromise;
         try {
            toolPromise = cancelClient.callTool({
               name: "malloy/executeQuery",
               arguments: {
                  projectName: PROJECT_NAME,
                  packageName: PACKAGE_NAME,
                  modelPath: "flights.malloy",
                  // Use a query known to take a little time if possible, otherwise a simple one
                  query: "run: flights->{aggregate: c is count() for 100}",
               },
            });

            // Give the request a moment to start on the server
            await new Promise((resolve) => setTimeout(resolve, 100));

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

      // Test invalid usage - nested view called without sourceName
      it("should return application error for nested view without sourceName", async () => {
         if (!env) throw new Error("Test environment not initialized");
         const params = {
            projectName: PROJECT_NAME,
            packageName: PACKAGE_NAME,
            modelPath: "flights.malloy",
            queryName: "top_carriers", // Nested view, but sourceName is missing
         };

         // Expect RESOLUTION with error because it's invalid usage processed by the handler
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         const result: any = await mcpClient.callTool({
            name: "malloy/executeQuery",
            arguments: params,
         });

         expect(result).toBeDefined();
         expect(result.isError).toBe(true); // Expecting error
         expect(result.content).toBeDefined();
         expect(Array.isArray(result.content)).toBe(true);
         expect(result.content.length).toBeGreaterThan(0);

         // Check the error structure: resource -> application/json
         const errorBlock = result.content![0];
         expect(errorBlock.type).toBe("resource");
         expect(errorBlock.resource).toBeDefined();
         expect(errorBlock.resource.type).toBe("application/json");

         // Check for Malloy error indicating the query/view wasn't found at the top level
         const errorJsonText = errorBlock.resource.text as string;
         const errorPayload = JSON.parse(errorJsonText);
         // Expect error about the query/view itself not found
         expect(errorPayload.error).toMatch(
            /Query 'top_carriers' not found|Reference to undefined object 'top_carriers'/i,
         );
         expect(Array.isArray(errorPayload.suggestions)).toBe(true);
      });
   });
});
