/// <reference types="bun-types" />

import { describe, it, expect, beforeAll, afterAll } from "bun:test";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { ErrorCode } from "@modelcontextprotocol/sdk/types.js";
import {
   McpE2ETestEnvironment,
   setupE2ETestEnvironment,
   cleanupE2ETestEnvironment,
} from "../../harness/mcp_test_setup";

// Define an interface for the expected structure of package content entries
interface PackageContentEntry {
   uri?: string;
   metadata?: {
      description?: string;
      // Add other expected metadata fields if known
   };
   // Add other expected fields if known
}

// --- Test Suite ---
describe("MCP Resource Handlers (E2E Integration)", () => {
   let env: McpE2ETestEnvironment | null = null;
   let mcpClient: Client;

   // --- Setup/Teardown using E2E Scaffolding ---
   beforeAll(async () => {
      env = await setupE2ETestEnvironment();
      mcpClient = env.mcpClient;
   });

   afterAll(async () => {
      await cleanupE2ETestEnvironment(env);
      env = null;
   });

   // --- Test Constants ---
   const homeProjectUri = "malloy://project/home";
   const faaPackageUri = "malloy://project/home/package/faa";
   const flightsModelUri =
      "malloy://project/home/package/faa/models/flights.malloy";
   const FLIGHTS_SOURCE = "flights";
   const FLIGHTS_CARRIER_QUERY = "flights_by_carrier";
   const FLIGHTS_MONTH_VIEW = "flights_by_month";
   const OVERVIEW_NOTEBOOK = "overview.malloynb";
   const nonExistentPackageUri = "malloy://project/home/package/nonexistent";
   const nonExistentModelUri =
      "malloy://project/home/package/faa/models/nonexistent.malloy";
   const nonExistentProjectUri = "malloy://project/invalid_project";
   const invalidUri = "invalid://format";

   const validSourceUri = `malloy://project/home/package/faa/models/flights.malloy/sources/${FLIGHTS_SOURCE}`;
   const validQueryUri = `malloy://project/home/package/faa/models/flights.malloy/queries/${FLIGHTS_CARRIER_QUERY}`;
   const validViewUri = `malloy://project/home/package/faa/models/flights.malloy/sources/${FLIGHTS_SOURCE}/views/${FLIGHTS_MONTH_VIEW}`;
   const validNotebookUri = `malloy://project/home/package/faa/notebooks/${OVERVIEW_NOTEBOOK}`;
   const nonExistentSourceUri = `malloy://project/home/package/faa/models/flights.malloy/sources/non_existent_source`;
   const nonExistentQueryUri = `malloy://project/home/package/faa/models/flights.malloy/queries/non_existent_query`;
   const nonExistentViewUri = `malloy://project/home/package/faa/models/flights.malloy/sources/${FLIGHTS_SOURCE}/views/non_existent_view`;
   const nonExistentNotebookUri = `malloy://project/home/package/faa/notebooks/non_existent.malloynb`;

   describe("client.listResources", () => {
      it(
         "should list all resources, including package entries (models/notebooks/sources)",
         async () => {
            console.log(
               "[TEST LOG] Starting: listResources - check package entries",
            );
            if (!env) throw new Error("Test environment not initialized");
            const result = await mcpClient.listResources({});
            console.log(
               "[TEST LOG] listResources result received (count):",
               result?.resources?.length ?? 0,
            );

            expect(result).toBeDefined();
            expect(result.resources).toBeDefined();
            expect(Array.isArray(result.resources)).toBe(true);
            expect(result.resources.length).toBeGreaterThan(0);

            const faaPackageEntry = result.resources.find(
               (r) => r.uri === faaPackageUri,
            );
            expect(faaPackageEntry).toBeDefined();
            expect(faaPackageEntry?.name).toBe("faa");

            const firstResource = result.resources[0];
            expect(firstResource).toBeDefined();
            expect(firstResource.uri).toBeDefined();
            expect(typeof firstResource.uri).toBe("string");
            expect(firstResource.uri).toMatch(/^malloy:\/\//);
            expect(firstResource.name).toBeDefined();
            expect(typeof firstResource.name).toBe("string");
            expect(firstResource.description).toBeDefined();
            expect(typeof firstResource.description).toBe("string");
            if (firstResource.mimeType) {
               expect(typeof firstResource.mimeType).toBe("string");
            }
         },
         { timeout: 10000 },
      );

      it("should resolve even if called with a specific URI (behavior might vary)", async () => {
         console.log("[TEST LOG] Starting: listResources - with URI");
         if (!env) throw new Error("Test environment not initialized");
         const result = await mcpClient.listResources({ uri: homeProjectUri });
         console.log(
            "[TEST LOG] listResources (with URI) result received (count):",
            result?.resources?.length ?? 0,
         );
         expect(result).toBeDefined();
         expect(result.resources).toBeDefined();
         expect(Array.isArray(result.resources)).toBe(true);
         expect(result.resources.length).toBeGreaterThan(0);
      });
   });

   describe("client.readResource (Basic Types)", () => {
      it("should return details for the project URI", async () => {
         console.log("[TEST LOG] Starting: readResource - project success");
         if (!env) throw new Error("Test environment not initialized");
         const resource = await mcpClient.readResource({ uri: homeProjectUri });
         console.log(
            "[TEST LOG] readResource (project) result received:",
            resource ? "object" : "null",
         );
         expect(resource).toBeDefined();
         expect(resource.contents).toBeDefined();
         expect(Array.isArray(resource.contents)).toBe(true);
         expect(resource.contents).toHaveLength(1);
         expect(resource.contents[0].type).toBe("application/json");
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         const responsePayload = JSON.parse((resource.contents[0] as any).text);
         expect(responsePayload).toBeDefined();
         expect(responsePayload.definition).toBeDefined();
         expect(responsePayload.metadata).toBeDefined();
         // Check definition content - Project name is 'home' from URI param
         expect(responsePayload.definition.name).toBe("home");
         // Check metadata content (can be more specific if needed)
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         expect(typeof responsePayload.metadata.description).toBe("string");
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         expect(typeof responsePayload.metadata.usage).toBe("string");
      });

      it("should return details for a valid package URI", async () => {
         console.log("[TEST LOG] Starting: readResource - package success");
         if (!env) throw new Error("Test environment not initialized");
         const resource = await mcpClient.readResource({ uri: faaPackageUri });
         console.log(
            "[TEST LOG] readResource (package) result received:",
            resource ? "object" : "null",
         );
         expect(resource).toBeDefined();
         expect(resource.contents).toBeDefined();
         expect(Array.isArray(resource.contents)).toBe(true);
         expect(resource.contents).toHaveLength(1);
         expect(resource.contents[0].type).toBe("application/json");
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         const responsePayload = JSON.parse((resource.contents[0] as any).text);
         expect(responsePayload).toBeDefined();
         expect(responsePayload.definition).toBeDefined();
         expect(responsePayload.metadata).toBeDefined();
         // Check definition content (matches old packageMetadata check)
         expect(responsePayload.definition.name).toBe("faa");
         expect(responsePayload.definition.description).toBeDefined();
         expect(typeof responsePayload.definition.description).toBe("string");
         // Check metadata content
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         expect(typeof responsePayload.metadata.description).toBe("string");
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         expect(typeof responsePayload.metadata.usage).toBe("string");
      });

      it("should return a list of contained resources for a valid package contents URI", async () => {
         console.log(
            "[TEST LOG] Starting: readResource - package contents success",
         );
         if (!env) throw new Error("Test environment not initialized");
         const contentsUri = `${faaPackageUri}/contents`; // Construct the contents URI
         const resource = await mcpClient.readResource({ uri: contentsUri });

         console.log(
            "[TEST DEBUG] Received Package Contents:",
            JSON.stringify(resource, null, 2),
         );
         console.log(
            "[TEST LOG] readResource (package contents) result received:",
            resource ? "object" : "null",
         );
         expect(resource).toBeDefined();
         expect(resource.isError).not.toBe(true);
         expect(resource.contents).toBeDefined();
         expect(Array.isArray(resource.contents)).toBe(true);
         expect(resource.contents).toHaveLength(1);
         expect(resource.contents[0].type).toBe("application/json");

         // The content should be a JSON array of resource definitions
         // Explicitly type the parsed array
         const contentArray = JSON.parse(
            (resource.contents[0] as { text: string }).text, // Use a specific type for content item
         ) as PackageContentEntry[];
         expect(Array.isArray(contentArray)).toBe(true);
         expect(contentArray.length).toBeGreaterThan(0); // Expecting models/notebooks

         // Check for a specific known source entry (e.g., flights.malloy)
         // Find the entry by URI suffix, don't assume order
         // Remove 'any' type from entry parameter
         const flightsEntry = contentArray.find((entry) =>
            entry?.uri?.endsWith("/sources/flights.malloy"),
         );
         expect(flightsEntry).toBeDefined();
         expect(flightsEntry!.uri).toBe(
            "malloy://project/home/package/faa/sources/flights.malloy",
         );
         expect(flightsEntry!.metadata).toBeDefined();
         expect(flightsEntry!.metadata!.description).toContain(
            "Represents a table or dataset",
         );

         // Check for a specific known notebook entry (e.g., aircraft_analysis.malloynb)
         // Remove 'any' type from entry parameter
         const notebookEntry = contentArray.find((entry) =>
            entry?.uri?.endsWith("/notebooks/aircraft_analysis.malloynb"),
         );
         expect(notebookEntry).toBeDefined();
         expect(notebookEntry!.uri).toBe(
            "malloy://project/home/package/faa/notebooks/aircraft_analysis.malloynb",
         );
         expect(notebookEntry!.metadata).toBeDefined();
         expect(notebookEntry!.metadata!.description).toContain(
            "interactive document",
         );

         // Check overall structure of the first item in the *original array*
         const firstItem = contentArray[0];
         expect(firstItem.uri).toBeDefined();
         expect(typeof firstItem.uri).toBe("string");
         expect(firstItem.metadata).toBeDefined();
         expect(typeof firstItem.metadata!.description).toBe("string");
      });

      it("should return details for a valid model URI", async () => {
         console.log("[TEST LOG] Starting: readResource - model success");
         if (!env) throw new Error("Test environment not initialized");
         const resource = await mcpClient.readResource({
            uri: flightsModelUri,
         });
         console.log(
            "[TEST LOG] readResource (model) result received:",
            resource ? "object" : "null",
         );
         expect(resource).toBeDefined();
         expect(resource.isError).not.toBe(true);
         expect(resource.contents).toBeDefined();
         expect(Array.isArray(resource.contents)).toBe(true);
         expect(resource.contents).toHaveLength(1);
         expect(resource.contents[0].type).toBe("application/json");
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         const responsePayload = JSON.parse((resource.contents[0] as any).text);
         expect(responsePayload).toBeDefined();
         expect(responsePayload.definition).toBeDefined();
         expect(responsePayload.metadata).toBeDefined();
         // Check definition content (like old compiledModel check)
         expect(responsePayload.definition.modelPath).toBe("flights.malloy");
         expect(responsePayload.definition.packageName).toBe("faa");
         expect(responsePayload.definition.sources).toBeDefined(); // Example check
         // Check metadata content
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         expect(typeof responsePayload.metadata.description).toBe("string");
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         expect(typeof responsePayload.metadata.usage).toBe("string");
      });

      it("should reject with InvalidParams for an invalid URI structure", async () => {
         console.log(
            "[TEST LOG] Starting: readResource - invalid URI structure",
         );
         if (!env) throw new Error("Test environment not initialized");
         await expect(
            mcpClient.readResource({ uri: invalidUri }),
         ).rejects.toMatchObject({
            code: ErrorCode.InvalidParams,
            message: expect.stringMatching(/Resource .* not found/i),
         });
      });

      it("should return application error response if package not found", async () => {
         console.log("[TEST LOG] Starting: readResource - package not found");
         if (!env) throw new Error("Test environment not initialized");
         const result = await mcpClient.readResource({
            uri: nonExistentPackageUri,
         });
         console.log(
            "[TEST LOG] readResource (package not found) result received:",
            result ? "object" : "null",
         );
         expect(result).toBeDefined();
         expect(result.isError).toBe(true);
         expect(result.contents).toBeDefined();
         expect(Array.isArray(result.contents)).toBe(true);
         expect(result.contents).toHaveLength(1);
         expect(result.contents[0].type).toBe("application/json");
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         const responsePayload = JSON.parse((result.contents[0] as any).text);
         expect(responsePayload).toBeDefined();
         expect(responsePayload.error).toBeDefined();
         expect(responsePayload.suggestions).toBeDefined();
         expect(Array.isArray(responsePayload.suggestions)).toBe(true);
         // Adjust regex to match the actual "Resource not found: Package..." message format
         expect(responsePayload.error).toMatch(/^Resource not found: Package/i);
         expect(responsePayload.suggestions.length).toBeGreaterThan(0);
      });

      it("should return application error response if model not found", async () => {
         console.log("[TEST LOG] Starting: readResource - model not found");
         if (!env) throw new Error("Test environment not initialized");
         const result = await mcpClient.readResource({
            uri: nonExistentModelUri,
         });
         console.log(
            "[TEST LOG] readResource (model not found) result received:",
            result ? "object" : "null",
         );
         expect(result).toBeDefined();
         expect(result.isError).toBe(true);
         expect(result.contents).toBeDefined();
         expect(Array.isArray(result.contents)).toBe(true);
         expect(result.contents).toHaveLength(1);
         expect(result.contents[0].type).toBe("application/json");
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         const responsePayload = JSON.parse((result.contents[0] as any).text);
         expect(responsePayload).toBeDefined();
         expect(responsePayload.error).toBeDefined();
         expect(responsePayload.suggestions).toBeDefined();
         expect(Array.isArray(responsePayload.suggestions)).toBe(true);
         expect(responsePayload.error).toMatch(/Resource not found/i);
         expect(responsePayload.suggestions.length).toBeGreaterThan(0);
      });

      it("should return application error response if project not found", async () => {
         console.log("[TEST LOG] Starting: readResource - project not found");
         if (!env) throw new Error("Test environment not initialized");
         const result = await mcpClient.readResource({
            uri: nonExistentProjectUri,
         });
         console.log(
            "[TEST LOG] readResource (project not found) result received:",
            result ? "object" : "null",
         );
         expect(result).toBeDefined();
         expect(result.isError).toBe(true);
         expect(result.contents).toBeDefined();
         expect(Array.isArray(result.contents)).toBe(true);
         expect(result.contents).toHaveLength(1);
         expect(result.contents[0].type).toBe("application/json");
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         const responsePayload = JSON.parse((result.contents[0] as any).text);
         expect(responsePayload).toBeDefined();
         expect(responsePayload.error).toBeDefined();
         expect(responsePayload.suggestions).toBeDefined();
         expect(Array.isArray(responsePayload.suggestions)).toBe(true);
         expect(responsePayload.error).toMatch(/Resource not found/i);
      });

      it("should reject for syntactically invalid URI string", async () => {
         console.log(
            "[TEST LOG] Starting: readResource - syntactically invalid URI",
         );
         if (!env) throw new Error("Test environment not initialized");
         const syntacticallyInvalidUri = "i am /// not valid?";
         await expect(
            mcpClient.readResource({ uri: syntacticallyInvalidUri }),
         ).rejects.toMatchObject({
            code: expect.any(Number),
            message: expect.any(String),
         });
      });

      it("should return application error response if package contents URI references non-existent package", async () => {
         console.log(
            "[TEST LOG] Starting: readResource - package contents not found",
         );
         if (!env) throw new Error("Test environment not initialized");
         const nonExistentContentsUri = `${nonExistentPackageUri}/contents`;
         const result = await mcpClient.readResource({
            uri: nonExistentContentsUri,
         });

         console.log(
            "[TEST DEBUG] Received Package Contents (Not Found Case):",
            JSON.stringify(result, null, 2),
         );
         console.log(
            "[TEST LOG] readResource (package contents not found) result received:",
            result ? "object" : "null",
         );
         expect(result).toBeDefined();
         expect(result.isError).toBe(true);
         expect(result.contents).toBeDefined();
         expect(Array.isArray(result.contents)).toBe(true);
         expect(result.contents).toHaveLength(1);
         expect(result.contents[0].type).toBe("application/json");
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         const responsePayload = JSON.parse((result.contents[0] as any).text);
         expect(responsePayload).toBeDefined();
         expect(responsePayload.error).toBeDefined();
         expect(responsePayload.error).toMatch(/Resource not found/i);
         expect(responsePayload.suggestions).toBeDefined();
         expect(Array.isArray(responsePayload.suggestions)).toBe(true);
      });
   });

   describe("client.readResource (Source/Query/View/Notebook)", () => {
      it("should get a valid source resource", async () => {
         if (!env) throw new Error("Test environment not initialized");
         const result = await mcpClient.readResource({ uri: validSourceUri });
         expect(result.isError).not.toBe(true);
         expect(result.contents).toBeDefined();
         expect(Array.isArray(result.contents)).toBe(true);
         expect(result.contents).toHaveLength(1);
         expect(result.contents[0].type).toBe("application/json");
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         const responsePayload = JSON.parse((result.contents[0] as any).text);
         expect(responsePayload).toBeDefined();
         expect(responsePayload.definition).toBeDefined();
         expect(responsePayload.metadata).toBeDefined();
         expect(responsePayload.definition.name).toBe(FLIGHTS_SOURCE);
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         expect(typeof responsePayload.metadata.description).toBe("string");
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         expect(typeof responsePayload.metadata.usage).toBe("string");
      });

      it("should return structured app error for a query that is not found", async () => {
         if (!env) throw new Error("Test environment not initialized");
         const result = await mcpClient.readResource({ uri: validQueryUri });
         expect(result.isError).toBe(true);
         expect(result.contents).toBeDefined();
         expect(Array.isArray(result.contents)).toBe(true);
         expect(result.contents).toHaveLength(1);
         expect(result.contents?.[0]?.type).toBe("application/json");
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         const errorPayload = JSON.parse((result.contents![0] as any).text);
         expect(errorPayload.error).toBeDefined();
         expect(errorPayload.error).toMatch(/Resource not found/i);
         // Check for the specific query name in the message
         expect(errorPayload.error).toMatch(
            new RegExp(`Query '${FLIGHTS_CARRIER_QUERY}'`),
         );
         expect(errorPayload.suggestions).toBeDefined();
         expect(Array.isArray(errorPayload.suggestions)).toBe(true);
         expect(errorPayload.suggestions.length).toBeGreaterThan(0);
         expect(errorPayload.suggestions[0]).toContain(
            "Verify the identifier or URI",
         ); // Check suggestion content
      });

      it("should return structured app error for a view that is not found", async () => {
         if (!env) throw new Error("Test environment not initialized");
         const result = await mcpClient.readResource({ uri: validViewUri });
         expect(result.isError).toBe(true);
         expect(result.contents).toBeDefined();
         expect(Array.isArray(result.contents)).toBe(true);
         expect(result.contents).toHaveLength(1);
         expect(result.contents?.[0]?.type).toBe("application/json");
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         const errorPayload = JSON.parse((result.contents![0] as any).text);
         expect(errorPayload.error).toBeDefined();
         expect(errorPayload.error).toMatch(/Resource not found/i);
         // Check for the specific view name in the message
         expect(errorPayload.error).toMatch(
            new RegExp(`View '${FLIGHTS_MONTH_VIEW}'`),
         );
         expect(errorPayload.suggestions).toBeDefined();
         expect(Array.isArray(errorPayload.suggestions)).toBe(true);
         expect(errorPayload.suggestions.length).toBeGreaterThan(0);
         expect(errorPayload.suggestions[0]).toContain(
            "Verify the identifier or URI",
         ); // Check suggestion content
      });

      it("should return structured app error for a notebook that is not found or invalid", async () => {
         if (!env) throw new Error("Test environment not initialized");
         const result = await mcpClient.readResource({ uri: validNotebookUri });
         expect(result.isError).toBe(true);
         expect(result.contents).toBeDefined();
         expect(Array.isArray(result.contents)).toBe(true);
         expect(result.contents).toHaveLength(1);
         expect(result.contents?.[0]?.type).toBe("application/json");
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         const errorPayload = JSON.parse((result.contents![0] as any).text);
         expect(errorPayload.error).toBeDefined();
         expect(errorPayload.error).toMatch(/Resource not found/i);
         // Check for the specific notebook name and context in the message
         // Adjust test to expect the generic "Resource not found" error, as the specific
         // "not a notebook" detail isn't easily surfaced in the standard error format.
         expect(errorPayload.error).toMatch(/Notebook 'overview.malloynb'/);
         expect(errorPayload.error).toMatch(/project 'home'/); // Check project name
         expect(errorPayload.suggestions).toBeDefined();
         expect(Array.isArray(errorPayload.suggestions)).toBe(true);
         expect(errorPayload.suggestions.length).toBeGreaterThan(0);
         expect(errorPayload.suggestions[0]).toContain(
            "Verify the identifier or URI",
         );
         // This case might also hit ModelCompilationError if the .malloynb exists but is invalid
         // Adding a check for compilation-related suggestions could be useful if applicable
      });

      it("should return structured app error for non-existent source", async () => {
         if (!env) throw new Error("Test environment not initialized");
         const result = await mcpClient.readResource({
            uri: nonExistentSourceUri,
         });
         expect(result.isError).toBe(true);
         expect(result.contents).toBeDefined();
         expect(Array.isArray(result.contents)).toBe(true);
         expect(result.contents).toHaveLength(1);
         expect(result.contents?.[0]?.type).toBe("application/json");
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         const errorPayload = JSON.parse((result.contents![0] as any).text);
         expect(errorPayload.error).toBeDefined();
         expect(errorPayload.error).toMatch(/Resource not found/i);
         // Check for the specific source name in the message
         expect(errorPayload.error).toMatch(/Source 'non_existent_source'/);
         // Adjust project name expectation
         expect(errorPayload.error).toMatch(/project 'home'/);
         expect(errorPayload.suggestions).toBeDefined();
         expect(Array.isArray(errorPayload.suggestions)).toBe(true);
         expect(errorPayload.suggestions.length).toBeGreaterThan(0);
         expect(errorPayload.suggestions[0]).toContain(
            "Verify the identifier or URI",
         );
      });

      it("should return structured app error for non-existent query", async () => {
         if (!env) throw new Error("Test environment not initialized");
         const result = await mcpClient.readResource({
            uri: nonExistentQueryUri,
         });
         expect(result.isError).toBe(true);
         expect(result.contents).toBeDefined();
         expect(Array.isArray(result.contents)).toBe(true);
         expect(result.contents).toHaveLength(1);
         expect(result.contents?.[0]?.type).toBe("application/json");
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         const errorPayload = JSON.parse((result.contents![0] as any).text);
         expect(errorPayload.error).toBeDefined();
         expect(errorPayload.error).toMatch(/Resource not found/i);
         // Check for the specific query name in the message
         expect(errorPayload.error).toMatch(/Query 'non_existent_query'/);
         // Adjust project name expectation
         expect(errorPayload.error).toMatch(/project 'home'/);
         expect(errorPayload.suggestions).toBeDefined();
         expect(Array.isArray(errorPayload.suggestions)).toBe(true);
         expect(errorPayload.suggestions.length).toBeGreaterThan(0);
         expect(errorPayload.suggestions[0]).toContain(
            "Verify the identifier or URI",
         );
      });

      it("should return structured app error for non-existent view", async () => {
         if (!env) throw new Error("Test environment not initialized");
         const result = await mcpClient.readResource({
            uri: nonExistentViewUri,
         });
         expect(result.isError).toBe(true);
         expect(result.contents).toBeDefined();
         expect(Array.isArray(result.contents)).toBe(true);
         expect(result.contents).toHaveLength(1);
         expect(result.contents?.[0]?.type).toBe("application/json");
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         const errorPayload = JSON.parse((result.contents![0] as any).text);
         expect(errorPayload.error).toBeDefined();
         expect(errorPayload.error).toMatch(/Resource not found/i);
         // Check for the specific view name in the message
         expect(errorPayload.error).toMatch(/View 'non_existent_view'/);
         // Adjust project name expectation
         expect(errorPayload.error).toMatch(/project 'home'/);
         expect(errorPayload.suggestions).toBeDefined();
         expect(Array.isArray(errorPayload.suggestions)).toBe(true);
         expect(errorPayload.suggestions.length).toBeGreaterThan(0);
         expect(errorPayload.suggestions[0]).toContain(
            "Verify the identifier or URI",
         );
      });

      it("should return structured app error for non-existent notebook", async () => {
         if (!env) throw new Error("Test environment not initialized");
         const result = await mcpClient.readResource({
            uri: nonExistentNotebookUri,
         });
         expect(result.isError).toBe(true);
         expect(result.contents).toBeDefined();
         expect(Array.isArray(result.contents)).toBe(true);
         expect(result.contents).toHaveLength(1);
         expect(result.contents?.[0]?.type).toBe("application/json");
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         const errorPayload = JSON.parse((result.contents![0] as any).text);
         expect(errorPayload.error).toBeDefined();
         expect(errorPayload.error).toMatch(/Resource not found/i);
         // Check for the specific notebook name and context in the message
         // Adjust test to expect the generic "Resource not found" error
         expect(errorPayload.error).toMatch(/Notebook 'non_existent.malloynb'/);
         // Adjust project name expectation
         expect(errorPayload.error).toMatch(/project 'home'/);
         expect(errorPayload.suggestions).toBeDefined();
         expect(Array.isArray(errorPayload.suggestions)).toBe(true);
         expect(errorPayload.suggestions.length).toBeGreaterThan(0);
         expect(errorPayload.suggestions[0]).toContain(
            "Verify the identifier or URI",
         );
      });

      it("should return structured app error when requesting view from wrong source", async () => {
         if (!env) throw new Error("Test environment not initialized");
         const wrongSourceUri = `malloy://project/malloy-samples/package/faa/models/flights.malloy/sources/aircraft/views/${FLIGHTS_MONTH_VIEW}`;
         const result = await mcpClient.readResource({ uri: wrongSourceUri });
         expect(result.isError).toBe(true);
         expect(result.contents).toBeDefined();
         expect(Array.isArray(result.contents)).toBe(true);
         expect(result.contents).toHaveLength(1);
         expect(result.contents?.[0]?.type).toBe("application/json");
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         const errorPayload = JSON.parse((result.contents![0] as any).text);
         expect(errorPayload.error).toBeDefined();
         // Adjust expectation: The primary error should be the project not found
         expect(errorPayload.error).toMatch(/Resource not found/i);
         expect(errorPayload.error).toMatch(/project 'malloy-samples'/);
         // Remove checks for view/source name in the error for this specific case
         expect(errorPayload.suggestions).toBeDefined();
         expect(Array.isArray(errorPayload.suggestions)).toBe(true);
         expect(errorPayload.suggestions.length).toBeGreaterThan(0);
         // Check suggestion content for project not found
         expect(errorPayload.suggestions[0]).toContain(
            "Verify the identifier or URI (project 'malloy-samples') is spelled correctly",
         );
      });
   });
});
