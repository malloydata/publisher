/// <reference types="bun-types" />

// @ts-ignore - bun:test types
import { describe, it, expect, beforeAll, afterAll } from "bun:test";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { ErrorCode } from "@modelcontextprotocol/sdk/types.js";

// --- Import E2E Test Setup ---
import { McpE2ETestEnvironment, setupE2ETestEnvironment, cleanupE2ETestEnvironment } from "./mcp_test_setup";

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

    const homeProjectUri = 'malloy://project/home';
    const faaPackageUri = 'malloy://project/home/package/faa';
    const flightsModelUri = 'malloy://project/home/package/faa/models/flights.malloy';
    const nonExistentPackageUri = 'malloy://project/home/package/nonexistent';
    const nonExistentModelUri = 'malloy://project/home/package/faa/models/nonexistent.malloy';
    const invalidUri = 'invalid://format';

    describe("client.listResources", () => {
        it("should successfully list all available resources", async () => {
            if (!env) throw new Error("Test environment not initialized");
            const result = await mcpClient.listResources();
            expect(result).toBeDefined();
            expect(result.resources).toBeDefined();
            expect(Array.isArray(result.resources)).toBe(true);
        });

        // Added Test
        it("should resolve for syntactically invalid URI string", async () => {
            if (!env) throw new Error("Test environment not initialized");
            const syntacticallyInvalidUri = "i am /// not valid?";
            const result = await mcpClient.listResources({ uri: syntacticallyInvalidUri });
            expect(result).toBeDefined();
            expect(result.resources).toBeDefined();
            expect(Array.isArray(result.resources)).toBe(true);
        });
    });

    describe("client.readResource", () => {
        it("should return details for the project URI", async () => {
            if (!env) throw new Error("Test environment not initialized");
            const resource = await mcpClient.readResource({ uri: homeProjectUri });
            expect(resource).toBeDefined();
            expect(resource.contents).toBeDefined();
            expect(Array.isArray(resource.contents)).toBe(true);
            expect(resource.contents.length).toBeGreaterThan(0);
            expect(resource.contents[0].type).toBe('text');
            expect(resource.contents[0].text).toBe('Project: home');
        });

        it("should return details for a valid package URI", async () => {
            if (!env) throw new Error("Test environment not initialized");
            const resource = await mcpClient.readResource({ uri: faaPackageUri });
            expect(resource).toBeDefined();
            expect(resource.contents).toBeDefined();
            expect(Array.isArray(resource.contents)).toBe(true);
            expect(resource.contents.length).toBeGreaterThan(0);
            expect(resource.contents[0].text).toBe('Package: faa');
        });

        it("should return details for a valid model URI", async () => {
            if (!env) throw new Error("Test environment not initialized");
            const resource = await mcpClient.readResource({ uri: flightsModelUri });
            expect(resource).toBeDefined();
            expect(resource.contents).toBeDefined();
            expect(Array.isArray(resource.contents)).toBe(true);
            expect(resource.contents).toHaveLength(1);
            expect(resource.contents[0].type).toBe('application/json'); 
            const compiledModel = JSON.parse((resource.contents[0] as any).text);
            expect(compiledModel).toBeDefined();
            expect(compiledModel.modelPath).toBe('flights.malloy');
            expect(compiledModel.packageName).toBe('faa');
        });

        it("should reject with InvalidParams for an invalid URI structure", async () => {
            if (!env) throw new Error("Test environment not initialized");
            // Protocol Error: Expect REJECTION because the URI doesn't match any known resource/template
            await expect(mcpClient.readResource({ uri: invalidUri }))
                .rejects.toMatchObject({
                    code: ErrorCode.InvalidParams,
                    // Update to expect the actual "Resource not found" message from the SDK handler
                    message: expect.stringMatching(/Resource .* not found/i) 
                });
        });

        it("should return application error response if package not found", async () => {
            if (!env) throw new Error("Test environment not initialized");
            // Application Error: Expect RESOLUTION with isError: true and error in contents
            const result = await mcpClient.readResource({ uri: nonExistentPackageUri });
            expect(result).toBeDefined();
            expect(result.isError).toBe(true); // Expect application error flag
            // Check the contents for the JSON error message
            expect(result.contents).toBeDefined();
            expect(Array.isArray(result.contents)).toBe(true);
            expect(result.contents).toHaveLength(1);
            // Assert type is application/json now
            expect(result.contents[0].type).toBe('application/json');
            // Safely access text after checking type
            const errorPayload = JSON.parse((result.contents[0] as any).text);
            expect(errorPayload).toBeDefined();
            expect(errorPayload.error).toBeDefined();
            // Check the specific error message format
            expect(errorPayload.error).toMatch(/Package manifest.*nonexistent.*does not exist/i); 
        });

        it("should return application error response if model not found", async () => {
            if (!env) throw new Error("Test environment not initialized");
             // Application Error: Expect RESOLUTION with isError: true
            const result = await mcpClient.readResource({ uri: nonExistentModelUri });
             expect(result).toBeDefined();
            expect(result.isError).toBe(true);
            // Check the contents for the JSON error message
            expect(result.contents).toBeDefined();
            expect(Array.isArray(result.contents)).toBe(true);
            expect(result.contents).toHaveLength(1);
            expect(result.contents[0].type).toBe('application/json');
            // Safely access text after checking type
            const errorPayload = JSON.parse((result.contents[0] as any).text);
            expect(errorPayload).toBeDefined();
            expect(errorPayload.error).toBeDefined();
            expect(errorPayload.error).toMatch(/Model.*nonexistent.malloy.*not found/i); 
        });

        // Added Test
        it("should return application error response if project not found", async () => {
            if (!env) throw new Error("Test environment not initialized");
            const nonExistentProjectUri = "malloy://project/invalid_project"; 
            // Application Error: Expect RESOLUTION with isError: true and error in contents
            const result = await mcpClient.readResource({ uri: nonExistentProjectUri });
            expect(result).toBeDefined();
            expect(result.isError).toBe(true); // Expect application error flag
            // Check the contents for the JSON error message
            expect(result.contents).toBeDefined();
            expect(Array.isArray(result.contents)).toBe(true);
            expect(result.contents).toHaveLength(1);
            expect(result.contents[0].type).toBe('application/json');
            // Safely access text after checking type
            const errorPayload = JSON.parse((result.contents[0] as any).text);
            expect(errorPayload).toBeDefined();
            expect(errorPayload.error).toBeDefined();
            // Check the specific error message format from MCP_ERROR_MESSAGES.PROJECT_NOT_FOUND
            expect(errorPayload.error).toMatch(/Project.*invalid_project.*not available/i); 
        });

        // Added Test
        it("should reject for syntactically invalid URI string", async () => {
            if (!env) throw new Error("Test environment not initialized");
            const syntacticallyInvalidUri = "i am /// not valid?";
            // readResource should fail at new URL() parsing
            await expect(mcpClient.readResource({ uri: syntacticallyInvalidUri }))
                .rejects.toMatchObject({
                    // Expect a protocol error, likely InvalidParams or InternalError
                    code: expect.any(Number), // Check for any error code for now
                    message: expect.any(String) // Check for any message
                });
        });
    });

    // --- Tests for Package Models Listing Resource ---
    describe("client.readResource (for packageModels)", () => {
        const faaModelsListUri = 'malloy://project/home/package/faa/models'; // URI to list models in faa
        const nonExistentPkgModelsListUri = 'malloy://project/home/package/nonexistent/models';

        it("should return list of models for a valid package", async () => {
            if (!env) throw new Error("Test environment not initialized");
            const result = await mcpClient.readResource({ uri: faaModelsListUri });
            
            expect(result).toBeDefined();
            expect(result.isError).not.toBe(true); // Should not be an error
            expect(result.contents).toBeDefined();
            expect(Array.isArray(result.contents)).toBe(true);
            expect(result.contents.length).toBeGreaterThan(0); // Expecting at least one model

            // Check the first model entry
            const firstModelContent = result.contents[0];
            expect(firstModelContent.type).toBe('application/json');
            const modelInfo = JSON.parse((firstModelContent as any).text);
            expect(modelInfo).toBeDefined();
            expect(modelInfo.path).toBeDefined(); // e.g., flights.malloy
            expect(modelInfo.type).toBeDefined(); // e.g., source or notebook
            expect(modelInfo.name).toBe(modelInfo.path);
            expect(modelInfo.description).toContain(modelInfo.path);
             // Check that the URI in the content matches a specific model URI format
            expect(firstModelContent.uri).toMatch(/malloy:\/\/project\/home\/package\/faa\/models\/.+/);
        });

        it("should return application error if package not found when listing models", async () => {
            if (!env) throw new Error("Test environment not initialized");
            const result = await mcpClient.readResource({ uri: nonExistentPkgModelsListUri });

            expect(result).toBeDefined();
            expect(result.isError).toBe(true); // Expect application error flag
            expect(result.contents).toBeDefined();
            expect(Array.isArray(result.contents)).toBe(true);
            expect(result.contents).toHaveLength(1);
            expect(result.contents[0].type).toBe('application/json');
            const errorPayload = JSON.parse((result.contents[0] as any).text);
            expect(errorPayload).toBeDefined();
            expect(errorPayload.error).toBeDefined();
            expect(errorPayload.error).toMatch(/Package manifest.*nonexistent.*does not exist/i); 
        });
    });

}); 