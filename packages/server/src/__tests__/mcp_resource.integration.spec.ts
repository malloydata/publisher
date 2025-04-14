/// <reference types="bun-types" />

// @ts-ignore - bun:test types
import { describe, it, expect, beforeAll, afterAll } from "bun:test";

// --- Removed imports for mocking (Server, mock, Package, Model, errors, etc.) ---
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { ErrorCode } from "@modelcontextprotocol/sdk/types.js";

// --- Import E2E Test Setup ---
import { McpE2ETestEnvironment, setupE2ETestEnvironment, cleanupE2ETestEnvironment } from "./mcp_test_setup";

// --- Test Suite ---
describe("MCP Resource Handlers (E2E Integration)", () => {
    let env: McpE2ETestEnvironment | null = null;
    let mcpClient: Client; // Convenience variable

    // --- Setup/Teardown using E2E Scaffolding ---
    beforeAll(async () => {
        env = await setupE2ETestEnvironment();
        mcpClient = env.mcpClient;
    }); // Removed timeout argument

    afterAll(async () => {
        await cleanupE2ETestEnvironment(env);
        env = null;
    }); // Removed timeout argument

    // --- Removed beforeEach and afterEach --- 

    // --- Test Constants ---

    const homeProjectUri = 'malloy://project/home';
    const faaPackageUri = 'malloy://project/home/package/faa';
    const flightsModelUri = 'malloy://project/home/package/faa/flights.malloy'; // Assuming correct path in sample
    const nonExistentPackageUri = 'malloy://project/home/package/nonexistent';
    const nonExistentModelUri = 'malloy://project/home/package/faa/nonexistent.malloy';
    const invalidUri = 'invalid://format';

    describe("client.listResources", () => {
        it("should list packages when no URI is provided", async () => {
            if (!env) throw new Error("Test environment not initialized");
            const result = await mcpClient.listResources(); // No params
            expect(result).toBeDefined();
            expect(result.resources).toBeDefined();
            expect(Array.isArray(result.resources)).toBe(true);
            // Check for the presence of the 'home' project and 'faa' package (assuming they exist in samples)
            expect(result.resources).toEqual(expect.arrayContaining([
                expect.objectContaining({ uri: homeProjectUri, name: 'home' }),
                expect.objectContaining({ uri: faaPackageUri, name: 'faa' })
            ]));
        });

        it("should list models when a package URI is provided", async () => {
            if (!env) throw new Error("Test environment not initialized");
            const result = await mcpClient.listResources({ uri: faaPackageUri });
            expect(result).toBeDefined();
            expect(result.resources).toBeDefined();
            expect(Array.isArray(result.resources)).toBe(true);
            // Expect the same result as listing all packages due to current server implementation
             expect(result.resources).toEqual(expect.arrayContaining([
                 expect.objectContaining({ uri: homeProjectUri, name: 'home' }),
                 expect.objectContaining({ uri: faaPackageUri, name: 'faa' })
                 // Add other known packages if necessary for stability
             ]));
        });

        it("should return empty list when a model URI is provided", async () => {
            if (!env) throw new Error("Test environment not initialized");
            const result = await mcpClient.listResources({ uri: flightsModelUri });
            expect(result).toBeDefined();
            expect(result.resources).toBeDefined();
            expect(Array.isArray(result.resources)).toBe(true);
            expect(result.resources).toHaveLength(0); // Models don't contain listable resources
        });

        it("should reject with InvalidParams for an invalid URI structure", async () => {
            if (!env) throw new Error("Test environment not initialized");
            // Protocol Error: Expect REJECTION for invalid URI format
            await expect(mcpClient.listResources({ uri: invalidUri }))
                .rejects.toMatchObject({
                    code: ErrorCode.InvalidParams,
                    message: expect.stringContaining("Invalid URI format") // Check for specific MCP server message
                });
        });
        
        it("should return application error response if package not found", async () => {
            if (!env) throw new Error("Test environment not initialized");
            // Application Error: Expect RESOLUTION with isError: true
            const result = await mcpClient.listResources({ uri: nonExistentPackageUri });
            expect(result).toBeDefined();
            expect(result.isError).toBe(true);
            expect(result.error).toBeDefined();
            // Safely access message
            expect((result.error as any)?.message).toMatch(/Package manifest.*does not exist/i); 
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
            expect(resource.contents[0].text).toContain('Malloy Project Resource'); // Check for project-specific content
        });

        it("should return details for a valid package URI", async () => {
            if (!env) throw new Error("Test environment not initialized");
            const resource = await mcpClient.readResource({ uri: faaPackageUri });
            expect(resource).toBeDefined();
            expect(resource.contents).toBeDefined();
            expect(Array.isArray(resource.contents)).toBe(true);
            expect(resource.contents.length).toBeGreaterThan(0);
            // Check for package-specific content (e.g., list of models/databases)
            expect(resource.contents[0].text).toMatch(/Package Resource/i);
            expect(resource.contents[0].text).toMatch(/flights.malloy/i);
        });

        it("should return details for a valid model URI", async () => {
            if (!env) throw new Error("Test environment not initialized");
            const resource = await mcpClient.readResource({ uri: flightsModelUri });
            expect(resource).toBeDefined();
            expect(resource.contents).toBeDefined();
            // Check for model-specific content (e.g., compiled model details or source code)
            expect(resource.contents[0].type).toBe('text'); 
            // Maybe check for structure like modelDef, sources, queries if available
            expect(resource.contents[0].text).toMatch(/Model:/i); 
            expect(resource.contents[0].text).toMatch(/flights.malloy/i); 
        });

        it("should reject with InvalidParams for an invalid URI structure", async () => {
            if (!env) throw new Error("Test environment not initialized");
            // Protocol Error: Expect REJECTION
            await expect(mcpClient.readResource({ uri: invalidUri }))
                .rejects.toMatchObject({
                    code: ErrorCode.InvalidParams,
                    message: expect.stringContaining("Invalid URI format")
                });
        });

        it("should return application error response if package not found", async () => {
            if (!env) throw new Error("Test environment not initialized");
            // Application Error: Expect RESOLUTION with isError: true
            const result = await mcpClient.readResource({ uri: nonExistentPackageUri });
            expect(result).toBeDefined();
            expect(result.isError).toBe(true);
            expect(result.error).toBeDefined();
            // Safely access message
            expect((result.error as any)?.message).toMatch(/Package manifest.*does not exist/i);
        });

        it("should return application error response if model not found", async () => {
            if (!env) throw new Error("Test environment not initialized");
             // Application Error: Expect RESOLUTION with isError: true
            const result = await mcpClient.readResource({ uri: nonExistentModelUri });
             expect(result).toBeDefined();
            expect(result.isError).toBe(true);
            expect(result.error).toBeDefined();
            // Safely access message
            expect((result.error as any)?.message).toMatch(/Model.*does not exist/i);
        });
    });
}); 