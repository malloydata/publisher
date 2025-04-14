import { describe, it, expect, beforeAll, afterAll, mock, beforeEach } from "bun:test";
import { Server } from "http";
import { app, httpServer } from "../server";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { SSEClientTransport } from '@modelcontextprotocol/sdk/client/sse.js';
import { Package } from "../service/package";
import { Model } from "../service/model";
import { PackageNotFoundError, ModelNotFoundError } from "../errors";
import { ErrorCode } from "@modelcontextprotocol/sdk/types.js";

// --- Mocking Setup ---
const mockListModels = mock(() => Promise.resolve([{ path: 'models/flights.malloy' }]));
const mockGetModel = mock(() => new Model('models/flights.malloy'));
const mockGetPackageMetadata = mock(() => Promise.resolve({ name: 'faa', description: 'FAA Dataset' }));
const mockGetPackage = mock(() => Promise.resolve(new Package('faa')));
const mockListPackages = mock(() => Promise.resolve([{ name: 'faa', description: 'FAA Dataset' }]));

mock.module("../service/package.service", () => ({
    PackageService: mock(() => ({
        getPackage: mockGetPackage,
        listPackages: mockListPackages
    })),
}));

describe("MCP Resource Handlers (Integration - SDK Client)", () => {
    let serverInstance: Server;
    let serverUrl: string;
    let mcpClient: Client;
    let transport: SSEClientTransport;

    beforeAll(async () => {
        // Start the server
        serverInstance = httpServer.listen(0);
        const address = serverInstance.address();
        if (!address || typeof address === 'string') {
            throw new Error("Invalid server address");
        }
        serverUrl = `http://localhost:${address.port}`;
        console.log(`Test server running on ${serverUrl}`);

        // Create and connect client
        mcpClient = new Client({ name: "test-client", version: "1.0" });
        transport = new SSEClientTransport(new URL(`${serverUrl}/api/v0/mcp/sse`));
        await mcpClient.connect(transport);
        console.log("MCP client connected");
    });

    beforeEach(() => {
        // Reset mocks
        mockListModels.mockClear();
        mockGetModel.mockClear();
        mockGetPackageMetadata.mockClear();
        mockGetPackage.mockClear();
        mockListPackages.mockClear();
    });

    afterAll(async () => {
        if (mcpClient) {
            await mcpClient.close();
        }
        if (serverInstance?.listening) {
            await new Promise<void>((resolve, reject) => {
                serverInstance.close((err) => err ? reject(err) : resolve());
            });
        }
    });

    describe("client.listResources", () => {
        const homeProjectUri = 'malloy://project/home';
        const faaPackageUri = 'malloy://project/home/package/faa';
        const flightsModelUri = 'malloy://project/home/package/faa/models/flights.malloy';

        it("should list packages when no URI is provided", async () => {
            const result = await mcpClient.listResources();
            expect(result.resources).toBeDefined();
            expect(result.resources).toHaveLength(2); // Project + 1 package
            expect(result.resources[0]).toMatchObject({
                uri: homeProjectUri,
                name: 'home'
            });
            expect(result.resources[1]).toMatchObject({
                uri: faaPackageUri,
                name: 'faa',
                description: 'FAA Dataset'
            });
        });

        it("should list models when a package URI is provided", async () => {
            const result = await mcpClient.listResources({ uri: faaPackageUri });
            expect(result.resources).toBeDefined();
            expect(result.resources).toHaveLength(1);
            expect(result.resources[0]).toMatchObject({
                uri: flightsModelUri,
                name: 'models/flights.malloy'
            });
        });

        it("should return empty list when a model URI is provided", async () => {
            const result = await mcpClient.listResources({ uri: flightsModelUri });
            expect(result.resources).toBeDefined();
            expect(result.resources).toHaveLength(0);
        });

        it("should reject with InvalidParams for an unsupported URI", async () => {
            const invalidUri = 'malloy://invalid/uri';
            await expect(mcpClient.listResources({ uri: invalidUri }))
                .rejects.toMatchObject({
                    code: ErrorCode.InvalidParams,
                    message: expect.stringContaining("Invalid URI format")
                });
        });

        it("should resolve with error response if package not found", async () => {
            const packageUri = 'malloy://project/home/package/nonexistent';
            mockGetPackage.mockRejectedValue(new PackageNotFoundError("Package not found"));

            const result = await mcpClient.listResources({ uri: packageUri });
            expect(result.isError).toBe(true);
            expect(result.error).toMatchObject({
                message: expect.stringContaining("Package not found")
            });
        });

        it("should reject with InternalError if listModels throws unexpected error", async () => {
            const packageUri = 'malloy://project/home/package/faa';
            mockListModels.mockRejectedValue(new Error("Failed to list models"));

            await expect(mcpClient.listResources({ uri: packageUri }))
                .rejects.toMatchObject({
                    code: ErrorCode.InternalError,
                    message: expect.stringContaining("Failed to list models")
                });
        });
    });

    describe("client.readResource", () => {
        const homeProjectUri = 'malloy://project/home';
        const faaPackageUri = 'malloy://project/home/package/faa';
        const flightsModelUri = 'malloy://project/home/package/faa/models/flights.malloy';
        const modelPath = 'models/flights.malloy';

        it("should return details for the project URI", async () => {
            const resource = await mcpClient.readResource({ uri: homeProjectUri });
            expect(resource).toBeDefined();
            expect(resource.contents).toBeDefined();
            expect(resource.contents[0].text).toContain('Malloy Project Resource');
        });

        it("should return details for a valid package URI", async () => {
            const resource = await mcpClient.readResource({ uri: faaPackageUri });
            expect(resource).toBeDefined();
            expect(mockGetPackage).toHaveBeenCalledWith('faa');
            expect(mockListModels).toHaveBeenCalled();
        });

        it("should return details for a valid model URI", async () => {
            const resource = await mcpClient.readResource({ uri: flightsModelUri });
            expect(resource).toBeDefined();
            expect(mockGetPackage).toHaveBeenCalledWith('faa');
            expect(mockGetModel).toHaveBeenCalledWith(modelPath);
        });

        it("should reject with InvalidParams for an invalid URI structure", async () => {
            const invalidUri = 'malloy://invalid/uri';
            await expect(mcpClient.readResource({ uri: invalidUri }))
                .rejects.toMatchObject({
                    code: ErrorCode.InvalidParams,
                    message: expect.stringContaining("Invalid URI format")
                });
        });

        it("should resolve with error response if package not found", async () => {
            const packageUri = 'malloy://project/home/package/nonexistent';
            mockGetPackage.mockRejectedValue(new PackageNotFoundError("Package not found"));

            const result = await mcpClient.readResource({ uri: packageUri });
            expect(result.isError).toBe(true);
            expect(result.error).toMatchObject({
                message: expect.stringContaining("Package not found")
            });
        });

        it("should resolve with error response if model not found", async () => {
            const modelUri = 'malloy://project/home/package/faa/models/nonexistent.malloy';
            mockGetModel.mockReturnValue(null);

            const result = await mcpClient.readResource({ uri: modelUri });
            expect(result.isError).toBe(true);
            expect(result.error).toMatchObject({
                message: expect.stringContaining("Model not found")
            });
        });
    });
}); 