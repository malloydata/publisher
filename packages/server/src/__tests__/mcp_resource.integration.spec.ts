import { describe, it, expect, beforeAll, afterAll, mock, beforeEach } from "bun:test";
import express from "express";
import type { Request, Response, Express, NextFunction } from 'express'; 
import http from 'http';
import { AddressInfo } from 'net';
import { z } from "zod"; 
import { PackageService } from "../service/package.service"; 
import { initializeMcpServer, ResourceDescriptorSchema } from "../mcp/server";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { SSEServerTransport } from "@modelcontextprotocol/sdk/server/sse.js";
import { ErrorCode, JSONRPCError } from "@modelcontextprotocol/sdk/types.js";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { SSEClientTransport } from '@modelcontextprotocol/sdk/client/sse.js';
import { URL } from 'url'; 
import { Package } from "../service/package"; 
import { Model } from "../service/model"; 
import { PackageNotFoundError, ModelNotFoundError } from "../errors"; 

// --- Mocking Setup --- 
const mockListModels = mock<Package['listModels']>();
const mockGetModel = mock<Package['getModel']>();
const mockGetPackageMetadata = mock<Package['getPackageMetadata']>();
const mockGetPackage = mock<PackageService['getPackage']>();
const mockListPackages = mock<PackageService['listPackages']>();

mock.module("../service/package.service", () => ({
    PackageService: mock(() => ({
        getPackage: mockGetPackage,
        listPackages: mockListPackages
    })),
}));

// --- Type Definitions --- 
type ResourceDescriptor = z.infer<typeof ResourceDescriptorSchema>;

// --- Test Setup --- 
describe("MCP Resource Handlers (Integration - SDK Client)", () => {
  let testApp: Express;
  let testMcpServer: McpServer;
  let httpServer: http.Server;
  let serverUrl: string;
  let testPackageService: PackageService = new PackageService();
  let mcpClient: Client;
  let clientTransport: SSEClientTransport;

  beforeAll(async () => {
    // --- Server Setup ---
    testApp = express();
    testMcpServer = initializeMcpServer(testPackageService); // Uses mocked service 

    // Setup MCP routes without middleware
    let mcpTransport: SSEServerTransport | null = null;

    testApp.get('/api/v0/mcp/sse', async (req, res) => {
      if (mcpTransport) {
        await mcpTransport.close();
      }
      mcpTransport = new SSEServerTransport('/api/v0/mcp/messages', res);
      await testMcpServer.connect(mcpTransport);
    });

    testApp.post('/api/v0/mcp/messages', async (req, res) => {
      if (mcpTransport) {
        await mcpTransport.handlePostMessage(req, res);
      } else {
        res.status(400).send(JSON.stringify({ error: 'No active SSE connection' }));
      }
    });

    httpServer = http.createServer(testApp).listen(0, '127.0.0.1');
    await new Promise<void>(resolve => httpServer.once('listening', resolve));
    const address = httpServer.address() as AddressInfo;
    serverUrl = `http://127.0.0.1:${address.port}`;
    console.log(`Resource Test server listening on ${serverUrl}`);

    // --- Client Setup ---
    mcpClient = new Client({ name: "resource-test-client", version: "1.0" });
    clientTransport = new SSEClientTransport(new URL(`${serverUrl}/api/v0/mcp/sse`));
    console.log("Connecting MCP client...");
    await mcpClient.connect(clientTransport);
    console.log("MCP client connected.");
  });

  beforeEach(() => {
      // Reset mocks
      mockListModels.mockClear();
      mockGetModel.mockClear();
      mockGetPackageMetadata.mockClear();
      mockGetPackage.mockClear();
      mockListPackages.mockClear();

      const mockPackageInstance = {
           getModel: mockGetModel,
           listModels: mockListModels,
           getPackageMetadata: mockGetPackageMetadata
      } as any;
      mockGetPackage.mockResolvedValue(mockPackageInstance);
      // Default listPackages returns empty array
      mockListPackages.mockResolvedValue([]);
  });

  afterAll(async () => {
       console.log("Closing MCP client...");
      await mcpClient?.close(); 
      console.log("MCP client closed.");
      console.log("Closing MCP server instance...");
      await testMcpServer?.close();
      console.log("MCP server instance closed.");
      if (httpServer?.listening) {
         console.log("Closing HTTP server...");
         await new Promise<void>((resolve, reject) => {
             httpServer.close((err) => err ? reject(err) : resolve());
         });
         console.log("HTTP server closed.");
      }
  });

  // --- Test Suites --- 

  describe.only("client.listResources", () => {
      it("should list packages when no URI is provided", async () => {
          const mockPkgList = [{ name: 'faa', description: 'FAA Data' }];
          mockListPackages.mockResolvedValue(mockPkgList);

          const result = await mcpClient.listResources(); // No URI

          expect(result).toBeDefined();
          expect(result.resources).toBeDefined();
          expect(mockListPackages).toHaveBeenCalled();
          // Check against the mocked data structure
          expect(result.resources).toEqual([
             { 
                  uri: 'malloy://project/home/package/faa',
                  name: 'faa',
                  description: 'FAA Data'
              }
          ]);
      });

       it("should list models when a package URI is provided", async () => {
           const packageUri = 'malloy://project/home/package/faa';
           const mockModelList = [
               { path: 'models/flights.malloy', type: 'model' },
               { path: 'models/aircraft.malloy', type: 'model' }
            ];
           mockListModels.mockReturnValue(mockModelList as any);

           const result = await mcpClient.listResources({ uri: packageUri });

           expect(result).toBeDefined();
           expect(result.resources).toBeDefined();
           expect(mockGetPackage).toHaveBeenCalledWith('faa'); 
           expect(mockListModels).toHaveBeenCalled(); 
           // Check against the mocked model data structure
           expect(result.resources).toEqual([
               {
                    uri: 'malloy://project/home/package/faa/models/flights.malloy',
                    name: 'models/flights.malloy', 
                    description: undefined // Add description: undefined if schema includes it
               },
               {
                    uri: 'malloy://project/home/package/faa/models/aircraft.malloy',
                    name: 'models/aircraft.malloy',
                    description: undefined // Add description: undefined if schema includes it
               }
           ]);
           expect(result.resources.length).toBe(mockModelList.length);
       });

       it("should return empty list when a model URI is provided", async () => {
            const modelUri = 'malloy://project/home/package/faa/models/flights.malloy';
            const result = await mcpClient.listResources({ uri: modelUri });

            expect(result).toBeDefined();
            expect(result.resources).toEqual([]);
            // Ensure service methods weren't called unnecessarily
            expect(mockGetPackage).not.toHaveBeenCalled(); 
            expect(mockListModels).not.toHaveBeenCalled();
       });

       it("should reject with InvalidParams for an unsupported URI", async () => {
           const invalidUri = 'malloy://invalid/uri';
           await expect(mcpClient.listResources({ uri: invalidUri }))
               .rejects.toMatchObject({ code: ErrorCode.InvalidParams });
       });

       it("should reject if PackageService.listPackages throws", async () => {
           const testError = new Error("Failed to list packages");
           mockListPackages.mockRejectedValue(testError);
           await expect(mcpClient.listResources())
               .rejects.toMatchObject({ code: ErrorCode.InternalError, message: expect.stringContaining(testError.message) });
       });
        it("should reject if Package.listModels throws", async () => {
            const packageUri = 'malloy://project/home/package/faa';
            const testError = new Error("Failed to list models");
            mockListModels.mockImplementation(() => { throw testError; });
            await expect(mcpClient.listResources({ uri: packageUri }))
                .rejects.toMatchObject({ code: ErrorCode.InternalError, message: expect.stringContaining(testError.message) });
        });
  });

  describe("client.readResource", () => {
      const homeProjectUri = 'malloy://project/home';
      const faaPackageUri = 'malloy://project/home/package/faa';
      const flightsModelUri = 'malloy://project/home/package/faa/models/flights.malloy';
      const invalidUri = 'malloy://invalid/foo';

      it("should return details for the project URI", async () => {
          const resource = await mcpClient.readResource({ uri: homeProjectUri });
          expect(resource).toBeDefined();
          // Check content returned by the simple project handler
          expect(resource.contents).toBeDefined();
          expect(resource.contents[0].text).toContain('Malloy Project: home');
          expect(resource.contents[0].uri).toBe(homeProjectUri);
      });

      it("should return details for a valid package URI", async () => {
           const mockPkgMeta = { name: 'faa', description: 'FAA Package Data' };
           mockGetPackageMetadata.mockReturnValue(mockPkgMeta);

           const resource = await mcpClient.readResource({ uri: faaPackageUri });

           expect(resource).toBeDefined();
           expect(mockGetPackage).toHaveBeenCalledWith('faa');
           expect(mockGetPackageMetadata).toHaveBeenCalled();
           expect(resource.contents).toBeDefined();
           expect(resource.contents.length).toBeGreaterThan(0);
           const textContent = resource.contents[0].text;
           if (typeof textContent !== 'string') {
               throw new Error('Expected text content to be a string');
           }
           const parsedContent = JSON.parse(textContent);
           expect(parsedContent).toEqual(mockPkgMeta);
      });

       it("should return details for a valid model URI", async () => {
           const modelPath = 'models/flights.malloy';
           const mockModelInfo = { path: modelPath, type: 'model' };
           // Mock getModel to return an object with getModelType
           mockGetModel.mockReturnValue({ getModelType: () => 'model' } as any); 

           const resource = await mcpClient.readResource({ uri: flightsModelUri });

           expect(resource).toBeDefined();
           expect(mockGetPackage).toHaveBeenCalledWith('faa');
           expect(mockGetModel).toHaveBeenCalledWith(modelPath);
           expect(resource.contents).toBeDefined();
           expect(resource.contents.length).toBeGreaterThan(0);
           const textContent = resource.contents[0].text;
           if (typeof textContent !== 'string') {
               throw new Error('Expected text content to be a string');
           }
           const parsedContent = JSON.parse(textContent);
           expect(parsedContent).toEqual(mockModelInfo);
       });

      it("should reject with InvalidParams for an invalid URI structure", async () => {
         await expect(mcpClient.readResource({ uri: invalidUri }))
             .rejects.toMatchObject({ code: ErrorCode.InvalidParams });
      });

       it("should reject with InvalidParams if package not found", async () => {
           const testError = new PackageNotFoundError("Package not found");
           mockGetPackage.mockRejectedValue(testError);
           await expect(mcpClient.readResource({ uri: faaPackageUri }))
               .rejects.toMatchObject({ code: ErrorCode.InvalidParams });
       });

        it("should reject with InvalidParams if model not found", async () => {
            const testError = new ModelNotFoundError("Model not found");
            mockGetModel.mockImplementation(() => { throw testError; });
            await expect(mcpClient.readResource({ uri: flightsModelUri }))
                .rejects.toMatchObject({ code: ErrorCode.InvalidParams });
        });
  });

}); 