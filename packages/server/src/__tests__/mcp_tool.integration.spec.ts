import { describe, it, expect, beforeAll, afterAll, mock, spyOn, beforeEach } from "bun:test";

import express from "express";
import type { Express } from 'express'; 

import http from 'http';
import { AddressInfo } from 'net';
import * as path from 'path';

import { z } from "zod";
import { PackageService } from "../service/package.service";

// --- Add necessary imports for mocking ---
import { Package } from "../service/package"; 
import { Model } from "../service/model"; 
import { ModelCompilationError, PackageNotFoundError, ModelNotFoundError } from "../errors"; 
import { MalloyError, Result } from "@malloydata/malloy"; 
// --- End added imports --- 

import { initializeMcpServer, ExecuteQueryResultSchema } from "../mcp/server"; 
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { ErrorCode, JSONRPCError } from "@modelcontextprotocol/sdk/types.js";

import { SSEServerTransport } from "@modelcontextprotocol/sdk/server/sse.js";

// --- Import MCP Client SDK --- 
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { SSEClientTransport } from '@modelcontextprotocol/sdk/client/sse.js';
import { URL } from 'url'; // Needed for SSEClientTransport

// --- Mocking Setup ---
// Create mocks for the service methods we need to control
const mockGetQueryResults = mock<Model['getQueryResults']>();
const mockGetModel = mock<Package['getModel']>();
const mockGetPackage = mock<PackageService['getPackage']>();

// Mock the entire PackageService module
mock.module("../service/package.service", () => ({
    PackageService: mock(() => ({
        getPackage: mockGetPackage,
        // Add mocks for other methods if needed by other tests/setup
        listPackages: mock(async () => []) 
    })),
}));

// Keep the type guard if needed for asserting results
interface ToolResultWithContent {
    content: { type: string, text: string }[];
    [key: string]: any; 
}
function isToolResultWithContent(result: any): result is ToolResultWithContent {
    return result && typeof result === 'object' && Array.isArray(result.content) && result.content.length > 0 && result.content[0].type === 'text';
}

// --- Test Setup --- 
describe("MCP Tool Handlers (Integration - Mocked Service)", () => {
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
    // Initialize MCP server - it will use the mocked PackageService
    testMcpServer = initializeMcpServer(testPackageService); 

    // Setup REAL MCP routes using the REAL SSEServerTransport 
    // (Client connects to this, not a mocked transport)
    const mcpRouter = express.Router();
    const serverTransports: {[sessionId: string]: SSEServerTransport} = {}; // Server still needs to manage transports

    mcpRouter.get('/sse', async (req, res) => {
        try {
            const messagePath = '/api/v0/mcp/messages'; 
            const transport = new SSEServerTransport(messagePath, res);
            serverTransports[transport.sessionId] = transport;
            res.on("close", () => { delete serverTransports[transport.sessionId]; });
            await testMcpServer.connect(transport);
        } catch (error) {
            console.error("Test Server SSE Error:", error);
            if (!res.headersSent) res.status(500).send("SSE Error");
        }
    });
    mcpRouter.post('/messages', async (req, res) => {
        const sessionId = req.query.sessionId as string;
        const transport = serverTransports[sessionId];
        if (transport) {
            try { await transport.handlePostMessage(req, res); }
            catch (error) { 
                 console.error("Test Server POST Error:", error); 
                 if (!res.headersSent) res.status(500).send("POST Error");
             }
        } else { res.status(400).send("Invalid session"); }
    });
    testApp.use('/api/v0/mcp', mcpRouter);

    httpServer = http.createServer(testApp).listen(0, '127.0.0.1');
    await new Promise<void>(resolve => httpServer.once('listening', resolve));
    const address = httpServer.address() as AddressInfo;
    serverUrl = `http://127.0.0.1:${address.port}`;
    console.log(`Tool Test server listening on ${serverUrl}`);

    // --- Client Setup ---
    mcpClient = new Client({ name: "tool-test-client", version: "1.0" });
    clientTransport = new SSEClientTransport(new URL(`${serverUrl}/api/v0/mcp/sse`));
    console.log("Connecting MCP client...");
    await mcpClient.connect(clientTransport);
    console.log("MCP client connected.");
  });

  beforeEach(() => {
      // Reset mocks before each test
      mockGetQueryResults.mockClear();
      mockGetModel.mockClear();
      mockGetPackage.mockClear();

      // Default mock implementations (can be overridden in tests)
      mockGetModel.mockReturnValue({ getQueryResults: mockGetQueryResults } as any);
      mockGetPackage.mockResolvedValue({ getModel: mockGetModel } as any);
  });

  afterAll(async () => {
      console.log("Closing MCP client...");
      await mcpClient?.close(); // Close client first
      console.log("MCP client closed.");
      
      console.log("Closing MCP server instance...");
      await testMcpServer?.close(); // Close server instance
      console.log("MCP server instance closed.");
      
      // No need to clear local transports map manually

      if (httpServer?.listening) {
         console.log("Closing HTTP server...");
         await new Promise<void>((resolve, reject) => {
             httpServer.close((err) => err ? reject(err) : resolve());
         });
         console.log("HTTP server closed.");
      }
  });

  describe("malloy/executeQuery Tool", () => {
    it("should execute a valid ad-hoc query successfully", async () => {
      const params = {
        packageName: "faa",
        modelPath: "models/flights.malloy",
        query: "run: flights->{group_by: carrier_name, aggregate: flight_count is count()}" 
      };
      
      const mockQueryResult = { _queryResult: { /* fake data */ } } as Result;
      
      mockGetQueryResults.mockResolvedValue({ 
          queryResults: mockQueryResult, 
          modelDef: { /* fake */ }, 
          dataStyles: { /* fake */ } 
      });

      const result = await mcpClient.callTool({ name: "malloy/executeQuery", arguments: params });

      expect(result).toBeDefined();
      expect(mockGetPackage).toHaveBeenCalledWith(params.packageName);
      expect(mockGetModel).toHaveBeenCalledWith(params.modelPath);
      expect(mockGetQueryResults).toHaveBeenCalled();
      if (isToolResultWithContent(result)) { 
          expect(result.content[0].type).toBe('text');
          // Parse and check for essential properties
          const parsedResultText = JSON.parse(result.content[0].text);
          expect(parsedResultText).toHaveProperty('queryResult');
          expect(parsedResultText).toHaveProperty('modelDef');
          expect(parsedResultText).toHaveProperty('dataStyles');
          // Optionally, check if queryResult is a string (as it was stringified)
          expect(typeof parsedResultText.queryResult).toBe('string'); 
      } else {
          throw new Error("Tool call result missing expected content structure");
      }
    });

    it("should execute a valid named query successfully", async () => {
      const params = {
        packageName: "faa",
        modelPath: "models/flights.malloy",
        sourceName: "flights", 
        queryName: "carrier_analysis" 
      };
      
      const mockQueryResult = { _queryResult: { /* fake data */ } } as Result;
      
      mockGetQueryResults.mockResolvedValue({ 
          queryResults: mockQueryResult, 
          modelDef: { /* fake */ }, 
          dataStyles: { /* fake */ } 
      });
      
      const result = await mcpClient.callTool({ name: "malloy/executeQuery", arguments: params });

      expect(result).toBeDefined();
      if (isToolResultWithContent(result)) {
         expect(result.content[0].type).toBe('text');
         // Parse and check for essential properties
         const parsedResultText = JSON.parse(result.content[0].text);
         expect(parsedResultText).toHaveProperty('queryResult');
         expect(parsedResultText).toHaveProperty('modelDef');
         expect(parsedResultText).toHaveProperty('dataStyles');
         expect(typeof parsedResultText.queryResult).toBe('string');
      } else {
         throw new Error("Tool call result missing expected content structure");
      }
    });

    it("should return error for invalid Malloy query syntax", async () => {
       const params = {
        packageName: "faa",
        modelPath: "models/flights.malloy",
        query: "run: flights->{group_by: carrier_name, BAD SYNTAX aggregate: flight_count is count()}" 
      };
      
      const expectedError = new ModelCompilationError("Malloy model compilation failed: Bad Syntax");

      // Configure mock to throw the expected error
      mockGetQueryResults.mockRejectedValue(expectedError);
      
      await expect(mcpClient.callTool({ name: "malloy/executeQuery", arguments: params }))
          .rejects.toMatchObject({
              code: ErrorCode.InvalidRequest,
              message: expect.stringContaining(expectedError.message)
          });
        expect(mockGetQueryResults).toHaveBeenCalled();
    });

    it("should return InvalidParams error for conflicting parameters (query and queryName)", async () => {
       const params = {
        packageName: "faa",
        modelPath: "models/flights.malloy",
        query: "run: flights->{aggregate: c is count()}",
        sourceName: "flights",
        queryName: "carrier_analysis"
      };
      
       await expect(mcpClient.callTool({ name: "malloy/executeQuery", arguments: params }))
          .rejects.toMatchObject({
              code: ErrorCode.InvalidParams, 
              message: expect.stringMatching(/Cannot provide both 'query' and 'queryName'/i)
          });
         // Verify service wasn't called
         expect(mockGetPackage).not.toHaveBeenCalled(); 
    });

    it("should return InvalidParams error if required params are missing", async () => {
       const params = {
        packageName: "faa",
        modelPath: "models/flights.malloy",
        sourceName: "flights" // Missing queryName or query
      };
      
       await expect(mcpClient.callTool({ name: "malloy/executeQuery", arguments: params }))
          .rejects.toMatchObject({
              code: ErrorCode.InvalidParams,
              message: expect.stringMatching(/Either 'query' or both 'sourceName' and 'queryName' must be provided/i)
          });
       expect(mockGetPackage).not.toHaveBeenCalled();
    });

    it("should return error if package/model not found", async () => {
      const params = {
        packageName: "nonexistent_package", // Bad package
        modelPath: "models/flights.malloy",
        query: "run: flights->{aggregate: c is count()}"
      };
      
      const expectedError = new PackageNotFoundError("Package not found");
      
      // Configure getPackage mock to throw
      mockGetPackage.mockRejectedValue(expectedError);

      await expect(mcpClient.callTool({ name: "malloy/executeQuery", arguments: params }))
          .rejects.toMatchObject({
              code: ErrorCode.InvalidParams,
              message: expect.stringMatching(/Resource not found/i) 
          });
       expect(mockGetPackage).toHaveBeenCalledWith(params.packageName);
       expect(mockGetModel).not.toHaveBeenCalled(); // getModel shouldn't be called if getPackage fails
       expect(mockGetQueryResults).not.toHaveBeenCalled();
    });

    // Add a test case for ModelNotFoundError from getModel
    it("should return error if model not found within package", async () => {
        const params = {
          packageName: "faa",
          modelPath: "models/flights.malloy",
          query: "run: flights->{aggregate: c is count()}"
        };
        const expectedError = new ModelNotFoundError("Model not found");

        // Configure getModel to throw
        mockGetModel.mockImplementation(() => { throw expectedError; }); // Throw directly

        await expect(mcpClient.callTool({ name: "malloy/executeQuery", arguments: params }))
            .rejects.toMatchObject({
                code: ErrorCode.InvalidParams, 
                message: expect.stringMatching(/Resource not found/i) 
            });
        expect(mockGetPackage).toHaveBeenCalledWith(params.packageName);
        expect(mockGetModel).toHaveBeenCalledWith(params.modelPath);
        expect(mockGetQueryResults).not.toHaveBeenCalled();
    });

  });
}); 