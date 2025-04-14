import { describe, it, expect, beforeAll, afterAll, mock, spyOn, beforeEach } from "bun:test";

import express from "express";
import type { Express } from 'express'; 

import http from 'http';
import { AddressInfo } from 'net';
import * as path from 'path';

import { z } from "zod";
import { PackageService } from "../service/package.service";

// --- Add necessary imports for mocking ---
import type { Package } from "../service/package"; // Keep types for mocks
import type { Model } from "../service/model"; // Keep types for mocks
import { PackageNotFoundError, ModelNotFoundError } from "../errors"; // Import specific error types
import type { Result } from "@malloydata/malloy"; // Import Result type for casting
// --- End added imports --- 

import { initializeMcpServer } from "../mcp/server"; // Remove ExecuteQueryResultSchema if unused 
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { ErrorCode, JSONRPCError } from "@modelcontextprotocol/sdk/types.js";
import { MCP_ERROR_MESSAGES } from "../mcp/mcp_constants"; // Corrected import path

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

    // Add a minimal global error handler for the test app
    // This helps ensure async errors thrown in handlers are caught 
    // and allow the MCP layer to generate proper JSON-RPC errors.
    testApp.use((err: Error, req: express.Request, res: express.Response, next: express.NextFunction) => {
        console.error("Integration Test App Unhandled Error:", err);
        if (!res.headersSent) {
            res.status(500).send("Internal Server Error");
        }
    });

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
      
      // Construct Mock Data Object (Plain JS Object representing stringified data)
      const mockMalloyResultData = {
          sql: "SELECT 'AA' as carrier_name, 100 as flight_count",
          result: [{ carrier_name: 'AA', flight_count: 100 }],
          totalRows: 1
      };
      
      // Configure Mock Implementation (Cast the data object)
      mockGetQueryResults.mockResolvedValue({ 
          queryResults: mockMalloyResultData as any, // Use 'as any' to bypass strict type check
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
          const parsedText = JSON.parse(result.content[0].text);
          expect(parsedText).toEqual(mockMalloyResultData);
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
      
      // Construct Mock Data Object (Plain JS Object representing stringified data)
      const mockMalloyResultData = {
          sql: "SELECT 'some_val' as some_col",
          result: [{ some_col: 'some_val' }],
          totalRows: 1
      };
      
      // Configure Mock Implementation (Cast the data object)
      mockGetQueryResults.mockResolvedValue({ 
          queryResults: mockMalloyResultData as any, // Use 'as any' to bypass strict type check
          modelDef: { /* fake */ }, 
          dataStyles: { /* fake */ } 
      });
      
      const result = await mcpClient.callTool({ name: "malloy/executeQuery", arguments: params });

      expect(result).toBeDefined();
      if (isToolResultWithContent(result)) {
         expect(result.content[0].type).toBe('text');
         const parsedText = JSON.parse(result.content[0].text);
         expect(parsedText).toEqual(mockMalloyResultData);
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
      
      const originalErrorMessage = "Malloy model compilation failed: Bad Syntax";

      // Configure mock to throw the expected error
      mockGetQueryResults.mockImplementation(() => { throw new Error(originalErrorMessage); });

      // Expect RESOLUTION with isError: true for application errors
       await expect(mcpClient.callTool({ name: "malloy/executeQuery", arguments: params })).resolves.toEqual({ // Use toEqual for exact match
           isError: true,
           content: [
               { 
                   type: 'text',
                   text: `${MCP_ERROR_MESSAGES.ERROR_EXECUTING_QUERY(`${params.packageName}/${params.modelPath}`)} ${originalErrorMessage}`
               }
           ]
       });
       expect(mockGetQueryResults).toHaveBeenCalled();
    });

    // Note on Protocol Error Testing (below):
    // Parameter validation errors (conflicting, missing) are caught first by the 
    // Zod schema's `.parseAsync().catch()` block in `server.ts`. 
    // This catch block *wraps* the original validation error message using 
    // `McpError` and `MCP_ERROR_MESSAGES.INVALID_ARGUMENTS`. 
    // This newly created `McpError` (with the wrapped message) is then thrown.
    // The MCP Server/SDK framework catches this thrown error and translates it 
    // into the resolved `{ isError: true, content: [...] }` response seen by the client.
    // Therefore, these tests assert against the final, wrapped error message 
    // generated by `MCP_ERROR_MESSAGES.INVALID_ARGUMENTS(...)`.
    // NOTE: This differs from the end-to-end test (mcp_query_tool.spec.ts) where protocol errors
    // correctly cause promise REJECTION. This test environment seems to handle the thrown 
    // error differently, leading to RESOLUTION.
    // UPDATE: Aligning with spec & E2E tests. Expect REJECTION now.
    it("should return InvalidParams error for conflicting parameters (query and queryName)", async () => {
       const params = {
        packageName: "faa",
        modelPath: "models/flights.malloy",
        query: "run: flights->{aggregate: c is count()}",
        sourceName: "flights",
        queryName: "carrier_analysis"
      };
      
       // Protocol Error: Expect REJECTION
       await expect(mcpClient.callTool({ name: "malloy/executeQuery", arguments: params })).rejects.toMatchObject({ 
           code: ErrorCode.InvalidParams,
           message: expect.stringContaining("Invalid arguments for tool malloy/executeQuery:") &&
                    expect.stringContaining(MCP_ERROR_MESSAGES.MUTUALLY_EXCLUSIVE_PARAMS)
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
      
       // Protocol Error: Expect REJECTION
       await expect(mcpClient.callTool({ name: "malloy/executeQuery", arguments: params })).rejects.toMatchObject({ 
            code: ErrorCode.InvalidParams,
            message: expect.stringContaining("Invalid arguments for tool malloy/executeQuery:") &&
                     expect.stringContaining(MCP_ERROR_MESSAGES.MISSING_REQUIRED_PARAMS)
       });
        expect(mockGetPackage).not.toHaveBeenCalled();
    });

    it("should return error if package/model not found", async () => {
      const params = {
        packageName: "nonexistent_package", // Bad package
        modelPath: "models/flights.malloy",
        query: "run: flights->{aggregate: c is count()}"
      };
      
      // Simulate service throwing PackageNotFoundError (Application Error)
      mockGetPackage.mockImplementation(() => { throw new PackageNotFoundError(MCP_ERROR_MESSAGES.PACKAGE_NOT_FOUND(params.packageName)); });

      // Expect RESOLUTION with isError: true
      await expect(mcpClient.callTool({ name: "malloy/executeQuery", arguments: params })).resolves.toEqual({
          isError: true,
          content: [
              { 
                  type: 'text',
                  text: MCP_ERROR_MESSAGES.PACKAGE_NOT_FOUND(params.packageName)
              }
          ]
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
        // Simulate service throwing ModelNotFoundError (Application Error)
        mockGetModel.mockImplementation(() => { 
            throw new ModelNotFoundError(MCP_ERROR_MESSAGES.MODEL_NOT_FOUND(params.packageName, params.modelPath)); 
        }); 

        // Expect RESOLUTION with isError: true
        await expect(mcpClient.callTool({ name: "malloy/executeQuery", arguments: params })).resolves.toEqual({
            isError: true,
            content: [
                { 
                    type: 'text',
                    text: MCP_ERROR_MESSAGES.MODEL_NOT_FOUND(params.packageName, params.modelPath)
                }
            ]
        });
        expect(mockGetPackage).toHaveBeenCalledWith(params.packageName);
        expect(mockGetModel).toHaveBeenCalledWith(params.modelPath);
        expect(mockGetQueryResults).not.toHaveBeenCalled();
    });

  });
}); 