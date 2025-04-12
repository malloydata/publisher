import { describe, it, expect, beforeAll, afterAll } from "bun:test";
import request from "supertest";
import express from "express";
import type { Request, Response, Express, NextFunction } from 'express';
import bodyParser from "body-parser";
import http from 'http';
import { AddressInfo } from 'net';
import EventSource from 'eventsource';
import { z } from "zod";
import { PackageService } from "../service/package.service"; 

// MCP Imports
// TODO: Define/import Tool schemas later when implementing the handler
import { handleMcpPost, handleMcpGetSse, mcpExpressTransport, createJsonRpcError } from "../mcp/transport";
import { initializeMcpServer } from "../mcp/server";
import type { Server as McpServer } from "../../../../node_modules/@modelcontextprotocol/sdk/dist/esm/server";
import { ErrorCode } from "../../../../node_modules/@modelcontextprotocol/sdk/dist/esm/types";
import type { JSONRPCError, JSONRPCResponse, JSONRPCRequest } from "../../../../node_modules/@modelcontextprotocol/sdk/dist/esm/types";

// --- Test Setup --- 
describe("MCP Tool Handlers (Integration - Isolated)", () => {
  let testApp: Express;
  let testMcpServer: McpServer;
  let httpServer: http.Server;
  let serverUrl: string;
  let nextRequestId = 1;
  let testPackageService: PackageService;

  const getRequestId = () => nextRequestId++;

  beforeAll(async () => {
    testApp = express();
    testApp.use(bodyParser.json());
    testApp.use((err: Error, req: Request, res: Response, next: NextFunction) => {
      if (err instanceof SyntaxError && (err as any).status === 400 && 'body' in err) {
        const errorPayload = createJsonRpcError(ErrorCode.ParseError, "Failed to parse JSON request body.", null);
        res.status(200).json(errorPayload);
      } else {
        next(err);
      }
    });

    testPackageService = new PackageService();
    testMcpServer = initializeMcpServer(testPackageService);

    testApp.post("/api/v0/mcp", (req: Request, res: Response) => {
        handleMcpPost(req, res).catch(error => {
            console.error("Test MCP POST handler error:", error);
            res.status(500).json({ message: "Internal Server Error" });
        });
    });
    testApp.get("/api/v0/mcp", handleMcpGetSse);

    await testMcpServer.connect(mcpExpressTransport);

    httpServer = http.createServer(testApp).listen(0, '127.0.0.1');
    await new Promise<void>(resolve => httpServer.once('listening', resolve));
    const address = httpServer.address() as AddressInfo;
    serverUrl = `http://127.0.0.1:${address.port}`;
    console.log(`Tool Test server listening on ${serverUrl}`);
  });

  afterAll(async () => {
      await testMcpServer.close();
      await mcpExpressTransport.close();
      if (httpServer?.listening) {
         await new Promise<void>((resolve, reject) => {
             httpServer.close((err) => err ? reject(err) : resolve());
         });
         console.log("Tool Test HTTP server closed.");
      }
  });

  // --- Helper function (Copied from resource tests - consider refactoring) ---
  async function sendRequestAndWaitForResponse(method: string, params?: any): Promise<JSONRPCResponse> {
    const requestId = getRequestId();
    const eventSource = new EventSource(`${serverUrl}/api/v0/mcp`);
    // ... (rest of the helper implementation as before) ...
        const responsePromise = new Promise<JSONRPCResponse>((resolve, reject) => {
            let opened = false;
            const timeoutId = setTimeout(() => {
                eventSource.close();
                reject(new Error(`Response timeout for ${method} (ID: ${requestId})`));
            }, 5000); // 5 second timeout

            eventSource.onopen = () => {
                opened = true;
                console.log(`Test: EventSource opened for request ${requestId}`);
                const jsonRpcRequest: JSONRPCRequest = {
                    jsonrpc: "2.0",
                    id: requestId,
                    method: method,
                    params: params,
                };
                request(testApp)
                    .post("/api/v0/mcp")
                    .send(jsonRpcRequest)
                    .expect(204)
                    .end((err, res) => {
                        if (err) {
                            console.error(`Test: POST request failed for ${requestId}:`, err);
                            clearTimeout(timeoutId);
                            eventSource.close();
                            reject(err);
                        }
                         console.log(`Test: POST request sent successfully for ${requestId}`);
                    });
            };

            eventSource.onmessage = (event) => {
                try {
                    const message: JSONRPCResponse = JSON.parse(event.data);
                    if (message.id === requestId) {
                        console.log(`Test: Received response for ${requestId}:`, message);
                        clearTimeout(timeoutId);
                        eventSource.close();
                        resolve(message);
                    }
                } catch (e) {
                    console.error("Test: Error parsing SSE message data:", event.data, e);
                }
            };

            eventSource.onerror = (err) => {
                 if (!opened && eventSource.readyState === EventSource.CONNECTING) return;
                 console.error('Test: EventSource error:', err);
                 clearTimeout(timeoutId);
                 eventSource.close();
                 reject(new Error(`EventSource error: ${err.message || 'Unknown error'}`));
            };
        });
        return responsePromise;
    }


  // --- Test Suites --- 

  describe("malloy/executeQuery Tool", () => {

    const testParamsBase = {
        projectName: "home", 
        packageName: "faa",
        modelPath: "flights.malloy", // Correct path relative to package root
    };
    const testSourceName = "flights"; // Source within flights.malloy

    it("should execute a valid ad-hoc query successfully", async () => {
        const adHocQuery = `run: ${testSourceName} -> { aggregate: total_flights is count() }`;
        const params = { 
            ...testParamsBase, 
            query: adHocQuery 
        };

        const response = await sendRequestAndWaitForResponse('malloy/executeQuery', params);

        expect(response.jsonrpc).toBe("2.0");
        expect(response).not.toHaveProperty("error");
        expect(response).toHaveProperty("result");
        
        // TODO: Define QueryResult schema and parse response.result
        // For now, just check if result exists
        expect(response.result).toBeDefined();
        // Add more specific checks once schema is defined:
        // expect(parsedResult.queryResult).toBeDefined();
        // expect(parsedResult.modelDef).toBeDefined(); // Maybe optional?
        // expect(JSON.parse(parsedResult.queryResult).data.rows[0].total_flights).toBeGreaterThan(0); 

    }, 15000); // Longer timeout for potential query execution

    it.todo("should execute a valid named query successfully");
    // Assuming 'flight_count' query exists in 'flights' source
    // const namedQueryName = "flight_count"; 
    // sendRequestAndWaitForResponse('malloy/executeQuery', { ...testParams, queryName: namedQueryName });
    // Assert success, check result structure

    it.todo("should return error for invalid Malloy query syntax");
    // const invalidQuery = "query: flights -> { aggregate: count( }" // Syntax error
    // sendRequestAndWaitForResponse('malloy/executeQuery', { ...testParams, query: invalidQuery });
    // Assert error response, check code/message

    it.todo("should return InvalidParams error for missing required parameter (modelPath)");
    // const { modelPath, ...missingParams } = testParams; // Omit modelPath
    // sendRequestAndWaitForResponse('malloy/executeQuery', { ...missingParams, query: adHocQuery });
    // Assert error response, check code is InvalidParams (-32602)

    it.todo("should return error if sourceName is required but missing for named query");
    // const { sourceName, ...missingParams } = testParams;
    // sendRequestAndWaitForResponse('malloy/executeQuery', { ...missingParams, queryName: namedQueryName });
    // Assert error response, check code is InvalidParams or specific internal error

    it.todo("should return error if package/model not found");
    // sendRequestAndWaitForResponse('malloy/executeQuery', { ...testParams, packageName: 'invalid' });
    // Assert error response, check code/message (e.g., ResourceNotFound/InternalError)

  });

}); 