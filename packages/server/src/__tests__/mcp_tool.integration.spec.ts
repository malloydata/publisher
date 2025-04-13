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


import { initializeMcpServer, ExecuteQueryResultSchema } from "../mcp/server"; 
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { ErrorCode } from "@modelcontextprotocol/sdk/types.js";
import type { JSONRPCError, JSONRPCResponse, JSONRPCRequest } from "@modelcontextprotocol/sdk/types.js";
import { SSEServerTransport } from "@modelcontextprotocol/sdk/server/sse.js";

// Define a type guard for successful tool call results with content
interface ToolResultWithContent {
    content: { type: string, [key: string]: any }[];
    [key: string]: any; // Allow other properties
}
function isToolResultWithContent(result: any): result is ToolResultWithContent {
    return result && typeof result === 'object' && Array.isArray(result.content);
}

// --- Test Setup --- 
describe("MCP Tool Handlers (Integration - Isolated)", () => {
  let testApp: Express;
  let testMcpServer: McpServer;
  let httpServer: http.Server;
  let serverUrl: string;
  let nextRequestId = 1;
  let testPackageService: PackageService;
  let sseClient: EventSource | null = null; // Track SSE client
  // Store transports similar to the main server for testing
  const transports: {[sessionId: string]: SSEServerTransport} = {};

  const getRequestId = () => nextRequestId++;

  beforeAll(async () => {
    testApp = express();
    // Apply body parser ONLY if other non-MCP routes were added to testApp
    // For just MCP tests, it might not be needed, but keep for safety/consistency
    testApp.use(bodyParser.json()); 
    testApp.use((err: Error, req: Request, res: Response, next: NextFunction) => {
      // Handle bodyParser syntax errors specifically if they occur
      if (err instanceof SyntaxError && (err as any).status === 400 && 'body' in err) {
        // Note: MCP expects errors formatted differently, 
        // but this catches parsing issues before MCP handling.
        console.error("Test Setup: Malformed JSON detected by bodyParser");
        res.status(400).json({ error: "Malformed JSON" });
      } else {
        next(err);
      }
    });

    testPackageService = new PackageService();
    testMcpServer = initializeMcpServer(testPackageService);

    // --- Setup MCP routes using SSEServerTransport --- 
    const mcpRouter = express.Router();

    mcpRouter.get('/sse', async (req, res) => {
        console.log("Tool Test: /sse endpoint hit");
        try {
            const messagePath = '/api/v0/mcp/messages'; // Needs to match POST path
            const transport = new SSEServerTransport(messagePath, res);
            transports[transport.sessionId] = transport; // Store transport locally for test
            console.log(`Tool Test: Transport created for session: ${transport.sessionId}`);

            res.on("close", () => {
                console.log(`Tool Test: SSE connection closed for session: ${transport.sessionId}`);
                delete transports[transport.sessionId];
            });

            await testMcpServer.connect(transport);
            console.log(`Tool Test: MCP Server connected to transport for session: ${transport.sessionId}`);
        } catch (error) {
            console.error("Tool Test: Error setting up SSE connection:", error);
            if (!res.headersSent) {
                res.status(500).send("Tool Test: Failed to establish SSE connection");
            }
        }
    });

    mcpRouter.post('/messages', async (req, res) => {
        const sessionId = req.query.sessionId as string;
        console.log(`Tool Test: Received POST /messages for session: ${sessionId}`);
        if (!sessionId) {
            return res.status(400).send('sessionId query parameter is required');
        }
        const transport = transports[sessionId];
        if (transport) {
            console.log(`Tool Test: Found transport for session ${sessionId}, handling message.`);
            try {
                // Let the transport handle the raw request
                await transport.handlePostMessage(req, res); 
                console.log(`Tool Test: Successfully handled POST message for session ${sessionId}`);
            } catch (error) {
                console.error(`Tool Test: Error handling POST message for session ${sessionId}:`, error);
                if (!res.headersSent) {
                    res.status(500).send('Tool Test: Internal Server Error handling message');
                }
            }
        } else {
            console.warn(`Tool Test: No transport found for session ${sessionId}.`);
            res.status(400).send(`Tool Test: No active SSE connection found for session ${sessionId}`);
        }
    });

    // Mount the router
    testApp.use('/api/v0/mcp', mcpRouter);

    // Remove direct connection to old transport
    // await testMcpServer.connect(mcpExpressTransport); 

    httpServer = http.createServer(testApp).listen(0, '127.0.0.1');
    await new Promise<void>(resolve => httpServer.once('listening', resolve));
    const address = httpServer.address() as AddressInfo;
    serverUrl = `http://127.0.0.1:${address.port}`;
    console.log(`Tool Test server listening on ${serverUrl}`);
  });

  afterAll(async () => {
      sseClient?.close();
      sseClient = null;
      // Clear local transports
      for (const id in transports) delete transports[id]; 
      await testMcpServer.close(); // Close the MCP server instance
      // await mcpExpressTransport.close(); // Remove old transport close
      if (httpServer?.listening) {
         await new Promise<void>((resolve, reject) => {
             httpServer.close((err) => err ? reject(err) : resolve());
         });
         console.log("Tool Test HTTP server closed.");
      }
  });

  // --- Helper Functions (adapted from resource tests) --- 
  const connectSSE = (): Promise<string> => {
    return new Promise((resolve, reject) => {
        const sseUrl = `${serverUrl}/api/v0/mcp/sse`;
        console.log(`Tool Test Helper: Connecting SSE client to ${sseUrl}`);
        sseClient = new EventSource(sseUrl);

        sseClient.onopen = () => {
            console.log("Tool Test Helper: SSE client connection opened.");
        };

        sseClient.onerror = (error) => {
            console.error("Tool Test Helper: SSE client connection error:", error);
            sseClient?.close();
            sseClient = null;
            reject(new Error("Failed to connect SSE client"));
        };

        sseClient.addEventListener('mcp-ready', (event) => {
            console.log("Tool Test Helper: SSE client received mcp-ready event");
            try {
                 const data = JSON.parse(event.data);
                 if (data.connectionId) {
                    console.log(`Tool Test Helper: SSE client ready, session ID: ${data.connectionId}`);
                     resolve(data.connectionId); // Resolve with the session ID
                 } else {
                     reject(new Error("mcp-ready event missing connectionId"));
                 }
            } catch (e) {
                reject(new Error("Failed to parse mcp-ready event data"));
            }
        });

         setTimeout(() => {
            if (sseClient && sseClient.readyState !== EventSource.OPEN) {
                 console.error("Tool Test Helper: SSE client connection timed out.");
                 sseClient.close();
                 sseClient = null;
                 reject(new Error("SSE connection timed out"));
            }
        }, 5000); 
    });
  };

  const sendPost = async (sessionId: string, payload: JSONRPCRequest): Promise<request.Response> => {
      const messagesUrl = `${serverUrl}/api/v0/mcp/messages?sessionId=${sessionId}`;
      console.log(`Tool Test Helper: Sending POST to ${messagesUrl} with payload:`, JSON.stringify(payload));
      // Use supertest request against the running httpServer
      return request(httpServer) 
          .post(`/api/v0/mcp/messages?sessionId=${sessionId}`)
          .send(payload)
          .set('Content-Type', 'application/json')
          .set('Accept', 'application/json');
  };

  const waitForResponse = (expectedId: number | string): Promise<JSONRPCResponse> => {
      return new Promise((resolve, reject) => {
          if (!sseClient) {
              return reject(new Error("SSE client not connected"));
          }

          const timeout = setTimeout(() => {
              console.error(`Tool Test Helper: Timeout waiting for response ID ${expectedId}`);
              cleanListeners();
              reject(new Error(`Timeout waiting for response ID ${expectedId}`));
          }, 5000); 

          const messageHandler = (event: MessageEvent) => {
              try {
                  const message = JSON.parse(event.data);
                   console.log(`Tool Test Helper: SSE client received message:`, JSON.stringify(message));
                  if (message.id === expectedId) {
                      console.log(`Tool Test Helper: Received expected response ID ${expectedId}`);
                      clearTimeout(timeout);
                      cleanListeners();
                      resolve(message);
                  }
              } catch (e) {
                   console.error("Tool Test Helper: Error parsing SSE message data:", e);
              }
          };

          const errorHandler = (error: Event) => {
              console.error("Tool Test Helper: SSE client error during wait:", error);
              clearTimeout(timeout);
              cleanListeners();
              reject(new Error("SSE client error while waiting for response"));
          };
          
          const cleanListeners = () => {
             if(sseClient) {
                sseClient.removeEventListener('message', messageHandler);
                sseClient.removeEventListener('error', errorHandler);
             }
          };

          sseClient.addEventListener('message', messageHandler);
          sseClient.addEventListener('error', errorHandler);
      });
  };
  // --- End Helper Functions ---

  describe("malloy/executeQuery Tool", () => {
    it("should execute a valid ad-hoc query successfully", async () => {
      const sessionId = await connectSSE();
      const requestId = getRequestId();
      const params = {
        packageName: "faa",
        modelPath: "models/flights.malloy",
        query: "run: flights->{group_by: carrier_name, aggregate: flight_count is count()}" // Example ad-hoc query
      };
      const requestPayload: JSONRPCRequest = {
          jsonrpc: "2.0",
          id: requestId,
          // MCP standard method for tools
          method: "tools/call", 
          // Parameters include tool name and arguments
          params: { name: "malloy/executeQuery", arguments: params } 
      };

      // Send POST, expect 2xx acknowledgment (often 204 No Content or 202 Accepted)
      const postResponse = await sendPost(sessionId, requestPayload);
      expect(postResponse.status).toBeGreaterThanOrEqual(200);
      expect(postResponse.status).toBeLessThan(300);

      // Wait for the actual result via SSE
      const sseResponse = await waitForResponse(requestId);

      expect(sseResponse.jsonrpc).toBe("2.0");
      expect(sseResponse.id).toBe(requestId);
      // Use hasOwnProperty check for non-error
      expect(sseResponse).not.toHaveProperty('error'); 
      expect(sseResponse.result).toBeDefined();

      if (isToolResultWithContent(sseResponse.result)) {
          // Explicitly assert type after guard
          const result = sseResponse.result as ToolResultWithContent; 
          expect(Array.isArray(result.content)).toBe(true);
          expect(result.content.length).toBeGreaterThan(0);
          expect(result.content[0].type).toBe('text');
          expect(typeof result.content[0].text).toBe('string');

          try {
              const queryResult = JSON.parse(result.content[0].text);
              expect(queryResult).toHaveProperty('queryResult');
              expect(queryResult).toHaveProperty('modelDef');
              expect(queryResult).toHaveProperty('dataStyles');
          } catch (e) {
              throw new Error("Failed to parse queryResult JSON from text content");
          }
      } else {
          throw new Error("Test Error: SSE response result did not have expected content structure");
      }
    });

    it("should execute a valid named query successfully", async () => {
      const sessionId = await connectSSE();
      const requestId = getRequestId();
      const params = {
        packageName: "faa",
        modelPath: "models/flights.malloy",
        sourceName: "flights", // Source for named query
        queryName: "carrier_analysis" // Named query
      };
      const requestPayload: JSONRPCRequest = {
          jsonrpc: "2.0",
          id: requestId,
          method: "tools/call",
          params: { name: "malloy/executeQuery", arguments: params }
      };

      const postResponse = await sendPost(sessionId, requestPayload);
      expect(postResponse.status).toBeGreaterThanOrEqual(200);
      expect(postResponse.status).toBeLessThan(300);

      const sseResponse = await waitForResponse(requestId);

      expect(sseResponse.jsonrpc).toBe("2.0");
      expect(sseResponse.id).toBe(requestId);
      // Use hasOwnProperty check for non-error
      expect(sseResponse).not.toHaveProperty('error'); 
      expect(sseResponse.result).toBeDefined();
      
      if (isToolResultWithContent(sseResponse.result)) { 
         // Explicitly assert type after guard
         const result = sseResponse.result as ToolResultWithContent;
         expect(result.content[0].type).toBe('text');
         JSON.parse(result.content[0].text); // Check if parsable
      } else {
         throw new Error("Test Error: SSE response result did not have expected content structure");
      }
    });

    it("should return error for invalid Malloy query syntax", async () => {
       const sessionId = await connectSSE();
       const requestId = getRequestId();
       const params = {
        packageName: "faa",
        modelPath: "models/flights.malloy",
        query: "run: flights->{group_by: carrier_name, BAD SYNTAX aggregate: flight_count is count()}" 
      };
      const requestPayload: JSONRPCRequest = {
          jsonrpc: "2.0",
          id: requestId,
          method: "tools/call",
          params: { name: "malloy/executeQuery", arguments: params }
      };

      const postResponse = await sendPost(sessionId, requestPayload);
      expect(postResponse.status).toBeGreaterThanOrEqual(200);
      expect(postResponse.status).toBeLessThan(300);

      const sseResponse = await waitForResponse(requestId);
      
      expect(sseResponse.jsonrpc).toBe("2.0");
      expect(sseResponse.id).toBe(requestId);
      // Use hasOwnProperty check for non-result
      expect(sseResponse).not.toHaveProperty('result'); 
      expect(sseResponse.error).toBeDefined();
      if (sseResponse.error) { 
         expect(sseResponse.error.code).toBe(ErrorCode.InvalidRequest);
         expect(sseResponse.error.message).toMatch(/Malloy model compilation failed/i); 
      }
    });

    it("should return InvalidParams error for conflicting parameters (query and queryName)", async () => {
       const sessionId = await connectSSE();
       const requestId = getRequestId();
       const params = {
        packageName: "faa",
        modelPath: "models/flights.malloy",
        query: "run: flights->{aggregate: c is count()}",
        sourceName: "flights",
        queryName: "carrier_analysis"
      };
       const requestPayload: JSONRPCRequest = {
          jsonrpc: "2.0",
          id: requestId,
          method: "tools/call",
          params: { name: "malloy/executeQuery", arguments: params }
      };

      const postResponse = await sendPost(sessionId, requestPayload);
      expect(postResponse.status).toBeGreaterThanOrEqual(200);
      expect(postResponse.status).toBeLessThan(300);

      const sseResponse = await waitForResponse(requestId);
      
      expect(sseResponse.jsonrpc).toBe("2.0");
      expect(sseResponse.id).toBe(requestId);
      // Use hasOwnProperty check for non-result
      expect(sseResponse).not.toHaveProperty('result'); 
      expect(sseResponse.error).toBeDefined();
      if (sseResponse.error) {
         expect(sseResponse.error.code).toBe(ErrorCode.InvalidParams); 
         expect(sseResponse.error.message).toMatch(/Cannot provide both 'query' and 'queryName'/i);
      }
    });

    it("should return InvalidParams error if required params are missing (no query or queryName)", async () => {
       const sessionId = await connectSSE();
       const requestId = getRequestId();
       const params = {
        packageName: "faa",
        modelPath: "models/flights.malloy",
        sourceName: "flights" // Missing queryName or query
      };
       const requestPayload: JSONRPCRequest = {
          jsonrpc: "2.0",
          id: requestId,
          method: "tools/call",
          params: { name: "malloy/executeQuery", arguments: params }
      };

      const postResponse = await sendPost(sessionId, requestPayload);
      expect(postResponse.status).toBeGreaterThanOrEqual(200);
      expect(postResponse.status).toBeLessThan(300);

      const sseResponse = await waitForResponse(requestId);
      
      expect(sseResponse.jsonrpc).toBe("2.0");
      expect(sseResponse.id).toBe(requestId);
      // Use hasOwnProperty check for non-result
      expect(sseResponse).not.toHaveProperty('result'); 
      expect(sseResponse.error).toBeDefined();
      if (sseResponse.error) {
         expect(sseResponse.error.code).toBe(ErrorCode.InvalidParams);
         expect(sseResponse.error.message).toMatch(/Either 'query' or both 'sourceName' and 'queryName' must be provided/i);
      }
    });

    it("should return error if package/model not found", async () => {
      const sessionId = await connectSSE();
      const requestId = getRequestId();
      const params = {
        packageName: "nonexistent_package", // Bad package
        modelPath: "models/flights.malloy",
        query: "run: flights->{aggregate: c is count()}"
      };
      const requestPayload: JSONRPCRequest = {
          jsonrpc: "2.0",
          id: requestId,
          method: "tools/call",
          params: { name: "malloy/executeQuery", arguments: params }
      };

      const postResponse = await sendPost(sessionId, requestPayload);
      expect(postResponse.status).toBeGreaterThanOrEqual(200);
      expect(postResponse.status).toBeLessThan(300);

      const sseResponse = await waitForResponse(requestId);

      expect(sseResponse.jsonrpc).toBe("2.0");
      expect(sseResponse.id).toBe(requestId);
      // Use hasOwnProperty check for non-result
      expect(sseResponse).not.toHaveProperty('result'); 
      expect(sseResponse.error).toBeDefined();
      if (sseResponse.error) {
         expect(sseResponse.error.code).toBe(ErrorCode.InvalidParams); // Package/Model not found maps to InvalidParams
         expect(sseResponse.error.message).toMatch(/Resource not found/i);
      }
    });
  });
}); 