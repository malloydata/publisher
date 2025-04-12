import { describe, it, expect, beforeAll, afterAll } from "bun:test";
import request from "supertest"; // Still needed for POST requests
import express from "express";
import type { Request, Response, Express, NextFunction } from 'express';
import bodyParser from "body-parser";
import http from 'http';
import { AddressInfo } from 'net';
import EventSource from 'eventsource';

// MCP Imports
import { handleMcpPost, handleMcpGetSse, mcpExpressTransport, createJsonRpcError } from "../mcp/transport";
import { initializeMcpServer } from "../mcp/server";
import type { Server as McpServer } from "../../../../node_modules/@modelcontextprotocol/sdk/dist/esm/server";
import { ErrorCode } from "../../../../node_modules/@modelcontextprotocol/sdk/dist/esm/types";
import type { JSONRPCError, JSONRPCResponse, JSONRPCRequest } from "../../../../node_modules/@modelcontextprotocol/sdk/dist/esm/types";


describe("MCP Resource Handlers (Integration - Isolated)", () => {
  let testApp: Express;
  let testMcpServer: McpServer;
  let httpServer: http.Server;
  let serverUrl: string;
  let nextRequestId = 1;

  // Helper to create a unique request ID
  const getRequestId = () => nextRequestId++;

  beforeAll(async () => {
    testApp = express();
    testApp.use(bodyParser.json());
    // Add bodyParser error handler
    testApp.use((err: Error, req: Request, res: Response, next: NextFunction) => {
      if (err instanceof SyntaxError && (err as any).status === 400 && 'body' in err) {
        console.error('Malformed JSON received:', err);
        const errorPayload = createJsonRpcError(ErrorCode.ParseError, "Failed to parse JSON request body.", null);
        res.status(200).json(errorPayload);
      } else {
        next(err);
      }
    });

    testMcpServer = initializeMcpServer();

    // Define MCP routes on the test app
    testApp.post("/api/v0/mcp", (req: Request, res: Response) => {
        handleMcpPost(req, res).catch(error => {
            console.error("Test MCP POST handler error:", error);
            res.status(500).json({ message: "Internal Server Error" });
        });
    });
    testApp.get("/api/v0/mcp", handleMcpGetSse);

    // Connect the *test* server instance to the *shared* transport
    await testMcpServer.connect(mcpExpressTransport);

    // Start the HTTP server
    httpServer = http.createServer(testApp).listen(0, '127.0.0.1');
    await new Promise<void>(resolve => httpServer.once('listening', resolve));
    const address = httpServer.address() as AddressInfo;
    serverUrl = `http://127.0.0.1:${address.port}`;
    console.log(`Resource Test server listening on ${serverUrl}`);
  });

  afterAll(async () => {
      await testMcpServer.close();
      await mcpExpressTransport.close();
      if (httpServer?.listening) {
         await new Promise<void>((resolve, reject) => {
             httpServer.close((err) => err ? reject(err) : resolve());
         });
         console.log("Resource Test HTTP server closed.");
      }
  });

  // --- Helper function to send request and wait for SSE response ---
  async function sendRequestAndWaitForResponse(method: string, params?: any): Promise<JSONRPCResponse> {
    const requestId = getRequestId();
    const eventSource = new EventSource(`${serverUrl}/api/v0/mcp`);

    const responsePromise = new Promise<JSONRPCResponse>((resolve, reject) => {
        let opened = false;
        const timeoutId = setTimeout(() => {
            eventSource.close();
            reject(new Error(`Response timeout for ${method} (ID: ${requestId})`));
        }, 5000); // 5 second timeout

        eventSource.onopen = () => {
            opened = true;
            console.log(`Test: EventSource opened for request ${requestId}`);
            // Send the POST request *after* SSE connection is open
            const jsonRpcRequest: JSONRPCRequest = {
                jsonrpc: "2.0",
                id: requestId,
                method: method,
                params: params,
            };
            request(testApp) // Use the testApp instance for POST
                .post("/api/v0/mcp")
                .send(jsonRpcRequest)
                .expect(204) // Expect acknowledgement
                .end((err, res) => {
                    if (err) {
                        console.error(`Test: POST request failed for ${requestId}:`, err);
                        clearTimeout(timeoutId);
                        eventSource.close();
                        reject(err);
                    }
                     console.log(`Test: POST request sent successfully for ${requestId}`);
                    // Now just wait for the response via SSE
                });
        };

        eventSource.onmessage = (event) => {
            try {
                const message: JSONRPCResponse = JSON.parse(event.data);
                // Check if this message is the response we are waiting for
                if (message.id === requestId) {
                    console.log(`Test: Received response for ${requestId}:`, message);
                    clearTimeout(timeoutId);
                    eventSource.close();
                    resolve(message);
                }
            } catch (e) {
                console.error("Test: Error parsing SSE message data:", event.data, e);
                // Ignore parse errors for messages not matching our ID?
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

  describe("mcp/listResources", () => {
      it("should return a list containing the 'home' project", async () => {
        const response = await sendRequestAndWaitForResponse('mcp/listResources');

        // Check for success response structure
        expect(response.jsonrpc).toBe("2.0");
        expect(response).not.toHaveProperty("error");
        expect(response).toHaveProperty("result");

        // Check the result content (will fail until implemented)
        expect(response.result).toBeDefined();
        expect(Array.isArray(response.result.resources)).toBe(true);
        expect(response.result.resources).toContainEqual(
            expect.objectContaining({ uri: 'malloy://project/home' })
        );
      }, 10000); // Increase test timeout
  });

  describe("mcp/getResource", () => {
      it.todo("should return details for a valid project URI");
      it.todo("should return details for a valid package URI");
      it.todo("should return details for a valid model URI");
      it.todo("should return ResourceNotFound error for an invalid URI");
  });

}); 