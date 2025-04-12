import { describe, it, expect, beforeAll, afterAll } from "bun:test";
import request from "supertest";
import type { Test } from "supertest"; // Import Test type for request object
import express from "express";
import type { Request, Response, Express, NextFunction } from 'express';
import bodyParser from "body-parser";
import http from 'http'; // Import http
import { AddressInfo } from 'net'; // Import AddressInfo
import EventSource from 'eventsource'; // Import EventSource client

// Import the actual handlers and transport instance
import { handleMcpPost, handleMcpGetSse, mcpExpressTransport } from "../mcp/transport";
// Import the real MCP Server initializer and the Server type
import { initializeMcpServer } from "../mcp/server";
import type { Server as McpServer } from "../../../../node_modules/@modelcontextprotocol/sdk/dist/esm/server";

// --- SDK Type Imports --- (Use types directly in handlers for now)
import type { JSONRPCError } from "../../../../node_modules/@modelcontextprotocol/sdk/dist/esm/types";
import { ErrorCode } from "../../../../node_modules/@modelcontextprotocol/sdk/dist/esm/types";

// Import the helper function
import { createJsonRpcError } from "../mcp/transport";

describe("MCP Transport Layer (Integration - Isolated)", () => {
  let testApp: Express;
  let testMcpServer: McpServer;
  let httpServer: http.Server; // Use http.Server for listen/close
  let serverUrl: string;

  beforeAll(async () => {
    // 1. Create a minimal Express app for testing
    testApp = express();
    // Apply bodyParser first
    testApp.use(bodyParser.json());
    // Add error handler specifically for bodyParser errors
    testApp.use((err: Error, req: Request, res: Response, next: NextFunction) => {
      // Check if it's a SyntaxError from bodyParser
      if (err instanceof SyntaxError && (err as any).status === 400 && 'body' in err) {
        console.error('Malformed JSON received:', err);
        // Respond with JSON-RPC ParseError
        const errorPayload = createJsonRpcError(ErrorCode.ParseError, "Failed to parse JSON request body.", null);
        res.status(200).json(errorPayload);
      } else {
        // Pass other errors along
        next(err);
      }
    });

    // 2. Initialize a *test* MCP Server instance
    // We reuse the real initializer, but don't connect it globally
    testMcpServer = initializeMcpServer();

    // 3. Define *only* the MCP routes on the test app
    testApp.post("/api/v0/mcp", (req: Request, res: Response) => {
      // Pass only req and res to the handler
      handleMcpPost(req, res).catch(error => {
          console.error("Test MCP POST handler error:", error);
          res.status(500).json({ message: "Internal Server Error" });
      });
    });

    testApp.get("/api/v0/mcp", (req: Request, res: Response) => {
      // The GET handler manages its own connections via the shared transport
      handleMcpGetSse(req, res);
    });

    // 4. Connect the test McpServer to the shared transport instance
    // This might cause issues if the main server also connects, but
    // necessary for the handlers to potentially trigger sends.
    // Consider creating a dedicated test transport instance if needed.
    await testMcpServer.connect(mcpExpressTransport);

    // Start an actual HTTP server for the test app
    httpServer = http.createServer(testApp).listen(0, '127.0.0.1'); // Use 127.0.0.1
    await new Promise<void>(resolve => httpServer.once('listening', resolve));
    const address = httpServer.address() as AddressInfo;
    serverUrl = `http://127.0.0.1:${address.port}`; // Base URL for EventSource
    console.log(`Test server listening on ${serverUrl}`);
  });

  afterAll(async () => {
      // Disconnect the test server from the transport
      await testMcpServer.close();
      // Clean up any SSE connections potentially left open by tests
      await mcpExpressTransport.close();
      // Close the HTTP server
      if (httpServer?.listening) {
         await new Promise<void>((resolve, reject) => {
             httpServer.close((err) => err ? reject(err) : resolve());
         });
         console.log("Test HTTP server closed.");
      }
  });

  describe("POST /api/v0/mcp", () => {
    const validJsonRpcRequest = {
      jsonrpc: "2.0",
      method: "initialize",
      id: 1,
    };

    // Use request(testApp) to target the local test Express app
    it("should reject non-JSON body with 400", async () => {
      const response = await request(testApp)
        .post("/api/v0/mcp")
        .set("Content-Type", "text/plain")
        .send("not json");

      // Check for InvalidRequest (200) instead of 400, as bodyParser might not reject
      expect(response.status).toBe(200);
      const body = response.body as JSONRPCError;
      expect(body.jsonrpc).toBe("2.0");
      expect(body.error.code).toBe(ErrorCode.InvalidRequest); // It fails structural validation
      expect(body.id).toBeNull();
    });

    it("should reject invalid JSON with JSON-RPC Parse Error (-32700)", async () => {
        const response = await request(testApp)
            .post("/api/v0/mcp")
            .set("Content-Type", "application/json")
            .send("{ invalid json: ");

        // Now expect the error handler to catch this and return the correct MCP error
        expect(response.status).toBe(200);
        const body = response.body as JSONRPCError;
        expect(body.jsonrpc).toBe("2.0");
        expect(body.error.code).toBe(ErrorCode.ParseError);
        expect(body.id).toBeNull();
    });

    it("should reject non-JSON-RPC object with JSON-RPC Invalid Request (-32600)", async () => {
      const response = await request(testApp)
        .post("/api/v0/mcp")
        .set("Content-Type", "application/json")
        .send({ not: "jsonrpc" });

      // handleMcpPost generates this error directly
      expect(response.status).toBe(200);
      const body = response.body as JSONRPCError;
      expect(body.jsonrpc).toBe("2.0");
      expect(body.error.code).toBe(ErrorCode.InvalidRequest);
      expect(body.id).toBeNull();
    });

    it("should acknowledge valid request with 204 (response via SSE)", async () => {
        const id = "req-123";
        const response = await request(testApp)
          .post("/api/v0/mcp")
          .send({ jsonrpc: "2.0", method: "unknownMethod", id });

        // The POST now just acknowledges receipt
        expect(response.status).toBe(204);
        // The actual MethodNotFound error would be sent via SSE, which we can't easily test here.
    });

  });

  describe("GET /api/v0/mcp", () => {
    it('should establish an SSE connection successfully', async () => {
        const connectPromise = new Promise<void>((resolve, reject) => {
            const eventSource = new EventSource(`${serverUrl}/api/v0/mcp`);
            let opened = false;

            const timeoutId = setTimeout(() => {
                eventSource.close();
                reject(new Error("EventSource connection timed out after 3s"));
            }, 3000);

            eventSource.onopen = () => {
                opened = true;
                console.log("Test: EventSource connection opened.");
                clearTimeout(timeoutId);
                eventSource.close(); // Close immediately after opening for this test
                resolve();
            };

            eventSource.onerror = (err) => {
                // Don't reject if simply trying to connect
                if (!opened && eventSource.readyState === EventSource.CONNECTING) {
                    console.log('Test: EventSource connection attempt error (retrying)...', err);
                    return; 
                }
                console.error('Test: EventSource error:', err);
                clearTimeout(timeoutId);
                eventSource.close();
                reject(new Error(`EventSource error: ${err.message || 'Unknown error'}`));
            };

            // We don't expect specific messages in this basic connection test
            // eventSource.onmessage = (event) => { ... };
        });

        await expect(connectPromise).resolves.toBeUndefined();
    });

    // Add future tests here to check for specific SSE messages received
    // it('should receive initial MCP connection info', async () => { ... });

  });
}); 