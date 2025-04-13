import type { Request, Response } from "express";
import type { Server as HttpServer } from "http";
import type { Server as McpServer } from "../../../../node_modules/@modelcontextprotocol/sdk/dist/esm/server";
import type { JSONRPCRequest, JSONRPCResponse, JSONRPCMessage, JSONRPCError } from "../../../../node_modules/@modelcontextprotocol/sdk/dist/esm/types";
import { ErrorCode } from "../../../../node_modules/@modelcontextprotocol/sdk/dist/esm/types";
import * as crypto from 'crypto';
// Import the actual Transport interface
import type { Transport } from "../../../../node_modules/@modelcontextprotocol/sdk/dist/esm/shared/transport";

// Interface for the transport expected by the MCP Server (structure is assumed)
interface McpTransport {
  send(payload: object): void;
  // close?(): void;
}

// Simple in-memory store for active SSE connections mapped by a generated ID
const sseConnections = new Map<string, Response>();

// --- Define specific Error Payload Type --- 
interface JsonRpcErrorObject {
  code: ErrorCode | number; 
  message: string;
  data?: unknown;
}

interface JsonRpcErrorPayload {
  jsonrpc: "2.0";
  id: string | number | null; // Allow null ID
  error: JsonRpcErrorObject;
}
// --- End Define specific Error Payload Type ---

/**
 * Class implementing the MCP Transport interface over Express SSE.
 */
class McpExpressTransport implements Transport {
  private connectionIds = new Set<string>();

  // Callbacks set by the Protocol class via connect()
  onclose?: () => void;
  onerror?: (error: Error) => void;
  onmessage?: (message: JSONRPCMessage) => void;
  sessionId?: string; // Optional session ID (not strictly needed for server-side transport?)

  // --- Transport Interface Implementation ---

  async start(): Promise<void> {
    // Nothing specific to start for HTTP/SSE model, connections are request-driven
    console.log("McpExpressTransport started.");
  }

  async close(): Promise<void> {
    // Close all active SSE connections
    console.log(`McpExpressTransport closing ${this.connectionIds.size} connections.`);
    this.connectionIds.forEach(id => {
      const res = sseConnections.get(id);
      res?.end(); // End the SSE response stream
    });
    sseConnections.clear();
    this.connectionIds.clear();
    // Trigger the protocol's close handler
    this.onclose?.();
  }

  /**
   * Sends a payload FROM the MCP Server TO all connected clients via SSE.
   */
  async send(payload: JSONRPCMessage): Promise<void> {
    if (this.connectionIds.size === 0) {
      console.warn("MCP Server tried to send message, but no clients connected.");
      return;
    }
    console.log(`Transport sending to ${this.connectionIds.size} connections:`, payload);
    const payloadString = `data: ${JSON.stringify(payload)}\n\n`;

    const closedConnections: string[] = [];
    this.connectionIds.forEach(connectionId => {
      const res = sseConnections.get(connectionId);
      if (res) {
        try {
          if (res.writableEnded) {
             throw new Error('Stream ended'); // Handle case where connection closed unexpectedly
          }
          res.write(payloadString);
          if ((res as any).flush) {
            (res as any).flush();
          }
        } catch (error) {
          console.error(`Error writing to SSE connection ${connectionId}:`, error);
          closedConnections.push(connectionId);
        }
      } else {
        console.warn(`Connection ID ${connectionId} in transport set but not in global map.`);
        closedConnections.push(connectionId);
      }
    });

    // Clean up any connections that failed during send
    closedConnections.forEach(id => this.removeConnection(id));
  }

  // --- Helper methods for Express handlers ---

  addConnection(connectionId: string, res: Response): void {
    this.connectionIds.add(connectionId);
    sseConnections.set(connectionId, res);
    console.log(`SSE connection ${connectionId} added to transport.`);
    // Optional: Associate session ID if needed
    // this.sessionId = connectionId; // Example if only one client is expected per transport instance
  }

  removeConnection(connectionId: string): void {
    const existed = this.connectionIds.delete(connectionId);
    sseConnections.delete(connectionId);
    if (existed) {
        console.log(`SSE connection ${connectionId} removed from transport.`);
        // If this was the last connection, maybe trigger onclose?
        // Depends on how the Protocol expects single vs multiple clients per transport.
        // if (this.connectionIds.size === 0) {
        //     this.onclose?.();
        // }
    }
  }

  /**
   * Called by the POST handler when a message is received from a client.
   * It forwards the message to the connected Protocol instance.
   */
  simulateMessageReceive(message: JSONRPCMessage): void {
    if (!this.onmessage) {
      console.error("Transport received message, but no onmessage handler is set (protocol not connected?)");
      return;
    }
    try {
        this.onmessage(message);
    } catch (error) {
        console.error("Error in onmessage handler:", error);
        this.onerror?.(error instanceof Error ? error : new Error(String(error)));
    }
  }

  /**
   * Called by Express handlers if a transport-level error occurs.
   */
  simulateError(error: Error): void {
      this.onerror?.(error);
  }
}

// Create a single transport instance for the server
export const mcpExpressTransport = new McpExpressTransport();

/**
 * Creates a JSON-RPC Error Response Payload.
 */
// Export the function
export function createJsonRpcError(code: ErrorCode, message: string, id: string | number | null = null): JsonRpcErrorPayload { 
  return {
    jsonrpc: "2.0",
    id: id, 
    error: { code, message },
  };
}

/**
 * Handles incoming JSON-RPC requests via HTTP POST.
 * Parses the request and forwards it to the transport's onmessage handler.
 */
export async function handleMcpPost(req: Request, res: Response) {
    const requestBody = req.body;
    let responsePayload: JsonRpcErrorPayload | undefined = undefined; // Expect our specific error type

    // 1. Validate JSON was parsed
    if (!requestBody || typeof requestBody !== 'object') {
        return res.status(400).json({ message: "Invalid request body. JSON object expected." });
    }

    // 2. Validate JSON-RPC structure and get potential ID
    const potentialId = requestBody.id;
    const id = (typeof potentialId === 'string' || typeof potentialId === 'number' || potentialId === null) ? potentialId : null;

    if (requestBody.jsonrpc !== '2.0' || typeof requestBody.method !== 'string') {
        responsePayload = createJsonRpcError(ErrorCode.InvalidRequest, "Invalid JSON-RPC request object.", id);
    } else {
        // 3. Forward the valid JSON-RPC message to the transport
        mcpExpressTransport.simulateMessageReceive(requestBody as JSONRPCRequest);
        // Acknowledge receipt - actual response via SSE
        return res.status(204).send();
    }

    // Send back immediate errors (InvalidRequest) if generated
    if (responsePayload) {
        return res.status(200).json(responsePayload); // Send the JsonRpcErrorPayload
    }
}

/**
 * Sets up an SSE connection for server-to-client messages.
 */
export function handleMcpGetSse(req: Request, res: Response) {
    res.writeHead(200, {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no", // Useful for nginx
    });

    const connectionId = crypto.randomUUID();
    mcpExpressTransport.addConnection(connectionId, res);

    // Send an initial *event* instead of just a comment to ensure client onopen fires.
    res.write(`event: mcp-ready\ndata: ${JSON.stringify({ connectionId })}\n\n`);

    // Attempt to flush the headers and initial event immediately.
    if (typeof (res as any).flush === 'function') {
        (res as any).flush();
    }

    req.on("close", () => {
        mcpExpressTransport.removeConnection(connectionId);
        // Optionally trigger transport.onclose if this implies protocol closure
        // if (mcpExpressTransport.connectionIds.size === 0) { // Example check
        //     mcpExpressTransport.onclose?.();
        // }
    });

    req.on("error", (err) => {
        console.error(`SSE connection error for ${connectionId}:`, err);
        mcpExpressTransport.removeConnection(connectionId);
        // Optionally trigger transport.onerror
        // mcpExpressTransport.onerror?.(err);
    });
}

// TODO: We need to connect the MCP server to a mechanism that allows it
// to send messages *out* to potentially multiple clients. The SDK likely expects
// server.connect(transport) where 'transport' implements an interface to
// broadcast or route messages. The current setup only handles *incoming* messages
// or sending back direct responses on the POST route. 