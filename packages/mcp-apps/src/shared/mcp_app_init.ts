// The `/app-with-deps` entry, not the bare package, and not by preference.
//
// The bare entry imports `@modelcontextprotocol/sdk` and `zod/v4` as peer
// dependencies, and the version it wants does not match the SDK the server pins:
// ext-apps declares a peer of `@modelcontextprotocol/sdk` ^1.29, while Publisher
// resolves 1.18.1, which exports neither `mergeCapabilities` nor
// `CreateMessageResultWithToolsSchema`. Bundling against it fails.
//
// `/app-with-deps` has those dependencies compiled in and no external imports at
// all, which also makes the coupling honest: this file speaks the MCP Apps client
// protocol to the host, and that is a separate concern from the version of the
// MCP SDK the server's transport uses. The two should move independently.
import { App } from "@modelcontextprotocol/ext-apps/app-with-deps";
import {
   extractPayload,
   type ExtractedPayload,
   type ToolResultBlock,
} from "./tool_result_payload";

// Suppress the noisy per-message debug logs PostMessageTransport emits.
const originalDebug = console.debug;
console.debug = function (...args: unknown[]) {
   if (
      typeof args[0] === "string" &&
      (args[0] === "Parsed message" ||
         args[0] === "Sending message" ||
         args[0] === "Ignoring non-JSON-RPC message" ||
         args[0] === "Ignoring message from unknown source")
   ) {
      return;
   }
   originalDebug.apply(console, args);
};

export interface McpAppCallbacks {
   onToolInput: (args: Record<string, unknown>) => void;
   /** A payload was found. It is raw text; the caller parses it. */
   onToolResult: (payload: ExtractedPayload) => void;
   /** No content block carried a payload at all. A distinct failure. */
   onMissingPayload: () => void;
}

/**
 * Creates the MCP App, wires the tool-input and tool-result handlers, and
 * connects to the host.
 */
export function initMcpApp(
   name: string,
   version: string,
   callbacks: McpAppCallbacks,
): App {
   const app = new App({ name, version });

   app.ontoolinput = (input) => {
      callbacks.onToolInput((input.arguments as Record<string, unknown>) ?? {});
   };

   app.ontoolresult = (result) => {
      const payload = extractPayload(
         result.content as readonly ToolResultBlock[] | undefined,
      );
      if (payload === null) {
         callbacks.onMissingPayload();
         return;
      }
      callbacks.onToolResult(payload);
   };

   app.connect();
   return app;
}
