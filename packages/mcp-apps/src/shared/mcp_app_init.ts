// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

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

// Suppress the two per-message debug logs PostMessageTransport emits for every
// frame, which otherwise bury anything useful in the host's console.
//
// Deliberately NOT suppressing "Ignoring non-JSON-RPC message" or "Ignoring
// message from unknown source". Those fire when a host sends something this
// widget did not expect, which is exactly the line worth having when a host
// misbehaves, and swallowing them was hiding the diagnostic along with the noise.
//
// Matching on a third-party library's log text is fragile by nature: an ext-apps
// release that rewords these silently restores the noise. That failure mode is
// noise rather than breakage, and the alternative is patching the transport, so
// it is accepted rather than solved. Kept to the two highest-volume strings so
// there is less to go stale.
const SUPPRESSED_TRANSPORT_LOGS = new Set([
   "Parsed message",
   "Sending message",
]);

const originalDebug = console.debug;
console.debug = function (...args: unknown[]) {
   if (typeof args[0] === "string" && SUPPRESSED_TRANSPORT_LOGS.has(args[0])) {
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
   /** The `ui/initialize` handshake with the host failed. Nothing will arrive. */
   onConnectFailed: (message: string) => void;
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

   // `connect()` is async and its rejection was unhandled, so a failed
   // handshake surfaced as an unhandled promise rejection in the host's console
   // and the widget sat on "Waiting for query result..." indefinitely. There is
   // nothing to retry against, so the only useful thing is to say so in the card.
   app.connect().catch((error: unknown) => {
      callbacks.onConnectFailed(
         error instanceof Error ? error.message : String(error),
      );
   });
   return app;
}
