import type { ErrorDetails } from "./error_messages";

/**
 * Shared construction of the MCP content blocks every malloy_* tool returns.
 *
 * Two things here are load-bearing and were previously wrong at every call site:
 *
 * 1. The resource block carries `mimeType`, not `type`. The MCP
 *    ResourceContents schema is `z.object({uri, mimeType?, _meta?}).passthrough()`,
 *    so a `type: "application/json"` field was accepted by passthrough and then
 *    ignored by every client, leaving the payload untyped on the wire.
 *
 * 2. An error result also emits a plain text block. A client that renders only
 *    text blocks when `isError` is set sees nothing at all in an embedded
 *    resource, and reports a bare "Unknown error" while the server's real
 *    message and suggestions sit unread in the resource. The text block is what
 *    makes a diagnostic legible; the resource block is what keeps it parseable.
 *
 * Ordering matters: the resource block stays at index 0 so callers (and specs)
 * can keep reading `content[0].resource.text` for the structured payload.
 */

const JSON_MIME_TYPE = "application/json";

// The index signatures mirror the MCP SDK's CallToolResult types, which declare
// `[x: string]: unknown` on the result and on every content block. Without them
// these are not assignable to the SDK's tool-callback return type.
type ResourceBlock = {
   [x: string]: unknown;
   type: "resource";
   resource: {
      [x: string]: unknown;
      uri: string;
      mimeType: string;
      text: string;
   };
};

type TextBlock = { [x: string]: unknown; type: "text"; text: string };

export type ToolContent = ResourceBlock | TextBlock;

export interface ToolResult {
   [x: string]: unknown;
   isError: boolean;
   content: ToolContent[];
}

function resourceBlock(
   uri: string,
   payload: unknown,
   space?: number,
): ResourceBlock {
   return {
      type: "resource" as const,
      resource: {
         uri,
         mimeType: JSON_MIME_TYPE,
         text: JSON.stringify(payload, null, space),
      },
   };
}

/**
 * Renders ErrorDetails as the prose a client will actually show the agent.
 *
 * The suggestions are included, not just the message: they carry the part that
 * makes an error actionable (which source was denied, which view to look for,
 * that a reload is needed), and a client that cannot read the resource block
 * has no other way to reach them. They are a short, bounded list by
 * construction in error_messages.ts.
 *
 * Exported so a spec can pin the rendering rather than merely its presence.
 */
export function formatErrorText(details: ErrorDetails): string {
   if (details.suggestions.length === 0) {
      return details.message;
   }
   const suggestions = details.suggestions.map((s) => `- ${s}`).join("\n");
   return `${details.message}\n\nSuggestions:\n${suggestions}`;
}

/**
 * Wraps a JSON payload in the resource-content shape, optionally adding a text
 * block alongside it.
 *
 * `text` is deliberately opt-in rather than automatic on success: a query result
 * is already size-capped (assertWithinModelResponseLimits), and duplicating it
 * as prose would double the payload for no gain. Success paths that have
 * something short and useful to say (render-tag warnings) pass it explicitly.
 *
 * @param space JSON.stringify indentation. Left undefined (compact) except for
 *   query results, which have always been pretty-printed for readability.
 */
export function jsonResource(
   uri: string,
   payload: unknown,
   options?: { isError?: boolean; text?: string; space?: number },
): ToolResult {
   const content: ToolContent[] = [resourceBlock(uri, payload, options?.space)];
   if (options?.text !== undefined) {
      content.push({ type: "text" as const, text: options.text });
   }
   return { isError: options?.isError ?? false, content };
}

/**
 * Builds the standard error result: the {error, suggestions} JSON a parsing
 * client wants, plus the same information as text so every client can read it.
 *
 * @param extraPayload Merged into the JSON payload. malloy_getContext uses it
 *   to keep its `results: []` contract on the error path, which its callers
 *   (and specs) rely on to treat every tier's response shape uniformly.
 */
export function jsonToolError(
   uri: string,
   details: ErrorDetails,
   extraPayload?: Record<string, unknown>,
): ToolResult {
   return jsonResource(
      uri,
      {
         error: details.message,
         suggestions: details.suggestions,
         ...extraPayload,
      },
      { isError: true, text: formatErrorText(details) },
   );
}
