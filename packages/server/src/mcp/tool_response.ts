// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { ErrorDetails } from "./error_messages";

/**
 * Shared construction of the MCP content blocks every Malloy tool returns.
 *
 * 1. The resource block carries `mimeType`, not `type`: ResourceContents is a
 *    passthrough schema, so a `type` field would be accepted and then ignored.
 * 2. An error result also emits a text block, because a client that renders
 *    only text when `isError` is set would otherwise show "Unknown error".
 *
 * The resource block stays at index 0: callers and specs read
 * `content[0].resource.text` for the structured payload.
 */

const JSON_MIME_TYPE = "application/json";

// The index signatures mirror the SDK's CallToolResult types; without them these
// are not assignable to the tool-callback return type.
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
   replacer?: (key: string, value: unknown) => unknown,
): ResourceBlock {
   return {
      type: "resource" as const,
      resource: {
         uri,
         mimeType: JSON_MIME_TYPE,
         text: JSON.stringify(payload, replacer, space),
      },
   };
}

/**
 * Renders ErrorDetails as the prose a client shows the agent. Suggestions are
 * included because they make the error actionable, and a client that cannot
 * read the resource block has no other way to reach them.
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
 * block. `text` is opt-in on success: duplicating a query result as prose would
 * double the payload.
 *
 * @param space JSON.stringify indentation; compact unless given.
 */
export function jsonResource(
   uri: string,
   payload: unknown,
   options?: {
      isError?: boolean;
      text?: string;
      space?: number;
      /** Needed when the payload can carry BigInt: see json_utils.bigIntReplacer. */
      replacer?: (key: string, value: unknown) => unknown;
   },
): ToolResult {
   const content: ToolContent[] = [
      resourceBlock(uri, payload, options?.space, options?.replacer),
   ];
   if (options?.text !== undefined) {
      content.push({ type: "text" as const, text: options.text });
   }
   return { isError: options?.isError ?? false, content };
}

/**
 * Builds the standard error result: the {error, suggestions} JSON a parsing
 * client wants, plus the same information as text so every client can read it.
 *
 * @param extraPayload Merged into the JSON payload, e.g. get_context's
 *   `sources: []`, so callers can read it without branching on success.
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
