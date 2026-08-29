// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Finding the JSON payload in an MCP tool result.
 *
 * Kept in its own module, free of the DOM and of
 * `@modelcontextprotocol/ext-apps`, so it can be unit tested without an MCP
 * host. It is the piece most likely to be wrong against a given server, so it is
 * the piece that most needs tests.
 */

/**
 * One content block of an MCP tool result, structurally typed.
 *
 * Declared rather than imported: these are the only two shapes read here, and a
 * structural type keeps this module free of protocol dependencies.
 */
export type ToolResultBlock =
   | { type: "text"; text?: string }
   | {
        type: "resource";
        resource?: { uri?: string; mimeType?: string; text?: string };
     }
   | { type: string };

/** Which kind of block a payload came from, so a caller can word a failure. */
export type PayloadSource = "resource" | "text";

export interface ExtractedPayload {
   text: string;
   from: PayloadSource;
}

function isJsonMimeType(mimeType: string | undefined): boolean {
   if (mimeType === undefined) return false;
   // `application/json`, plus the `+json` structured-suffix forms.
   return /(^|\/|\+)json\b/i.test(mimeType);
}

function isResourceBlock(
   block: ToolResultBlock,
): block is Extract<ToolResultBlock, { type: "resource" }> {
   return block.type === "resource";
}

function resourceText(block: ToolResultBlock): string | undefined {
   if (!isResourceBlock(block)) return undefined;
   const text = block.resource?.text;
   return typeof text === "string" ? text : undefined;
}

/**
 * Finds the JSON payload among a tool result's content blocks.
 *
 * Publisher puts the query envelope in a RESOURCE block, at
 * `content[0].resource.text` with mimeType `application/json`, and uses a text
 * block only for short prose an agent needs stated outright: a truncation
 * warning, or render-tag problems. (See `tool_response.jsonResource` on the
 * server, whose `text` option is deliberately opt-in so that a result which is
 * already size-capped is not duplicated as prose.)
 *
 * So reading the first text block, which is what a widget written against a
 * server that returns its payload AS text would do, is wrong here twice over: it
 * finds nothing at all on a clean result, and on a result carrying a warning it
 * finds prose and cannot parse it. The second case is worse than the first,
 * because it fires exactly on the results that already have something wrong with
 * them, and a naive caller reports the parse failure as "the output was
 * truncated" when it was not.
 *
 * Resources are preferred, JSON-typed ones first. A text block is still accepted
 * as a fallback, so a server that does return its payload as text keeps working.
 *
 * Returns null when no block carries anything. That is its own failure and the
 * caller must report it as such, not fold it into the truncation case.
 */
export function extractPayload(
   content: readonly ToolResultBlock[] | undefined,
): ExtractedPayload | null {
   if (!content) return null;

   const jsonResource = content.find(
      (block) =>
         isResourceBlock(block) &&
         isJsonMimeType(block.resource?.mimeType) &&
         resourceText(block) !== undefined,
   );
   if (jsonResource) {
      return { text: resourceText(jsonResource)!, from: "resource" };
   }

   const anyResource = content.find(
      (block) => isResourceBlock(block) && resourceText(block) !== undefined,
   );
   if (anyResource) {
      return { text: resourceText(anyResource)!, from: "resource" };
   }

   const textBlock = content.find(
      (block): block is Extract<ToolResultBlock, { type: "text" }> =>
         block.type === "text" &&
         typeof (block as { text?: unknown }).text === "string",
   );
   if (textBlock) {
      return { text: textBlock.text!, from: "text" };
   }

   return null;
}
