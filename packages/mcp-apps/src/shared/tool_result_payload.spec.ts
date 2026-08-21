// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { extractPayload } from "./tool_result_payload";

/**
 * The fixtures below are the shapes Publisher's malloy_executeQuery actually
 * returns, from packages/server/src/mcp/tool_response.ts:
 *
 *   success            -> [resource]
 *   success + warning  -> [resource, text]   where the text is PROSE
 *   error              -> [resource, text]   where the text is PROSE
 *
 * The middle case is the one that matters. A widget that reads the first text
 * block finds the prose, fails to parse it, and reports a truncated result. It
 * would do so precisely on the results that already carry a problem.
 */
const ENVELOPE = JSON.stringify({ rows: [{ a: 1 }], _meta: { schema: {} } });
const WARNING_PROSE =
   "Returned exactly 1000 rows, the row limit applied to this query, so there are probably more.";

function resourceBlock(text: string, mimeType = "application/json") {
   return {
      type: "resource" as const,
      resource: { uri: "malloy://x/result", mimeType, text },
   };
}

function textBlock(text: string) {
   return { type: "text" as const, text };
}

describe("extractPayload", () => {
   it("reads the envelope from a clean Publisher success (resource block only)", () => {
      expect(extractPayload([resourceBlock(ENVELOPE)])).toEqual({
         text: ENVELOPE,
         from: "resource",
      });
   });

   it("prefers the resource over a prose text block that sits beside it", () => {
      // The regression this module exists for. Reading the text block here
      // yields prose, which does not parse, and the widget then tells the agent
      // its result was truncated when it was not.
      const result = extractPayload([
         resourceBlock(ENVELOPE),
         textBlock(WARNING_PROSE),
      ]);
      expect(result).toEqual({ text: ENVELOPE, from: "resource" });
      expect(result?.text).not.toBe(WARNING_PROSE);
   });

   it("prefers the resource even when the text block comes first", () => {
      // Block order is not part of the contract, so it must not be relied on.
      expect(
         extractPayload([textBlock(WARNING_PROSE), resourceBlock(ENVELOPE)]),
      ).toEqual({ text: ENVELOPE, from: "resource" });
   });

   it("prefers a JSON-typed resource over an untyped one", () => {
      const other = {
         type: "resource" as const,
         resource: { uri: "malloy://x/other", text: "not json" },
      };
      expect(extractPayload([other, resourceBlock(ENVELOPE)])).toEqual({
         text: ENVELOPE,
         from: "resource",
      });
   });

   it("accepts the +json structured-suffix mime types", () => {
      expect(
         extractPayload([resourceBlock(ENVELOPE, "application/vnd.foo+json")]),
      ).toEqual({ text: ENVELOPE, from: "resource" });
   });

   it("falls back to a text block, so a text-payload server still works", () => {
      expect(extractPayload([textBlock(ENVELOPE)])).toEqual({
         text: ENVELOPE,
         from: "text",
      });
   });

   it("falls back to an untyped resource before a text block", () => {
      const untyped = {
         type: "resource" as const,
         resource: { uri: "malloy://x/result", text: ENVELOPE },
      };
      expect(extractPayload([untyped, textBlock(WARNING_PROSE)])).toEqual({
         text: ENVELOPE,
         from: "resource",
      });
   });

   it("returns null when no block carries a payload", () => {
      // Distinct from an unparseable payload: nothing arrived to render, so the
      // caller must not blame the result's size.
      expect(extractPayload([{ type: "image" }])).toBeNull();
      expect(extractPayload([])).toBeNull();
      expect(extractPayload(undefined)).toBeNull();
   });

   it("ignores a resource block with no text (a binary blob)", () => {
      const blob = {
         type: "resource" as const,
         resource: { uri: "malloy://x/blob", mimeType: "application/json" },
      };
      expect(extractPayload([blob])).toBeNull();
      expect(extractPayload([blob, textBlock(ENVELOPE)])).toEqual({
         text: ENVELOPE,
         from: "text",
      });
   });

   it("preserves an empty-string payload rather than treating it as absent", () => {
      // "" is falsy, so a truthiness check here would misreport an empty payload
      // as no payload and send the caller down the wrong branch.
      expect(extractPayload([resourceBlock("")])).toEqual({
         text: "",
         from: "resource",
      });
   });
});
