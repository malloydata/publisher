// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { formatErrorText, jsonResource, jsonToolError } from "./tool_response";

// Pins the wire shape every malloy_* tool returns. Two properties here are
// regressions waiting to happen, so they are asserted explicitly rather than
// left to the tools' own specs:
//
//   - the resource carries `mimeType`, not `type`. `type` is not in the MCP
//     ResourceContents schema; it was accepted by .passthrough() and ignored.
//   - an error result carries a text block. The structured payload alone is
//     invisible to a client that renders only text for isError results.

describe("jsonResource", () => {
   it("puts the resource block first and types it with mimeType", () => {
      const r = jsonResource("malloy:/x", { a: 1 });

      expect(r.isError).toBe(false);
      expect(r.content).toHaveLength(1);
      expect(r.content[0].type).toBe("resource");

      const resource = (r.content[0] as { resource: Record<string, unknown> })
         .resource;
      expect(resource.mimeType).toBe("application/json");
      expect(resource.uri).toBe("malloy:/x");
      // The pre-fix spelling must not come back.
      expect(resource.type).toBeUndefined();
      expect(JSON.parse(resource.text as string)).toEqual({ a: 1 });
   });

   it("appends an optional text block after the resource", () => {
      const r = jsonResource("malloy:/x", { a: 1 }, { text: "heads up" });

      expect(r.content).toHaveLength(2);
      expect(r.content[0].type).toBe("resource");
      expect(r.content[1]).toEqual({ type: "text", text: "heads up" });
   });

   it("omits the text block when no text is supplied", () => {
      expect(jsonResource("malloy:/x", {}).content).toHaveLength(1);
   });

   it("honours the space option without changing the parsed payload", () => {
      const compact = jsonResource("malloy:/x", { a: 1 });
      const pretty = jsonResource("malloy:/x", { a: 1 }, { space: 2 });

      const text = (r: typeof compact) =>
         (r.content[0] as { resource: { text: string } }).resource.text;

      expect(text(pretty)).toContain("\n");
      expect(text(compact)).not.toContain("\n");
      expect(JSON.parse(text(pretty))).toEqual(JSON.parse(text(compact)));
   });
});

describe("formatErrorText", () => {
   it("renders the message and every suggestion", () => {
      const text = formatErrorText({
         message: "Resource not found: env/pkg",
         suggestions: ["Check the spelling.", "Call get_context."],
      });

      expect(text).toContain("Resource not found: env/pkg");
      expect(text).toContain("- Check the spelling.");
      expect(text).toContain("- Call get_context.");
   });

   it("returns the bare message when there are no suggestions", () => {
      expect(formatErrorText({ message: "boom", suggestions: [] })).toBe(
         "boom",
      );
   });
});

describe("jsonToolError", () => {
   const details = {
      message: "Cannot redefine 'overview'",
      suggestions: ["Rename the view."],
   };

   it("sets isError and carries the message in BOTH blocks", () => {
      const r = jsonToolError("error://compile", details);

      expect(r.isError).toBe(true);
      expect(r.content).toHaveLength(2);

      const payload = JSON.parse(
         (r.content[0] as { resource: { text: string } }).resource.text,
      );
      expect(payload.error).toBe("Cannot redefine 'overview'");
      expect(payload.suggestions).toEqual(["Rename the view."]);

      // The whole point of the change: a text-only client still sees it.
      const textBlock = r.content[1] as { type: string; text: string };
      expect(textBlock.type).toBe("text");
      expect(textBlock.text).toContain("Cannot redefine 'overview'");
      expect(textBlock.text).toContain("- Rename the view.");
   });

   it("merges extraPayload so getContext keeps its results key", () => {
      const r = jsonToolError("error://ctx", details, { results: [] });

      const payload = JSON.parse(
         (r.content[0] as { resource: { text: string } }).resource.text,
      );
      expect(payload.results).toEqual([]);
      expect(payload.error).toBe("Cannot redefine 'overview'");
   });
});
