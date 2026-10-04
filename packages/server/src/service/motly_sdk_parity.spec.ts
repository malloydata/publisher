// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { parseAnnotation } from "@malloydata/malloy-tag";
import {
   parseTagLines,
   quoteFilterLiterals as sdkQuoteFilterLiterals,
} from "../../../sdk/src/components/DashboardBuilder/tagParse";
import {
   ANNOTATION_TOO_LONG,
   ENV_REFERENCE_DROPPED,
   motlyParseErrors,
   motlyTag,
   quoteFilterLiterals as serverQuoteFilterLiterals,
   tagText,
   unwrapFilterLiteral,
} from "./motly";

/**
 * The SDK's tag reader (the builder) and the server's (discovery) are two
 * copies of one walk, and the server's has been wrong three times. Identical
 * cases go through both here; where they differ on purpose the difference is
 * asserted and says why.
 */
const sdkTag = (lines: string[]) =>
   parseTagLines(parseAnnotation as never, lines) as {
      tag: { toJSON(): unknown; text(...path: string[]): string | undefined };
      errors: string[];
   };

/** Annotations whose walk is a delimiter judgement: each form MOTLY delimits, with a bare literal on either side. */
const WALK_CASES: Array<[string, string]> = [
   ["plain value", `# a=f'US'`],
   ["double-quoted literal", `# a=f"US"`],
   ["array", `# a=[f'x', f"y", f'z']`],
   ["after a comma in a dict", `# g { a=f'x', b=f'y' }`],
   ["inside a string, left alone", `# a="=f'z'" b=f'US'`],
   ["inside a triple string, left alone", `# a="""x=f'q'""" b=f'US'`],
   ["triple-single string", `# a='''x=f"q"''' b=f'US'`],
   ["backtick key holding a literal", "# `k=f'x'`=1 b=f'US'"],
   ["escaped backtick in a key", "# `a\\`b`=1 c=f'US'"],
   ["escaped quote in a string", `# a="x\\" =f'q'" b=f'US'`],
   ["escaped quote inside the literal", `# a=f'it\\'s'`],
   ["embedded single quote in a double-quoted literal", `# a=f"it's"`],
   ["embedded double quote in a single-quoted literal", `# a=f'say "hi"'`],
   ["backslash in the literal", `# a=f'a\\\\b'`],
   [
      "heredoc with a decoy terminator after a bare return",
      "# a=<<<\nbody\r>>>\r x=f'US'\n>>>\n b=f'US'",
   ],
   ["heredoc terminator with padding", "# a=<<<\nbody\n  >>>  \n b=f'US'"],
   ["unterminated string", `# a="x=f'US'`],
   ["unterminated heredoc", `# a=<<<\nx=f'US'`],
   ["unterminated backtick", "# `a=f'US'"],
   ["whitespace before the literal", `# a=   f'US'`],
   ["no literal at all", `# a=1 b="x" c { d=[1, 2] }`],
];

describe("quoteFilterLiterals: the SDK and the server rewrite the same bytes", () => {
   for (const [name, annotation] of WALK_CASES) {
      it(name, () => {
         expect(sdkQuoteFilterLiterals(annotation)).toBe(
            serverQuoteFilterLiterals(annotation),
         );
      });
   }

   it("re-emits a double-quoted literal with single quotes in both, so the on-disk spelling flips", () => {
      for (const quote of [sdkQuoteFilterLiterals, serverQuoteFilterLiterals])
         expect(quote(`# a=f"US"`)).toBe(`# a="f'US'"`);
   });

   it("keeps an embedded single quote in the body, which MOTLY then reads back with its quotes", () => {
      for (const quote of [sdkQuoteFilterLiterals, serverQuoteFilterLiterals])
         expect(quote(`# a=f"it's"`)).toBe(`# a="f'it's'"`);
      const read = (
         tag: { text(...path: string[]): string | undefined } | undefined,
      ) => tag?.text("a");
      expect(read(sdkTag([`# a=f"it's"`]).tag)).toBe("f'it's'");
      expect(read(motlyTag([`# a=f"it's"`]))).toBe("f'it's'");
      // The server hands the query endpoint the body; the SDK keeps the wrapper to write it back.
      expect(unwrapFilterLiteral("f'it's'")).toBe("it's");
   });
});

describe("tag parsing: the SDK and the server read the same tag", () => {
   for (const [name, annotation] of WALK_CASES) {
      it(name, () => {
         const sdk = sdkTag([annotation]);
         expect(sdk.tag?.toJSON()).toEqual(motlyTag([annotation])?.toJSON());
         expect(sdk.errors).toEqual(motlyParseErrors([annotation]));
      });
   }

   it("rescues one line without touching a valid sibling, in both", () => {
      const lines = [`# label="K"`, `# a=f'US'`];
      expect(sdkTag(lines).tag.toJSON()).toEqual(motlyTag(lines)?.toJSON());
      expect(sdkTag(lines).errors).toEqual([]);
   });
});

describe("tag parsing: where the SDK deliberately differs from the server", () => {
   // Discovery never serves a tag that names `@env.`, since the server hydrates it; the builder never hydrates, so it can still open and edit the file.
   it("reads a line carrying @env. where the server drops it with a finding", () => {
      const line = `# a=@env.X b=1`;
      expect(motlyTag([line])).toBeUndefined();
      expect(motlyParseErrors([line])).toEqual([ENV_REFERENCE_DROPPED]);
      const sdk = sdkTag([line]);
      expect(sdk.errors).toEqual([]);
      expect(sdk.tag.text("b")).toBe("1");
   });

   // The server bounds what it parses on a shared event loop; the builder parses one user's own file in their tab.
   it("parses an annotation past the server's length bound", () => {
      const line = `# title="${"x".repeat(9000)}"`;
      expect(motlyParseErrors([line])).toEqual([ANNOTATION_TOO_LONG]);
      expect(sdkTag([line]).tag.text("title")).toHaveLength(9000);
   });

   // `Tag.text()` throws on an invalid date literal: the server drops the one field, and the SDK reader turns the throw into a refusal to open (see readDocument.spec.ts), because dropping it would delete it on the next save.
   it("throws on a malformed date where the server drops the field", () => {
      const line = `# since=@2024-13-01 b=1`;
      expect(tagText(motlyTag([line]), "since")).toBeUndefined();
      expect(tagText(motlyTag([line]), "b")).toBe("1");
      expect(() => sdkTag([line]).tag.text("since")).toThrow();
   });
});
