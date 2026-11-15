// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { rerootDeclaration } from "./served_copy";
import { keepsGivenTerms } from "./served_copy_serve";

/**
 * The served copy's tenant-isolation guard: a source is re-rooted only when its
 * served form applies every `where:` term that reads a given. The rewrite
 * replaces a declaration's base, so a term written there is dropped; this
 * guard, not the rewrite, is what keeps such a source off the served copy.
 */

const givenTerm = (code: string, given = "ORG") => ({
   code,
   expressionType: "scalar",
   e: {
      node: "=",
      kids: {
         left: { node: "field", path: ["org"] },
         right: { node: "given", refName: given },
      },
   },
});
const plainTerm = (code: string) => ({
   code,
   e: { node: "=", kids: { left: { node: "field", path: ["org"] } } },
});

describe("the given-term guard", () => {
   it("the re-root drops a term written in a parenthesised head", () => {
      const out = rerootDeclaration(
         "s is (ev extend { where: org = $ORG }) extend { measure: n is count() }",
         {
            sourceName: "s",
            connectionAlias: "x",
            virtualHandle: "h",
            typeName: "s__served",
            storedDimensions: [],
         },
      );
      expect(out).toBe(
         "s is x.virtual('h')::s__served extend { measure: n is count() }",
      );
      expect(out).not.toContain("$ORG");
   });

   it("refuses a served source missing a given-reading term the author's applies", () => {
      expect(
         keepsGivenTerms(
            { filterList: [givenTerm("org = $ORG"), plainTerm("org != ''")] },
            { filterList: [plainTerm("org != ''")] },
         ),
      ).toBe(false);
   });

   it("admits one that applies each term, whitespace aside, and ignores terms without givens", () => {
      expect(
         keepsGivenTerms(
            { filterList: [givenTerm("org = $ORG"), plainTerm("a > 1")] },
            { filterList: [givenTerm("org  =\n  $ORG")] },
         ),
      ).toBe(true);
   });

   it("refuses the same text reading another given, an unrecorded term and an unreadable source", () => {
      expect(
         keepsGivenTerms(
            { filterList: [givenTerm("org = $ORG")] },
            { filterList: [givenTerm("org = $ORG", "OTHER")] },
         ),
      ).toBe(false);
      expect(
         keepsGivenTerms(
            { filterList: [givenTerm("")] },
            { filterList: [givenTerm("")] },
         ),
      ).toBe(false);
      expect(keepsGivenTerms(undefined, { filterList: [] })).toBe(false);
      expect(keepsGivenTerms({ filterList: [] }, undefined)).toBe(false);
   });
});
