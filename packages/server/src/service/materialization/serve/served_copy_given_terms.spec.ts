// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { rerootDeclaration } from "./served_copy";

/**
 * The served copy's tenant-isolation guard: a source is re-rooted only when its
 * served form applies every `where:` term that reads a given. The rewrite
 * replaces a declaration's base, so a term written there is dropped; this
 * guard (the bind guard's `keepsGivenTerms`, `filter_binding_guard.ts`), not
 * the rewrite, is what keeps such a source off the served copy.
 */

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
});
