// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The two IR readers gate classification rests on, against hand-built IR: the
// transitive given expansion (which must refuse, never shrink, on a path it
// cannot resolve) and the negated-membership detector.
import type { SourceDef } from "@malloydata/malloy";
import { describe, expect, it } from "bun:test";
import {
   containsNegatedMembership,
   expandRefSummaryGivenIds,
} from "./gate_dimension";

/** A struct whose fields carry the given refSummaries. */
function struct(fields: object[]): SourceDef {
   return { type: "table", name: "t", fields } as unknown as SourceDef;
}

describe("expandRefSummaryGivenIds", () => {
   it("follows field usage transitively, which givenUsage alone does not", () => {
      const s = struct([
         { name: "wrapper", refSummary: { fieldUsage: [{ path: ["inner"] }] } },
         { name: "inner", refSummary: { givenUsage: [{ id: "ROLE" }] } },
      ]);
      expect(
         expandRefSummaryGivenIds(s, {
            givenUsage: [{ id: "GROUPS" }],
            fieldUsage: [{ path: ["wrapper"] }],
         }),
      ).toEqual({ ok: true, givenIds: new Set(["GROUPS", "ROLE"]) });
   });

   it("follows a join to the field it reaches", () => {
      const joined = {
         name: "orgs",
         join: "one",
         type: "table",
         fields: [
            { name: "region", refSummary: { givenUsage: [{ id: "R" }] } },
         ],
      };
      expect(
         expandRefSummaryGivenIds(struct([joined]), {
            fieldUsage: [{ path: ["orgs", "region"] }],
         }),
      ).toEqual({ ok: true, givenIds: new Set(["R"]) });
   });

   it("refuses, rather than returning fewer ids, on a path it cannot resolve", () => {
      expect(
         expandRefSummaryGivenIds(struct([]), {
            givenUsage: [{ id: "ROLE" }],
            fieldUsage: [{ path: ["missing", "col"] }],
         }),
      ).toEqual({ ok: false, unresolvedPath: "missing.col" });
   });

   it("skips the empty path Malloy emits for a function call", () => {
      expect(
         expandRefSummaryGivenIds(struct([]), {
            givenUsage: [{ id: "ROLE" }],
            fieldUsage: [{ path: [] }],
         }),
      ).toEqual({ ok: true, givenIds: new Set(["ROLE"]) });
   });

   it("terminates on a field that reads itself", () => {
      const s = struct([
         {
            name: "loop",
            refSummary: {
               givenUsage: [{ id: "G" }],
               fieldUsage: [{ path: ["loop"] }],
            },
         },
      ]);
      expect(
         expandRefSummaryGivenIds(s, { fieldUsage: [{ path: ["loop"] }] }),
      ).toEqual({ ok: true, givenIds: new Set(["G"]) });
   });
});

describe("containsNegatedMembership", () => {
   const member = { node: "inGiven", not: false };
   const paren = (e: object) => ({ node: "()", e });

   it("finds a membership compiled with its own negation", () => {
      expect(containsNegatedMembership({ node: "inGiven", not: true })).toBe(
         true,
      );
   });

   it("finds `not (x in $Y)` through the parentheses", () => {
      expect(
         containsNegatedMembership({ node: "not", e: paren(paren(member)) }),
      ).toBe(true);
   });

   it("finds one under either side of and/or", () => {
      const negated = { node: "not", e: paren(member) };
      for (const op of ["and", "or"]) {
         expect(
            containsNegatedMembership({
               node: op,
               kids: { left: member, right: paren(negated) },
            }),
         ).toBe(true);
      }
   });

   it("is false for a plain membership and for nodes it does not scan", () => {
      expect(containsNegatedMembership(member)).toBe(false);
      expect(
         containsNegatedMembership({ node: "=", kids: { left: member } }),
      ).toBe(false);
      expect(containsNegatedMembership(null)).toBe(false);
   });
});
