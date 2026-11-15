// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   classificationNotKept,
   keepsAccess,
   keepsGivenTerms,
   keepsTermsAsWritten,
   proveReroot,
   type RerootProofInput,
} from "./filter_binding_guard";
import { givenReadingFilterTerms } from "./materialization/plan/persist_dynamic_terms";

/**
 * The bind guard: a re-rooted source is served only with a proof that it
 * still reads what it was written against (its fields, each as hidden, and
 * every filter applied at read), and that its served surface leaks no class
 * the compile of record does not.
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
   it("refuses a served source missing a given-reading term the author's applies", () => {
      expect(
         keepsGivenTerms(
            { filterList: [givenTerm("org = $ORG"), plainTerm("org != ''")] },
            { filterList: [plainTerm("org != ''")] },
            givenReadingFilterTerms,
         ),
      ).toBe(false);
   });

   it("admits one that applies each term, whitespace aside, and ignores terms without givens", () => {
      expect(
         keepsGivenTerms(
            { filterList: [givenTerm("org = $ORG"), plainTerm("a > 1")] },
            { filterList: [givenTerm("org  =\n  $ORG")] },
            givenReadingFilterTerms,
         ),
      ).toBe(true);
   });

   it("refuses the same text reading another given, an unrecorded term and an unreadable source", () => {
      expect(
         keepsGivenTerms(
            { filterList: [givenTerm("org = $ORG")] },
            { filterList: [givenTerm("org = $ORG", "OTHER")] },
            givenReadingFilterTerms,
         ),
      ).toBe(false);
      expect(
         keepsGivenTerms(
            { filterList: [givenTerm("")] },
            { filterList: [givenTerm("")] },
            givenReadingFilterTerms,
         ),
      ).toBe(false);
      expect(
         keepsGivenTerms(
            undefined,
            { filterList: [] },
            givenReadingFilterTerms,
         ),
      ).toBe(false);
      expect(
         keepsGivenTerms(
            { filterList: [] },
            undefined,
            givenReadingFilterTerms,
         ),
      ).toBe(false);
   });
});

/**
 * The load-time access backstop (P3): a re-rooted source is carried only when
 * every field it has is exactly as hidden as the author's field of that name,
 * and a field only the served copy has is never public.
 */
describe("keepsAccess", () => {
   const def = (...fields: [string, string?][]) => ({
      fields: fields.map(([name, accessModifier]) => ({
         name,
         ...(accessModifier ? { accessModifier } : {}),
      })),
   });
   const author = def(
      ["amount"],
      ["org_id", "internal"],
      ["secret", "private"],
   );

   it("carries a source whose fields keep the author's modifiers", () => {
      expect(
         keepsAccess(
            author,
            def(
               ["amount", "public"],
               ["org_id", "internal"],
               ["secret", "private"],
            ),
         ),
      ).toBe(true);
   });

   it("refuses a hidden field served public, or private served internal", () => {
      expect(
         keepsAccess(
            author,
            def(["amount"], ["org_id"], ["secret", "private"]),
         ),
      ).toBe(false);
      expect(
         keepsAccess(
            author,
            def(["amount"], ["org_id", "internal"], ["secret", "internal"]),
         ),
      ).toBe(false);
   });

   it("reads an unknown modifier as private", () => {
      expect(keepsAccess(def(["x", "protected"]), def(["x", "private"]))).toBe(
         true,
      );
      expect(keepsAccess(def(["x", "protected"]), def(["x"]))).toBe(false);
   });

   it("allows a hidden field only the served copy has, never a public one", () => {
      expect(
         keepsAccess(
            def(["order_month"]),
            def(["order_month"], ["order_month__stored", "internal"]),
         ),
      ).toBe(true);
      expect(keepsAccess(def(["amount"]), def(["amount"], ["org_id"]))).toBe(
         false,
      );
   });

   it("never serves a hidden field's stored column less hidden than the field", () => {
      // A private timeframe field's stored column renamed `internal` would
      // let a caller's own `extend` read it; likewise a private join's.
      const priv = def(["order_month", "private"], ["carriers", "private"]);
      expect(
         keepsAccess(
            priv,
            def(
               ["order_month", "private"],
               ["order_month__stored", "private"],
               ["carriers", "private"],
               ["carriers__name__stored", "private"],
            ),
         ),
      ).toBe(true);
      expect(
         keepsAccess(
            priv,
            def(
               ["order_month", "private"],
               ["order_month__stored", "internal"],
            ),
         ),
      ).toBe(false);
      expect(
         keepsAccess(
            priv,
            def(
               ["carriers", "private"],
               ["carriers__name__stored", "internal"],
            ),
         ),
      ).toBe(false);
      // An internal one's may be internal or private, never public.
      const internal = def(["order_month", "internal"]);
      for (const [access, ok] of [
         ["internal", true],
         ["private", true],
         ["public", false],
      ] as const) {
         expect(
            keepsAccess(
               internal,
               def(
                  ["order_month", "internal"],
                  ["order_month__stored", access],
               ),
            ),
         ).toBe(ok);
      }
   });

   it("fails closed on an unreadable source", () => {
      expect(keepsAccess(undefined, def(["a"]))).toBe(false);
      expect(keepsAccess(def(["a"]), {})).toBe(false);
   });
});

describe("keepsTermsAsWritten", () => {
   it("holds a term only when the served filters carry it as written, whitespace aside", () => {
      const served = { filterList: [{ code: "t2  >= now -\n 60 days" }] };
      expect(keepsTermsAsWritten(served, [])).toBe(true);
      expect(keepsTermsAsWritten(served, ["t2 >= now - 60 days"])).toBe(true);
      expect(keepsTermsAsWritten(served, ["t2 >= now - 30 days"])).toBe(false);
      expect(keepsTermsAsWritten({}, ["t2 >= now - 60 days"])).toBe(false);
      expect(keepsTermsAsWritten(undefined, ["t2 >= now - 60 days"])).toBe(
         false,
      );
   });
});

describe("proveReroot", () => {
   const field = (name: string, accessModifier?: string) => ({
      name,
      ...(accessModifier ? { accessModifier } : {}),
   });
   const proof = (over: Partial<RerootProofInput>): RerootProofInput => ({
      author: { fields: [field("a"), field("b")], filterList: [] },
      served: { fields: [field("a"), field("b")], filterList: [] },
      servedHasOwnNode: true,
      excused: new Set(),
      termsKeptAsWritten: [],
      givenTerms: givenReadingFilterTerms,
      ...over,
   });

   it("proves a re-root that keeps every field, modifier and filter", () => {
      expect(proveReroot(proof({}))).toBeUndefined();
   });

   it("finds a lost field, unless it is excused or private on the author's", () => {
      const served = { fields: [field("a")], filterList: [] };
      expect(proveReroot(proof({ served }))).toBe("fields");
      expect(
         proveReroot(proof({ served, excused: new Set(["b"]) })),
      ).toBeUndefined();
      expect(
         proveReroot(
            proof({
               author: { fields: [field("a"), field("b", "private")] },
               served,
            }),
         ),
      ).toBeUndefined();
      expect(proveReroot(proof({ served: undefined }))).toBe("fields");
   });

   it("finds a widened modifier", () => {
      expect(
         proveReroot(
            proof({
               author: { fields: [field("a"), field("b", "internal")] },
               served: { fields: [field("a"), field("b")] },
            }),
         ),
      ).toBe("access");
   });

   it("finds a lost given term, and fails closed when the terms cannot be read", () => {
      const author = {
         fields: [field("a"), field("b")],
         filterList: [givenTerm("org = $ORG")],
      };
      expect(proveReroot(proof({ author }))).toBe("given_terms");
      expect(
         proveReroot(
            proof({
               givenTerms: () => {
                  throw new Error("unreadable");
               },
            }),
         ),
      ).toBe("given_terms");
   });

   it("finds a term not kept as written", () => {
      expect(
         proveReroot(proof({ termsKeptAsWritten: ["t >= now - 1 day"] })),
      ).toBe("terms_as_written");
   });

   it("finds a filtered source held only as a join", () => {
      const author = {
         fields: [field("a"), field("b")],
         filterList: [plainTerm("org != ''")],
      };
      expect(proveReroot(proof({ author, servedHasOwnNode: false }))).toBe(
         "filtered_join",
      );
      expect(
         proveReroot(proof({ author, served: { ...author } })),
      ).toBeUndefined();
   });

   it("reports the first loss, in the order fields, access, terms", () => {
      expect(
         proveReroot(
            proof({
               author: {
                  fields: [field("a"), field("b", "internal")],
                  filterList: [givenTerm("org = $ORG")],
               },
               served: { fields: [field("b")] },
            }),
         ),
      ).toBe("fields");
      expect(
         proveReroot(
            proof({
               author: {
                  fields: [field("a", "internal")],
                  filterList: [givenTerm("org = $ORG")],
               },
               served: { fields: [field("a")] },
            }),
         ),
      ).toBe("access");
   });
});

describe("classificationNotKept", () => {
   it("names each finding the served compile has and the compile of record does not", () => {
      const record = new Map([
         ["s", { leaks: ["old leak"], coverage: [] }],
         ["t", { leaks: [], coverage: [] }],
      ]);
      const served = new Map([
         ["s", { leaks: ["old leak", "new leak"], coverage: ["`j.x`"] }],
         ["t", { leaks: [], coverage: [] }],
         ["generated", { leaks: ["only the copy has it"], coverage: [] }],
      ]);
      expect([...classificationNotKept(record, served)]).toEqual([
         ["s", { leaks: ["new leak"], coverage: ["`j.x`"] }],
      ]);
   });
});
