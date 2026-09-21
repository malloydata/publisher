// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// One test per `AuthorizeGrammarRejectionCause`, plus the shapes the
// term-by-term rule alone does not settle (a mixed-scope body; `and` inside
// a string literal, which must be ACCEPTED; the same column gated under two
// different attributes), plus the fan-out refusal and the retired-route
// refusal — both IR-level, so they compile a REAL model rather than parsing
// bare text.
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
   type ModelDef,
   type SourceDef,
} from "@malloydata/malloy";
import { beforeAll, describe, expect, it } from "bun:test";
import {
   assertAuthorizeGrammarTermsCoherent,
   AuthorizeGrammarError,
   parseAuthorizeGrammarBody,
   type AuthorizeGrammarRoutedTerm,
} from "./authorize_grammar";
import { ACCESS_FILTER_ROUTE, AUTHORIZE_ROUTE } from "./authorize_routes";
import {
   assertNoFanoutFieldPath,
   assertNoRetiredRouteMarkers,
   collectRetiredRouteMarkers,
} from "./gate_classification";

const SCALAR_GIVENS = new Map([["REGION", "string"]]);
const LIST_GIVENS = new Map([["GROUPS", "array"]]);
const MIXED_GIVENS = new Map([
   ["REGION", "string"],
   ["GROUPS", "array"],
]);

describe("parseAuthorizeGrammarBody — rejection causes", () => {
   it("empty_body", () => {
      expect(() =>
         parseAuthorizeGrammarBody("X", "   ", new Map(), ACCESS_FILTER_ROUTE),
      ).toThrow(AuthorizeGrammarError);
      try {
         parseAuthorizeGrammarBody("X", "", new Map(), ACCESS_FILTER_ROUTE);
         throw new Error("expected a throw");
      } catch (err) {
         expect((err as AuthorizeGrammarError).rejectionCause).toBe(
            "empty_body",
         );
      }
   });

   function expectCause(
      body: string,
      givens: ReadonlyMap<string, string>,
      cause: string,
      route: string = ACCESS_FILTER_ROUTE,
   ): void {
      try {
         parseAuthorizeGrammarBody("X", body, givens, route);
         throw new Error(`expected a throw for \`${body}\``);
      } catch (err) {
         expect(err).toBeInstanceOf(AuthorizeGrammarError);
         expect((err as AuthorizeGrammarError).rejectionCause).toBe(
            cause as never,
         );
      }
   }

   it("compound_boolean — `or`", () => {
      expectCause(
         "region = $REGION or org_id = $REGION",
         SCALAR_GIVENS,
         "compound_boolean",
      );
   });

   it("compound_boolean — `not`", () => {
      expectCause("not region = $REGION", SCALAR_GIVENS, "compound_boolean");
   });

   it("negated_operator", () => {
      expectCause("region != $REGION", SCALAR_GIVENS, "negated_operator");
   });

   it("comparison_operator", () => {
      expectCause("region > $REGION", SCALAR_GIVENS, "comparison_operator");
   });

   it("left_not_field_path — a function call", () => {
      expectCause(
         "upper(region) = $REGION",
         SCALAR_GIVENS,
         "left_not_field_path",
      );
   });

   it("left_not_field_path — a bare literal on both sides", () => {
      expectCause("1 = 1", new Map(), "left_not_field_path");
   });

   it("missing_given_reference — right side is not `$NAME`", () => {
      expectCause("region = 'east'", SCALAR_GIVENS, "missing_given_reference");
   });

   it("malformed_body — no `=`/`in` operator", () => {
      expectCause("region like $REGION", SCALAR_GIVENS, "malformed_body");
   });

   it("duplicate_given — the same given used by two terms", () => {
      expectCause(
         "org_id = $REGION and region = $REGION",
         SCALAR_GIVENS,
         "duplicate_given",
      );
   });

   it("duplicate_field_path — the same column gated under two attributes", () => {
      const givens = new Map([
         ["A", "string"],
         ["B", "string"],
      ]);
      expectCause(
         "region = $A and region = $B",
         givens,
         "duplicate_field_path",
      );
   });

   it("duplicate_field_path — a backtick-quoted spelling and a bare spelling of the same column", () => {
      // `` `region` `` and `region` differ as authored strings but resolve to
      // the identical `fieldPathSegments` — must be caught on the SEGMENTS,
      // not the raw text.
      const givens = new Map([
         ["A", "string"],
         ["B", "string"],
      ]);
      expectCause(
         "`region` = $A and region = $B",
         givens,
         "duplicate_field_path",
      );
   });

   it("a genuinely different dotted path is still accepted", () => {
      const givens = new Map([
         ["A", "string"],
         ["B", "string"],
      ]);
      const terms = parseAuthorizeGrammarBody(
         "X",
         "region = $A and org.region = $B",
         givens,
         ACCESS_FILTER_ROUTE,
      );
      expect(terms.length).toBe(2);
   });

   // A mixed-scope body needs no cause of its own: each route admits exactly
   // one scope, so whichever term is the wrong one for the route the author
   // wrote is what gets named.
   it("a mixed-scope body is refused by the scope its route does not admit", () => {
      const givens = new Map([
         ["REGION", "string"],
         ["ROLE", "string"],
      ]);
      expectCause(
         "region = $REGION and 'admin' = $ROLE",
         givens,
         "source_level_term_in_access_filter",
      );
      expectCause(
         "region = $REGION and 'admin' = $ROLE",
         givens,
         "row_level_term_in_authorize",
         AUTHORIZE_ROUTE,
      );
   });

   it("operator_arity_mismatch — `=` against a list-typed given", () => {
      expectCause("org_id = $GROUPS", LIST_GIVENS, "operator_arity_mismatch");
   });

   it("operator_arity_mismatch — `in` against a scalar given", () => {
      expectCause(
         "region in $REGION",
         SCALAR_GIVENS,
         "operator_arity_mismatch",
      );
   });
});

describe("parseAuthorizeGrammarBody — #(authorize) route", () => {
   it("row_level_term_in_authorize — a field-on-the-left term is refused", () => {
      try {
         parseAuthorizeGrammarBody(
            "X",
            "region = $REGION",
            SCALAR_GIVENS,
            AUTHORIZE_ROUTE,
         );
         throw new Error("expected a throw");
      } catch (err) {
         expect(err).toBeInstanceOf(AuthorizeGrammarError);
         expect((err as AuthorizeGrammarError).rejectionCause).toBe(
            "row_level_term_in_authorize" as never,
         );
      }
   });

   // The mirror, which did not exist before the flip: a source-level body on
   // the filter route grafts as a constant predicate, so a caller it excludes
   // is served zero rows instead of being refused.
   it("source_level_term_in_access_filter — a literal-on-the-left term is refused", () => {
      try {
         parseAuthorizeGrammarBody(
            "X",
            "'finance' in $GROUPS",
            LIST_GIVENS,
            ACCESS_FILTER_ROUTE,
         );
         throw new Error("expected a throw");
      } catch (err) {
         expect(err).toBeInstanceOf(AuthorizeGrammarError);
         expect((err as AuthorizeGrammarError).rejectionCause).toBe(
            "source_level_term_in_access_filter" as never,
         );
      }
   });

   it("accepts a source-level term ('literal' in/= $GIVEN)", () => {
      const [term] = parseAuthorizeGrammarBody(
         "X",
         "'admin' = $ROLE",
         new Map([["ROLE", "string"]]),
         AUTHORIZE_ROUTE,
      );
      expect(term).toEqual({
         scope: "source_level",
         literal: "'admin'",
         given: "ROLE",
      });
   });

   it("a bare `false` parses to the SAME deny_all sentinel as the authorize route", () => {
      // `false` means the same thing on both routes — it names no row at all,
      // so the row-level-only restriction has a carve-out for it.
      const [onAuthorize] = parseAuthorizeGrammarBody(
         "X",
         "false",
         new Map(),
         AUTHORIZE_ROUTE,
      );
      const [onSourceAuthorize] = parseAuthorizeGrammarBody(
         "X",
         "false",
         new Map(),
         AUTHORIZE_ROUTE,
      );
      expect(onSourceAuthorize).toEqual(onAuthorize);
      expect(onSourceAuthorize).toEqual({ scope: "deny_all" });
   });

   it("a row-level term alongside a sibling still refuses as row_level_term_in_authorize, not compound_boolean", () => {
      try {
         parseAuthorizeGrammarBody(
            "X",
            "region = $REGION and 'admin' = $ROLE",
            new Map([
               ["REGION", "string"],
               ["ROLE", "string"],
            ]),
            AUTHORIZE_ROUTE,
         );
         throw new Error("expected a throw");
      } catch (err) {
         expect(err).toBeInstanceOf(AuthorizeGrammarError);
         expect((err as AuthorizeGrammarError).rejectionCause).toBe(
            "row_level_term_in_authorize" as never,
         );
      }
   });
});

// `assertAuthorizeGrammarTermsCoherent` is what catches these four mistakes
// once they're spread across NOTES rather than terms in one body — the
// pure-parse cases above already pin the within-one-body forms via
// `parseAuthorizeGrammarBody` itself.
describe("assertAuthorizeGrammarTermsCoherent — cross-note", () => {
   function routed(
      terms: readonly AuthorizeGrammarRoutedTerm["term"][],
   ): AuthorizeGrammarRoutedTerm[] {
      return terms.map((term) => ({ term, route: ACCESS_FILTER_ROUTE }));
   }

   function expectCoherenceCause(
      terms: readonly AuthorizeGrammarRoutedTerm["term"][],
      cause: string,
   ): void {
      try {
         assertAuthorizeGrammarTermsCoherent("X", routed(terms));
         throw new Error("expected a throw");
      } catch (err) {
         expect(err).toBeInstanceOf(AuthorizeGrammarError);
         expect((err as AuthorizeGrammarError).rejectionCause).toBe(
            cause as never,
         );
      }
   }

   it("duplicate_given across two notes", () => {
      const [a] = parseAuthorizeGrammarBody(
         "X",
         "a = $G",
         SCALAR_GIVENS,
         ACCESS_FILTER_ROUTE,
      );
      const [b] = parseAuthorizeGrammarBody(
         "X",
         "b = $G",
         SCALAR_GIVENS,
         ACCESS_FILTER_ROUTE,
      );
      expectCoherenceCause([a, b], "duplicate_given");
   });

   it("duplicate_field_path across two notes", () => {
      const givens = new Map([
         ["A", "string"],
         ["B", "string"],
      ]);
      const [a] = parseAuthorizeGrammarBody(
         "X",
         "region = $A",
         givens,
         ACCESS_FILTER_ROUTE,
      );
      const [b] = parseAuthorizeGrammarBody(
         "X",
         "region = $B",
         givens,
         ACCESS_FILTER_ROUTE,
      );
      expectCoherenceCause([a, b], "duplicate_field_path");
   });

   it("duplicate_field_path across two notes, one backtick-quoted", () => {
      const givens = new Map([
         ["A", "string"],
         ["B", "string"],
      ]);
      const [a] = parseAuthorizeGrammarBody(
         "X",
         "`region` = $A",
         givens,
         ACCESS_FILTER_ROUTE,
      );
      const [b] = parseAuthorizeGrammarBody(
         "X",
         "region = $B",
         givens,
         ACCESS_FILTER_ROUTE,
      );
      expectCoherenceCause([a, b], "duplicate_field_path");
   });

   it("deny_all_with_sibling — a bare `false` note plus any other note is refused", () => {
      const [denyAll] = parseAuthorizeGrammarBody(
         "X",
         "false",
         new Map(),
         AUTHORIZE_ROUTE,
      );
      const [sibling] = parseAuthorizeGrammarBody(
         "X",
         "region = $REGION",
         SCALAR_GIVENS,
         ACCESS_FILTER_ROUTE,
      );
      expectCoherenceCause([denyAll, sibling], "deny_all_with_sibling");
   });

   it("a lone deny_all (no sibling) does not throw", () => {
      const [denyAll] = parseAuthorizeGrammarBody(
         "X",
         "false",
         new Map(),
         AUTHORIZE_ROUTE,
      );
      expect(() =>
         assertAuthorizeGrammarTermsCoherent("X", routed([denyAll])),
      ).not.toThrow();
   });

   it("the same given reused across TWO DIFFERENT routes is not a duplicate", () => {
      // Pins the scoping rule the four checks depend on: a term declared
      // under one route and a term declared under a different route are
      // meant to AND, not agree on given or scope — see
      // `assertAuthorizeGrammarTermsCoherent`'s doc. Exercises the actual
      // second route this module implements, `#(authorize)`.
      const [a] = parseAuthorizeGrammarBody(
         "X",
         "org_id in $G",
         LIST_GIVENS,
         ACCESS_FILTER_ROUTE,
      );
      const [b] = parseAuthorizeGrammarBody(
         "X",
         "'finance' in $G",
         LIST_GIVENS,
         AUTHORIZE_ROUTE,
      );
      expect(() =>
         assertAuthorizeGrammarTermsCoherent("X", [
            { term: a, route: ACCESS_FILTER_ROUTE },
            { term: b, route: AUTHORIZE_ROUTE },
         ]),
      ).not.toThrow();
   });
});

describe("parseAuthorizeGrammarBody — the `true` admit-all sentinel", () => {
   it("`true`, `TRUE`, and a padded ` true ` all parse to the admit_all sentinel", () => {
      for (const spelling of ["true", "TRUE", "  true  "]) {
         expect(
            parseAuthorizeGrammarBody("X", spelling, new Map(), AUTHORIZE_ROUTE),
         ).toEqual([{ scope: "admit_all" }]);
      }
   });

   it("both sentinels are refused on #(access_filter), naming the lock form", () => {
      for (const spelling of ["true", "false"]) {
         try {
            parseAuthorizeGrammarBody(
               "X",
               spelling,
               new Map(),
               ACCESS_FILTER_ROUTE,
            );
            throw new Error(`expected a throw for \`${spelling}\``);
         } catch (err) {
            expect(err).toBeInstanceOf(AuthorizeGrammarError);
            expect((err as AuthorizeGrammarError).rejectionCause).toBe(
               "sentinel_in_access_filter" as never,
            );
            expect((err as Error).message).toContain("#(authorize)");
         }
      }
   });

   it("`true and org_id = $A` is NOT the sentinel — it falls through to the ordinary per-term errors", () => {
      // The whole-body check runs BEFORE `splitTerms`, so only an EXACT
      // (trimmed) `true` takes the sentinel path — this is a two-term body
      // whose first term (`true`, no `=`/`in`) is malformed, not an admit-all
      // plus a dead conjunct.
      try {
         parseAuthorizeGrammarBody(
            "X",
            "true and org_id = $A",
            new Map([["A", "string"]]),
            ACCESS_FILTER_ROUTE,
         );
         throw new Error("expected a throw");
      } catch (err) {
         expect(err).toBeInstanceOf(AuthorizeGrammarError);
         expect((err as AuthorizeGrammarError).rejectionCause).toBe(
            "malformed_body",
         );
      }
   });

   it("admit_all plus a sibling term — same route — is refused as admit_all_with_sibling", () => {
      const [admitAll] = parseAuthorizeGrammarBody(
         "X",
         "true",
         new Map(),
         AUTHORIZE_ROUTE,
      );
      const [sibling] = parseAuthorizeGrammarBody(
         "X",
         "region = $REGION",
         SCALAR_GIVENS,
         ACCESS_FILTER_ROUTE,
      );
      try {
         assertAuthorizeGrammarTermsCoherent("X", [
            { term: admitAll, route: ACCESS_FILTER_ROUTE },
            { term: sibling, route: ACCESS_FILTER_ROUTE },
         ]);
         throw new Error("expected a throw");
      } catch (err) {
         expect(err).toBeInstanceOf(AuthorizeGrammarError);
         expect((err as AuthorizeGrammarError).rejectionCause).toBe(
            "admit_all_with_sibling",
         );
      }
   });

   it("an own `#(authorize) true` plus a term on that same route is refused as admit_all_with_sibling", () => {
      const [admitAll] = parseAuthorizeGrammarBody(
         "X",
         "true",
         new Map(),
         AUTHORIZE_ROUTE,
      );
      const [sibling] = parseAuthorizeGrammarBody(
         "X",
         "'finance' in $G",
         LIST_GIVENS,
         AUTHORIZE_ROUTE,
      );
      try {
         assertAuthorizeGrammarTermsCoherent("X", [
            { term: admitAll, route: AUTHORIZE_ROUTE },
            { term: sibling, route: AUTHORIZE_ROUTE },
         ]);
         throw new Error("expected a throw");
      } catch (err) {
         expect((err as AuthorizeGrammarError).rejectionCause).toBe(
            "admit_all_with_sibling",
         );
      }
   });

   it("admit_all beside a term on the OTHER route is legal — `true` sheds only its own route's inherited gate, so the sibling is live rather than dead text", () => {
      const [admitAll] = parseAuthorizeGrammarBody(
         "X",
         "true",
         new Map(),
         AUTHORIZE_ROUTE,
      );
      const [callerTerm] = parseAuthorizeGrammarBody(
         "X",
         "'finance' in $G",
         LIST_GIVENS,
         AUTHORIZE_ROUTE,
      );
      expect(() =>
         assertAuthorizeGrammarTermsCoherent("X", [
            { term: admitAll, route: ACCESS_FILTER_ROUTE },
            { term: callerTerm, route: AUTHORIZE_ROUTE },
         ]),
      ).not.toThrow();

      // The mirror: an own `#(authorize) true` beside an own row-level
      // `#(authorize)` opens the caller route while the row filter still runs.
      const [rowTerm] = parseAuthorizeGrammarBody(
         "X",
         "region = $REGION",
         SCALAR_GIVENS,
         ACCESS_FILTER_ROUTE,
      );
      const [callerAdmitAll] = parseAuthorizeGrammarBody(
         "X",
         "true",
         new Map(),
         AUTHORIZE_ROUTE,
      );
      expect(() =>
         assertAuthorizeGrammarTermsCoherent("X", [
            { term: rowTerm, route: ACCESS_FILTER_ROUTE },
            { term: callerAdmitAll, route: AUTHORIZE_ROUTE },
         ]),
      ).not.toThrow();
   });

   it("THE ORDER PIN — admit_all and deny_all on ONE route resolve to deny_all_with_sibling, the deliberate fail-closed reading of the contradiction", () => {
      const [admitAll] = parseAuthorizeGrammarBody(
         "X",
         "true",
         new Map(),
         AUTHORIZE_ROUTE,
      );
      const [denyAll] = parseAuthorizeGrammarBody(
         "X",
         "false",
         new Map(),
         AUTHORIZE_ROUTE,
      );
      try {
         assertAuthorizeGrammarTermsCoherent("X", [
            { term: admitAll, route: ACCESS_FILTER_ROUTE },
            { term: denyAll, route: ACCESS_FILTER_ROUTE },
         ]);
         throw new Error("expected a throw");
      } catch (err) {
         expect((err as AuthorizeGrammarError).rejectionCause).toBe(
            "deny_all_with_sibling",
         );
      }
   });

   it("a deny_all on the OTHER route still beats an admit_all — the deny check is the one that is not route-scoped", () => {
      const [admitAll] = parseAuthorizeGrammarBody(
         "X",
         "true",
         new Map(),
         AUTHORIZE_ROUTE,
      );
      const [denyAll] = parseAuthorizeGrammarBody(
         "X",
         "false",
         new Map(),
         AUTHORIZE_ROUTE,
      );
      try {
         assertAuthorizeGrammarTermsCoherent("X", [
            { term: admitAll, route: ACCESS_FILTER_ROUTE },
            { term: denyAll, route: AUTHORIZE_ROUTE },
         ]);
         throw new Error("expected a throw");
      } catch (err) {
         expect((err as AuthorizeGrammarError).rejectionCause).toBe(
            "deny_all_with_sibling",
         );
      }
   });
});

describe("parseAuthorizeGrammarBody — accepted shapes", () => {
   it("`and` inside a string literal is not a compound boolean", () => {
      const terms = parseAuthorizeGrammarBody(
         "X",
         "'research and development' in $GROUPS",
         LIST_GIVENS,
         AUTHORIZE_ROUTE,
      );
      expect(terms).toEqual([
         {
            scope: "source_level",
            literal: "'research and development'",
            given: "GROUPS",
         },
      ]);
   });

   it("`or`/`not` inside a string literal are not refused", () => {
      const terms = parseAuthorizeGrammarBody(
         "X",
         "'rock or roll' in $GROUPS",
         LIST_GIVENS,
         AUTHORIZE_ROUTE,
      );
      expect(terms).toEqual([
         { scope: "source_level", literal: "'rock or roll'", given: "GROUPS" },
      ]);
   });

   it("a row-level and a second row-level term joined by `and`", () => {
      const terms = parseAuthorizeGrammarBody(
         "X",
         "region = $REGION and org_id in $GROUPS",
         MIXED_GIVENS,
         ACCESS_FILTER_ROUTE,
      );
      expect(terms).toEqual([
         {
            scope: "row_level",
            fieldPath: "region",
            fieldPathSegments: ["region"],
            given: "REGION",
            operator: "=",
         },
         {
            scope: "row_level",
            fieldPath: "org_id",
            fieldPathSegments: ["org_id"],
            given: "GROUPS",
            operator: "in",
         },
      ]);
   });

   it("a dotted join path on the left", () => {
      const terms = parseAuthorizeGrammarBody(
         "X",
         "child.name in $GROUPS",
         LIST_GIVENS,
         ACCESS_FILTER_ROUTE,
      );
      expect(terms).toEqual([
         {
            scope: "row_level",
            fieldPath: "child.name",
            fieldPathSegments: ["child", "name"],
            given: "GROUPS",
            operator: "in",
         },
      ]);
   });

   it("a source-level term reads either way round: given on the left", () => {
      // The spelling publisher's own fixtures and docs used first. Same
      // comparison as `'analyst' = $REGION`, so refusing it would break
      // working models over operand order alone.
      const terms = parseAuthorizeGrammarBody(
         "X",
         "$REGION = 'analyst'",
         SCALAR_GIVENS,
         AUTHORIZE_ROUTE,
      );
      expect(terms).toEqual([
         { scope: "source_level", literal: "'analyst'", given: "REGION" },
      ]);
   });

   it("a reversed `in` ($GIVEN in 'literal') is refused, not silently swapped, on #(authorize)", () => {
      // Unlike `=`, `in` is not reversible: the graft compiles the author's
      // ORIGINAL text unchanged, and Malloy rejects array-in-string
      // membership, so silently swapping here would validate a body that
      // then fails at model compilation.
      try {
         parseAuthorizeGrammarBody(
            "X",
            "$GROUPS in 'finance'",
            LIST_GIVENS,
            ACCESS_FILTER_ROUTE,
         );
         throw new Error("expected a throw");
      } catch (err) {
         expect(err).toBeInstanceOf(AuthorizeGrammarError);
         expect((err as AuthorizeGrammarError).rejectionCause).toBe(
            "reversed_in_operands" as never,
         );
         expect((err as AuthorizeGrammarError).message).toMatch(
            /'finance' in \$GROUPS/,
         );
      }
   });

   it("a reversed `in` is refused the same way on #(authorize)", () => {
      try {
         parseAuthorizeGrammarBody(
            "X",
            "$GROUPS in 'finance'",
            LIST_GIVENS,
            ACCESS_FILTER_ROUTE,
         );
         throw new Error("expected a throw");
      } catch (err) {
         expect(err).toBeInstanceOf(AuthorizeGrammarError);
         expect((err as AuthorizeGrammarError).rejectionCause).toBe(
            "reversed_in_operands" as never,
         );
      }
   });

   it("literal-first `in` ('literal' in $GIVEN) still parses", () => {
      const terms = parseAuthorizeGrammarBody(
         "X",
         "'finance' in $GROUPS",
         LIST_GIVENS,
         AUTHORIZE_ROUTE,
      );
      expect(terms).toEqual([
         { scope: "source_level", literal: "'finance'", given: "GROUPS" },
      ]);
   });

   it("`=` still reverses either way round (unaffected by the `in` restriction)", () => {
      const terms = parseAuthorizeGrammarBody(
         "X",
         "$ROLE = 'admin'",
         new Map([["ROLE", "string"]]),
         AUTHORIZE_ROUTE,
      );
      expect(terms).toEqual([
         { scope: "source_level", literal: "'admin'", given: "ROLE" },
      ]);
   });

   it("a row-level term is NOT reversible — the column stays on the left", () => {
      // The field path is what the build scan groups by and what the graft
      // filters on, so a flipped row-level term is a different statement.
      try {
         parseAuthorizeGrammarBody(
            "X",
            "$GROUPS = org_id",
            LIST_GIVENS,
            ACCESS_FILTER_ROUTE,
         );
         throw new Error("expected a throw");
      } catch (err) {
         expect(err).toBeInstanceOf(AuthorizeGrammarError);
         expect((err as AuthorizeGrammarError).rejectionCause).toBe(
            "left_not_field_path",
         );
      }
   });

   it("a comparison character inside a string literal is not a comparison", () => {
      const terms = parseAuthorizeGrammarBody(
         "X",
         "'a>b' in $GROUPS",
         LIST_GIVENS,
         AUTHORIZE_ROUTE,
      );
      expect(terms).toEqual([
         { scope: "source_level", literal: "'a>b'", given: "GROUPS" },
      ]);
   });

   it("`!=` inside a string literal is not a negated operator", () => {
      const terms = parseAuthorizeGrammarBody(
         "X",
         "'a!=b' in $GROUPS",
         LIST_GIVENS,
         AUTHORIZE_ROUTE,
      );
      expect(terms).toEqual([
         { scope: "source_level", literal: "'a!=b'", given: "GROUPS" },
      ]);
   });

   it("a backtick-quoted column hides the tokens inside it", () => {
      const terms = parseAuthorizeGrammarBody(
         "X",
         "`org in region` = $REGION",
         SCALAR_GIVENS,
         ACCESS_FILTER_ROUTE,
      );
      expect(terms).toEqual([
         {
            scope: "row_level",
            fieldPath: "`org in region`",
            fieldPathSegments: ["org in region"],
            given: "REGION",
            operator: "=",
         },
      ]);
   });

   it("a backtick-quoted column containing `and` is one term, not two", () => {
      const terms = parseAuthorizeGrammarBody(
         "X",
         "`research and development` in $GROUPS",
         LIST_GIVENS,
         ACCESS_FILTER_ROUTE,
      );
      expect(terms).toEqual([
         {
            scope: "row_level",
            fieldPath: "`research and development`",
            fieldPathSegments: ["research and development"],
            given: "GROUPS",
            operator: "in",
         },
      ]);
   });

   it("a backtick-quoted column containing a literal dot is one segment, not two", () => {
      const terms = parseAuthorizeGrammarBody(
         "X",
         "`cost.center`.name in $GROUPS",
         LIST_GIVENS,
         ACCESS_FILTER_ROUTE,
      );
      expect(terms).toEqual([
         {
            scope: "row_level",
            fieldPath: "`cost.center`.name",
            fieldPathSegments: ["cost.center", "name"],
            given: "GROUPS",
            operator: "in",
         },
      ]);
   });

   it("a given absent from the declared-type map skips the arity check", () => {
      const terms = parseAuthorizeGrammarBody(
         "X",
         "region = $UNKNOWN",
         new Map(),
         ACCESS_FILTER_ROUTE,
      );
      expect(terms).toEqual([
         {
            scope: "row_level",
            fieldPath: "region",
            fieldPathSegments: ["region"],
            given: "UNKNOWN",
            operator: "=",
         },
      ]);
   });
});

// ---------------------------------------------------------------------------
// Fan-out and retired-route refusals — both IR-level, so real compiled
// models rather than bare text.
// ---------------------------------------------------------------------------

const ROOT = "file:///authorize-grammar/";
let connections: FixedConnectionMap;

beforeAll(() => {
   const duckdb = new DuckDBConnection("duckdb", ":memory:");
   connections = new FixedConnectionMap(
      new Map([["duckdb", duckdb]]),
      "duckdb",
   );
});

async function compileModel(model: string): Promise<ModelDef> {
   const urlReader = new InMemoryURLReader(
      new Map([[`${ROOT}m.malloy`, model]]),
   );
   const runtime = new Runtime({ urlReader, connections });
   const materializer = runtime.loadModel(new URL(`${ROOT}m.malloy`), {
      importBaseURL: new URL(ROOT),
   });
   const compiled = await materializer.getModel();
   /* eslint-disable @typescript-eslint/no-explicit-any */
   return (compiled as any)._modelDef as ModelDef;
   /* eslint-enable @typescript-eslint/no-explicit-any */
}

function source(modelDef: ModelDef, name: string): SourceDef {
   const found = modelDef.contents[name];
   if (!found) throw new Error(`no source named ${name} in compiled model`);
   return found as SourceDef;
}

/** Like {@link compileModel}, but for a multi-file import chain — needed to
 *  reach a marker that Malloy stores only in `annotations.inherits`, on a
 *  declaring base struct that never appears in the entry model's own
 *  `modelDef.contents`. */
async function compileModelFiles(
   files: Readonly<Record<string, string>>,
   entry: string,
): Promise<ModelDef> {
   const urlReader = new InMemoryURLReader(
      new Map(
         Object.entries(files).map(([name, text]) => [`${ROOT}${name}`, text]),
      ),
   );
   const runtime = new Runtime({ urlReader, connections });
   const materializer = runtime.loadModel(new URL(`${ROOT}${entry}`), {
      importBaseURL: new URL(ROOT),
   });
   const compiled = await materializer.getModel();
   /* eslint-disable @typescript-eslint/no-explicit-any */
   return (compiled as any)._modelDef as ModelDef;
   /* eslint-enable @typescript-eslint/no-explicit-any */
}

describe("assertNoFanoutFieldPath", () => {
   it("refuses a path through a join_many", async () => {
      const modelDef = await compileModel(`
source: child is duckdb.sql("select 1 as parent_id, 'north' as name") extend {}

source: parent is duckdb.sql("select 1 as id") extend {
   join_many: kids is child on id = kids.parent_id
}
`);
      expect(() =>
         assertNoFanoutFieldPath(
            "parent",
            source(modelDef, "parent"),
            "kids.name",
            ["kids", "name"],
         ),
      ).toThrow(AuthorizeGrammarError);
      try {
         assertNoFanoutFieldPath(
            "parent",
            source(modelDef, "parent"),
            "kids.name",
            ["kids", "name"],
         );
         throw new Error("expected a throw");
      } catch (err) {
         expect((err as AuthorizeGrammarError).rejectionCause).toBe(
            "fanout_path",
         );
      }
   });

   it("admits a path through a join_one", async () => {
      const modelDef = await compileModel(`
source: child is duckdb.sql("select 1 as id, 'north' as name") extend {}

source: parent is duckdb.sql("select 1 as child_id") extend {
   join_one: kid is child on child_id = kid.id
}
`);
      expect(() =>
         assertNoFanoutFieldPath(
            "parent",
            source(modelDef, "parent"),
            "kid.name",
            ["kid", "name"],
         ),
      ).not.toThrow();
   });

   // Must-fix 1: a naive `fieldPath.split(".")` downstream would compare the
   // RAW (still-backtick-quoted) segment "`cost center`" against the join's
   // real name "cost center" and never match, reading a genuine fan-out join
   // as fully resolved and silently admitting it. `parseAuthorizeGrammarBody`
   // pre-splits and unquotes the segments precisely so this cannot happen;
   // this test pins that against a real fan-out join through a backtick-
   // quoted name, not just the segment splitter in isolation.
   it("refuses a path through a backtick-quoted join_many segment", async () => {
      const modelDef = await compileModel(`
source: child is duckdb.sql("select 1 as parent_id, 'north' as name") extend {}

source: parent is duckdb.sql("select 1 as id") extend {
   join_many: \`cost center\` is child on id = \`cost center\`.parent_id
}
`);
      const terms = parseAuthorizeGrammarBody(
         "parent",
         "`cost center`.name in $GROUPS",
         LIST_GIVENS,
         ACCESS_FILTER_ROUTE,
      );
      const term = terms[0];
      if (term.scope !== "row_level") throw new Error("expected row_level");
      expect(() =>
         assertNoFanoutFieldPath(
            "parent",
            source(modelDef, "parent"),
            term.fieldPath,
            term.fieldPathSegments,
         ),
      ).toThrow(AuthorizeGrammarError);
      try {
         assertNoFanoutFieldPath(
            "parent",
            source(modelDef, "parent"),
            term.fieldPath,
            term.fieldPathSegments,
         );
         throw new Error("expected a throw");
      } catch (err) {
         expect((err as AuthorizeGrammarError).rejectionCause).toBe(
            "fanout_path",
         );
      }
   });

   // Must-fix 1's other half: a segment this walk cannot resolve against the
   // struct at all must fail closed, not be read as "no fan-out here" —
   // exactly the bug a stripped-quote mismatch would otherwise produce.
   it("refuses a path whose segment does not resolve against the struct", async () => {
      const modelDef = await compileModel(`
source: parent is duckdb.sql("select 1 as id") extend {}
`);
      expect(() =>
         assertNoFanoutFieldPath(
            "parent",
            source(modelDef, "parent"),
            "nope.name",
            ["nope", "name"],
         ),
      ).toThrow(AuthorizeGrammarError);
   });
});

describe("retired-route markers", () => {
   it("a leftover #(partition) marker on the source line is caught", async () => {
      const modelDef = await compileModel(`
#(partition) org_id = $ORG
source: X is duckdb.sql("select 1 as org_id") extend {
   measure: c is count()
}
`);
      const found = collectRetiredRouteMarkers(modelDef);
      expect(found.length).toBeGreaterThan(0);
      expect(() => assertNoRetiredRouteMarkers(found)).toThrow();
   });

   it("a marker one level too low (on a field, not the source) is still caught", async () => {
      const modelDef = await compileModel(`
source: X is duckdb.sql("select 1 as org_id") extend {
   #(partition) org_id = $ORG
   measure: c is count()
}
`);
      const found = collectRetiredRouteMarkers(modelDef);
      expect(found.length).toBeGreaterThan(0);
      expect(() => assertNoRetiredRouteMarkers(found)).toThrow();
   });

   it("a marker on a joined source is caught through the join", async () => {
      const modelDef = await compileModel(`
source: Child is duckdb.sql("select 1 as org_id, 1 as k") extend {
   #(partition) org_id = $ORG
   measure: c is count()
}
source: X is duckdb.sql("select 1 as k") extend {
   join_one: Child on k = Child.k
   measure: c is count()
}
`);
      const found = collectRetiredRouteMarkers(modelDef);
      expect(found.some((f) => f.includes('"X"'))).toBe(true);
      expect(() => assertNoRetiredRouteMarkers(found)).toThrow();
   });

   it("a model with no retired marker is clean", async () => {
      const modelDef = await compileModel(`
source: X is duckdb.sql("select 1 as org_id") extend {
   measure: c is count()
}
`);
      expect(collectRetiredRouteMarkers(modelDef)).toEqual([]);
   });

   // Also fix 3: `#(authorize)`'s own misplaced-annotation check
   // (`assertNoMisplacedAuthorizeAnnotations`) already covers a top-level
   // `query:` and the file level; the retired-route sweep did not, so a
   // leftover `#(partition)` in either position loaded clean.
   it("a leftover #(partition) marker on a top-level query: is caught", async () => {
      const modelDef = await compileModel(`
source: X is duckdb.sql("select 1 as org_id") extend {
   measure: c is count()
}

#(partition) org_id = $ORG
query: secret is X -> { aggregate: c is count() }
`);
      const found = collectRetiredRouteMarkers(modelDef);
      expect(found.some((f) => f.includes('"secret"'))).toBe(true);
      expect(() => assertNoRetiredRouteMarkers(found)).toThrow();
   });

   it("a leftover file-level ##(partition) marker is caught", async () => {
      const modelDef = await compileModel(`
##(partition) org_id = $ORG
source: X is duckdb.sql("select 1 as org_id") extend {
   measure: c is count()
}
`);
      const found = collectRetiredRouteMarkers(modelDef);
      expect(found.some((f) => f.includes("model itself"))).toBe(true);
      expect(() => assertNoRetiredRouteMarkers(found)).toThrow();
   });

   // Gap (a): the block-annotation form (`##|...|#`) lands on `blockNotes`,
   // which a caller reading only `.notes` never sees — see `annotations.ts`'s
   // own warning that reading one key "silently skips the latter two forms".
   it("a leftover file-level ##|(partition)|# BLOCK marker is caught", async () => {
      const modelDef = await compileModel(`
##|(partition)
org_id = $ORG
|##
source: X is duckdb.sql("select 1 as org_id") extend {
   measure: c is count()
}
`);
      const found = collectRetiredRouteMarkers(modelDef);
      expect(found.some((f) => f.includes("model itself"))).toBe(true);
      expect(() => assertNoRetiredRouteMarkers(found)).toThrow();
   });

   // Gap (b): a model that reaches a partitioned source only through a
   // transitive import, where the intermediate derivation carries its own
   // annotation, stores the marker ONLY in `annotations.inherits` — and the
   // declaring base (here, `Base`) never appears in the entry model's own
   // `modelDef.contents` at all (confirmed below). A caller reading only the
   // struct's own-level notes would see "mid own doc" and stop there.
   it("a #(partition) marker reachable only through an inherited annotation chain is caught", async () => {
      const modelDef = await compileModelFiles(
         {
            "base.malloy": `
#(partition) org_id = $ORG
source: Base is duckdb.sql("select 1 as org_id") extend {
   measure: c is count()
}
`,
            "mid.malloy": `
import "base.malloy"
#(doc) mid own doc
source: Mid is Base extend {
   measure: c2 is count()
}
`,
            "entry.malloy": `
import "mid.malloy"
source: Entry is Mid extend {}
`,
         },
         "entry.malloy",
      );
      // The declaring base is reached only via the annotation chain, not as
      // its own entry — pinning the shape this test exists to cover.
      expect(Object.keys(modelDef.contents)).not.toContain("Base");
      const found = collectRetiredRouteMarkers(modelDef);
      expect(found.some((f) => f.includes('"Entry"'))).toBe(true);
      expect(() => assertNoRetiredRouteMarkers(found)).toThrow();
   });
});
