// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, test } from "bun:test";
import assert from "node:assert";
import vectors from "./__fixtures__/access-filter-grammar-vectors.json";
import {
   AuthorizeGrammarError,
   parseAuthorizeGrammarBody,
} from "../authorize_grammar";
import {
   parseAccessFilterBody,
   parseGateAnnotation,
   resolveGates,
   type GateStructDef,
} from "./access_gates";

describe("parseAccessFilterBody conformance with the authorize grammar", () => {
   // `parseAuthorizeGrammarBody` accepts a backtick segment containing a dot; a dotted `field_path`
   // can't round-trip it, so it is refused here and stays opaque (safe direction).
   const NARROWER = new Map([
      ["account.`cost.center` = $REGION", "unsupported_field_path"],
   ]);

   for (const v of vectors.vectors) {
      test(`\`${v.body}\` ${v.expect.ok ? "parses" : `is refused (${v.expect.cause})`}`, () => {
         const got = parseAccessFilterBody(
            v.body,
            new Map(Object.entries(v.givens)),
         );
         const narrowed = NARROWER.get(v.body);
         if (narrowed) {
            assert.deepStrictEqual(got, { ok: false, cause: narrowed });
         } else if (v.expect.ok) {
            assert.deepStrictEqual(got, { ok: true, terms: v.expect.terms });
         } else {
            assert.deepStrictEqual(got, { ok: false, cause: v.expect.cause });
         }
      });
   }

   test("the vector set exercises both outcomes, so a parser that refused everything would fail", () => {
      assert.ok(vectors.vectors.filter((v) => v.expect.ok).length >= 10);
      assert.ok(vectors.vectors.filter((v) => !v.expect.ok).length >= 20);
   });
});

describe("the recorded vectors still describe the server's live grammar", () => {
   // The vectors were recorded from `parseAuthorizeGrammarBody`; re-deriving
   // each one live means a grammar change fails here instead of drifting.
   for (const v of vectors.vectors) {
      test(`\`${v.body}\``, () => {
         let live:
            | {
                 ok: true;
                 terms: { field_path: string; given: string; op: string }[];
              }
            | { ok: false; cause: string };
         try {
            const terms = parseAuthorizeGrammarBody(
               "s",
               v.body,
               new Map(Object.entries(v.givens)),
               "access_filter",
            );
            live = {
               ok: true,
               terms: terms.flatMap((t) =>
                  t.scope === "row_level"
                     ? [
                          {
                             field_path: t.fieldPathSegments.join("."),
                             given: t.given,
                             op: t.operator,
                          },
                       ]
                     : [],
               ),
            };
         } catch (error) {
            if (!(error instanceof AuthorizeGrammarError)) throw error;
            live = { ok: false, cause: error.rejectionCause };
         }
         if (v.expect.ok) {
            assert.deepStrictEqual(live, { ok: true, terms: v.expect.terms });
         } else {
            assert.deepStrictEqual(live, { ok: false, cause: v.expect.cause });
         }
      });
   }
});

describe("parseGateAnnotation", () => {
   test("reads the route from every bracket spelling", () => {
      assert.deepStrictEqual(parseGateAnnotation("#(authorize) false"), {
         route: "authorize",
         expr: "false",
      });
      assert.deepStrictEqual(parseGateAnnotation("#[access_filter] a = $A"), {
         route: "access_filter",
         expr: "a = $A",
      });
      assert.deepStrictEqual(
         parseGateAnnotation('##|<access_filter> "a = $A"'),
         {
            route: "access_filter",
            expr: "a = $A",
         },
      );
   });

   test("ignores a near-miss route name", () => {
      assert.strictEqual(
         parseGateAnnotation("#(access_filtered) a = $A"),
         null,
      );
      assert.strictEqual(parseGateAnnotation("#(authorized-users) a"), null);
   });

   test("a spelling Malloy does not route as a gate is a near miss and throws (callers fail closed)", () => {
      // Malloy's routing decides, as on the query path; the load refuses
      // these, and a reader that meets one anyway denies rather than
      // reporting the source ungated.
      for (const text of [
         "# (authorize) false",
         "#( authorize ) false",
         "#(AUTHORIZE) false",
         "#( access_filter ) a = $A",
      ]) {
         assert.throws(() => parseGateAnnotation(text), /near miss/);
      }
   });

   test("an ordinary render tag is not a gate", () => {
      assert.strictEqual(parseGateAnnotation("# bar_chart"), null);
      assert.strictEqual(parseGateAnnotation("#(doc) about authorize"), null);
   });
});

describe("resolveGates", () => {
   const note = (text: string) => ({ text });
   const table = (
      blockNotes: string[],
      inherits?: GateStructDef["annotations"],
   ) =>
      ({
         type: "table",
         annotations: { blockNotes: blockNotes.map(note), inherits },
      }) satisfies GateStructDef;

   test("an own #(access_filter) does not shed an ancestor's #(authorize)", () => {
      const locked = table(["#(authorize) false"]);
      const child = table(["#(access_filter) a = $A"], locked.annotations);

      assert.deepStrictEqual(resolveGates(child, new Map()).gates, [
         { route: "authorize", expr: "false" },
         { route: "access_filter", expr: "a = $A" },
      ]);
   });

   test("an own #(authorize) does not shed an ancestor's #(access_filter)", () => {
      const filtered = table(["#(access_filter) a = $A"]);
      const child = table(["#(authorize) true"], filtered.annotations);

      assert.deepStrictEqual(resolveGates(child, new Map()).gates, [
         { route: "authorize", expr: "true" },
         { route: "access_filter", expr: "a = $A" },
      ]);
   });

   test("an own gate on the same route replaces the ancestor's", () => {
      const base = table(["#(access_filter) a = $A"]);
      const child = table(["#(access_filter) b = $B"], base.annotations);

      assert.deepStrictEqual(resolveGates(child, new Map()).gates, [
         { route: "access_filter", expr: "b = $B" },
      ]);
   });

   test("a derivation ANDs its own filter with its base's, per route", () => {
      const base = table(["#(access_filter) a = $A", "#(authorize) false"]);
      const derived: GateStructDef = {
         type: "query_source",
         annotations: { blockNotes: [note("#(access_filter) b = $B")] },
         query: { structRef: base },
      };

      assert.deepStrictEqual(resolveGates(derived, new Map()).gates, [
         { route: "authorize", expr: "false" },
         { route: "access_filter", expr: "b = $B" },
         { route: "access_filter", expr: "a = $A" },
      ]);
   });

   test("the same predicate copied onto a derivation is reported once", () => {
      const base = table(["#(access_filter) a = $A"]);
      const derived: GateStructDef = {
         type: "query_source",
         annotations: base.annotations,
         query: { structRef: base },
      };

      assert.deepStrictEqual(resolveGates(derived, new Map()).gates, [
         { route: "access_filter", expr: "a = $A" },
      ]);
   });

   test("a composite anywhere in the walk is flagged", () => {
      const composite: GateStructDef = {
         type: "composite",
         sources: [table(["#(access_filter) a = $A"])],
      };
      const derived: GateStructDef = {
         type: "query_source",
         query: { structRef: "c" },
      };

      assert.strictEqual(resolveGates(composite, new Map()).composite, true);
      assert.strictEqual(
         resolveGates(derived, new Map([["c", composite]])).composite,
         true,
      );
      assert.strictEqual(
         resolveGates(table(["#(access_filter) a = $A"]), new Map()).composite,
         false,
      );
   });

   test("an own-gated derivation whose string base is not in the lookup throws", () => {
      const derived: GateStructDef = {
         type: "query_source",
         // Own gates on BOTH routes, so neither route's walk reaches the base empty-handed.
         annotations: {
            blockNotes: [
               note("#(access_filter) b = $B"),
               note("#(authorize) true"),
            ],
         },
         query: { structRef: "deep_imported_base" },
      };

      assert.throws(
         () => resolveGates(derived, new Map()),
         /'deep_imported_base' did not resolve/,
      );
   });

   test("a malformed note of either route fails the whole resolution", () => {
      assert.throws(
         () => resolveGates(table(["#(access_filter)"]), new Map()),
         /empty expression body/,
      );
   });
});
