// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, test } from "bun:test";
import assert from "node:assert";
import { parseAuthorizeGrammarBody } from "../authorize_grammar";
import {
   accessFilterTerms,
   parseGateAnnotation,
   resolveGates,
   type GateStructDef,
} from "./access_gates";

describe("accessFilterTerms", () => {
   // The grammar's own cases live in ../authorize_grammar.spec.ts; these pin
   // only the reshaping into the /entities wire shape.
   test("maps each row-level term to field_path, given and op, in order", () => {
      assert.deepStrictEqual(
         accessFilterTerms(
            "`region` = $REGION and org.id in $GROUPS",
            new Map([
               ["REGION", "string"],
               ["GROUPS", "array"],
            ]),
         ),
         [
            { field_path: "region", given: "REGION", op: "=" },
            { field_path: "org.id", given: "GROUPS", op: "in" },
         ],
      );
   });

   test("a backticked segment holding a dot is opaque: a dotted field_path cannot carry it", () => {
      // The grammar accepts it; the wire shape cannot say it.
      assert.deepStrictEqual(
         parseAuthorizeGrammarBody(
            "s",
            "account.`cost.center` = $REGION",
            new Map([["REGION", "string"]]),
            "access_filter",
         ).map((t) => (t.scope === "row_level" ? t.fieldPathSegments : [])),
         [["account", "cost.center"]],
      );
      assert.strictEqual(
         accessFilterTerms(
            "account.`cost.center` = $REGION",
            new Map([["REGION", "string"]]),
         ),
         undefined,
      );
   });

   test("a body the grammar refuses is opaque, never a throw", () => {
      for (const body of ["", "true", "region != $R", "'admin' = $ROLE"]) {
         assert.strictEqual(accessFilterTerms(body), undefined, body);
      }
   });
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

   test("an expression that starts with a string literal is read verbatim, as the load reads it", () => {
      // The load accepts `"admin" = $ROLE` as an ordinary expression; only a
      // payload that is ONE string literal is the refused legacy form.
      assert.deepStrictEqual(
         parseGateAnnotation('#(authorize) "admin" = $ROLE'),
         {
            route: "authorize",
            expr: '"admin" = $ROLE',
         },
      );
      assert.deepStrictEqual(
         parseGateAnnotation(
            `#(access_filter) 'us' = $REGION and region = $REGION`,
         ),
         {
            route: "access_filter",
            expr: `'us' = $REGION and region = $REGION`,
         },
      );
   });

   test("a payload that is one string literal is the legacy form, decoded to the expression it meant", () => {
      assert.deepStrictEqual(
         parseGateAnnotation(`#(authorize) "$ROLE = 'admin'"`),
         { route: "authorize", expr: "$ROLE = 'admin'" },
      );
      assert.deepStrictEqual(
         parseGateAnnotation(`#(authorize) "a = \\"b\\""`),
         { route: "authorize", expr: 'a = "b"' },
      );
   });

   test("an unbalanced quote throws (callers fail closed), as the load refuses it", () => {
      for (const text of [
         `#(authorize) "$ROLE = 'admin'`,
         `#(authorize) $ROLE = 'admin`,
      ]) {
         assert.throws(() => parseGateAnnotation(text), /mismatched quotes/);
      }
   });

   test("a quote inside a backtick-quoted identifier is part of the name, read verbatim as the load reads it", () => {
      for (const [text, route, expr] of [
         [
            "#(access_filter) `owner's region` = $X",
            "access_filter",
            "`owner's region` = $X",
         ],
         [
            '#(access_filter) `say "hi"` = $X',
            "access_filter",
            '`say "hi"` = $X',
         ],
         // Malloy's BQ_STRING escapes with a backslash, a backtick included.
         ["#(access_filter) `it\\`s` = $X", "access_filter", "`it\\`s` = $X"],
         [
            "#(authorize) `a'b` = $X and 'x' = $Y",
            "authorize",
            "`a'b` = $X and 'x' = $Y",
         ],
      ] as const) {
         assert.deepStrictEqual(
            parseGateAnnotation(text),
            { route, expr },
            text,
         );
      }
   });

   test("a stray quote outside every backtick identifier still throws", () => {
      for (const text of [
         "#(access_filter) `owner's region` = $X and region = 'us",
         "#(access_filter) `owner` region' = $X",
         // A backtick identifier does not span a line, so it hides nothing here.
         "#(access_filter) `owner's\nregion` = $X",
         // A backtick inside a string literal opens no identifier.
         "#(access_filter) 'a`b' = $X and `c'd = $Y",
      ]) {
         assert.throws(
            () => parseGateAnnotation(text),
            /mismatched quotes/,
            text,
         );
      }
   });

   test("an ordinary render tag is not a gate", () => {
      assert.strictEqual(parseGateAnnotation("# bar_chart"), null);
      assert.strictEqual(parseGateAnnotation("#(doc) about authorize"), null);
   });
});

describe("resolveGates", () => {
   test("a source gated on a backtick identifier holding an apostrophe is reported with that gate, not as locked", () => {
      const gated = {
         type: "table",
         annotations: {
            blockNotes: [{ text: "#(access_filter) `owner's region` = $X" }],
         },
      } satisfies GateStructDef;
      assert.deepStrictEqual(resolveGates(gated, new Map()).gates, [
         { route: "access_filter", expr: "`owner's region` = $X" },
      ]);
   });

   test("a source gated by an expression that starts with a string literal is reported with that gate, not as locked", () => {
      const gated = {
         type: "table",
         annotations: {
            blockNotes: [{ text: '#(authorize) "admin" = $ROLE' }],
         },
      } satisfies GateStructDef;
      // Before, the reader threw on the leading quote and the caller reported
      // the source as locked (`false`), a gate the server never applies.
      assert.deepStrictEqual(resolveGates(gated, new Map()).gates, [
         { route: "authorize", expr: '"admin" = $ROLE' },
      ]);
   });

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
