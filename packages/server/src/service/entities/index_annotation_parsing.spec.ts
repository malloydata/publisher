// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, test } from "bun:test";
import assert from "node:assert";

import { findForbiddenIndexPolicyKeys } from "./index_annotation_parsing";

describe("findForbiddenIndexPolicyKeys", () => {
   test("detects sharing=", () => {
      assert.deepStrictEqual(
         findForbiddenIndexPolicyKeys(["#(index sharing=private)"]),
         ["sharing"],
      );
   });

   test("detects schedule=", () => {
      assert.deepStrictEqual(
         findForbiddenIndexPolicyKeys(['#(index schedule="0 * * * *")']),
         ["schedule"],
      );
   });

   test("detects both sharing and schedule", () => {
      const found = findForbiddenIndexPolicyKeys([
         '#(index sharing=private schedule="0 * * * *")',
      ]);
      assert.ok(found.includes("sharing") && found.includes("schedule"));
   });

   test("returns empty for a policy with only freshness/refresh", () => {
      assert.deepStrictEqual(
         findForbiddenIndexPolicyKeys([
            '#(index freshness.window="1h" refresh=full)',
         ]),
         [],
      );
   });

   test("returns empty when no #(index) tagline is present", () => {
      assert.deepStrictEqual(findForbiddenIndexPolicyKeys([]), []);
   });
});

// Pins which taglines are read and what each reports. The accepted shapes are
// the ones `hasIndexTag` opts a field in by: `#(` (or `# (`) and the word
// `index` / `index_values`, at the object level, with the payload inside the
// parens (`#(index <body>)`, which Malloy calls a malformed route), after
// them (`#(index) <body>`), or glued to them (`#(index)<body>`).
describe("findForbiddenIndexPolicyKeys: accepted shapes", () => {
   const cases: Array<[string, string[]]> = [
      ["#(index sharing=private)", ["sharing"]],
      ["#(index) sharing=private", ["sharing"]],
      ["#( index_values n=200 sharing=x )", ["sharing"]],
      ['#(index_values) schedule="0 * * * *"', ["schedule"]],
      ["#(index n=5) schedule=x", ["schedule"]],
      ["#(index)sharing=x", ["sharing"]],
      ["# (index) sharing=x", ["sharing"]],
      ["  #(index) sharing=x  ", ["sharing"]],
      ["#(index) Sharing=x", ["sharing"]],
      ["#(index) sharing=@env.X", ["sharing"]],
      ["#(index) sharing='a b'", ["sharing"]],
      ["#(index) sharing=private n=", ["sharing"]],
      ["#(index) sharing=x sharing=y schedule=z", ["sharing", "schedule"]],
      ["#(index) n=200 refresh=full", []],
      // Not an index tagline: another bracket, a block, the model level, another word.
      ["#|(index) sharing=x", []],
      ["#[index] sharing=x", []],
      ["##(index) sharing=x", []],
      ["#(indexer) sharing=x", []],
      // An unclosed paren is no tagline at all.
      ["#(index sharing=x", []],
   ];
   for (const [tagline, expected] of cases) {
      test(tagline, () => {
         assert.deepStrictEqual(
            findForbiddenIndexPolicyKeys([tagline]),
            expected,
         );
      });
   }

   test("only the first tagline that reads is inspected", () => {
      assert.deepStrictEqual(
         findForbiddenIndexPolicyKeys(["#(index)", "#(index) sharing=x"]),
         [],
      );
      assert.deepStrictEqual(
         findForbiddenIndexPolicyKeys([
            "#(index sharing=x",
            "#(index) schedule=y",
         ]),
         ["schedule"],
      );
   });
});

// Payload edges a whitespace split and a tag parser read differently. The
// payload is read as a tag, the way every MOTLY reader reads it: a bare
// `sharing` or a spaced `sharing = x` declares the key (a whitespace split did
// not), and a quoted `"sharing=x"` or a nested `foo { sharing=x }` declares no
// top-level key (a whitespace split saw one), nor does a key deleted again.
describe("findForbiddenIndexPolicyKeys: payload edges", () => {
   const cases: Array<[string, string[]]> = [
      ["#(index) sharing", ["sharing"]],
      ["#(index) sharing = x", ["sharing"]],
      ['#(index) "sharing=x"', []],
      ["#(index) foo { sharing=x }", []],
      ["#(index) sharing=x -sharing", []],
   ];
   for (const [tagline, expected] of cases) {
      test(tagline, () => {
         assert.deepStrictEqual(
            findForbiddenIndexPolicyKeys([tagline]),
            expected,
         );
      });
   }
});
