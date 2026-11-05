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
