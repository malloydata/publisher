// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, test } from "bun:test";
import assert from "node:assert";

import {
   parseIndexPolicy,
   findForbiddenIndexPolicyKeys,
} from "./index_annotation_parsing";

describe("parseIndexPolicy", () => {
   test("returns null for a bare #(index) with no policy keys", () => {
      assert.strictEqual(parseIndexPolicy(["#(index)"]), null);
      assert.strictEqual(parseIndexPolicy(["#( index )"]), null);
   });

   test("returns null when no #(index) tagline is present", () => {
      assert.strictEqual(parseIndexPolicy([]), null);
      assert.strictEqual(
         parseIndexPolicy(["#(filter) dimension=status type=equal"]),
         null,
      );
   });

   test("parses a freshness window", () => {
      assert.deepStrictEqual(
         parseIndexPolicy(['#(index freshness.window="24h")']),
         {
            freshness: { window: "24h" },
         },
      );
   });

   test("parses window + fallback together", () => {
      assert.deepStrictEqual(
         parseIndexPolicy([
            '#(index freshness.window="7d" freshness.fallback=stale_ok)',
         ]),
         { freshness: { window: "7d", fallback: "stale_ok" } },
      );
   });

   test("parses refresh", () => {
      assert.deepStrictEqual(parseIndexPolicy(["#(index refresh=full)"]), {
         refresh: "full",
      });
   });

   test("ignores no-longer-supported sharing/schedule keys (rejected elsewhere)", () => {
      // parseIndexPolicy no longer carries scope/schedule; presence is a
      // publish rejection via findForbiddenIndexPolicyKeys, not part of policy.
      assert.deepStrictEqual(
         parseIndexPolicy([
            '#(index sharing="private" refresh=full schedule="0 */6 * * *")',
         ]),
         { refresh: "full" },
      );
   });

   test("is robust to whitespace inside the parens", () => {
      assert.deepStrictEqual(
         parseIndexPolicy([
            '#(  index   refresh=full   freshness.window="1h" )',
         ]),
         { refresh: "full", freshness: { window: "1h" } },
      );
   });

   test("ignores unknown keys (index_values n=)", () => {
      assert.deepStrictEqual(
         parseIndexPolicy(['#(index_values n=200 freshness.window="1h")']),
         { freshness: { window: "1h" } },
      );
   });

   test("drops invalid enum values (reported absent, never defaulted)", () => {
      assert.strictEqual(
         parseIndexPolicy(["#(index refresh=sometimes)"]),
         null,
      );
      assert.strictEqual(
         parseIndexPolicy(["#(index freshness.fallback=maybe)"]),
         null,
      );
      // A dropped invalid enum leaves the still-valid siblings intact.
      assert.deepStrictEqual(
         parseIndexPolicy(['#(index refresh=bogus freshness.window="1h")']),
         { freshness: { window: "1h" } },
      );
   });

   test("reads the #(index) tagline even when other taglines precede it", () => {
      assert.deepStrictEqual(
         parseIndexPolicy([
            "#(filter) dimension=status type=equal",
            '#(index freshness.window="12h")',
         ]),
         { freshness: { window: "12h" } },
      );
   });
});

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
