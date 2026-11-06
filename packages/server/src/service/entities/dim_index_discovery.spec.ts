// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, test } from "bun:test";
import assert from "node:assert";

import {
   assertAtMostOneRequiredFilterPerIndexedDimension,
   assertNoRequiredParametersOnIndexedDimension,
   assertIndexedFieldIsDimension,
   assertNoForbiddenIndexPolicyKeys,
   type DimIndexCandidate,
} from "./dim_index_discovery";

/** Build a candidate with sensible empties so each test states only what it exercises. */
function candidate(
   overrides: Partial<DimIndexCandidate> = {},
): DimIndexCandidate {
   return {
      sourceName: "orders",
      fieldName: "carrier",
      requiredFilterDimensions: [],
      gateDimensions: [],
      requiredParameterNames: [],
      ...overrides,
   };
}

describe("assertAtMostOneRequiredFilterPerIndexedDimension", () => {
   test("throws when an indexed dimension has more than one required filter", () => {
      const candidates = [
         candidate({ requiredFilterDimensions: ["tier", "region"] }),
      ];

      assert.throws(
         () =>
            assertAtMostOneRequiredFilterPerIndexedDimension(
               candidates,
               "orders.malloy",
            ),
         /at most one required filter/i,
      );
   });

   test("does not count access_filter gate columns as required filters", () => {
      const candidates = [
         candidate({
            requiredFilterDimensions: ["tier", "report_id", "account.region"],
            gateDimensions: ["report_id", "account.region"],
         }),
      ];

      assert.doesNotThrow(() =>
         assertAtMostOneRequiredFilterPerIndexedDimension(
            candidates,
            "orders.malloy",
         ),
      );
   });

   test("still rejects two declared filters when a gate column is also present", () => {
      const candidates = [
         candidate({
            requiredFilterDimensions: ["tier", "region", "report_id"],
            gateDimensions: ["report_id"],
         }),
      ];

      assert.throws(
         () =>
            assertAtMostOneRequiredFilterPerIndexedDimension(
               candidates,
               "orders.malloy",
            ),
         /2 required filters \(tier, region\)/,
      );
   });

   test("allows exactly one required filter", () => {
      const candidates = [candidate({ requiredFilterDimensions: ["tier"] })];

      assert.doesNotThrow(() =>
         assertAtMostOneRequiredFilterPerIndexedDimension(
            candidates,
            "orders.malloy",
         ),
      );
   });

   test("allows zero required filters", () => {
      const candidates = [candidate({ requiredFilterDimensions: [] })];

      assert.doesNotThrow(() =>
         assertAtMostOneRequiredFilterPerIndexedDimension(
            candidates,
            "orders.malloy",
         ),
      );
   });
});

describe("assertNoRequiredParametersOnIndexedDimension", () => {
   test("throws when an indexed dimension's source has a required parameter", () => {
      // A source like `source: report(report_id::number)`.
      const candidates = [
         candidate({
            sourceName: "report",
            fieldName: "platform",
            requiredParameterNames: ["report_id"],
         }),
      ];

      assert.throws(
         () =>
            assertNoRequiredParametersOnIndexedDimension(
               candidates,
               "report.malloy",
            ),
         /required parameter.*report_id|#\(index\) dimension may not be declared on a parameterized source/i,
      );
   });

   test("names every required parameter when the source declares several", () => {
      // A source with several required parameters: `period_report(p_region, p_period, p_end)`.
      const candidates = [
         candidate({
            sourceName: "period_report",
            fieldName: "period_mode",
            requiredParameterNames: ["p_region", "p_period", "p_end"],
         }),
      ];

      assert.throws(
         () =>
            assertNoRequiredParametersOnIndexedDimension(
               candidates,
               "is_report.malloy",
            ),
         /p_region, p_period, p_end/,
      );
   });

   test("allows an indexed dimension on a non-parameterized source", () => {
      const candidates = [candidate({ requiredParameterNames: [] })];

      assert.doesNotThrow(() =>
         assertNoRequiredParametersOnIndexedDimension(
            candidates,
            "orders.malloy",
         ),
      );
   });
});

describe("assertIndexedFieldIsDimension", () => {
   const base = {
      sourceName: "orders",
      fieldName: "total",
      modelFilePath: "orders.malloy",
   };

   test("rejects #(index) on a non-dimension (measure/view/join)", () => {
      assert.throws(
         () =>
            assertIndexedFieldIsDimension({
               ...base,
               isDimension: false,
            }),
         /is not a dimension|only dimension\/column fields can be indexed/i,
      );
   });

   test("allows #(index) on a dimension", () => {
      // Non-public fields never reach this assert — they are skipped upstream
      // in collectDimIndexCandidates — so it only guards the placement (kind)
      // of a public tagged field.
      assert.doesNotThrow(() =>
         assertIndexedFieldIsDimension({
            ...base,
            fieldName: "carrier",
            isDimension: true,
         }),
      );
   });
});

describe("assertNoForbiddenIndexPolicyKeys", () => {
   test("throws when the #(index) declares schedule=", () => {
      assert.throws(
         () =>
            assertNoForbiddenIndexPolicyKeys(
               candidate(),
               ['#(index schedule="0 */6 * * *")'],
               "orders.malloy",
            ),
         /schedule.*package-level|Remove schedule= from #\(index\)/i,
      );
   });

   test("throws when the #(index) declares sharing=", () => {
      assert.throws(
         () =>
            assertNoForbiddenIndexPolicyKeys(
               candidate(),
               ["#(index sharing=private)"],
               "orders.malloy",
            ),
         /scope is a package-level mode|Remove sharing= from #\(index\)/i,
      );
   });

   test("allows an #(index) with only freshness/refresh", () => {
      assert.doesNotThrow(() =>
         assertNoForbiddenIndexPolicyKeys(
            candidate(),
            ['#(index freshness.window="24h" refresh=full)'],
            "orders.malloy",
         ),
      );
   });

   test("allows a bare #(index)", () => {
      assert.doesNotThrow(() =>
         assertNoForbiddenIndexPolicyKeys(
            candidate(),
            ["#(index)"],
            "orders.malloy",
         ),
      );
   });
});
