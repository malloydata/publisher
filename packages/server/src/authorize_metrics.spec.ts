// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";

import {
   recordAuthorizeBypass,
   recordAuthorizeGuardRejection,
   recordRowLevelGateDecision,
   recordRowLevelGateRejected,
   resetAuthorizeGuardTelemetryForTesting,
} from "./authorize_metrics";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "./test_helpers/metrics_harness";

describe("authorize_metrics", () => {
   let harness: MetricsHarness;

   beforeEach(async () => {
      harness = await startMetricsHarness();
      // Drop cached instruments so they re-init against the new provider;
      // otherwise this test's writes go to a counter bound to the previous
      // provider's reader.
      resetAuthorizeGuardTelemetryForTesting();
   });

   afterEach(async () => {
      resetAuthorizeGuardTelemetryForTesting();
      await harness.shutdown();
   });

   it("publisher_authorize_guard_rejected_total ticks per call, labeled by field", async () => {
      recordAuthorizeGuardRejection("query");
      recordAuthorizeGuardRejection("query");
      recordAuthorizeGuardRejection("source_name");

      expect(
         await harness.collectCounter(
            "publisher_authorize_guard_rejected_total",
            {
               field: "query",
            },
         ),
      ).toBe(2);
      expect(
         await harness.collectCounter(
            "publisher_authorize_guard_rejected_total",
            {
               field: "source_name",
            },
         ),
      ).toBe(1);
   });

   it("publisher_authorize_bypass_total ticks per call, labeled by entry_point", async () => {
      recordAuthorizeBypass("source");
      recordAuthorizeBypass("runnable");
      recordAuthorizeBypass("runnable");

      expect(
         await harness.collectCounter("publisher_authorize_bypass_total", {
            entry_point: "source",
         }),
      ).toBe(1);
      expect(
         await harness.collectCounter("publisher_authorize_bypass_total", {
            entry_point: "runnable",
         }),
      ).toBe(2);
   });

   it("publisher_authorize_row_level_total ticks per call, labeled by decision", async () => {
      recordRowLevelGateDecision("denied_by_gate");
      recordRowLevelGateDecision("denied_by_gate");
      recordRowLevelGateDecision("empty_after_filter");

      expect(
         await harness.collectCounter("publisher_authorize_row_level_total", {
            decision: "denied_by_gate",
         }),
      ).toBe(2);
      expect(
         await harness.collectCounter("publisher_authorize_row_level_total", {
            decision: "empty_after_filter",
         }),
      ).toBe(1);
   });

   it("publisher_authorize_row_level_rejected_total ticks per call, labeled by cause", async () => {
      recordRowLevelGateRejected("unreachable_given");
      recordRowLevelGateRejected("unreachable_given");
      recordRowLevelGateRejected("entry_point_unexpressible");
      recordRowLevelGateRejected("gate_dimension_no_given_reference");
      recordRowLevelGateRejected("gate_dimension_negated_membership");
      recordRowLevelGateRejected("legacy_string_gate");

      expect(
         await harness.collectCounter(
            "publisher_authorize_row_level_rejected_total",
            { cause: "unreachable_given" },
         ),
      ).toBe(2);
      expect(
         await harness.collectCounter(
            "publisher_authorize_row_level_rejected_total",
            { cause: "entry_point_unexpressible" },
         ),
      ).toBe(1);
      expect(
         await harness.collectCounter(
            "publisher_authorize_row_level_rejected_total",
            { cause: "gate_dimension_no_given_reference" },
         ),
      ).toBe(1);
      expect(
         await harness.collectCounter(
            "publisher_authorize_row_level_rejected_total",
            { cause: "gate_dimension_negated_membership" },
         ),
      ).toBe(1);
      expect(
         await harness.collectCounter(
            "publisher_authorize_row_level_rejected_total",
            { cause: "legacy_string_gate" },
         ),
      ).toBe(1);
   });
});
