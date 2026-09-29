// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";

import {
   recordAuthorizeAdmitAllGate,
   recordAuthorizeBypass,
   recordAuthorizeGuardRejection,
   recordLockDecision,
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

   it("publisher_authorize_lock_total ticks per call, labeled by decision", async () => {
      // The two denials are separate labels on purpose: both are a 403, but
      // only `denied_unresolvable` means the gate could not be decided, and it
      // is the one worth alerting on. Folding either into the other — or onto
      // `publisher_authorize_row_level_total`, whose `denied_by_gate` an
      // operator already alerts on — makes routine traffic page someone.
      recordLockDecision("admitted");
      recordLockDecision("denied_by_lock");
      recordLockDecision("denied_by_lock");
      recordLockDecision("denied_unresolvable");

      expect(
         await harness.collectCounter("publisher_authorize_lock_total", {
            decision: "admitted",
         }),
      ).toBe(1);
      expect(
         await harness.collectCounter("publisher_authorize_lock_total", {
            decision: "denied_by_lock",
         }),
      ).toBe(2);
      expect(
         await harness.collectCounter("publisher_authorize_lock_total", {
            decision: "denied_unresolvable",
         }),
      ).toBe(1);
      // A lock decision must not land on the row-level counter.
      expect(
         await harness.collectCounter("publisher_authorize_row_level_total", {
            decision: "denied_by_gate",
         }),
      ).toBe(0);
   });

   it("row-level and lock decisions carry a site label, entry_point by default", async () => {
      recordRowLevelGateDecision("denied_by_gate");
      recordRowLevelGateDecision("denied_by_gate", "caller_join");
      recordLockDecision("admitted", "caller_join");
      recordLockDecision("admitted");
      recordLockDecision("admitted");

      expect(
         await harness.collectCounter("publisher_authorize_row_level_total", {
            decision: "denied_by_gate",
            site: "entry_point",
         }),
      ).toBe(1);
      expect(
         await harness.collectCounter("publisher_authorize_row_level_total", {
            decision: "denied_by_gate",
            site: "caller_join",
         }),
      ).toBe(1);
      expect(
         await harness.collectCounter("publisher_authorize_lock_total", {
            decision: "admitted",
            site: "caller_join",
         }),
      ).toBe(1);
      expect(
         await harness.collectCounter("publisher_authorize_lock_total", {
            decision: "admitted",
            site: "entry_point",
         }),
      ).toBe(2);
   });

   it("publisher_authorize_row_level_rejected_total ticks per call, labeled by cause", async () => {
      recordRowLevelGateRejected("unreachable_given");
      recordRowLevelGateRejected("unreachable_given");
      recordRowLevelGateRejected("entry_point_unexpressible");
      recordRowLevelGateRejected("source_line_gate_no_given_reference");
      recordRowLevelGateRejected("source_line_gate_negated_membership");
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
            { cause: "source_line_gate_no_given_reference" },
         ),
      ).toBe(1);
      expect(
         await harness.collectCounter(
            "publisher_authorize_row_level_rejected_total",
            { cause: "source_line_gate_negated_membership" },
         ),
      ).toBe(1);
      expect(
         await harness.collectCounter(
            "publisher_authorize_row_level_rejected_total",
            { cause: "legacy_string_gate" },
         ),
      ).toBe(1);
   });

   it("publisher_authorize_admit_all_total ticks per call, labeled by route", async () => {
      recordAuthorizeAdmitAllGate("access_filter");
      recordAuthorizeAdmitAllGate("access_filter");
      recordAuthorizeAdmitAllGate("authorize");

      expect(
         await harness.collectCounter("publisher_authorize_admit_all_total", {
            route: "access_filter",
         }),
      ).toBe(2);
      expect(
         await harness.collectCounter("publisher_authorize_admit_all_total", {
            route: "authorize",
         }),
      ).toBe(1);
   });

   it("resetAuthorizeGuardTelemetryForTesting drops the cached admit-all instrument", async () => {
      recordAuthorizeAdmitAllGate("access_filter");
      expect(
         await harness.collectCounter("publisher_authorize_admit_all_total", {
            route: "access_filter",
         }),
      ).toBe(1);

      resetAuthorizeGuardTelemetryForTesting();
      const freshHarness = await startMetricsHarness();
      try {
         // A fresh provider with no prior emissions sees nothing until this
         // call re-inits the instrument against it — proves the OLD cached
         // instrument (bound to the first harness's reader) was dropped.
         recordAuthorizeAdmitAllGate("access_filter");
         expect(
            await freshHarness.collectCounter(
               "publisher_authorize_admit_all_total",
               { route: "access_filter" },
            ),
         ).toBe(1);
      } finally {
         await freshHarness.shutdown();
      }
   });
});
