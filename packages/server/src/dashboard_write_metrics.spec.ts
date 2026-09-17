// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";

import {
   recordDashboardWrite,
   resetDashboardWriteMetricsForTest,
   type DashboardWriteOutcome,
} from "./dashboard_write_metrics";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "./test_helpers/metrics_harness";

describe("dashboard_write_metrics", () => {
   let harness: MetricsHarness;

   beforeEach(async () => {
      harness = await startMetricsHarness();
      // Drop cached instruments so they re-init against the new provider;
      // otherwise this test's writes go to instruments bound to the previous
      // provider's reader.
      resetDashboardWriteMetricsForTest();
   });

   afterEach(async () => {
      resetDashboardWriteMetricsForTest();
      await harness.shutdown();
   });

   it("counts each attempt once, labelled by outcome", async () => {
      recordDashboardWrite("created", 120);
      recordDashboardWrite("replaced", 95);
      recordDashboardWrite("replaced", 80);
      recordDashboardWrite("conflict", 4);

      expect(
         await harness.collectCounter("publisher_dashboard_writes_total", {
            outcome: "replaced",
         }),
      ).toBe(2);
      expect(
         await harness.collectCounter("publisher_dashboard_writes_total", {
            outcome: "created",
         }),
      ).toBe(1);
      expect(
         await harness.collectCounter("publisher_dashboard_writes_total", {
            outcome: "conflict",
         }),
      ).toBe(1);
   });

   it("records the duration against the same outcome label", async () => {
      recordDashboardWrite("created", 100);
      recordDashboardWrite("created", 300);

      const created = await harness.collectHistogram(
         "publisher_dashboard_write_duration_ms",
         { outcome: "created" },
      );
      expect(created.count).toBe(2);
      expect(created.sum).toBe(400);
   });

   /**
    * The rollback is the reason this module exists: a write that compiled,
    * landed, and then failed to reload leaves the author certain they saved
    * something the package is not serving. It is rare by design, so "rare" and
    * "started happening after the upgrade" are only distinguishable by a
    * counter that is definitely being written.
    */
   it("counts a rollback under its own outcome, not with the refusals", async () => {
      recordDashboardWrite("refused", 1);
      recordDashboardWrite("rolled_back", 900);

      expect(
         await harness.collectCounter("publisher_dashboard_writes_total", {
            outcome: "rolled_back",
         }),
      ).toBe(1);
      expect(
         await harness.collectCounter("publisher_dashboard_writes_total", {
            outcome: "refused",
         }),
      ).toBe(1);
   });

   it("emits every outcome the type allows, so none is unreachable by a dashboard", async () => {
      // A label an operator cannot chart is worse than no label: the panel
      // reads as zero rather than as missing.
      const outcomes: DashboardWriteOutcome[] = [
         "created",
         "replaced",
         "conflict",
         "compile_failed",
         "refused",
         "rolled_back",
      ];
      for (const outcome of outcomes) recordDashboardWrite(outcome, 10);
      for (const outcome of outcomes) {
         expect(
            await harness.collectCounter("publisher_dashboard_writes_total", {
               outcome,
            }),
         ).toBe(1);
      }
   });
});
