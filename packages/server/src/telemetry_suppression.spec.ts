// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import {
   lazyCounter,
   lazyHistogram,
   resetInstrumentsForTesting,
   withoutMetrics,
} from "./telemetry";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "./test_helpers/metrics_harness";

const counter = lazyCounter("publisher_test_suppression_total", "test");
const histogram = lazyHistogram("publisher_test_suppression_ms", "test");

describe("withoutMetrics", () => {
   let harness: MetricsHarness;
   beforeEach(async () => {
      harness = await startMetricsHarness();
      resetInstrumentsForTesting();
   });
   afterEach(async () => {
      await harness.shutdown();
   });

   it("records nothing through everything the work awaits, and everything outside it", async () => {
      counter().add(1);
      histogram().record(5);
      await withoutMetrics(async () => {
         counter().add(10);
         await new Promise((r) => setTimeout(r, 1));
         histogram().record(50);
      });
      // Concurrent work outside the context still records.
      await Promise.all([
         withoutMetrics(async () => counter().add(100)),
         (async () => counter().add(2))(),
      ]);
      expect(
         await harness.collectCounter("publisher_test_suppression_total"),
      ).toBe(3);
      expect(
         (await harness.collectHistogram("publisher_test_suppression_ms"))
            .count,
      ).toBe(1);
   });
});
