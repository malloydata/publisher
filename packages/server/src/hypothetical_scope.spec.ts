// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import Transport from "winston-transport";
import { isHypothetical, runHypothetical } from "./hypothetical_scope";
import { logger } from "./logger";
import { lazyCounter, lazyHistogram } from "./telemetry";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "./test_helpers/metrics_harness";

/** A transport that keeps each entry's message. */
class Capture extends Transport {
   readonly messages: string[] = [];
   override log(info: { message?: unknown }, next: () => void): void {
      this.messages.push(String(info.message));
      next();
   }
}

describe("hypothetical work", () => {
   let harness: MetricsHarness;
   let capture: Capture;
   let level: string;

   beforeEach(async () => {
      harness = await startMetricsHarness();
      capture = new Capture();
      logger.add(capture);
      level = logger.level;
   });

   afterEach(async () => {
      logger.remove(capture);
      logger.level = level;
      await harness.shutdown();
   });

   it("is scoped to the call it wraps, through awaits", async () => {
      expect(isHypothetical()).toBe(false);
      const inside = await runHypothetical(async () => {
         await Promise.resolve();
         return isHypothetical();
      });
      expect(inside).toBe(true);
      expect(isHypothetical()).toBe(false);
   });

   it("drops every log line inside the scope, and none outside it", async () => {
      logger.level = "info";
      await runHypothetical(async () => {
         logger.info("hypothetical info");
         logger.warn("hypothetical warn");
         logger.error("hypothetical error");
      });
      logger.info("served info");
      expect(capture.messages).toEqual(["served info"]);
   });

   it("keeps the lines, marked, when debug is on", async () => {
      logger.level = "debug";
      await runHypothetical(async () => {
         logger.warn("hypothetical warn");
      });
      expect(capture.messages).toEqual(["hypothetical warn"]);
   });

   it("records no metric inside the scope", async () => {
      const counter = lazyCounter("publisher_test_hypothetical_total", "test");
      const histogram = lazyHistogram("publisher_test_hypothetical_ms", "test");
      await runHypothetical(async () => {
         counter().add(1);
         histogram().record(5);
      });
      counter().add(2);
      expect(
         await harness.collectCounter("publisher_test_hypothetical_total"),
      ).toBe(2);
   });
});
