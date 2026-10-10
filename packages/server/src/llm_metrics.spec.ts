// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import {
   recordLlmCall,
   recordLlmJsonRepair,
   recordLlmRetry,
   recordLlmTokens,
   resetLlmTelemetryForTesting,
} from "./llm_metrics";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "./test_helpers/metrics_harness";

describe("llm_metrics", () => {
   let harness: MetricsHarness;

   beforeEach(async () => {
      harness = await startMetricsHarness();
      resetLlmTelemetryForTesting();
   });

   afterEach(async () => {
      resetLlmTelemetryForTesting();
      await harness.shutdown();
   });

   it("counts calls by outcome and records a failure reason only on failure", async () => {
      recordLlmCall("anthropic", "chat", 12);
      recordLlmCall("anthropic", "chat", 30, "rate_limited");
      expect(
         await harness.collectCounter("publisher_llm_calls_total", {
            outcome: "success",
         }),
      ).toBe(1);
      expect(
         await harness.collectCounter("publisher_llm_calls_total", {
            outcome: "failure",
         }),
      ).toBe(1);
      expect(
         await harness.collectCounter("publisher_llm_failures_total", {
            reason: "rate_limited",
         }),
      ).toBe(1);
      const latency = await harness.collectHistogram(
         "publisher_llm_call_duration_ms",
      );
      expect(latency?.count).toBe(2);
      expect(latency?.sum).toBe(42);
   });

   it("splits tokens by direction and skips absent values", async () => {
      recordLlmTokens("google", { inputTokens: 5, outputTokens: 2 });
      recordLlmTokens("google", {});
      expect(
         await harness.collectCounter("publisher_llm_tokens_total", {
            direction: "input",
         }),
      ).toBe(5);
      expect(
         await harness.collectCounter("publisher_llm_tokens_total", {
            direction: "output",
         }),
      ).toBe(2);
   });

   it("counts retries and JSON repairs", async () => {
      recordLlmRetry("openai", "embedding");
      recordLlmJsonRepair("openai");
      expect(await harness.collectCounter("publisher_llm_retries_total")).toBe(
         1,
      );
      expect(
         await harness.collectCounter("publisher_llm_json_repairs_total"),
      ).toBe(1);
   });
});
