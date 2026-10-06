// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Telemetry for calls to model vendors (chat and embeddings).
 *
 * An operator needs to answer "is the LLM answering, how slow is it, what is
 * it costing in tokens, and is it being rate limited?" without reading logs.
 * Labels are bounded: `provider` is one of a fixed list, `operation` is
 * `chat` or `embedding`, `reason` is a status class, never a message.
 *
 * Instruments are created lazily for the same reason as
 * {@link ./query_cap_metrics}: one created before `setGlobalMeterProvider`
 * binds to a NoOp meter.
 */

import { type Counter, type Histogram } from "@opentelemetry/api";
import { publisherMeter } from "./telemetry";

export type LlmOperation = "chat" | "embedding";
export type LlmFailureReason =
   | "auth"
   | "rate_limited"
   | "timeout"
   | "server_error"
   | "client_error"
   | "invalid_response"
   | "cooldown"
   | "other";

const resetHooks: (() => void)[] = [];

function lazyCounter(name: string, description: string): () => Counter {
   let instrument: Counter | null = null;
   resetHooks.push(() => (instrument = null));
   return () =>
      (instrument ??= publisherMeter().createCounter(name, { description }));
}

function lazyHistogram(
   name: string,
   description: string,
   unit: string,
): () => Histogram {
   let instrument: Histogram | null = null;
   resetHooks.push(() => (instrument = null));
   return () =>
      (instrument ??= publisherMeter().createHistogram(name, {
         description,
         unit,
      }));
}

const callsCounter = lazyCounter(
   "publisher_llm_calls_total",
   "Calls to a model vendor, counted once per logical call (retries are not " +
      "separate calls). Labels: provider, operation ('chat'|'embedding'), " +
      "outcome ('success'|'failure').",
);
const failuresCounter = lazyCounter(
   "publisher_llm_failures_total",
   "Model-vendor calls that failed after their retries. Labels: provider, " +
      "operation, reason (LlmFailureReason).",
);
const retriesCounter = lazyCounter(
   "publisher_llm_retries_total",
   "Retries of a model-vendor call after a 429, 408, 5xx or timeout. Labels: provider, operation.",
);
const tokensCounter = lazyCounter(
   "publisher_llm_tokens_total",
   "Tokens a vendor reported using. Labels: provider, direction ('input'|'output').",
);
const jsonRepairsCounter = lazyCounter(
   "publisher_llm_json_repairs_total",
   "Chat replies that were not valid JSON for the caller and were re-asked once. Label: provider.",
);
const latencyHistogram = lazyHistogram(
   "publisher_llm_call_duration_ms",
   "Wall-clock duration of a logical model-vendor call, retries included. Labels: provider, operation.",
   "ms",
);

export function recordLlmCall(
   provider: string,
   operation: LlmOperation,
   durationMs: number,
   failure?: LlmFailureReason,
): void {
   callsCounter().add(1, {
      provider,
      operation,
      outcome: failure ? "failure" : "success",
   });
   latencyHistogram().record(durationMs, { provider, operation });
   if (failure) {
      failuresCounter().add(1, { provider, operation, reason: failure });
   }
}

export function recordLlmRetry(
   provider: string,
   operation: LlmOperation,
): void {
   retriesCounter().add(1, { provider, operation });
}

export function recordLlmTokens(
   provider: string,
   usage: { inputTokens?: number; outputTokens?: number },
): void {
   if (usage.inputTokens) {
      tokensCounter().add(usage.inputTokens, { provider, direction: "input" });
   }
   if (usage.outputTokens) {
      tokensCounter().add(usage.outputTokens, {
         provider,
         direction: "output",
      });
   }
}

export function recordLlmJsonRepair(provider: string): void {
   jsonRepairsCounter().add(1, { provider });
}

/** Test seam: drop cached instruments so they re-bind to a new MeterProvider. */
export function resetLlmTelemetryForTesting(): void {
   for (const reset of resetHooks) reset();
}
