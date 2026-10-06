// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   DEFAULT_EMBEDDING_RETRY,
   EmbeddingProvider,
   EmbeddingRequestError,
   EmbeddingRetryPolicy,
   withEmbeddingRetry,
} from "./embedding_provider";
import { DEFAULT_EMBEDDING_MIN_SIMILARITY } from "../config";

const API_KEY = "sk-secret-key-123";

/** A provider whose fetch answers from a script, one entry per request. */
function scripted(responses: Array<() => Response | Promise<Response>>) {
   const calls: number[] = [];
   const fetchStub = (async () => {
      const i = calls.length;
      calls.push(i);
      const next = responses[Math.min(i, responses.length - 1)];
      return next();
   }) as unknown as typeof fetch;
   const provider = new EmbeddingProvider(
      {
         apiKey: API_KEY,
         model: "m",
         baseUrl: "https://stub.example.com/v1",
         minSimilarity: DEFAULT_EMBEDDING_MIN_SIMILARITY,
      },
      fetchStub,
   );
   return { provider, calls };
}

const ok = () =>
   new Response(JSON.stringify({ data: [{ index: 0, embedding: [1, 0] }] }), {
      status: 200,
   });
const status =
   (code: number, headers: Record<string, string> = {}) =>
   () =>
      new Response("upstream said no", { status: code, headers });

/** A policy that records the waits instead of waiting; jitter pinned to 0. */
function recordingPolicy(
   overrides: Partial<EmbeddingRetryPolicy> = {},
): EmbeddingRetryPolicy & { slept: number[] } {
   const slept: number[] = [];
   return {
      ...DEFAULT_EMBEDDING_RETRY,
      random: () => 0,
      sleep: async (ms: number) => {
         slept.push(ms);
      },
      ...overrides,
      slept,
   };
}

describe("embedding retry", () => {
   it("succeeds on the third attempt after two 503s, waiting with backoff", async () => {
      const { provider, calls } = scripted([
         status(503),
         status(503),
         () => ok(),
      ]);
      const policy = recordingPolicy();
      const vectors = await provider.embedBatch(["x"], 1_000, policy);
      expect(vectors).toEqual([[1, 0]]);
      expect(calls.length).toBe(3);
      // random() = 0 puts each wait at the bottom of its jitter range: half
      // of 1000ms, then half of 2000ms.
      expect(policy.slept).toEqual([500, 1_000]);
   });

   it("spreads the wait over the upper half of the backoff window", async () => {
      const { provider } = scripted([status(503), () => ok()]);
      const policy = recordingPolicy({ random: () => 0.999 });
      await provider.embedBatch(["x"], 1_000, policy);
      expect(policy.slept.length).toBe(1);
      expect(policy.slept[0]).toBeGreaterThan(990);
      expect(policy.slept[0]).toBeLessThanOrEqual(1_000);
   });

   it("honours Retry-After when it is longer than the backoff", async () => {
      const { provider, calls } = scripted([
         status(429, { "Retry-After": "7" }),
         () => ok(),
      ]);
      const policy = recordingPolicy();
      await provider.embedBatch(["x"], 1_000, policy);
      expect(calls.length).toBe(2);
      expect(policy.slept).toEqual([7_000]);
   });

   it("gives up at once when Retry-After is longer than it will wait", async () => {
      const { provider, calls } = scripted([
         status(429, { "Retry-After": "3600" }),
         () => ok(),
      ]);
      const policy = recordingPolicy();
      await expect(provider.embedBatch(["x"], 1_000, policy)).rejects.toThrow(
         "(429)",
      );
      expect(calls.length).toBe(1);
      expect(policy.slept).toEqual([]);
   });

   it("does not retry a 401, and the message never carries the key", async () => {
      const { provider, calls } = scripted([
         () => new Response(`bad key ${API_KEY}`, { status: 401 }),
         () => ok(),
      ]);
      const policy = recordingPolicy();
      const failure = await provider
         .embedBatch(["x"], 1_000, policy)
         .catch((e: Error) => e);
      expect(failure).toBeInstanceOf(EmbeddingRequestError);
      expect((failure as Error).message).toContain("authentication failed");
      expect((failure as Error).message).not.toContain(API_KEY);
      expect(calls.length).toBe(1);
      expect(policy.slept).toEqual([]);
   });

   it("does not retry other 4xx errors", async () => {
      const { provider, calls } = scripted([status(400), () => ok()]);
      await expect(
         provider.embedBatch(["x"], 1_000, recordingPolicy()),
      ).rejects.toThrow("(400)");
      expect(calls.length).toBe(1);
   });

   it("retries a network failure", async () => {
      const { provider, calls } = scripted([
         () => {
            throw new Error("connect ECONNRESET");
         },
         () => ok(),
      ]);
      const policy = recordingPolicy();
      await provider.embedBatch(["x"], 1_000, policy);
      expect(calls.length).toBe(2);
      expect(policy.slept.length).toBe(1);
   });

   it("stops after the maximum attempts and throws the last error", async () => {
      const { provider, calls } = scripted([status(500)]);
      const policy = recordingPolicy();
      await expect(provider.embedBatch(["x"], 1_000, policy)).rejects.toThrow(
         "(500)",
      );
      expect(calls.length).toBe(5);
      expect(policy.slept.length).toBe(4);
   });

   it("stops when the total wait budget is spent", async () => {
      const { provider, calls } = scripted([status(503)]);
      // Waits would be 500, 1000, 2000...; a 1600ms budget allows two.
      const policy = recordingPolicy({ maxTotalDelayMs: 1_600 });
      await expect(provider.embedBatch(["x"], 1_000, policy)).rejects.toThrow(
         "(503)",
      );
      expect(policy.slept).toEqual([500, 1_000]);
      expect(calls.length).toBe(3);
   });

   it("does not retry when no policy is passed (the query path)", async () => {
      const { provider, calls } = scripted([status(503), () => ok()]);
      await expect(provider.embedBatch(["x"], 1_000)).rejects.toThrow("(503)");
      expect(calls.length).toBe(1);
   });

   it("never retries an error that is not an EmbeddingRequestError", async () => {
      let n = 0;
      await expect(
         withEmbeddingRetry(async () => {
            n++;
            throw new Error("malformed");
         }, recordingPolicy()),
      ).rejects.toThrow("malformed");
      expect(n).toBe(1);
   });
});
