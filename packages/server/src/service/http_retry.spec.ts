// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   HttpRequestError,
   isRetryableStatus,
   parseRetryAfterMs,
   RetryPolicy,
   withRetry,
} from "./http_retry";

function policy(sleeps: number[]): RetryPolicy {
   return {
      maxAttempts: 5,
      baseDelayMs: 1_000,
      maxDelayMs: 30_000,
      maxTotalDelayMs: 90_000,
      sleep: async (ms) => {
         sleeps.push(ms);
      },
      random: () => 0,
   };
}

describe("withRetry", () => {
   it("retries a retryable error, counts each retry and honours Retry-After", async () => {
      const sleeps: number[] = [];
      let retries = 0;
      let calls = 0;
      const result = await withRetry(
         async () => {
            calls++;
            if (calls === 1)
               throw new HttpRequestError("busy", 429, true, 4_000);
            if (calls === 2) throw new HttpRequestError("down", 503, true);
            return "ok";
         },
         policy(sleeps),
         "Chat request",
         () => retries++,
      );
      expect(result).toBe("ok");
      expect(calls).toBe(3);
      expect(retries).toBe(2);
      // Attempt 1: jittered base is 500ms but Retry-After says 4s.
      expect(sleeps).toEqual([4_000, 1_000]);
   });

   it("does not retry a 4xx", async () => {
      let calls = 0;
      await expect(
         withRetry(async () => {
            calls++;
            throw new HttpRequestError("denied", 401, false);
         }, policy([])),
      ).rejects.toThrow("denied");
      expect(calls).toBe(1);
   });

   it("stops after maxAttempts and throws the last error", async () => {
      let calls = 0;
      await expect(
         withRetry(async () => {
            calls++;
            throw new HttpRequestError(`fail ${calls}`, 500, true);
         }, policy([])),
      ).rejects.toThrow("fail 5");
      expect(calls).toBe(5);
   });
});

describe("helpers", () => {
   it("classifies statuses", () => {
      expect([408, 429, 500, 503].every(isRetryableStatus)).toBe(true);
      expect([400, 401, 403, 404].some(isRetryableStatus)).toBe(false);
   });
   it("parses Retry-After seconds and rejects junk", () => {
      expect(parseRetryAfterMs("7")).toBe(7_000);
      expect(parseRetryAfterMs(null)).toBeUndefined();
      expect(parseRetryAfterMs("soon")).toBeUndefined();
   });
});
