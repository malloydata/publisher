// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * retrieval.llm.maxCallsPerRequest: the most chat requests one get_context
 * request may send, counted across every stage. The check runs before the
 * request that would pass it. A request is one HTTP call to the vendor: a
 * retry after a 429 is a request, and so is the re-ask that repairs a JSON
 * reply, so the ceiling bounds what is spent rather than how often the code
 * asked.
 */

import { describe, expect, it } from "bun:test";
import { createChatModel } from "../../providers/registry";
import type {
   ChatModel,
   ChatRequest,
   JsonChatRequest,
   LlmSettings,
} from "../../providers/types";
import type { RetryPolicy } from "../../service/http_retry";
import {
   instantRetry,
   jsonResponse,
   stubFetch,
} from "../../test_helpers/fetch_stub";
import { LlmCallLimitError, LlmMeter, REQUEST_RETRY } from "./get_context_llm";

/**
 * A chat model double that sends no HTTP. It reports one request per call, as
 * the real model does for each request it sends.
 */
function reportingChat(options: { delayMs?: number } = {}) {
   const prompts: string[] = [];
   const send = async (req: ChatRequest) => {
      req.onRequest?.();
      prompts.push(req.prompt);
      if (options.delayMs) {
         await new Promise((resolve) => setTimeout(resolve, options.delayMs));
      }
      return { text: "[]", usage: {} };
   };
   const model: ChatModel = {
      provider: "openai-compatible",
      model: "m",
      complete: send,
      completeJson: async <T>(req: JsonChatRequest<T>) => {
         const result = await send(req);
         return { value: req.validate(JSON.parse(result.text)), usage: {} };
      },
   };
   return { model, prompts };
}

describe("LlmMeter", () => {
   it("sends the requests up to the limit and refuses the next one before it is sent", async () => {
      const chat = reportingChat();
      const metered = new LlmMeter(2).wrap(chat.model);
      const ask = () =>
         metered.completeJson({ prompt: "x", validate: (v) => v });
      await ask();
      await ask();
      await expect(ask()).rejects.toBeInstanceOf(LlmCallLimitError);
      expect(chat.prompts).toHaveLength(2);
   });

   it("refuses requests started together once the limit is taken", async () => {
      const chat = reportingChat({ delayMs: 10 });
      const metered = new LlmMeter(3).wrap(chat.model);
      const results = await Promise.allSettled(
         Array.from({ length: 6 }, () =>
            metered.completeJson({ prompt: "x", validate: (v) => v }),
         ),
      );
      expect(results.filter((r) => r.status === "fulfilled")).toHaveLength(3);
      expect(chat.prompts).toHaveLength(3);
   });

   it("counts complete() too, and has no ceiling with a null limit", async () => {
      const limited = new LlmMeter(1).wrap(reportingChat().model);
      await limited.complete({ prompt: "x" });
      await expect(limited.complete({ prompt: "x" })).rejects.toBeInstanceOf(
         LlmCallLimitError,
      );
      const free = new LlmMeter(null);
      const unlimited = free.wrap(reportingChat().model);
      for (let i = 0; i < 50; i++) await unlimited.complete({ prompt: "x" });
      expect(free.snapshot().calls).toBe(50);
   });
});

describe("LlmMeter over the real chat model", () => {
   const settings: LlmSettings = {
      provider: "openai-compatible",
      model: "m-1",
      apiKey: "key-abc-123-secret",
      baseUrl: "https://llm.example.com/v1",
      timeoutMs: 5_000,
      concurrency: 1,
      maxCallsPerSync: 10,
      maxCallsPerRequest: 20,
   };
   const reply = (text: string) =>
      jsonResponse({
         choices: [{ message: { content: text } }],
         usage: { prompt_tokens: 1, completion_tokens: 1 },
      });
   const tooMany = () => new Response("slow down", { status: 429 });
   const ask = (chat: ChatModel) =>
      chat.completeJson({ prompt: "x", validate: (v) => v });
   // Retries here cost nothing: the point is how many requests are sent.
   const quick: RetryPolicy = { ...REQUEST_RETRY, sleep: async () => {} };

   it("counts a retry after a 429 as a request", async () => {
      const { fetchFn, requests } = stubFetch([tooMany, () => reply("[]")]);
      const meter = new LlmMeter(null, quick);
      const chat = meter.wrap(
         createChatModel(settings, { fetchFn, retry: instantRetry() }),
      );
      await ask(chat);
      expect(requests).toHaveLength(2);
      expect(meter.snapshot().calls).toBe(2);
   });

   it("counts the re-ask that repairs a JSON reply as a request", async () => {
      const { fetchFn, requests } = stubFetch([
         () => reply("not json"),
         () => reply("[]"),
      ]);
      const meter = new LlmMeter(null, quick);
      const chat = meter.wrap(
         createChatModel(settings, { fetchFn, retry: instantRetry() }),
      );
      await ask(chat);
      expect(requests).toHaveLength(2);
      expect(meter.snapshot().calls).toBe(2);
   });

   it("refuses the retry that would pass the ceiling, before it is sent", async () => {
      const { fetchFn, requests } = stubFetch([tooMany, () => reply("[]")]);
      const meter = new LlmMeter(1, quick);
      const chat = meter.wrap(
         createChatModel(settings, { fetchFn, retry: instantRetry() }),
      );
      await expect(ask(chat)).rejects.toBeInstanceOf(LlmCallLimitError);
      expect(requests).toHaveLength(1);
      expect(meter.snapshot().calls).toBe(1);
   });

   it("a ceiling of 4 allows 4 requests in total, however they are spent", async () => {
      // 429, then a JSON reply that needs a repair, then a 429, then a good
      // reply: four requests. A fifth is refused.
      const { fetchFn, requests } = stubFetch([
         tooMany,
         () => reply("not json"),
         tooMany,
         () => reply("[]"),
      ]);
      const meter = new LlmMeter(4, quick);
      const chat = meter.wrap(
         createChatModel(settings, { fetchFn, retry: instantRetry() }),
      );
      await ask(chat);
      expect(requests).toHaveLength(4);
      await expect(ask(chat)).rejects.toBeInstanceOf(LlmCallLimitError);
      expect(requests).toHaveLength(4);
   });
});

describe("a request does not wait out the sync's retry ladder", () => {
   const settings: LlmSettings = {
      provider: "openai-compatible",
      model: "m-1",
      apiKey: "key-abc-123-secret",
      baseUrl: "https://llm.example.com/v1",
      timeoutMs: 5_000,
      concurrency: 1,
      maxCallsPerSync: 10,
      maxCallsPerRequest: 20,
   };
   const tooMany = () => new Response("slow down", { status: 429 });
   const ask = (chat: ChatModel) =>
      chat.completeJson({ prompt: "x", validate: (v) => v });

   it("the request policy is one short retry at most", () => {
      expect(REQUEST_RETRY.maxAttempts).toBeLessThanOrEqual(2);
      expect(REQUEST_RETRY.maxTotalDelayMs).toBeLessThanOrEqual(1_000);
      expect(REQUEST_RETRY.maxDelayMs).toBeLessThanOrEqual(1_000);
   });

   it("a 429 that keeps coming costs a request two sends, not five", async () => {
      const sleeps: number[] = [];
      const requestRetry: RetryPolicy = {
         ...REQUEST_RETRY,
         sleep: async (ms) => {
            sleeps.push(ms);
         },
         random: () => 0,
      };
      const { fetchFn, requests } = stubFetch([tooMany]);
      // The model itself is built with the sync's ladder: 5 attempts.
      const chat = new LlmMeter(null, requestRetry).wrap(
         createChatModel(settings, { fetchFn, retry: instantRetry() }),
      );
      await expect(ask(chat)).rejects.toThrow();
      expect(requests).toHaveLength(2);
      expect(sleeps.reduce((a, b) => a + b, 0)).toBeLessThanOrEqual(1_000);
   });

   it("a Retry-After longer than the request policy allows fails at once", async () => {
      const { fetchFn, requests } = stubFetch([
         () =>
            new Response("slow down", {
               status: 429,
               headers: { "retry-after": "30" },
            }),
      ]);
      const chat = new LlmMeter(null, {
         ...REQUEST_RETRY,
         sleep: async () => {},
      }).wrap(createChatModel(settings, { fetchFn, retry: instantRetry() }));
      await expect(ask(chat)).rejects.toThrow();
      expect(requests).toHaveLength(1);
   });

   it("the sync, which does not go through the meter, still uses the full ladder", async () => {
      const { fetchFn, requests } = stubFetch([tooMany]);
      const chat = createChatModel(settings, {
         fetchFn,
         retry: instantRetry(),
      });
      await expect(ask(chat)).rejects.toThrow();
      expect(requests).toHaveLength(5);
   });
});
