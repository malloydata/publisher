// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * retrieval.llm.maxCallsPerRequest: the most chat requests one get_context
 * request may send, counted across every stage (refine, rerank, source match). The check runs before the
 * request that would pass it. A request is one HTTP call to the vendor: a
 * retry after a 429 is a request, and so is the re-ask that repairs a JSON
 * reply, so the ceiling bounds what is spent rather than how often the code
 * asked.
 */

import {
   afterAll,
   afterEach,
   beforeAll,
   beforeEach,
   describe,
   expect,
   it,
} from "bun:test";
import { _clearChatModelForTests } from "../../providers/active";
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
import { shopPackage } from "../../test_helpers/get_context_llm_fixture";
import {
   keywordReply,
   scriptedChat,
   semanticHarness,
   untilSemantic,
   useChat,
   type SemanticHarness,
} from "../../test_helpers/get_context_llm_harness";
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

describe("get_context with maxCallsPerRequest", () => {
   let h: SemanticHarness;
   beforeAll(async () => {
      h = await semanticHarness();
   });
   afterAll(async () => {
      await h.close();
   });
   beforeEach(() => h.reset());
   afterEach(() => _clearChatModelForTests());

   const params = (pkg: string) => ({
      search_targets: [
         { target_type: "dimension", search_text: "state the order ships to" },
      ],
      scopes: [{ environment: "limit", package: pkg }],
   });

   it("stops rerank, which needs the second call, and names the setting", async () => {
      const chat = scriptedChat(keywordReply);
      useChat(chat.model, { maxCallsPerRequest: 1 });
      const handler = h.handlerFor(shopPackage());
      const { isError, payload } = await untilSemantic(handler, params("a"));
      expect(isError).toBe(true);
      expect(payload.retrieval_stage).toBe("rerank");
      expect(payload.error).toContain("failed in the rerank step");
      expect(payload.error).toContain("retrieval.llm.maxCallsPerRequest");
      expect(payload.error).toContain("1 LLM request");
      expect(payload.sources).toEqual([]);
      // Refine used the one call; the second was never sent.
      expect(chat.prompts).toHaveLength(1);
   });

   it("answers when the limit covers every call", async () => {
      const chat = scriptedChat(keywordReply);
      useChat(chat.model, { maxCallsPerRequest: 2 });
      const handler = h.handlerFor(shopPackage());
      const { isError, payload } = await untilSemantic(handler, params("b"));
      expect(isError).toBe(false);
      expect(payload.sources.length).toBeGreaterThan(0);
      expect(chat.prompts).toHaveLength(2);
   });

   it("a limit of 1 is enough when only one stage runs", async () => {
      const chat = scriptedChat(keywordReply);
      useChat(chat.model, { maxCallsPerRequest: 1 });
      const handler = h.handlerFor(
         shopPackage({ rerank: { enabled: false, topSources: 8 } }),
      );
      const { isError } = await untilSemantic(handler, params("c"));
      expect(isError).toBe(false);
      expect(chat.prompts).toHaveLength(1);
   });

   it("is counted per request: the next request starts again from zero", async () => {
      const chat = scriptedChat(keywordReply);
      useChat(chat.model, { maxCallsPerRequest: 2 });
      const handler = h.handlerFor(shopPackage());
      const first = await untilSemantic(handler, params("d"));
      const second = await untilSemantic(handler, params("d"));
      expect(first.isError).toBe(false);
      expect(second.isError).toBe(false);
      expect(chat.prompts).toHaveLength(4);
   });

   it("stops refine part way when its batches alone pass the limit", async () => {
      // 13 sources of 10 fields = 130 candidates, 8 batches of 15.
      const fields = (s: number) =>
         Array.from({ length: 10 }, (_, i) => ({
            kind: "dimension",
            name: `state_${s}_${i}`,
            annotations: ["#(doc) State the order ships to."],
         }));
      const model = {
         getSourceInfos: () =>
            Array.from({ length: 13 }, (_, s) => ({
               name: `src${s}`,
               annotations: [],
               schema: { fields: fields(s) },
            })),
         getQueries: () => [],
      };
      const pkg = {
         listModels: async () => [{ path: "big.malloy" }],
         getModel: () => model,
         getRetrievalSettings: () => ({
            representation: "single",
            keyphrases: "never",
            sourceSummary: { enabled: false },
            prompts: {},
         }),
      };
      const chat = scriptedChat(keywordReply);
      useChat(chat.model, { maxCallsPerRequest: 3, concurrency: 1 });
      const handler = h.handlerFor(pkg);
      const { isError, payload } = await untilSemantic(handler, params("e"));
      expect(isError).toBe(true);
      expect(payload.retrieval_stage).toBe("refine");
      expect(payload.error).toContain("maxCallsPerRequest");
      expect(chat.prompts).toHaveLength(3);
   });
});
