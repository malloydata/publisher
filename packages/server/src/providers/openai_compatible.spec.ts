// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import { resetLlmTelemetryForTesting } from "../llm_metrics";
import { logger } from "../logger";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "../test_helpers/metrics_harness";
import {
   instantRetry,
   jsonResponse,
   stubFetch,
} from "../test_helpers/fetch_stub";
import { LlmJsonError } from "./json";
import { createChatModel, createEmbeddingModel } from "./registry";
import type { LlmSettings } from "./types";

const KEY = "sk-secret-key-123";

function settings(over: Partial<LlmSettings> = {}): LlmSettings {
   return {
      provider: "openai-compatible",
      model: "chat-1",
      baseUrl: "https://llm.example.com/v1/",
      apiKey: KEY,
      timeoutMs: 5_000,
      concurrency: 4,
      maxCallsPerSync: 300,
      maxCallsPerRequest: 20,
      ...over,
   };
}

const reply = (content: string, usage: object = {}) =>
   jsonResponse({ choices: [{ message: { content } }], usage });

describe("openai-compatible chat adapter", () => {
   let harness: MetricsHarness;
   beforeEach(async () => {
      harness = await startMetricsHarness();
      resetLlmTelemetryForTesting();
   });
   afterEach(async () => {
      resetLlmTelemetryForTesting();
      await harness.shutdown();
   });

   it("posts to /chat/completions with a bearer key, temperature 0 and no response_format for plain text", async () => {
      const { fetchFn, requests } = stubFetch([
         () => reply("hello", { prompt_tokens: 11, completion_tokens: 3 }),
      ]);
      const chat = createChatModel(settings(), {
         fetchFn,
         retry: instantRetry(),
      });
      const out = await chat.complete({
         system: "be brief",
         prompt: "say hi",
         maxTokens: 50,
      });
      expect(out).toEqual({
         text: "hello",
         usage: { inputTokens: 11, outputTokens: 3 },
      });
      expect(requests).toHaveLength(1);
      const r = requests[0];
      expect(r.url).toBe("https://llm.example.com/v1/chat/completions");
      expect(r.headers.Authorization).toBe(`Bearer ${KEY}`);
      expect(r.body).toEqual({
         model: "chat-1",
         temperature: 0,
         max_tokens: 50,
         messages: [
            { role: "system", content: "be brief" },
            { role: "user", content: "say hi" },
         ],
      });
      expect(
         await harness.collectCounter("publisher_llm_tokens_total", {
            direction: "input",
         }),
      ).toBe(11);
      expect(
         await harness.collectCounter("publisher_llm_calls_total", {
            outcome: "success",
         }),
      ).toBe(1);
   });

   it("uses max_completion_tokens for openai and sends json_object only for JSON", async () => {
      const { fetchFn, requests } = stubFetch([() => reply('{"a":1}')]);
      const chat = createChatModel(
         settings({ provider: "openai", baseUrl: undefined }),
         { fetchFn, retry: instantRetry() },
      );
      const out = await chat.completeJson({
         prompt: "p",
         maxTokens: 9,
         validate: (v) => v as { a: number },
      });
      expect(out.value).toEqual({ a: 1 });
      expect(requests[0].url).toBe(
         "https://api.openai.com/v1/chat/completions",
      );
      expect(requests[0].body.max_completion_tokens).toBe(9);
      expect(requests[0].body.max_tokens).toBeUndefined();
      expect(requests[0].body.response_format).toEqual({
         type: "json_object",
      });
      expect(requests[0].body.messages[0].role).toBe("system");
   });

   it("ollama defaults to the local endpoint and sends no Authorization header", async () => {
      const { fetchFn, requests } = stubFetch([() => reply("x")]);
      const chat = createChatModel(
         settings({
            provider: "ollama",
            baseUrl: undefined,
            apiKey: undefined,
         }),
         { fetchFn, retry: instantRetry() },
      );
      await chat.complete({ prompt: "p" });
      expect(requests[0].url).toBe(
         "http://localhost:11434/v1/chat/completions",
      );
      expect(requests[0].headers.Authorization).toBeUndefined();
   });

   it("does not retry a 401, says to check LLM_API_KEY and never logs the key", async () => {
      const warn = spyOn(logger, "warn");
      const { fetchFn, requests } = stubFetch([
         () =>
            jsonResponse(
               { error: { message: `bad key ${KEY}` } },
               { status: 401 },
            ),
      ]);
      const chat = createChatModel(settings(), {
         fetchFn,
         retry: instantRetry(),
      });
      let message = "";
      try {
         await chat.complete({ prompt: "p" });
      } catch (e) {
         message = (e as Error).message;
      }
      expect(requests).toHaveLength(1);
      expect(message).toContain("(401)");
      expect(message).toContain("LLM_API_KEY");
      expect(message).not.toContain(KEY);
      expect(JSON.stringify(warn.mock.calls)).not.toContain(KEY);
      expect(
         await harness.collectCounter("publisher_llm_failures_total", {
            reason: "auth",
         }),
      ).toBe(1);
      warn.mockRestore();
   });

   it("retries a 429 and waits at least the Retry-After", async () => {
      const sleeps: number[] = [];
      const { fetchFn, requests } = stubFetch([
         () =>
            jsonResponse({}, { status: 429, headers: { "retry-after": "7" } }),
         () => reply("ok"),
      ]);
      const chat = createChatModel(settings(), {
         fetchFn,
         retry: instantRetry(sleeps),
      });
      const out = await chat.complete({ prompt: "p" });
      expect(out.text).toBe("ok");
      expect(requests).toHaveLength(2);
      expect(sleeps).toEqual([7_000]);
      expect(await harness.collectCounter("publisher_llm_retries_total")).toBe(
         1,
      );
      // One logical call, not two.
      expect(await harness.collectCounter("publisher_llm_calls_total")).toBe(1);
   });

   it("scrubs the key from a non-auth error body", async () => {
      const { fetchFn } = stubFetch([
         () => new Response(`oops ${KEY} oops`, { status: 400 }),
      ]);
      const chat = createChatModel(settings(), {
         fetchFn,
         retry: instantRetry(),
      });
      await expect(chat.complete({ prompt: "p" })).rejects.toThrow(
         "oops [REDACTED] oops",
      );
   });

   it("strips code fences, and re-asks once with the validation error when the JSON is wrong", async () => {
      const { fetchFn, requests } = stubFetch([
         () =>
            reply('Here you go:\n```json\n{"n": "three"}\n```', {
               prompt_tokens: 10,
               completion_tokens: 5,
            }),
         () => reply('{"n": 3}', { prompt_tokens: 20, completion_tokens: 4 }),
      ]);
      const chat = createChatModel(settings(), {
         fetchFn,
         retry: instantRetry(),
      });
      const out = await chat.completeJson({
         prompt: "give n",
         validate: (v) => {
            const n = (v as { n?: unknown }).n;
            if (typeof n !== "number") throw new Error("n must be a number");
            return n;
         },
      });
      expect(out.value).toBe(3);
      expect(out.usage).toEqual({ inputTokens: 30, outputTokens: 9 });
      expect(requests).toHaveLength(2);
      const repair = requests[1].body.messages.at(-1).content as string;
      expect(repair).toContain("n must be a number");
      expect(repair).toContain('{"n": "three"}');
      expect(
         await harness.collectCounter("publisher_llm_json_repairs_total"),
      ).toBe(1);
   });

   it("gives up with LlmJsonError after one repair", async () => {
      const { fetchFn, requests } = stubFetch([() => reply("not json at all")]);
      const chat = createChatModel(settings(), {
         fetchFn,
         retry: instantRetry(),
      });
      await expect(
         chat.completeJson({ prompt: "p", validate: (v) => v }),
      ).rejects.toBeInstanceOf(LlmJsonError);
      expect(requests).toHaveLength(2);
   });

   it("cools down after three outage-shaped failures and refuses the fourth without a request", async () => {
      const { fetchFn, requests } = stubFetch([
         () => jsonResponse({}, { status: 503 }),
      ]);
      const chat = createChatModel(settings(), {
         fetchFn,
         retry: instantRetry(),
      });
      for (let i = 0; i < 3; i++) {
         await expect(chat.complete({ prompt: "p" })).rejects.toThrow("(503)");
      }
      const sent = requests.length;
      await expect(chat.complete({ prompt: "p" })).rejects.toThrow(
         "cooling down",
      );
      expect(requests).toHaveLength(sent);
   });

   it("counts 429, 408 and a timeout toward the cooldown too", async () => {
      for (const status of [429, 408]) {
         const { fetchFn, requests } = stubFetch([
            () => jsonResponse({}, { status }),
         ]);
         const chat = createChatModel(settings(), {
            fetchFn,
            retry: instantRetry(),
         });
         for (let i = 0; i < 3; i++) {
            await expect(chat.complete({ prompt: "p" })).rejects.toThrow(
               `(${status})`,
            );
         }
         const sent = requests.length;
         await expect(chat.complete({ prompt: "p" })).rejects.toThrow(
            "cooling down",
         );
         expect(requests).toHaveLength(sent);
      }
      const timeout = Object.assign(new Error("slow"), {
         name: "TimeoutError",
      });
      const { fetchFn, requests } = stubFetch([
         () => {
            throw timeout;
         },
      ]);
      const chat = createChatModel(settings(), {
         fetchFn,
         retry: instantRetry(),
      });
      for (let i = 0; i < 3; i++) {
         await expect(chat.complete({ prompt: "p" })).rejects.toThrow(
            "timed out",
         );
      }
      const sent = requests.length;
      await expect(chat.complete({ prompt: "p" })).rejects.toThrow(
         "cooling down",
      );
      expect(requests).toHaveLength(sent);
   });

   it("does not cool down for a bad request, a rejected key or unusable JSON, so one package's bad prompts cannot block the rest", async () => {
      const cases: Array<{ name: string; respond: () => Response }> = [
         { name: "400", respond: () => jsonResponse({}, { status: 400 }) },
         { name: "401", respond: () => jsonResponse({}, { status: 401 }) },
         { name: "404", respond: () => jsonResponse({}, { status: 404 }) },
         { name: "not JSON", respond: () => reply("not json at all") },
      ];
      for (const { name, respond } of cases) {
         const { fetchFn, requests } = stubFetch([respond]);
         const chat = createChatModel(settings(), {
            fetchFn,
            retry: instantRetry(),
         });
         for (let i = 0; i < 6; i++) {
            const failure = await chat
               .completeJson({ prompt: "p", validate: (v) => v })
               .catch((e: unknown) => e as Error);
            expect(failure).toBeInstanceOf(Error);
            expect((failure as Error).message).not.toContain("cooling down");
         }
         // Every call reached the vendor; none was refused.
         expect(requests.length).toBeGreaterThanOrEqual(6);
         expect(name).toBeTruthy();
      }
   });

   it("openai-compatible without a baseUrl is an actionable error", () => {
      expect(() =>
         createChatModel(settings({ baseUrl: undefined }), {}),
      ).toThrow("Invalid retrieval.llm.baseUrl");
   });
});

describe("openai-compatible embedding model", () => {
   it("delegates to the existing EmbeddingProvider (path, auth header, dimensions)", async () => {
      const { fetchFn, requests } = stubFetch([
         () => jsonResponse({ data: [{ index: 0, embedding: [0.1, 0.2] }] }),
      ]);
      const model = createEmbeddingModel(
         {
            provider: "openai",
            model: "text-embedding-3-small",
            dimensions: 512,
            apiKey: KEY,
            minSimilarity: 0.2,
            queryPrefix: "q: ",
            documentPrefix: "d: ",
         },
         { fetchFn },
      );
      expect(model.maxBatch).toBe(512);
      expect(model.queryPrefix).toBe("q: ");
      expect(await model.embed(["hi"])).toEqual([[0.1, 0.2]]);
      expect(requests[0].url).toBe("https://api.openai.com/v1/embeddings");
      expect(requests[0].headers.Authorization).toBe(`Bearer ${KEY}`);
      expect(requests[0].body.dimensions).toBe(512);
   });
});
