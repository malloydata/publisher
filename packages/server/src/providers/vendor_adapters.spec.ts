// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it, spyOn } from "bun:test";
import { logger } from "../logger";
import {
   instantRetry,
   jsonResponse,
   stubFetch,
} from "../test_helpers/fetch_stub";
import { createChatModel, createEmbeddingModel } from "./registry";
import type { EmbeddingSettings, LlmSettings } from "./types";

const KEY = "key-abc-123-secret";

function llm(over: Partial<LlmSettings>): LlmSettings {
   return {
      provider: "anthropic",
      model: "m-1",
      apiKey: KEY,
      timeoutMs: 5_000,
      concurrency: 4,
      maxCallsPerSync: 300,
      maxCallsPerRequest: 20,
      ...over,
   };
}

function emb(over: Partial<EmbeddingSettings>): EmbeddingSettings {
   return {
      provider: "google",
      model: "e-1",
      apiKey: KEY,
      minSimilarity: 0.2,
      queryPrefix: "",
      documentPrefix: "",
      ...over,
   };
}

describe("anthropic adapter", () => {
   const reply = (text: string) =>
      jsonResponse({
         content: [{ type: "text", text }],
         usage: { input_tokens: 7, output_tokens: 2 },
      });

   it("posts to /v1/messages with x-api-key and anthropic-version, never a bearer", async () => {
      const { fetchFn, requests } = stubFetch([() => reply("hi")]);
      const chat = createChatModel(llm({}), { fetchFn, retry: instantRetry() });
      const out = await chat.complete({
         system: "sys",
         prompt: "p",
         maxTokens: 40,
      });
      expect(out).toEqual({
         text: "hi",
         usage: { inputTokens: 7, outputTokens: 2 },
      });
      const r = requests[0];
      expect(r.url).toBe("https://api.anthropic.com/v1/messages");
      expect(r.headers["x-api-key"]).toBe(KEY);
      expect(r.headers["anthropic-version"]).toBe("2023-06-01");
      expect(r.headers.Authorization).toBeUndefined();
      expect(r.body).toEqual({
         model: "m-1",
         max_tokens: 40,
         temperature: 0,
         system: "sys",
         messages: [{ role: "user", content: "p" }],
      });
   });

   it("sends a default max_tokens (required by the API) and a JSON-only instruction for JSON", async () => {
      const { fetchFn, requests } = stubFetch([() => reply('{"ok":true}')]);
      const chat = createChatModel(llm({}), { fetchFn, retry: instantRetry() });
      const out = await chat.completeJson({
         prompt: "p",
         validate: (v) => v as { ok: boolean },
      });
      expect(out.value.ok).toBe(true);
      expect(requests[0].body.max_tokens).toBe(1024);
      expect(requests[0].body.system).toContain("JSON");
   });

   it("does not retry a 401 and keeps the key out of the error and the logs", async () => {
      const warn = spyOn(logger, "warn");
      const { fetchFn, requests } = stubFetch([
         () => jsonResponse({ error: `invalid ${KEY}` }, { status: 401 }),
      ]);
      const chat = createChatModel(llm({}), { fetchFn, retry: instantRetry() });
      let message = "";
      try {
         await chat.complete({ prompt: "p" });
      } catch (e) {
         message = (e as Error).message;
      }
      expect(requests).toHaveLength(1);
      expect(message).toContain("(401)");
      expect(message).not.toContain(KEY);
      expect(JSON.stringify(warn.mock.calls)).not.toContain(KEY);
      warn.mockRestore();
   });

   it("retries a 429 honouring Retry-After, then parses", async () => {
      const sleeps: number[] = [];
      const { fetchFn, requests } = stubFetch([
         () =>
            jsonResponse({}, { status: 429, headers: { "retry-after": "3" } }),
         () => reply("ok"),
      ]);
      const chat = createChatModel(llm({}), {
         fetchFn,
         retry: instantRetry(sleeps),
      });
      expect((await chat.complete({ prompt: "p" })).text).toBe("ok");
      expect(requests).toHaveLength(2);
      expect(sleeps).toEqual([3_000]);
   });

   it("re-asks once when the JSON fails validation", async () => {
      const { fetchFn, requests } = stubFetch([
         () => reply("{}"),
         () => reply('{"v":1}'),
      ]);
      const chat = createChatModel(llm({}), { fetchFn, retry: instantRetry() });
      const out = await chat.completeJson({
         prompt: "p",
         validate: (v) => {
            if (!(v as { v?: number }).v) throw new Error("missing v");
            return v as { v: number };
         },
      });
      expect(out.value.v).toBe(1);
      expect(requests).toHaveLength(2);
      expect(requests[1].body.messages[0].content).toContain("missing v");
   });

   it("has no embeddings and says so", () => {
      expect(() =>
         createEmbeddingModel(emb({ provider: "anthropic" })),
      ).toThrow("no embeddings API");
   });

   it("needs a key", () => {
      expect(() => createChatModel(llm({ apiKey: undefined }))).toThrow(
         "LLM_API_KEY",
      );
   });
});

describe("google (Gemini API) adapter", () => {
   const reply = (text: string) =>
      jsonResponse({
         candidates: [{ content: { parts: [{ text }] } }],
         usageMetadata: { promptTokenCount: 9, candidatesTokenCount: 4 },
      });

   it("calls generateContent with x-goog-api-key, systemInstruction and temperature 0", async () => {
      const { fetchFn, requests } = stubFetch([() => reply("hello")]);
      const chat = createChatModel(
         llm({ provider: "google", model: "models/gemini-x" }),
         { fetchFn, retry: instantRetry() },
      );
      const out = await chat.complete({
         system: "sys",
         prompt: "p",
         maxTokens: 30,
      });
      expect(out).toEqual({
         text: "hello",
         usage: { inputTokens: 9, outputTokens: 4 },
      });
      const r = requests[0];
      expect(r.url).toBe(
         "https://generativelanguage.googleapis.com/v1beta/models/gemini-x:generateContent",
      );
      expect(r.headers["x-goog-api-key"]).toBe(KEY);
      expect(r.url).not.toContain(KEY);
      expect(r.body).toEqual({
         contents: [{ role: "user", parts: [{ text: "p" }] }],
         systemInstruction: { parts: [{ text: "sys" }] },
         generationConfig: { temperature: 0, maxOutputTokens: 30 },
      });
   });

   it("asks for application/json only when JSON is requested", async () => {
      const { fetchFn, requests } = stubFetch([() => reply('{"a":1}')]);
      const chat = createChatModel(llm({ provider: "google" }), {
         fetchFn,
         retry: instantRetry(),
      });
      await chat.completeJson({ prompt: "p", validate: (v) => v });
      expect(requests[0].body.generationConfig.responseMimeType).toBe(
         "application/json",
      );
   });

   it("does not retry a 403", async () => {
      const { fetchFn, requests } = stubFetch([
         () => jsonResponse({}, { status: 403 }),
      ]);
      const chat = createChatModel(llm({ provider: "google" }), {
         fetchFn,
         retry: instantRetry(),
      });
      await expect(chat.complete({ prompt: "p" })).rejects.toThrow("(403)");
      expect(requests).toHaveLength(1);
   });

   it("embeds with batchEmbedContents, outputDimensionality, and splits at 100", async () => {
      const { fetchFn, requests } = stubFetch([
         () =>
            jsonResponse({
               embeddings: Array.from({ length: 100 }, () => ({
                  values: [1, 2],
               })),
            }),
         () => jsonResponse({ embeddings: [{ values: [3, 4] }] }),
      ]);
      const model = createEmbeddingModel(emb({ dimensions: 256 }), { fetchFn });
      expect(model.maxBatch).toBe(100);
      const texts = Array.from({ length: 101 }, (_, i) => `t${i}`);
      const vectors = await model.embed(texts);
      expect(vectors).toHaveLength(101);
      expect(vectors[100]).toEqual([3, 4]);
      expect(requests).toHaveLength(2);
      expect(requests[0].url).toBe(
         "https://generativelanguage.googleapis.com/v1beta/models/e-1:batchEmbedContents",
      );
      expect(requests[0].headers["x-goog-api-key"]).toBe(KEY);
      expect(requests[0].body.requests).toHaveLength(100);
      expect(requests[0].body.requests[0]).toEqual({
         model: "models/e-1",
         content: { parts: [{ text: "t0" }] },
         outputDimensionality: 256,
      });
   });

   it("rejects a reply with the wrong number of vectors", async () => {
      const { fetchFn } = stubFetch([
         () => jsonResponse({ embeddings: [{ values: [1] }] }),
      ]);
      const model = createEmbeddingModel(emb({}), { fetchFn });
      await expect(model.embed(["a", "b"])).rejects.toThrow("malformed");
   });
});

describe("vertex adapter", () => {
   const token = async () => "ya29.fake-token";

   it("chats through the regional endpoint with a Bearer token, project and location", async () => {
      const { fetchFn, requests } = stubFetch([
         () =>
            jsonResponse({
               candidates: [{ content: { parts: [{ text: "hi" }] } }],
               usageMetadata: { promptTokenCount: 1, candidatesTokenCount: 1 },
            }),
      ]);
      const chat = createChatModel(
         llm({
            provider: "vertex",
            apiKey: undefined,
            projectId: "proj",
            location: "us-central1",
            model: "gemini-x",
         }),
         { fetchFn, getAccessToken: token, retry: instantRetry() },
      );
      expect((await chat.complete({ prompt: "p" })).text).toBe("hi");
      expect(requests[0].url).toBe(
         "https://us-central1-aiplatform.googleapis.com/v1/projects/proj/locations/us-central1/publishers/google/models/gemini-x:generateContent",
      );
      expect(requests[0].headers.Authorization).toBe("Bearer ya29.fake-token");
      expect(requests[0].headers["x-goog-api-key"]).toBeUndefined();
   });

   it("uses the global host for location global", async () => {
      const { fetchFn, requests } = stubFetch([
         () =>
            jsonResponse({
               candidates: [{ content: { parts: [{ text: "x" }] } }],
            }),
      ]);
      const chat = createChatModel(
         llm({
            provider: "vertex",
            apiKey: undefined,
            projectId: "proj",
            location: "global",
         }),
         { fetchFn, getAccessToken: token, retry: instantRetry() },
      );
      await chat.complete({ prompt: "p" });
      expect(
         requests[0].url.startsWith("https://aiplatform.googleapis.com/"),
      ).toBe(true);
   });

   it("embeds with :predict instances, outputDimensionality, and splits at 250", async () => {
      const { fetchFn, requests } = stubFetch([
         () =>
            jsonResponse({
               predictions: Array.from({ length: 250 }, () => ({
                  embeddings: { values: [1, 2] },
               })),
            }),
         () =>
            jsonResponse({ predictions: [{ embeddings: { values: [5, 6] } }] }),
      ]);
      const model = createEmbeddingModel(
         emb({
            provider: "vertex",
            apiKey: undefined,
            projectId: "proj",
            location: "us-central1",
            dimensions: 512,
         }),
         { fetchFn, getAccessToken: token },
      );
      expect(model.maxBatch).toBe(250);
      const vectors = await model.embed(
         Array.from({ length: 251 }, (_, i) => `t${i}`),
      );
      expect(vectors[250]).toEqual([5, 6]);
      expect(requests).toHaveLength(2);
      expect(requests[0].url).toBe(
         "https://us-central1-aiplatform.googleapis.com/v1/projects/proj/locations/us-central1/publishers/google/models/e-1:predict",
      );
      expect(requests[0].headers.Authorization).toBe("Bearer ya29.fake-token");
      expect(requests[0].body.instances[0]).toEqual({ content: "t0" });
      expect(requests[0].body.parameters).toEqual({
         outputDimensionality: 512,
      });
   });

   it("sends one input per request for the gemini-embedding models", async () => {
      const one = () =>
         jsonResponse({ predictions: [{ embeddings: { values: [1, 2] } }] });
      const { fetchFn, requests } = stubFetch([one, one, one]);
      const model = createEmbeddingModel(
         emb({
            provider: "vertex",
            apiKey: undefined,
            projectId: "proj",
            location: "us-central1",
            model: "gemini-embedding-001",
         }),
         { fetchFn, getAccessToken: token },
      );
      expect(model.maxBatch).toBe(1);
      const vectors = await model.embed(["a", "b", "c"]);
      expect(vectors).toHaveLength(3);
      expect(requests).toHaveLength(3);
      for (const r of requests) expect(r.body.instances).toHaveLength(1);
   });

   it("keeps the text-embedding models at 250 inputs per request", () => {
      const model = createEmbeddingModel(
         emb({
            provider: "vertex",
            apiKey: undefined,
            projectId: "proj",
            location: "us-central1",
            model: "text-embedding-005",
         }),
         { fetchFn: stubFetch([() => jsonResponse({})]).fetchFn },
      );
      expect(model.maxBatch).toBe(250);
   });

   it("stops waiting for an access token when the request times out", async () => {
      const { fetchFn, requests } = stubFetch([() => jsonResponse({})]);
      const chat = createChatModel(
         llm({
            provider: "vertex",
            apiKey: undefined,
            projectId: "proj",
            location: "us-central1",
            timeoutMs: 30,
         }),
         {
            fetchFn,
            // A metadata server that never answers.
            getAccessToken: () => new Promise<string>(() => {}),
            retry: { ...instantRetry(), maxAttempts: 1 },
         },
      );
      const started = Date.now();
      await expect(chat.complete({ prompt: "p" })).rejects.toThrow("timed out");
      expect(Date.now() - started).toBeLessThan(2_000);
      expect(requests).toHaveLength(0);
   });

   it("shows a caller the status and a fixed sentence for a Vertex error, and keeps the project path in the log message", async () => {
      const body = {
         error: {
            message:
               "Publisher Model `projects/secret-proj/locations/us-central1/publishers/google/models/gemini-x` not found.",
         },
      };
      const { fetchFn } = stubFetch([
         () => jsonResponse(body, { status: 404 }),
      ]);
      const chat = createChatModel(
         llm({
            provider: "vertex",
            apiKey: undefined,
            projectId: "secret-proj",
            location: "us-central1",
         }),
         { fetchFn, getAccessToken: token, retry: instantRetry() },
      );
      const failure = await chat.complete({ prompt: "p" }).catch((e) => e);
      const { publicMessage } = await import("../service/http_retry");
      expect(publicMessage(failure)).toContain("(404)");
      expect(publicMessage(failure)).not.toContain("secret-proj");
      expect(publicMessage(failure)).not.toContain("projects/");
      expect((failure as Error).message).toContain("secret-proj");
   });

   it("does not show a caller the credentials file path an ADC failure names", async () => {
      const { adcAccessToken } = await import("./vertex");
      const saved = process.env.GOOGLE_APPLICATION_CREDENTIALS;
      process.env.GOOGLE_APPLICATION_CREDENTIALS =
         "/home/someone/keys/secret-key.json";
      try {
         const failure = await adcAccessToken()().catch((e) => e);
         const { publicMessage } = await import("../service/http_retry");
         expect(publicMessage(failure)).toContain(
            "gcloud auth application-default login",
         );
         expect(publicMessage(failure)).not.toContain("secret-key.json");
         expect((failure as Error).message).toContain("secret-key.json");
      } finally {
         if (saved === undefined)
            delete process.env.GOOGLE_APPLICATION_CREDENTIALS;
         else process.env.GOOGLE_APPLICATION_CREDENTIALS = saved;
      }
   });

   it("does not retry when credentials cannot be resolved, and says how to fix it", async () => {
      const { fetchFn, requests } = stubFetch([() => jsonResponse({})]);
      const { HttpRequestError } = await import("../service/http_retry");
      const chat = createChatModel(
         llm({
            provider: "vertex",
            apiKey: undefined,
            projectId: "proj",
            location: "us-central1",
         }),
         {
            fetchFn,
            getAccessToken: async () => {
               throw new HttpRequestError(
                  "no ADC. Fix: login",
                  undefined,
                  false,
               );
            },
            retry: instantRetry(),
         },
      );
      await expect(chat.complete({ prompt: "p" })).rejects.toThrow("no ADC");
      expect(requests).toHaveLength(0);
   });

   it("needs projectId and location", () => {
      expect(() =>
         createChatModel(llm({ provider: "vertex", apiKey: undefined })),
      ).toThrow("needs projectId and location");
   });
});
