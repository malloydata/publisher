// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, describe, expect, it } from "bun:test";
import {
   LlmError,
   OpenAiCompatLlmProvider,
   _clearLlmProviderForTests,
   getLlmProvider,
   stripThinking,
   type LlmRequest,
} from "./llm_provider";

const CONFIG = { apiKey: "sk-secret", baseUrl: "https://llm.example.com/v1" };

const REQUEST: LlmRequest = {
   stage: "refine",
   model: "test-model",
   system: "be terse",
   user: "score these",
   timeoutMs: 1000,
};

interface Captured {
   url: string;
   headers: Record<string, string>;
   body: Record<string, any>;
}

function reply(content: unknown, extra: Record<string, unknown> = {}): Response {
   return new Response(
      JSON.stringify({
         model: "test-model",
         choices: [{ message: { content }, finish_reason: "stop" }],
         usage: { prompt_tokens: 11, completion_tokens: 7 },
         ...extra,
      }),
      { status: 200 },
   );
}

function stub(
   captured: Captured[],
   respond: (n: number, body: Record<string, any>) => Response | Promise<Response>,
): typeof fetch {
   return (async (url: RequestInfo | URL, init?: RequestInit) => {
      const body = JSON.parse(String(init?.body));
      captured.push({
         url: String(url),
         headers: init?.headers as Record<string, string>,
         body,
      });
      return respond(captured.length, body);
   }) as typeof fetch;
}

const fail = async (p: Promise<unknown>): Promise<LlmError> => {
   try {
      await p;
   } catch (e) {
      return e as LlmError;
   }
   throw new Error("expected the call to fail");
};

afterEach(() => _clearLlmProviderForTests());

describe("OpenAiCompatLlmProvider", () => {
   it("posts a chat completion and returns the text, usage and model", async () => {
      const captured: Captured[] = [];
      const p = new OpenAiCompatLlmProvider(CONFIG, stub(captured, () => reply("hello")));
      const r = await p.complete({ ...REQUEST, temperature: 0, seed: 7, maxTokens: 50 });
      expect(r.text).toBe("hello");
      expect(r.model).toBe("test-model");
      expect(r.finishReason).toBe("stop");
      expect(r.usage).toEqual({ promptTokens: 11, completionTokens: 7 });
      const c = captured[0];
      expect(c.url).toBe("https://llm.example.com/v1/chat/completions");
      expect(c.headers.Authorization).toBe("Bearer sk-secret");
      expect(c.body).toEqual({
         model: "test-model",
         messages: [
            { role: "system", content: "be terse" },
            { role: "user", content: "score these" },
         ],
         temperature: 0,
         seed: 7,
         max_tokens: 50,
         stream: false,
      });
   });

   it("omits the system message and merges extraBody last", async () => {
      const captured: Captured[] = [];
      const p = new OpenAiCompatLlmProvider(CONFIG, stub(captured, () => reply("x")));
      await p.complete({
         ...REQUEST,
         system: undefined,
         extraBody: { think: false, temperature: 0.5 },
      });
      expect(captured[0].body.messages).toEqual([
         { role: "user", content: "score these" },
      ]);
      expect(captured[0].body.think).toBe(false);
      // extraBody wins, so an operator can override any field.
      expect(captured[0].body.temperature).toBe(0.5);
   });

   it("sends no Authorization header without a key (Ollama)", async () => {
      const captured: Captured[] = [];
      const p = new OpenAiCompatLlmProvider(
         { apiKey: "", baseUrl: "http://localhost:11434/v1" },
         stub(captured, () => reply("x")),
      );
      await p.complete(REQUEST);
      expect(captured[0].headers.Authorization).toBeUndefined();
      expect(p.id).toBe("localhost:11434");
   });

   it("asks for JSON mode only when requested", async () => {
      const captured: Captured[] = [];
      const p = new OpenAiCompatLlmProvider(CONFIG, stub(captured, () => reply("{}")));
      await p.complete(REQUEST);
      await p.complete({ ...REQUEST, jsonMode: "json_object" });
      expect(captured[0].body.response_format).toBeUndefined();
      expect(captured[1].body.response_format).toEqual({ type: "json_object" });
   });

   it("retries once without JSON mode when the server rejects it, and remembers", async () => {
      const captured: Captured[] = [];
      const p = new OpenAiCompatLlmProvider(
         CONFIG,
         stub(captured, (_n, body) =>
            body.response_format
               ? new Response("unsupported parameter: response_format", { status: 400 })
               : reply("ok"),
         ),
      );
      const first = await p.complete({ ...REQUEST, jsonMode: "json_object" });
      expect(first.text).toBe("ok");
      expect(captured).toHaveLength(2);
      const second = await p.complete({ ...REQUEST, jsonMode: "json_object" });
      expect(second.text).toBe("ok");
      // No third attempt at JSON mode: the rejection was remembered.
      expect(captured).toHaveLength(3);
      expect(captured[2].body.response_format).toBeUndefined();
   });

   it("does not swallow an unrelated 400 as a JSON-mode problem", async () => {
      const p = new OpenAiCompatLlmProvider(
         CONFIG,
         stub([], () => new Response("model 'nope' not found", { status: 400 })),
      );
      const e = await fail(p.complete({ ...REQUEST, jsonMode: "json_object" }));
      expect(e.kind).toBe("http");
      expect(e.status).toBe(400);
      expect(e.retryable).toBe(false);
   });

   it("strips <think> blocks, closed or cut off", async () => {
      expect(stripThinking("<think>hmm</think>answer")).toBe("answer");
      expect(stripThinking("answer<think>never closed")).toBe("answer");
      const p = new OpenAiCompatLlmProvider(
         CONFIG,
         stub([], () => reply("<think>plan</think>[1,2]")),
      );
      expect((await p.complete(REQUEST)).text).toBe("[1,2]");
   });

   it("falls back to reasoning_content when content is empty", async () => {
      const p = new OpenAiCompatLlmProvider(
         CONFIG,
         stub(
            [],
            () =>
               new Response(
                  JSON.stringify({
                     choices: [
                        { message: { content: "", reasoning_content: "[3]" } },
                     ],
                  }),
                  { status: 200 },
               ),
         ),
      );
      expect((await p.complete(REQUEST)).text).toBe("[3]");
   });

   it("reads content given as a list of text parts", async () => {
      const p = new OpenAiCompatLlmProvider(
         CONFIG,
         stub([], () =>
            reply([
               { type: "text", text: "he" },
               { type: "text", text: "llo" },
            ]),
         ),
      );
      expect((await p.complete(REQUEST)).text).toBe("hello");
   });

   it("classifies an empty or non-JSON response as malformed and retryable", async () => {
      const empty = new OpenAiCompatLlmProvider(
         CONFIG,
         stub([], () => new Response(JSON.stringify({ choices: [] }), { status: 200 })),
      );
      const e1 = await fail(empty.complete(REQUEST));
      expect(e1.kind).toBe("malformed");
      expect(e1.retryable).toBe(true);
      const html = new OpenAiCompatLlmProvider(
         CONFIG,
         stub([], () => new Response("<html>", { status: 200 })),
      );
      expect((await fail(html.complete(REQUEST))).kind).toBe("malformed");
   });

   it("drops auth-failure bodies entirely (they can reflect the key)", async () => {
      const p = new OpenAiCompatLlmProvider(
         CONFIG,
         stub([], () => new Response("bad key sk-secret", { status: 401 })),
      );
      const e = await fail(p.complete(REQUEST));
      expect(e.kind).toBe("auth");
      expect(e.retryable).toBe(false);
      expect(e.message).not.toContain("sk-secret");
      expect(e.message).toContain("LLM_API_KEY");
   });

   it("scrubs the key from other error bodies and caps their length", async () => {
      const p = new OpenAiCompatLlmProvider(
         CONFIG,
         stub(
            [],
            () =>
               new Response(`upstream said sk-secret ${"x".repeat(500)}`, { status: 502 }),
         ),
      );
      const e = await fail(p.complete(REQUEST));
      expect(e.kind).toBe("http");
      expect(e.retryable).toBe(true);
      expect(e.message).not.toContain("sk-secret");
      expect(e.message).toContain("[REDACTED]");
      expect(e.message.length).toBeLessThan(400);
   });

   it("keeps a keyless error body intact", async () => {
      const p = new OpenAiCompatLlmProvider(
         { apiKey: "", baseUrl: "http://localhost:11434/v1" },
         stub([], () => new Response("model not found", { status: 404 })),
      );
      const e = await fail(p.complete(REQUEST));
      expect(e.message).toContain("(404): model not found");
      expect(e.retryable).toBe(false);
   });

   it("reads Retry-After on a 429", async () => {
      const p = new OpenAiCompatLlmProvider(
         CONFIG,
         stub(
            [],
            () =>
               new Response("slow down", {
                  status: 429,
                  headers: { "retry-after": "2" },
               }),
         ),
      );
      const e = await fail(p.complete(REQUEST));
      expect(e.kind).toBe("rate_limit");
      expect(e.retryable).toBe(true);
      expect(e.retryAfterMs).toBe(2000);
   });

   it("names a timeout", async () => {
      const hang = ((_u: unknown, init?: RequestInit) =>
         new Promise((_, reject) => {
            init?.signal?.addEventListener("abort", () => reject(init.signal!.reason));
         })) as unknown as typeof fetch;
      const p = new OpenAiCompatLlmProvider(CONFIG, hang);
      const e = await fail(p.complete({ ...REQUEST, timeoutMs: 20 }));
      expect(e.kind).toBe("timeout");
      expect(e.retryable).toBe(true);
      expect(e.message).toContain("timed out after 20ms");
   });

   it("classifies a refused connection as network and retryable", async () => {
      const p = new OpenAiCompatLlmProvider(CONFIG, (async () => {
         throw new TypeError("fetch failed");
      }) as unknown as typeof fetch);
      const e = await fail(p.complete(REQUEST));
      expect(e.kind).toBe("network");
      expect(e.retryable).toBe(true);
   });

   it("reports a caller abort as aborted, not retryable", async () => {
      const controller = new AbortController();
      const hang = ((_u: unknown, init?: RequestInit) =>
         new Promise((_, reject) => {
            init?.signal?.addEventListener("abort", () => reject(init.signal!.reason));
         })) as unknown as typeof fetch;
      const p = new OpenAiCompatLlmProvider(CONFIG, hang);
      const pending = fail(p.complete({ ...REQUEST, signal: controller.signal }));
      controller.abort();
      const e = await pending;
      expect(e.kind).toBe("aborted");
      expect(e.retryable).toBe(false);
   });
});

describe("getLlmProvider", () => {
   it("is null with no endpoint configured", () => {
      const saved = { ...process.env };
      delete process.env.LLM_API_BASE;
      delete process.env.LLM_API_KEY;
      try {
         expect(getLlmProvider()).toBeNull();
      } finally {
         process.env = saved;
      }
   });

   it("builds one provider per configuration and rebuilds on change", () => {
      const saved = { ...process.env };
      process.env.LLM_API_BASE = "http://localhost:11434/v1";
      try {
         const a = getLlmProvider();
         expect(a).not.toBeNull();
         expect(getLlmProvider()).toBe(a);
         process.env.LLM_API_BASE = "http://localhost:8000/v1";
         expect(getLlmProvider()).not.toBe(a);
      } finally {
         process.env = saved;
      }
   });
});
