// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A failed vendor call has two wordings. `message` goes to the server log and
 * names the endpoint, which on Vertex includes the cloud project path.
 * `publicMessage` is what an MCP caller or the status API may see, and never
 * names the endpoint. Every failure a vendor call can end in has to carry both,
 * including a reply that arrives but is malformed.
 */

import { describe, expect, it } from "bun:test";
import { DEFAULT_EMBEDDING_MIN_SIMILARITY } from "../config";
import { EmbeddingProvider } from "../service/embedding_provider";
import { HttpRequestError, publicMessage } from "../service/http_retry";
import {
   instantRetry,
   jsonResponse,
   stubFetch,
} from "../test_helpers/fetch_stub";
import { checkVectors } from "./embedding_http";
import { parseGeminiReply } from "./google";
import { createChatModel } from "./registry";
import type { LlmSettings } from "./types";

const VERTEX_URL =
   "https://us-central1-aiplatform.googleapis.com/v1/projects/secret-project-42/locations/us-central1/publishers/google/models/m:predict";

/** The error `fn` throws, so a spec can read both of its wordings. */
function thrown(fn: () => unknown): unknown {
   try {
      fn();
   } catch (error) {
      return error;
   }
   throw new Error("expected it to throw");
}

function expectPublic(error: unknown, ...hidden: string[]) {
   expect(error).toBeInstanceOf(HttpRequestError);
   const e = error as HttpRequestError;
   // The log keeps the endpoint.
   expect(e.message).toContain(hidden[0]);
   // The caller never sees it.
   expect(e.publicMessage).toBeDefined();
   for (const h of hidden) expect(e.publicMessage).not.toContain(h);
   expect(publicMessage(e)).toBe(e.publicMessage as string);
}

/** A malformed reply is not worth retrying: it would come back the same. */
function expectNotRetryable(error: unknown) {
   expect((error as HttpRequestError).retryable).toBe(false);
}

describe("publicMessage", () => {
   it("is the public wording of a vendor failure, and the message of anything else", () => {
      const vendor = new HttpRequestError(
         "Chat request to https://host.example.com/x failed (500): boom",
         500,
         true,
         undefined,
         "Chat request failed (500): boom",
      );
      expect(publicMessage(vendor)).toBe("Chat request failed (500): boom");
      expect(publicMessage(new Error("EMBEDDING_API_BASE is not a URL"))).toBe(
         "EMBEDDING_API_BASE is not a URL",
      );
      expect(publicMessage("plain")).toBe("plain");
   });

   it("falls back to the message when a failure carries no public wording", () => {
      const e = new HttpRequestError("it broke", 500, false);
      expect(publicMessage(e)).toBe("it broke");
   });
});

describe("a malformed reply does not name the endpoint", () => {
   it("checkVectors: a wrong count", () => {
      expectPublic(
         thrown(() => checkVectors([[1]], 2, VERTEX_URL)),
         "secret-project-42",
         "aiplatform.googleapis.com",
      );
      expectNotRetryable(thrown(() => checkVectors([[1]], 2, VERTEX_URL)));
   });

   it("checkVectors: a bad vector", () => {
      expectPublic(
         thrown(() => checkVectors([[1], ["x"]], 2, VERTEX_URL)),
         "secret-project-42",
         "aiplatform.googleapis.com",
      );
   });

   it("a Gemini chat reply with no text", () => {
      expectPublic(
         thrown(() => parseGeminiReply({}, VERTEX_URL)),
         "secret-project-42",
         "aiplatform.googleapis.com",
      );
   });

   const llm: LlmSettings = {
      provider: "openai",
      model: "m-1",
      apiKey: "key-abc-123-secret",
      baseUrl: "https://secret-host.example.com/v1",
      timeoutMs: 5_000,
      concurrency: 1,
      maxCallsPerSync: 10,
      maxCallsPerRequest: 20,
   };

   it("an OpenAI-compatible chat reply with no content", async () => {
      const { fetchFn } = stubFetch([() => jsonResponse({ choices: [] })]);
      const chat = createChatModel(llm, { fetchFn, retry: instantRetry() });
      const error = await chat.complete({ prompt: "p" }).then(
         () => undefined,
         (e: unknown) => e,
      );
      expectPublic(error, "secret-host.example.com");
      expectNotRetryable(error);
   });

   it("an OpenAI-compatible reply that is not JSON", async () => {
      const { fetchFn } = stubFetch([() => new Response("<html>oops</html>")]);
      const chat = createChatModel(llm, { fetchFn, retry: instantRetry() });
      const error = await chat.complete({ prompt: "p" }).then(
         () => undefined,
         (e: unknown) => e,
      );
      expectPublic(error, "secret-host.example.com");
   });

   it("an Anthropic chat reply with no text", async () => {
      const { fetchFn } = stubFetch([() => jsonResponse({ content: [] })]);
      const chat = createChatModel(
         {
            ...llm,
            provider: "anthropic",
            baseUrl: "https://secret-host.example.com",
         },
         { fetchFn, retry: instantRetry() },
      );
      const error = await chat.complete({ prompt: "p" }).then(
         () => undefined,
         (e: unknown) => e,
      );
      expectPublic(error, "secret-host.example.com");
   });
});

describe("the OpenAI-compatible embedding endpoint does not name itself to a caller", () => {
   const make = (script: Parameters<typeof stubFetch>[0]) =>
      new EmbeddingProvider(
         {
            apiKey: "sk-secret-key-123",
            model: "stub-model",
            baseUrl: "https://secret-host.example.com/v1",
            minSimilarity: DEFAULT_EMBEDDING_MIN_SIMILARITY,
         },
         stubFetch(script).fetchFn,
      );

   const failure = (p: EmbeddingProvider) =>
      p.embedBatch(["a"], 1_000).then(
         () => undefined,
         (e: unknown) => e,
      );

   it("a 500", async () => {
      const error = await failure(
         make([() => new Response("upstream down", { status: 500 })]),
      );
      expectPublic(error, "secret-host.example.com");
      expect((error as HttpRequestError).publicMessage).toContain("(500)");
   });

   it("a network failure", async () => {
      const p = new EmbeddingProvider(
         {
            apiKey: "sk-secret-key-123",
            model: "stub-model",
            baseUrl: "https://secret-host.example.com/v1",
            minSimilarity: DEFAULT_EMBEDDING_MIN_SIMILARITY,
         },
         (async () => {
            throw new Error("connect ECONNREFUSED 10.0.0.7:443");
         }) as unknown as typeof fetch,
      );
      const error = await failure(p);
      expect(error).toBeInstanceOf(HttpRequestError);
      const e = error as HttpRequestError;
      expect(e.publicMessage).toBeDefined();
      expect(e.publicMessage).not.toContain("secret-host.example.com");
      expect(e.publicMessage).not.toContain("10.0.0.7");
   });

   it("a reply with the wrong number of vectors", async () => {
      const error = await failure(make([() => jsonResponse({ data: [] })]));
      expectPublic(error, "secret-host.example.com");
      expectNotRetryable(error);
   });
});
