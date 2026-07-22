import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import {
   EmbeddingProvider,
   MAX_EMBED_BATCH_SIZE,
   MAX_EMBED_INPUT_CHARS,
   _clearEmbeddingProviderForTests,
   embeddingConfigured,
   getEmbeddingProvider,
   prepareEmbeddingInput,
} from "./embedding_provider";
import { EmbeddingConfig } from "../config";

const CONFIG: EmbeddingConfig = {
   apiKey: "test-key",
   model: "test-model",
   baseUrl: "https://embeddings.example.com/v1",
};

interface CapturedRequest {
   url: string;
   body: Record<string, unknown>;
   authorization: string | undefined;
}

/** A fetch stub that records requests and answers with unit vectors. */
function stubFetch(
   captured: CapturedRequest[],
   respond?: (inputs: string[]) => Response,
): typeof fetch {
   return (async (url: RequestInfo | URL, init?: RequestInit) => {
      const body = JSON.parse(String(init?.body)) as Record<string, unknown>;
      captured.push({
         url: String(url),
         body,
         authorization: (init?.headers as Record<string, string>)?.[
            "Authorization"
         ],
      });
      const inputs = body.input as string[];
      if (respond) return respond(inputs);
      return new Response(
         JSON.stringify({
            data: inputs.map((_, index) => ({ index, embedding: [index, 1] })),
         }),
         { status: 200 },
      );
   }) as typeof fetch;
}

describe("prepareEmbeddingInput", () => {
   it("collapses whitespace and newlines", () => {
      expect(prepareEmbeddingInput("a\nb\t c  d")).toBe("a b c d");
   });

   it("caps input length", () => {
      const long = "x".repeat(MAX_EMBED_INPUT_CHARS + 500);
      expect(prepareEmbeddingInput(long).length).toBe(MAX_EMBED_INPUT_CHARS);
   });
});

describe("EmbeddingProvider", () => {
   it("embeds a batch and returns vectors in input order", async () => {
      const captured: CapturedRequest[] = [];
      const provider = new EmbeddingProvider(CONFIG, stubFetch(captured));
      const vectors = await provider.embedBatch(["a", "b"], 1000);
      expect(vectors).toEqual([
         [0, 1],
         [1, 1],
      ]);
      expect(captured).toHaveLength(1);
      expect(captured[0].url).toBe(
         "https://embeddings.example.com/v1/embeddings",
      );
      expect(captured[0].body.model).toBe("test-model");
      expect(captured[0].authorization).toBe("Bearer test-key");
   });

   it("places vectors by the response's index fields, not response order", async () => {
      const captured: CapturedRequest[] = [];
      const provider = new EmbeddingProvider(
         CONFIG,
         stubFetch(
            captured,
            () =>
               new Response(
                  JSON.stringify({
                     data: [
                        { index: 1, embedding: [2, 2] },
                        { index: 0, embedding: [1, 1] },
                     ],
                  }),
                  { status: 200 },
               ),
         ),
      );
      const vectors = await provider.embedBatch(["a", "b"], 1000);
      expect(vectors).toEqual([
         [1, 1],
         [2, 2],
      ]);
   });

   it("splits large inputs into batches", async () => {
      const captured: CapturedRequest[] = [];
      const provider = new EmbeddingProvider(CONFIG, stubFetch(captured));
      const texts = Array.from(
         { length: MAX_EMBED_BATCH_SIZE + 1 },
         (_, i) => `t${i}`,
      );
      const vectors = await provider.embedBatch(texts, 1000);
      expect(vectors).toHaveLength(texts.length);
      expect(captured).toHaveLength(2);
      expect((captured[0].body.input as string[]).length).toBe(
         MAX_EMBED_BATCH_SIZE,
      );
      expect((captured[1].body.input as string[]).length).toBe(1);
   });

   it("sends the dimensions param only when configured", async () => {
      const captured: CapturedRequest[] = [];
      const without = new EmbeddingProvider(CONFIG, stubFetch(captured));
      await without.embedBatch(["a"], 1000);
      expect(captured[0].body.dimensions).toBeUndefined();

      const withDims = new EmbeddingProvider(
         { ...CONFIG, dimensions: 512 },
         stubFetch(captured),
      );
      await withDims.embedBatch(["a"], 1000);
      expect(captured[1].body.dimensions).toBe(512);
   });

   it("reports HTTP failures with a truncated body and no header echo", async () => {
      const provider = new EmbeddingProvider(
         CONFIG,
         stubFetch(
            [],
            () => new Response("boom ".repeat(100), { status: 401 }),
         ),
      );
      const err = await provider.embedBatch(["a"], 1000).then(
         () => undefined,
         (e: Error) => e,
      );
      expect(err).toBeDefined();
      expect(err!.message).toContain("(401)");
      expect(err!.message.length).toBeLessThan(400);
      expect(err!.message).not.toContain("test-key");
   });

   it("names a timeout in the failure message", async () => {
      const timeoutFetch = (async () => {
         const error = new Error("The operation timed out");
         error.name = "TimeoutError";
         throw error;
      }) as unknown as typeof fetch;
      const provider = new EmbeddingProvider(CONFIG, timeoutFetch);
      const err = await provider.embedBatch(["a"], 1234).then(
         () => undefined,
         (e: Error) => e,
      );
      expect(err!.message).toContain("timed out after 1234ms");
   });

   it("rejects a response with the wrong number of embeddings", async () => {
      const provider = new EmbeddingProvider(
         CONFIG,
         stubFetch(
            [],
            () =>
               new Response(
                  JSON.stringify({ data: [{ index: 0, embedding: [1] }] }),
                  { status: 200 },
               ),
         ),
      );
      expect(provider.embedBatch(["a", "b"], 1000)).rejects.toThrow(
         "malformed",
      );
   });
});

describe("getEmbeddingProvider / embeddingConfigured", () => {
   const ENV_KEYS = [
      "EMBEDDING_API_KEY",
      "EMBEDDING_MODEL",
      "EMBEDDING_API_BASE",
      "EMBEDDING_DIMENSIONS",
   ];
   const saved: Record<string, string | undefined> = {};

   beforeEach(() => {
      for (const key of ENV_KEYS) {
         saved[key] = process.env[key];
         delete process.env[key];
      }
      _clearEmbeddingProviderForTests();
   });

   afterEach(() => {
      for (const key of ENV_KEYS) {
         if (saved[key] === undefined) delete process.env[key];
         else process.env[key] = saved[key];
      }
      _clearEmbeddingProviderForTests();
   });

   it("returns null (feature off) without EMBEDDING_API_KEY", () => {
      expect(getEmbeddingProvider()).toBeNull();
      expect(embeddingConfigured()).toBe(false);
   });

   it("ignores an ambient OPENAI_API_KEY", () => {
      const before = process.env.OPENAI_API_KEY;
      process.env.OPENAI_API_KEY = "ambient";
      try {
         expect(getEmbeddingProvider()).toBeNull();
         expect(embeddingConfigured()).toBe(false);
      } finally {
         if (before === undefined) delete process.env.OPENAI_API_KEY;
         else process.env.OPENAI_API_KEY = before;
      }
   });

   it("builds a provider with defaults when the key is set", () => {
      process.env.EMBEDDING_API_KEY = "k";
      const provider = getEmbeddingProvider();
      expect(provider).not.toBeNull();
      expect(provider!.model).toBe("text-embedding-3-small");
      expect(embeddingConfigured()).toBe(true);
   });

   it("rebuilds the provider when the configuration changes", () => {
      process.env.EMBEDDING_API_KEY = "k";
      const first = getEmbeddingProvider();
      expect(getEmbeddingProvider()).toBe(first);
      process.env.EMBEDDING_MODEL = "other-model";
      const second = getEmbeddingProvider();
      expect(second).not.toBe(first);
      expect(second!.model).toBe("other-model");
   });

   it("does not stick to null after the key appears", () => {
      expect(getEmbeddingProvider()).toBeNull();
      process.env.EMBEDDING_API_KEY = "k";
      expect(getEmbeddingProvider()).not.toBeNull();
   });

   it("throws on a malformed base URL but still reports configured", () => {
      process.env.EMBEDDING_API_KEY = "k";
      process.env.EMBEDDING_API_BASE = "not a url";
      expect(() => getEmbeddingProvider()).toThrow("EMBEDDING_API_BASE");
      expect(embeddingConfigured()).toBe(true);
   });

   it("throws on a malformed EMBEDDING_DIMENSIONS", () => {
      process.env.EMBEDDING_API_KEY = "k";
      process.env.EMBEDDING_DIMENSIONS = "lots";
      expect(() => getEmbeddingProvider()).toThrow("EMBEDDING_DIMENSIONS");
   });
});
