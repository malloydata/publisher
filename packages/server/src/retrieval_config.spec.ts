// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import {
   getEmbeddingConfig,
   getEmbeddingSettings,
   getLlmSettings,
   parseRetrievalConfig,
} from "./config";
import { getEgressPreset, setRetrievalConfig } from "./retrieval_config";
import {
   _clearEmbeddingProviderForTests,
   embeddingConfigured,
   getEmbeddingProvider,
} from "./service/embedding_provider";

const VARS = [
   "LLM_API_KEY",
   "EMBEDDING_API_KEY",
   "EMBEDDING_API_BASE",
   "EMBEDDING_MODEL",
   "EMBEDDING_DIMENSIONS",
   "EMBEDDING_MIN_SIMILARITY",
];

describe("retrieval.llm validation", () => {
   it("applies the documented defaults", () => {
      const cfg = parseRetrievalConfig({
         llm: { provider: "anthropic", model: "m" },
      });
      expect(cfg?.llm).toEqual({
         provider: "anthropic",
         model: "m",
         timeoutMs: 30_000,
         concurrency: 4,
         maxCallsPerSync: 300,
      });
   });

   it("reads every key, including digit strings from ${VAR} substitution", () => {
      const cfg = parseRetrievalConfig({
         llm: {
            provider: "vertex",
            model: "m",
            projectId: "p",
            location: "us-central1",
            timeoutMs: "1500",
            concurrency: 2,
            maxCallsPerSync: 10,
         },
      });
      expect(cfg?.llm).toMatchObject({
         provider: "vertex",
         projectId: "p",
         location: "us-central1",
         timeoutMs: 1500,
         concurrency: 2,
         maxCallsPerSync: 10,
      });
   });

   it("an empty llm block means off, not an error", () => {
      expect(parseRetrievalConfig({ llm: {} })).toEqual({});
   });

   it("rejects an unknown provider with the valid list and a fix", () => {
      expect(() =>
         parseRetrievalConfig({ llm: { provider: "cohere", model: "m" } }),
      ).toThrow(
         'Invalid retrieval.llm.provider: expected one of openai, openai-compatible, ollama, anthropic, google, vertex, got "cohere". Fix:',
      );
   });

   it("rejects keys without a provider", () => {
      expect(() => parseRetrievalConfig({ llm: { model: "m" } })).toThrow(
         "Invalid retrieval.llm.provider: expected one of",
      );
   });

   it("requires a model", () => {
      expect(() =>
         parseRetrievalConfig({ llm: { provider: "openai" } }),
      ).toThrow("Invalid retrieval.llm.model: expected a non-empty string");
   });

   it("requires baseUrl for openai-compatible and projectId+location for vertex", () => {
      expect(() =>
         parseRetrievalConfig({
            llm: { provider: "openai-compatible", model: "m" },
         }),
      ).toThrow("Invalid retrieval.llm.baseUrl");
      expect(() =>
         parseRetrievalConfig({ llm: { provider: "vertex", model: "m" } }),
      ).toThrow("needs projectId and location");
   });

   it("rejects a bad URL, a non-positive number and an unknown key", () => {
      expect(() =>
         parseRetrievalConfig({
            llm: { provider: "ollama", model: "m", baseUrl: "not a url" },
         }),
      ).toThrow("Invalid retrieval.llm.baseUrl: expected a URL");
      expect(() =>
         parseRetrievalConfig({
            llm: { provider: "ollama", model: "m", concurrency: 0 },
         }),
      ).toThrow(
         "Invalid retrieval.llm.concurrency: expected a positive integer, got 0. Fix:",
      );
      expect(() =>
         parseRetrievalConfig({
            llm: { provider: "ollama", model: "m", apiKey: "sk-nope" },
         }),
      ).toThrow(
         "Invalid retrieval.llm: unknown key 'apiKey'. Valid keys: provider, model, baseUrl",
      );
      expect(() =>
         parseRetrievalConfig({
            llm: { provider: "ollama", model: "m", maxCallsPerRequest: 5 },
         }),
      ).toThrow(
         "Invalid retrieval.llm: unknown key 'maxCallsPerRequest'. Valid keys: provider, model, baseUrl, projectId, location, timeoutMs, concurrency, maxCallsPerSync",
      );
   });
});

describe("retrieval.embedding and retrieval.egress validation", () => {
   it("accepts provider, model, dimensions and both prefixes", () => {
      expect(
         parseRetrievalConfig({
            embedding: {
               provider: "ollama",
               model: "nomic-embed-text",
               dimensions: 768,
               queryPrefix: "search_query: ",
               documentPrefix: "search_document: ",
            },
         })?.embedding,
      ).toEqual({
         provider: "ollama",
         model: "nomic-embed-text",
         dimensions: 768,
         queryPrefix: "search_query: ",
         documentPrefix: "search_document: ",
      });
   });

   it("prefixes default to empty strings", () => {
      expect(
         parseRetrievalConfig({ embedding: { dimensions: 512 } })?.embedding,
      ).toEqual({ dimensions: 512, queryPrefix: "", documentPrefix: "" });
   });

   it("anthropic cannot embed", () => {
      expect(() =>
         parseRetrievalConfig({
            embedding: { provider: "anthropic", model: "m" },
         }),
      ).toThrow('"anthropic" has no embeddings API');
   });

   it("a non-OpenAI provider needs a model", () => {
      expect(() =>
         parseRetrievalConfig({ embedding: { provider: "google" } }),
      ).toThrow(
         'Invalid retrieval.embedding.model: provider "google" needs one',
      );
      // OpenAI has a default.
      expect(() =>
         parseRetrievalConfig({ embedding: { provider: "openai" } }),
      ).not.toThrow();
   });

   it("egress preset is default or full", () => {
      expect(
         parseRetrievalConfig({ egress: { preset: "full" } })?.egress,
      ).toEqual({ preset: "full" });
      expect(() =>
         parseRetrievalConfig({ egress: { preset: "wide" } }),
      ).toThrow(
         'Invalid retrieval.egress.preset: expected one of default, full, got "wide". Fix:',
      );
      expect(() =>
         parseRetrievalConfig({ egress: { preset: "full", code: true } }),
      ).toThrow("unknown key 'code'. Valid keys: preset");
   });

   it("egress defaults to default", () => {
      setRetrievalConfig(undefined);
      expect(getEgressPreset()).toBe("default");
      setRetrievalConfig({ egress: { preset: "full" } });
      expect(getEgressPreset()).toBe("full");
      setRetrievalConfig(undefined);
   });
});

describe("settings resolution and precedence", () => {
   const saved: Record<string, string | undefined> = {};
   beforeEach(() => {
      for (const v of VARS) {
         saved[v] = process.env[v];
         delete process.env[v];
      }
      setRetrievalConfig(undefined);
      _clearEmbeddingProviderForTests();
   });
   afterEach(() => {
      for (const v of VARS) {
         if (saved[v] === undefined) delete process.env[v];
         else process.env[v] = saved[v];
      }
      setRetrievalConfig(undefined);
      _clearEmbeddingProviderForTests();
   });

   it("LLM is off with no provider, with a keyed provider and no key, and never errors", () => {
      expect(getLlmSettings(undefined)).toBeNull();
      const cfg = parseRetrievalConfig({
         llm: { provider: "anthropic", model: "m" },
      })!;
      expect(getLlmSettings(cfg.llm)).toBeNull();
   });

   it("LLM is on with a key, and with no key for ollama and vertex", () => {
      process.env.LLM_API_KEY = "  sk-llm  ";
      const keyed = parseRetrievalConfig({
         llm: { provider: "anthropic", model: "m" },
      })!;
      expect(getLlmSettings(keyed.llm)?.apiKey).toBe("sk-llm");
      delete process.env.LLM_API_KEY;
      const ollama = parseRetrievalConfig({
         llm: { provider: "ollama", model: "llama" },
      })!;
      expect(getLlmSettings(ollama.llm)?.apiKey).toBeUndefined();
      const vertex = parseRetrievalConfig({
         llm: {
            provider: "vertex",
            model: "m",
            projectId: "p",
            location: "l",
         },
      })!;
      expect(getLlmSettings(vertex.llm)).not.toBeNull();
   });

   it("an ambient OPENAI_API_KEY does not turn anything on", () => {
      const before = process.env.OPENAI_API_KEY;
      process.env.OPENAI_API_KEY = "sk-ambient";
      try {
         const cfg = parseRetrievalConfig({
            llm: { provider: "openai", model: "m" },
         })!;
         expect(getLlmSettings(cfg.llm)).toBeNull();
         expect(getEmbeddingSettings(undefined)).toBeNull();
      } finally {
         if (before === undefined) delete process.env.OPENAI_API_KEY;
         else process.env.OPENAI_API_KEY = before;
      }
   });

   it("embedding env variables alone still work (the fallback)", () => {
      process.env.EMBEDDING_API_KEY = "sk-e";
      process.env.EMBEDDING_API_BASE = "https://env.example.com/v1/";
      process.env.EMBEDDING_MODEL = "env-model";
      process.env.EMBEDDING_DIMENSIONS = "256";
      expect(getEmbeddingConfig()).toMatchObject({
         apiKey: "sk-e",
         baseUrl: "https://env.example.com/v1",
         model: "env-model",
         dimensions: 256,
      });
   });

   it("a file key wins over the matching variable, and a variable fills what the file omits", () => {
      process.env.EMBEDDING_API_KEY = "sk-e";
      process.env.EMBEDDING_API_BASE = "https://env.example.com/v1";
      process.env.EMBEDDING_MODEL = "env-model";
      process.env.EMBEDDING_DIMENSIONS = "256";
      const file = parseRetrievalConfig({
         embedding: {
            provider: "openai-compatible",
            baseUrl: "https://file.example.com/v1",
            model: "file-model",
            dimensions: 512,
         },
      })!.embedding;
      expect(getEmbeddingConfig(file)).toMatchObject({
         baseUrl: "https://file.example.com/v1",
         model: "file-model",
         dimensions: 512,
      });
      // Only the model in the file: the rest comes from the variables.
      const partial = parseRetrievalConfig({
         embedding: { model: "file-model" },
      })!.embedding;
      expect(getEmbeddingConfig(partial)).toMatchObject({
         baseUrl: "https://env.example.com/v1",
         model: "file-model",
         dimensions: 256,
      });
   });

   it("the default embedding model stays text-embedding-3-small and dimensions stay unset", () => {
      process.env.EMBEDDING_API_KEY = "sk-e";
      const cfg = getEmbeddingConfig();
      expect(cfg?.model).toBe("text-embedding-3-small");
      expect(cfg?.baseUrl).toBe("https://api.openai.com/v1");
      expect(cfg?.dimensions).toBeUndefined();
   });

   it("ollama needs no key and defaults to the local endpoint, not OpenAI's", () => {
      const file = parseRetrievalConfig({
         embedding: { provider: "ollama", model: "nomic-embed-text" },
      })!.embedding;
      const settings = getEmbeddingSettings(file);
      expect(settings).toMatchObject({
         provider: "ollama",
         model: "nomic-embed-text",
         baseUrl: "http://localhost:11434/v1",
         apiKey: undefined,
      });
   });

   it("google needs EMBEDDING_API_KEY; vertex does not", () => {
      const google = parseRetrievalConfig({
         embedding: { provider: "google", model: "e" },
      })!.embedding;
      expect(getEmbeddingSettings(google)).toBeNull();
      process.env.EMBEDDING_API_KEY = "gk";
      expect(getEmbeddingSettings(google)?.apiKey).toBe("gk");
      delete process.env.EMBEDDING_API_KEY;
      const vertex = parseRetrievalConfig({
         embedding: {
            provider: "vertex",
            model: "e",
            projectId: "p",
            location: "l",
            dimensions: 512,
         },
      })!.embedding;
      expect(getEmbeddingSettings(vertex)).toMatchObject({
         provider: "vertex",
         dimensions: 512,
         projectId: "p",
      });
   });

   it("prefixes from the file reach the settings and the provider", () => {
      process.env.EMBEDDING_API_KEY = "sk-e";
      setRetrievalConfig(
         parseRetrievalConfig({
            embedding: { queryPrefix: "q: ", documentPrefix: "d: " },
         }),
      );
      expect(embeddingConfigured()).toBe(true);
      const provider = getEmbeddingProvider();
      expect(provider?.queryPrefix).toBe("q: ");
      expect(provider?.documentPrefix).toBe("d: ");
   });

   it("getEmbeddingProvider follows a changed file setting on the next call", () => {
      process.env.EMBEDDING_API_KEY = "sk-e";
      setRetrievalConfig(
         parseRetrievalConfig({ embedding: { model: "model-a" } }),
      );
      expect(getEmbeddingProvider()?.model).toBe("model-a");
      setRetrievalConfig(
         parseRetrievalConfig({ embedding: { model: "model-b" } }),
      );
      expect(getEmbeddingProvider()?.model).toBe("model-b");
   });
});
