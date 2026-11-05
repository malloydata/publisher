// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What a package's index status says: `lexical` when no embedding provider is
 * configured, an error when the configuration is invalid, and otherwise the
 * state of the find, list, judge engine's index build.
 */

import { afterEach, describe, expect, it } from "bun:test";
import { DEFAULT_EMBEDDING_MIN_SIMILARITY } from "../../config";
import {
   EmbeddingProvider,
   _clearEmbeddingProviderForTests,
   _setEmbeddingProviderForTests,
} from "../../service/embedding_provider";
import type { EnvironmentStore } from "../../service/environment_store";
import { getPackageEmbeddingStatus } from "./package_embedding";

afterEach(() => {
   _setEmbeddingProviderForTests(null);
   _clearEmbeddingProviderForTests();
});

const ZERO_COUNTS = {
   embeddedRows: 0,
   totalRows: 0,
   totalEntities: 0,
   embeddedEntities: 0,
};

/** A package with one source and two fields, and a store that holds it. */
function storeHolding(name: string) {
   const field = (n: string) => ({
      kind: "dimension",
      name: n,
      annotations: [],
   });
   const models: Record<string, unknown> = {
      "m.malloy": {
         getSourceInfos: () => [
            {
               name: "src",
               annotations: [],
               schema: { fields: [field("alpha"), field("beta")] },
            },
         ],
         getQueries: () => [],
      },
   };
   const pkg = {
      getPackageName: () => name,
      listModels: async () => Object.keys(models).map((p) => ({ path: p })),
      getModel: (p: string) => models[p],
   };
   const environment = {
      getPackage: async () => pkg,
      peekPackage: () => pkg,
      getStaleCompileErrors: () => new Map(),
   };
   return {
      getEnvironment: async () => environment,
      peekEnvironment: () => environment,
   } as unknown as EnvironmentStore;
}

describe("getPackageEmbeddingStatus", () => {
   it("is lexical, by design, when no provider is configured", async () => {
      _setEmbeddingProviderForTests(null);
      const status = await getPackageEmbeddingStatus(
         storeHolding("any"),
         "env",
         "any",
      );
      expect(status).toEqual({ status: "lexical", ...ZERO_COUNTS });
   });

   it("is an error with the reason when the embedding configuration is invalid", async () => {
      const saved = {
         key: process.env.EMBEDDING_API_KEY,
         base: process.env.EMBEDDING_API_BASE,
      };
      process.env.EMBEDDING_API_KEY = "k";
      process.env.EMBEDDING_API_BASE = "not a url";
      _clearEmbeddingProviderForTests();
      try {
         const status = await getPackageEmbeddingStatus(
            storeHolding("any"),
            "env",
            "any",
         );
         expect(status.status).toBe("error");
         expect(status.lastError?.message).toContain("EMBEDDING_API_BASE");
      } finally {
         if (saved.key === undefined) delete process.env.EMBEDDING_API_KEY;
         else process.env.EMBEDDING_API_KEY = saved.key;
         if (saved.base === undefined) delete process.env.EMBEDDING_API_BASE;
         else process.env.EMBEDDING_API_BASE = saved.base;
         _clearEmbeddingProviderForTests();
      }
   });

   it("reports the search index build, with its own state beside the status", async () => {
      _setEmbeddingProviderForTests(
         new EmbeddingProvider(
            {
               apiKey: "test",
               model: "stub-model",
               baseUrl: "https://stub.example.com/v1",
               minSimilarity: DEFAULT_EMBEDDING_MIN_SIMILARITY,
            },
            (async () =>
               new Response("{}", { status: 200 })) as unknown as typeof fetch,
         ),
      );
      // No build has started for this package instance, so the engine reports
      // it as indexing; the counts are zero because no per-entity index is kept.
      const status = await getPackageEmbeddingStatus(
         storeHolding("fresh"),
         "env",
         "fresh",
      );
      expect(status).toEqual({
         status: "indexing",
         ...ZERO_COUNTS,
         fcj: { engine: "fcj", status: "indexing" },
      });
   });
});
