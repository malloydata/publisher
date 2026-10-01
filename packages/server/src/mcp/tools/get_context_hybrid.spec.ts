// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Hybrid retrieval through the handler: fusing lunr's ranking into the
// embedding ranking, off by default.

import {
   afterAll,
   afterEach,
   beforeAll,
   beforeEach,
   describe,
   expect,
   it,
} from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { DEFAULT_EMBEDDING_MIN_SIMILARITY } from "../../config";
import {
   _clearRetrievalConfigForTests,
   _setRetrievalConfigForTests,
} from "../../retrieval/retrieval_config";
import {
   EmbeddingProvider,
   _clearEmbeddingProviderForTests,
   _setEmbeddingProviderForTests,
} from "../../service/embedding_provider";
import { DuckDBConnection } from "../../storage/duckdb/DuckDBConnection";
import { createEntityEmbeddingsTable } from "../../storage/duckdb/schema";
import { _resetEmbeddingIndexStateForTests } from "./embedding_index";
import {
   captureHandler,
   entityNames,
   parse,
   storeFor,
} from "./retrieval_test_kit";

// Cosine to the query "sku": alpha_score 0.9, sku_count 0.6, sku_legacy 0
// (under the floor). lunr finds the two whose docs say "sku".
const measure = (name: string, doc: string) => ({
   kind: "measure",
   name,
   annotations: [`#(doc) ${doc}`],
});
const model = {
   getSourceInfos: () => [
      {
         name: "shop",
         annotations: ["#(doc) One row per shop."],
         schema: {
            fields: [
               measure("alpha_score", "General alpha metric."),
               measure("sku_count", "Count of distinct sku values."),
               measure("sku_legacy", "Old sku counter."),
            ],
         },
      },
   ],
   getQueries: () => [],
};
const pkg = {
   listModels: async () => [{ path: "m.malloy" }],
   getModel: () => model,
};

const vectorFor = (text: string): number[] => {
   if (text === "sku") return [1, 0];
   if (/alpha/.test(text)) return [0.9, Math.sqrt(1 - 0.81)];
   if (/^sku count(:|$)/.test(text)) return [0.6, 0.8];
   return [0, 1];
};

function provider(): EmbeddingProvider {
   const fetchStub = (async (_u: RequestInfo | URL, init?: RequestInit) => {
      const body = JSON.parse(String(init?.body)) as { input: string[] };
      return new Response(
         JSON.stringify({
            data: body.input.map((t, index) => ({
               index,
               embedding: vectorFor(t),
            })),
         }),
         { status: 200 },
      );
   }) as typeof fetch;
   return new EmbeddingProvider(
      {
         apiKey: "t",
         model: "stub",
         baseUrl: "https://stub.example.com/v1",
         minSimilarity: DEFAULT_EMBEDDING_MIN_SIMILARITY,
      },
      fetchStub,
   );
}

const params = {
   search_targets: [{ target_type: "measure", search_text: "sku" }],
   scopes: [{ environment: "hy", package: "p" }],
};

describe("get_context hybrid retrieval", () => {
   let tempDir: string;
   let db: DuckDBConnection;

   beforeAll(async () => {
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "get-context-hybrid-"));
      db = new DuckDBConnection(path.join(tempDir, "test.db"));
      await db.initialize();
      await createEntityEmbeddingsTable(db);
   });
   afterAll(async () => {
      await db.close();
      fs.rmSync(tempDir, { recursive: true, force: true });
      _clearEmbeddingProviderForTests();
   });
   beforeEach(() => {
      _setEmbeddingProviderForTests(provider());
      _resetEmbeddingIndexStateForTests();
   });
   afterEach(() => _clearRetrievalConfigForTests());

   async function ask(hybrid?: Record<string, unknown>) {
      _setRetrievalConfigForTests(hybrid ? { hybrid } : {});
      const handler = captureHandler(storeFor(pkg, db));
      for (let i = 0; i < 400; i++) {
         const payload = parse(await handler(params as never));
         if (payload.retrieval === "semantic") return payload;
         await new Promise((r) => setTimeout(r, 5));
      }
      throw new Error("never became semantic");
   }

   it("is off by default: cosine order, and the lexical-only row is absent", async () => {
      const payload = await ask();
      expect(entityNames(payload)).toEqual(["alpha_score", "sku_count"]);
   });

   it("rerank-only lifts the row lunr also found, and admits nothing new", async () => {
      const payload = await ask({ mode: "rerank-only" });
      expect(entityNames(payload)).toEqual(["sku_count", "alpha_score"]);
      expect(payload.below_cutoff_count).toBeGreaterThan(0);
   });

   it("keeps the published relevance as the cosine", async () => {
      const payload = await ask({ mode: "rerank-only" });
      const byName = Object.fromEntries(
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         payload.sources.flatMap((c: any) =>
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            c.entities.map((e: any) => [e.name, e]),
         ),
      );
      expect(byName.sku_count.relevance).toBe(0.6);
      expect(byName.alpha_score.relevance).toBe(0.9);
   });

   it("union also returns a row only lunr found, with no relevance of its own", async () => {
      const payload = await ask({ mode: "union" });
      expect(entityNames(payload)).toContain("sku_legacy");
      const legacy = payload.sources
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         .flatMap((c: any) => c.entities)
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         .find((e: any) => e.name === "sku_legacy");
      expect(legacy.relevance).toBeUndefined();
   });
});
