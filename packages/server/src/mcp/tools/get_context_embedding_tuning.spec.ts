// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// An asymmetric embedding model through the whole index: documents are embedded
// with their prefix and task type, queries with theirs, a change to the index
// side rebuilds the vectors, and a change to the query side does not.

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
   EmbeddingProvider,
   _clearEmbeddingProviderForTests,
   _setEmbeddingProviderForTests,
   type EmbeddingTuning,
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

const model = {
   getSourceInfos: () => [
      {
         name: "orders",
         annotations: ["#(doc) Every order."],
         schema: {
            fields: [
               {
                  kind: "measure",
                  name: "total_revenue",
                  annotations: ["#(doc) Revenue."],
               },
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

const params = {
   search_targets: [{ target_type: "measure", search_text: "revenue" }],
   scopes: [{ environment: "tune", package: "p" }],
};

interface Call {
   input: string[];
   body: Record<string, unknown>;
}

/** A provider whose fetch gives every text a vector by its bare content. */
function provider(
   tuning: Partial<EmbeddingTuning>,
   calls: Call[],
): EmbeddingProvider {
   const fetchStub = (async (_u: RequestInfo | URL, init?: RequestInit) => {
      const body = JSON.parse(String(init?.body)) as { input: string[] };
      calls.push({ input: body.input, body: body as never });
      return new Response(
         JSON.stringify({
            data: body.input.map((text, index) => ({
               index,
               // "revenue" in any wrapping points at the revenue measure.
               embedding: /revenue/i.test(text) ? [1, 0] : [0, 1],
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
      {
         queryPrefix: "",
         documentPrefix: "",
         extraBody: {},
         queryExtraBody: {},
         ...tuning,
      },
   );
}

describe("get_context with an asymmetric embedding model", () => {
   let tempDir: string;
   let db: DuckDBConnection;

   beforeAll(async () => {
      tempDir = fs.mkdtempSync(
         path.join(os.tmpdir(), "get-context-tuning-embed-"),
      );
      db = new DuckDBConnection(path.join(tempDir, "test.db"));
      await db.initialize();
      await createEntityEmbeddingsTable(db);
   });
   afterAll(async () => {
      await db.close();
      fs.rmSync(tempDir, { recursive: true, force: true });
      _clearEmbeddingProviderForTests();
   });
   beforeEach(async () => {
      await db.run("DELETE FROM entity_embeddings");
      _resetEmbeddingIndexStateForTests();
   });
   afterEach(() => _setEmbeddingProviderForTests(null));

   const handler = () => captureHandler(storeFor(pkg, db));

   async function untilSemantic(h: ReturnType<typeof handler>) {
      for (let i = 0; i < 400; i++) {
         const payload = parse(await h(params));
         if (payload.retrieval === "semantic") return payload;
         await new Promise((r) => setTimeout(r, 5));
      }
      throw new Error("never became semantic");
   }

   it("embeds the index with the document side and the question with the query side", async () => {
      const calls: Call[] = [];
      _setEmbeddingProviderForTests(
         provider(
            {
               documentPrefix: "search_document: ",
               queryPrefix: "search_query: ",
               extraBody: { truncate: "END" },
               queryExtraBody: { input_type: "search_query" },
            },
            calls,
         ),
      );
      const payload = await untilSemantic(handler());
      expect(entityNames(payload)).toEqual(["total_revenue"]);
      const indexCalls = calls.filter((c) =>
         c.input.some((t) => t.startsWith("search_document: ")),
      );
      expect(indexCalls.length).toBeGreaterThan(0);
      expect(
         indexCalls.every((c) =>
            c.input.every((t) => t.startsWith("search_document: ")),
         ),
      ).toBe(true);
      expect(
         indexCalls.every(
            (c) => c.body.input_type === undefined && c.body.truncate === "END",
         ),
      ).toBe(true);
      const queryCalls = calls.filter(
         (c) => c.input.length === 1 && c.input[0] === "search_query: revenue",
      );
      expect(queryCalls.length).toBeGreaterThan(0);
      expect(queryCalls[0].body.input_type).toBe("search_query");
   });

   it("keeps what it embedded beside the vector, for explaining a hit", async () => {
      _setEmbeddingProviderForTests(
         provider({ documentPrefix: "search_document: " }, []),
      );
      await untilSemantic(handler());
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const rows = await db.all<any>(
         "SELECT entity_name, facet, embedded_text FROM entity_embeddings ORDER BY entity_name, facet",
      );
      const byKey = Object.fromEntries(
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         rows.map((r: any) => [`${r.entity_name}:${r.facet}`, r.embedded_text]),
      );
      // The prepared facet text, not the prefixed request: the prefix is the
      // provider's business and is recorded by the row's model key.
      expect(byKey["total_revenue:name"]).toBe("total revenue");
      expect(byKey["total_revenue:doc:0"]).toBe("total revenue: Revenue.");
      expect(byKey["orders:name"]).toBe("orders");
   });

   it("rebuilds the index when the document side changes", async () => {
      const first: Call[] = [];
      _setEmbeddingProviderForTests(
         provider({ documentPrefix: "search_document: " }, first),
      );
      const h = handler();
      await untilSemantic(h);
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const before = await db.all<any>(
         "SELECT DISTINCT embedding_model FROM entity_embeddings",
      );
      expect(before).toHaveLength(1);
      expect(before[0].embedding_model).toMatch(/^stub#[0-9a-f]{10}$/);

      const second: Call[] = [];
      _setEmbeddingProviderForTests(
         provider({ documentPrefix: "passage: " }, second),
      );
      await untilSemantic(h);
      // Everything was embedded again under the new prefix, and nothing under
      // the old one is left to be searched.
      const reembedded = second
         .flatMap((c) => c.input)
         .filter((t) => t.startsWith("passage: "));
      expect(reembedded.length).toBeGreaterThanOrEqual(4);
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const after = await db.all<any>(
         "SELECT DISTINCT embedding_model FROM entity_embeddings",
      );
      expect(after).toHaveLength(1);
      expect(after[0].embedding_model).not.toBe(before[0].embedding_model);
   });

   it("does not re-embed the index when only the query side changes", async () => {
      const first: Call[] = [];
      _setEmbeddingProviderForTests(
         provider({ queryPrefix: "search_query: " }, first),
      );
      const h = handler();
      await untilSemantic(h);
      const second: Call[] = [];
      _setEmbeddingProviderForTests(provider({ queryPrefix: "q: " }, second));
      const payload = await untilSemantic(h);
      expect(entityNames(payload)).toEqual(["total_revenue"]);
      // Only the question was embedded: no document went to the provider.
      expect(
         second.every(
            (c) => c.input.length === 1 && c.input[0] === "q: revenue",
         ),
      ).toBe(true);
   });

   it("leaves an untuned index keyed by the plain model name", async () => {
      _setEmbeddingProviderForTests(provider({}, []));
      await untilSemantic(handler());
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const rows = await db.all<any>(
         "SELECT DISTINCT embedding_model FROM entity_embeddings",
      );
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      expect(rows.map((r: any) => r.embedding_model)).toEqual(["stub"]);
   });
});
