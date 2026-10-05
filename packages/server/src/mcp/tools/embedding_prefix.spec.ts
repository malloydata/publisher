// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `retrieval.embedding.queryPrefix` / `documentPrefix`: text a model such as
 * nomic-embed-text needs before a query or before indexed text. Indexed text
 * carries the document prefix, a query carries the query prefix, and changing
 * the document prefix re-embeds through the ordinary content-hash diff.
 */

import {
   afterAll,
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
import { EmbeddingProvider } from "../../service/embedding_provider";
import type { Package } from "../../service/package";
import { DuckDBConnection } from "../../storage/duckdb/DuckDBConnection";
import { createEntityEmbeddingsTable } from "../../storage/duckdb/schema";
import {
   _resetEmbeddingIndexStateForTests,
   SemanticSearchResult,
   trySemanticSearch,
} from "./embedding_index";

let tempDir: string;
let db: DuckDBConnection;

beforeAll(async () => {
   tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "embedding-prefix-spec-"));
   db = new DuckDBConnection(path.join(tempDir, "test.db"));
   await db.initialize();
   await createEntityEmbeddingsTable(db);
});

afterAll(async () => {
   await db.close();
   fs.rmSync(tempDir, { recursive: true, force: true });
});

beforeEach(async () => {
   _resetEmbeddingIndexStateForTests();
   await db.run("DELETE FROM entity_embeddings");
});

/** Records every text sent; the vector is derived from the text's length. */
function recordingProvider(prefixes: {
   queryPrefix: string;
   documentPrefix: string;
}) {
   const sent: string[] = [];
   const fetchStub = (async (_url: RequestInfo | URL, init?: RequestInit) => {
      const body = JSON.parse(String(init?.body)) as { input: string[] };
      sent.push(...body.input);
      return new Response(
         JSON.stringify({
            data: body.input.map((t, index) => ({
               index,
               embedding: [1, t.length % 7, 0],
            })),
         }),
         { status: 200 },
      );
   }) as typeof fetch;
   return {
      sent,
      provider: new EmbeddingProvider(
         {
            apiKey: "k",
            model: "m",
            baseUrl: "https://stub.example.com/v1",
            minSimilarity: DEFAULT_EMBEDDING_MIN_SIMILARITY,
            ...prefixes,
         },
         fetchStub,
      ),
   };
}

const entities = [
   {
      kind: "measure",
      name: "total_sales",
      source: "orders",
      modelPath: "m.malloy",
      embedDoc: "",
   },
];

async function ready(
   provider: EmbeddingProvider,
   pkg: Package,
): Promise<SemanticSearchResult> {
   for (let i = 0; i < 200; i++) {
      const result = await trySemanticSearch({
         db,
         provider,
         pkg,
         environmentName: "env",
         packageName: "prefix",
         entities,
         queries: [{ targetIndex: 0, text: "revenue", kinds: ["measure"] }],
         perSourceWindow: 10,
      });
      if (!("unavailable" in result) || result.unavailable !== "indexing") {
         return result;
      }
      await new Promise((resolve) => setTimeout(resolve, 5));
   }
   throw new Error("sync never completed");
}

describe("embedding prefixes", () => {
   it("puts the document prefix on indexed text and the query prefix on the query", async () => {
      const { provider, sent } = recordingProvider({
         queryPrefix: "search_query: ",
         documentPrefix: "search_document: ",
      });
      await ready(provider, {} as unknown as Package);
      expect(sent).toContain("search_document: total sales");
      expect(sent).toContain("search_query: revenue");
      // Nothing went out bare.
      expect(sent).not.toContain("total sales");
      expect(sent).not.toContain("revenue");
   });

   it("sends text unchanged when no prefix is set", async () => {
      const { provider, sent } = recordingProvider({
         queryPrefix: "",
         documentPrefix: "",
      });
      await ready(provider, {} as unknown as Package);
      expect(sent).toContain("total sales");
      expect(sent).toContain("revenue");
   });

   it("a changed document prefix re-embeds the rows; an unchanged one does not", async () => {
      const first = recordingProvider({
         queryPrefix: "",
         documentPrefix: "a: ",
      });
      await ready(first.provider, {} as unknown as Package);
      expect(first.sent.filter((t) => t.endsWith("total sales"))).toHaveLength(
         1,
      );

      // A reload under the same prefix: nothing re-embeds.
      const same = recordingProvider({
         queryPrefix: "",
         documentPrefix: "a: ",
      });
      await ready(same.provider, {} as unknown as Package);
      expect(same.sent.filter((t) => t.endsWith("total sales"))).toHaveLength(
         0,
      );

      // A different prefix: the same text embeds again, with the new prefix.
      const changed = recordingProvider({
         queryPrefix: "",
         documentPrefix: "b: ",
      });
      await ready(changed.provider, {} as unknown as Package);
      expect(changed.sent).toContain("b: total sales");
   });

   it("a changed query prefix does not re-embed the index", async () => {
      const first = recordingProvider({
         queryPrefix: "x: ",
         documentPrefix: "",
      });
      await ready(first.provider, {} as unknown as Package);
      const second = recordingProvider({
         queryPrefix: "y: ",
         documentPrefix: "",
      });
      await ready(second.provider, {} as unknown as Package);
      expect(second.sent).toEqual(["y: revenue"]);
   });
});
