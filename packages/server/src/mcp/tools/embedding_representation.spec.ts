// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `retrieval.representation`: `single` (the default) embeds one row per
 * entity, `facets` embeds the name plus each doc chunk. The row texts come
 * from entityRows; the sync stores them and the content-hash diff re-embeds
 * only what a change touched.
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
import type { PackageRetrievalSettings } from "../../service/package_retrieval";
import { DuckDBConnection } from "../../storage/duckdb/DuckDBConnection";
import { createEntityEmbeddingsTable } from "../../storage/duckdb/schema";
import {
   EmbeddableEntity,
   SINGLE_FACET,
   _resetEmbeddingIndexStateForTests,
   entityRows,
   getEmbeddingIndexStatus,
   trySemanticSearch,
} from "./embedding_index";

let tempDir: string;
let db: DuckDBConnection;

beforeAll(async () => {
   tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "embedding-repr-spec-"));
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

const entity = (
   name: string,
   embedDoc = "",
   kind = "dimension",
): EmbeddableEntity => ({
   kind,
   name,
   source: "orders",
   modelPath: "m.malloy",
   embedDoc,
});

describe("entityRows", () => {
   it("single: a doc stands for the entity, without its name", () => {
      expect(
         entityRows(entity("order_status", "Lifecycle state."), "single"),
      ).toEqual([{ facet: SINGLE_FACET, text: "Lifecycle state." }]);
   });

   it("single: no doc falls back to the humanized name", () => {
      expect(entityRows(entity("order_status"), "single")).toEqual([
         { facet: SINGLE_FACET, text: "order status" },
      ]);
      // A punctuation-only identifier keeps its raw name rather than ''.
      expect(entityRows(entity("_"), "single")[0].text).toBe("_");
   });

   it("facets: a name row, then one row per chunk of the doc", () => {
      const e = entity("order_status", "Lifecycle state.");
      expect(entityRows(e, "facets")).toEqual([
         { facet: "name", text: "order status" },
         { facet: "doc:0", text: "order status: Lifecycle state." },
      ]);
   });
});

// ---------------------------------------------------------------------------
// Through the sync: what is stored, and what a change re-embeds
// ---------------------------------------------------------------------------

function recordingProvider() {
   const sent: string[] = [];
   const fetchStub = (async (_url: RequestInfo | URL, init?: RequestInit) => {
      const body = JSON.parse(String(init?.body)) as { input: string[] };
      sent.push(...body.input);
      return new Response(
         JSON.stringify({
            data: body.input.map((t, index) => ({
               index,
               embedding: [1, t.length % 5, 1],
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
         },
         fetchStub,
      ),
   };
}

const pkgWith = (retrieval: Partial<PackageRetrievalSettings>): Package =>
   ({
      getRetrievalSettings: () => ({
         representation: "single",
         ...retrieval,
      }),
   }) as unknown as Package;

const ENTITIES = Object.freeze([
   entity("status", "Order lifecycle status."),
   entity("total_revenue", "", "measure"),
   entity("ship_state", "State the order ships to. It is a two letter code."),
]);

async function syncUntilReady(
   provider: EmbeddingProvider,
   pkg: Package,
   entities: readonly EmbeddableEntity[] = ENTITIES,
) {
   for (let i = 0; i < 200; i++) {
      const result = await trySemanticSearch({
         db,
         provider,
         pkg,
         environmentName: "env",
         packageName: "repr",
         entities,
         queries: [{ targetIndex: 0, text: "status", kinds: ["dimension"] }],
         perSourceWindow: 10,
      });
      if (!("unavailable" in result) || result.unavailable !== "indexing") {
         return result;
      }
      await new Promise((resolve) => setTimeout(resolve, 5));
   }
   throw new Error("sync never completed");
}

const rows = () =>
   db.all<{ entity_name: string; facet: string }>(
      `SELECT entity_name, facet FROM entity_embeddings ORDER BY entity_name, facet`,
   );

describe("single vs facets through the sync", () => {
   it("single stores one row per entity with the documented text", async () => {
      const { provider, sent } = recordingProvider();
      await syncUntilReady(provider, pkgWith({ representation: "single" }));
      expect(await rows()).toEqual([
         { entity_name: "ship_state", facet: "single" },
         { entity_name: "status", facet: "single" },
         { entity_name: "total_revenue", facet: "single" },
      ]);
      expect(sent.slice(0, 3).sort()).toEqual(
         [
            "State the order ships to. It is a two letter code.",
            "Order lifecycle status.",
            "total revenue",
         ].sort(),
      );
   });

   it("facets stores a name row plus a row per doc chunk", async () => {
      const { provider } = recordingProvider();
      await syncUntilReady(provider, pkgWith({ representation: "facets" }));
      expect(await rows()).toEqual([
         { entity_name: "ship_state", facet: "doc:0" },
         { entity_name: "ship_state", facet: "name" },
         { entity_name: "status", facet: "doc:0" },
         { entity_name: "status", facet: "name" },
         { entity_name: "total_revenue", facet: "name" },
      ]);
   });

   it("a doc longer than 1,024 characters is cut for single, and one row per entity holds", async () => {
      const { provider, sent } = recordingProvider();
      const long = Object.freeze([entity("big", "word ".repeat(600))]);
      await syncUntilReady(
         provider,
         pkgWith({ representation: "single" }),
         long,
      );
      expect(await rows()).toHaveLength(1);
      expect(sent[0].length).toBeLessThanOrEqual(1_024);
   });

   it("switching representation drops the old rows and embeds the new ones", async () => {
      const first = recordingProvider();
      await syncUntilReady(
         first.provider,
         pkgWith({ representation: "facets" }),
      );
      expect((await rows()).length).toBe(5);

      const second = recordingProvider();
      await syncUntilReady(
         second.provider,
         pkgWith({ representation: "single" }),
      );
      expect((await rows()).every((r) => r.facet === "single")).toBe(true);
      expect(await rows()).toHaveLength(3);
      // Three entity rows and the query.
      expect(second.sent).toHaveLength(4);
   });

   it("a reload under the same representation re-embeds nothing", async () => {
      const first = recordingProvider();
      await syncUntilReady(first.provider, pkgWith({}));
      const again = recordingProvider();
      await syncUntilReady(again.provider, pkgWith({}));
      // Only the query is embedded.
      expect(again.sent).toEqual(["status"]);
   });

   it("status says indexing after the representation changes, then ready once synced", async () => {
      const { provider } = recordingProvider();
      await syncUntilReady(provider, pkgWith({ representation: "single" }));
      const ready = await getEmbeddingIndexStatus(
         db,
         provider,
         "env",
         "repr",
         ENTITIES,
         pkgWith({ representation: "single" }),
      );
      expect(ready.status).toBe("ready");
      expect(ready.totalRows).toBe(3);
      const changed = await getEmbeddingIndexStatus(
         db,
         provider,
         "env",
         "repr",
         ENTITIES,
         pkgWith({ representation: "facets" }),
      );
      expect(changed.status).toBe("indexing");
      expect(changed.totalRows).toBe(5);
   });

   it("an edit to one doc re-embeds only that entity under single", async () => {
      const first = recordingProvider();
      await syncUntilReady(first.provider, pkgWith({}));
      const edited = Object.freeze([
         entity("status", "Order lifecycle status."),
         entity("total_revenue", "", "measure"),
         entity("ship_state", "A different doc."),
      ]);
      const second = recordingProvider();
      await syncUntilReady(second.provider, pkgWith({}), edited);
      expect(second.sent.filter((t) => t !== "status")).toEqual([
         "A different doc.",
      ]);
   });
});
