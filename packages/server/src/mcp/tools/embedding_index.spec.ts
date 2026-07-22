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
import { DuckDBConnection } from "../../storage/duckdb/DuckDBConnection";
import { createEntityEmbeddingsTable } from "../../storage/duckdb/schema";
import { EmbeddingProvider } from "../../service/embedding_provider";
import type { Package } from "../../service/package";
import {
   EmbeddableEntity,
   MIN_SIMILARITY,
   SemanticSearchResult,
   _resetEmbeddingIndexStateForTests,
   embeddingText,
   humanizeName,
   trySemanticSearch,
} from "./embedding_index";

let tempDir: string;
let db: DuckDBConnection;

beforeAll(async () => {
   tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "embedding-index-spec-"));
   db = new DuckDBConnection(path.join(tempDir, "test.db"));
   await db.initialize();
   await createEntityEmbeddingsTable(db);
});

afterAll(async () => {
   // Close before removing: DuckDB holds an exclusive file handle and
   // Windows refuses to delete a directory containing an open file.
   await db.close();
   fs.rmSync(tempDir, { recursive: true, force: true });
});

beforeEach(async () => {
   _resetEmbeddingIndexStateForTests();
   await db.run("DELETE FROM entity_embeddings");
});

/**
 * A provider whose "embeddings" come from an explicit text -> vector map,
 * so cosine ranking in the tests is hand-computable. Also counts how
 * often each text was embedded, to pin the hash-diff behavior.
 */
function mapProvider(
   vectors: Record<string, number[]>,
   options: { model?: string; dimensions?: number; fail?: () => boolean } = {},
): { provider: EmbeddingProvider; counts: Map<string, number> } {
   const counts = new Map<string, number>();
   const fetchStub = (async (_url: RequestInfo | URL, init?: RequestInit) => {
      if (options.fail?.()) {
         return new Response("stub failure", { status: 500 });
      }
      const body = JSON.parse(String(init?.body)) as { input: string[] };
      const data = body.input.map((text, index) => {
         counts.set(text, (counts.get(text) ?? 0) + 1);
         const embedding = vectors[text];
         if (!embedding) throw new Error(`no stub vector for "${text}"`);
         return { index, embedding };
      });
      return new Response(JSON.stringify({ data }), { status: 200 });
   }) as typeof fetch;
   const provider = new EmbeddingProvider(
      {
         apiKey: "test",
         model: options.model ?? "stub-model",
         baseUrl: "https://stub.example.com/v1",
         dimensions: options.dimensions,
      },
      fetchStub,
   );
   return { provider, counts };
}

function entity(
   name: string,
   source: string | undefined,
   doc = "",
): EmbeddableEntity {
   return { kind: "measure", name, source, modelPath: "m.malloy", doc };
}

/** Poll through the cold-start "indexing" response until the sync lands. */
async function searchReady(
   args: Parameters<typeof trySemanticSearch>[0],
): Promise<SemanticSearchResult> {
   for (let i = 0; i < 200; i++) {
      const result = await trySemanticSearch(args);
      if (!("unavailable" in result) || result.unavailable !== "indexing") {
         return result;
      }
      await new Promise((resolve) => setTimeout(resolve, 5));
   }
   throw new Error("sync never completed");
}

const QUERY_VECTORS = {
   "find alpha": [1, 0, 0],
};

const ENTITY_VECTORS = {
   alpha: [1, 0, 0], // cosine 1.0 vs the query
   beta: [0.8, 0.6, 0], // cosine 0.8
   gamma: [0, 0, 1], // cosine 0 -> below MIN_SIMILARITY, dropped
};

describe("humanizeName / embeddingText", () => {
   it("splits snake_case, kebab, dots, and camelCase into words", () => {
      expect(humanizeName("dep_delay")).toBe("dep delay");
      expect(humanizeName("totalSales")).toBe("total sales");
      expect(humanizeName("sales-by.region")).toBe("sales by region");
   });

   it("appends the doc text when present", () => {
      expect(embeddingText(entity("total_sales", "s", "Total revenue"))).toBe(
         "total sales: Total revenue",
      );
      expect(embeddingText(entity("total_sales", "s"))).toBe("total sales");
   });
});

describe("trySemanticSearch", () => {
   it("cold start reports indexing, then ranks by cosine with a floor", async () => {
      const { provider } = mapProvider({ ...ENTITY_VECTORS, ...QUERY_VECTORS });
      const pkg = {} as unknown as Package;
      const args = {
         db,
         provider,
         pkg,
         environmentName: "env",
         packageName: "pkg",
         entities: [
            entity("alpha", "src"),
            entity("beta", "src"),
            entity("gamma", "src"),
         ],
         query: "find alpha",
         limit: 10,
      };

      const first = await trySemanticSearch(args);
      expect(first).toEqual({ unavailable: "indexing" });

      const ready = await searchReady(args);
      if (!("hits" in ready)) throw new Error("expected hits");
      expect(ready.hits.map((h) => h.name)).toEqual(["alpha", "beta"]);
      expect(ready.hits[0].score).toBeCloseTo(1.0, 3);
      expect(ready.hits[1].score).toBeCloseTo(0.8, 3);
      expect(ready.hits.every((h) => h.score >= MIN_SIMILARITY)).toBe(true);
   });

   it("does not re-embed unchanged entities for a new package instance", async () => {
      const { provider, counts } = mapProvider({
         ...ENTITY_VECTORS,
         ...QUERY_VECTORS,
      });
      const entities = [entity("alpha", "src"), entity("beta", "src")];
      const base = {
         db,
         provider,
         environmentName: "env",
         packageName: "pkg",
         entities,
         query: "find alpha",
         limit: 10,
      };

      await searchReady({ ...base, pkg: {} as unknown as Package });
      expect(counts.get("alpha")).toBe(1);

      // A "reload": same package name, new instance. The sync re-runs but
      // the content hashes match, so nothing re-embeds.
      await searchReady({ ...base, pkg: {} as unknown as Package });
      expect(counts.get("alpha")).toBe(1);
      expect(counts.get("beta")).toBe(1);
   });

   it("re-embeds only the entity whose text changed, via upsert", async () => {
      const vectors = {
         ...ENTITY_VECTORS,
         ...QUERY_VECTORS,
         "alpha: now documented": [0, 1, 0],
      };
      const { provider, counts } = mapProvider(vectors);
      const base = {
         db,
         provider,
         environmentName: "env",
         packageName: "pkg",
         query: "find alpha",
         limit: 10,
      };

      await searchReady({
         ...base,
         pkg: {} as unknown as Package,
         entities: [entity("alpha", "src"), entity("beta", "src")],
      });

      const changed = await searchReady({
         ...base,
         pkg: {} as unknown as Package,
         entities: [
            entity("alpha", "src", "now documented"),
            entity("beta", "src"),
         ],
      });
      expect(counts.get("alpha: now documented")).toBe(1);
      expect(counts.get("beta")).toBe(1);
      if (!("hits" in changed)) throw new Error("expected hits");
      // alpha's new vector is orthogonal to the query, so beta leads now.
      expect(changed.hits.map((h) => h.name)).toEqual(["beta"]);

      const rows = await db.all<{ n: number }>(
         "SELECT CAST(COUNT(*) AS INTEGER) AS n FROM entity_embeddings WHERE environment_name = 'env'",
      );
      expect(rows[0].n).toBe(2);
   });

   it("re-embeds everything on a model switch without a key collision", async () => {
      const first = mapProvider({ ...ENTITY_VECTORS, ...QUERY_VECTORS });
      const base = {
         db,
         environmentName: "env",
         packageName: "pkg",
         entities: [entity("alpha", "src"), entity("beta", "src")],
         query: "find alpha",
         limit: 10,
      };
      await searchReady({
         ...base,
         provider: first.provider,
         pkg: {} as unknown as Package,
      });

      const second = mapProvider(
         { ...ENTITY_VECTORS, ...QUERY_VECTORS },
         { model: "other-model" },
      );
      const result = await searchReady({
         ...base,
         provider: second.provider,
         pkg: {} as unknown as Package,
      });
      if (!("hits" in result)) throw new Error("expected hits");
      expect(result.hits.map((h) => h.name)).toEqual(["alpha", "beta"]);
      expect(second.counts.get("alpha")).toBe(1);

      const rows = await db.all<{ n: number }>(
         "SELECT CAST(COUNT(*) AS INTEGER) AS n FROM entity_embeddings WHERE embedding_model = 'other-model'",
      );
      expect(rows[0].n).toBe(2);
      const oldRows = await db.all<{ n: number }>(
         "SELECT CAST(COUNT(*) AS INTEGER) AS n FROM entity_embeddings WHERE embedding_model = 'stub-model'",
      );
      expect(oldRows[0].n).toBe(0);
   });

   it("re-syncs an already-synced instance when the model changes", async () => {
      const pkg = {} as unknown as Package;
      const first = mapProvider({ ...ENTITY_VECTORS, ...QUERY_VECTORS });
      const base = {
         db,
         pkg,
         environmentName: "env",
         packageName: "pkg",
         entities: [entity("alpha", "src")],
         query: "find alpha",
         limit: 10,
      };
      await searchReady({ ...base, provider: first.provider });

      // Same Package instance, new model: the provider-key mismatch must
      // trigger a re-sync instead of matching zero rows forever.
      const second = mapProvider(
         { ...ENTITY_VECTORS, ...QUERY_VECTORS },
         { model: "other-model" },
      );
      const result = await searchReady({ ...base, provider: second.provider });
      if (!("hits" in result)) throw new Error("expected hits");
      expect(result.hits.map((h) => h.name)).toEqual(["alpha"]);
   });

   it("deletes rows for entities that no longer exist", async () => {
      const { provider } = mapProvider({ ...ENTITY_VECTORS, ...QUERY_VECTORS });
      const base = {
         db,
         provider,
         environmentName: "env",
         packageName: "pkg",
         query: "find alpha",
         limit: 10,
      };
      await searchReady({
         ...base,
         pkg: {} as unknown as Package,
         entities: [entity("alpha", "src"), entity("gamma", "src")],
      });
      await searchReady({
         ...base,
         pkg: {} as unknown as Package,
         entities: [entity("alpha", "src")],
      });
      const rows = await db.all<{ entity_name: string }>(
         "SELECT entity_name FROM entity_embeddings WHERE environment_name = 'env' ORDER BY entity_name",
      );
      expect(rows.map((r) => r.entity_name)).toEqual(["alpha"]);
   });

   it("narrows to one source with sourceName", async () => {
      const { provider } = mapProvider({ ...ENTITY_VECTORS, ...QUERY_VECTORS });
      const args = {
         db,
         provider,
         pkg: {} as unknown as Package,
         environmentName: "env",
         packageName: "pkg",
         entities: [entity("alpha", "orders"), entity("beta", "customers")],
         query: "find alpha",
         limit: 10,
         sourceName: "customers",
      };
      const result = await searchReady(args);
      if (!("hits" in result)) throw new Error("expected hits");
      expect(result.hits.map((h) => h.name)).toEqual(["beta"]);
   });

   it("scopes rows per package", async () => {
      const { provider } = mapProvider({ ...ENTITY_VECTORS, ...QUERY_VECTORS });
      const base = {
         db,
         provider,
         environmentName: "env",
         query: "find alpha",
         limit: 10,
      };
      await searchReady({
         ...base,
         packageName: "pkg-a",
         pkg: {} as unknown as Package,
         entities: [entity("alpha", "src")],
      });
      const other = await searchReady({
         ...base,
         packageName: "pkg-b",
         pkg: {} as unknown as Package,
         entities: [entity("beta", "src")],
      });
      if (!("hits" in other)) throw new Error("expected hits");
      expect(other.hits.map((h) => h.name)).toEqual(["beta"]);
   });

   it("purges and re-syncs when the provider's dimensionality changes under the same model", async () => {
      // Sync at 3 dims (EMBEDDING_DIMENSIONS unset everywhere here).
      const first = mapProvider({ ...ENTITY_VECTORS, ...QUERY_VECTORS });
      const base = {
         db,
         environmentName: "env",
         packageName: "pkg",
         entities: [entity("alpha", "src")],
         query: "find alpha",
         limit: 10,
      };
      await searchReady({
         ...base,
         provider: first.provider,
         pkg: {} as unknown as Package,
      });

      // The same model name now returns 4-dim vectors (a different
      // backend behind EMBEDDING_API_BASE). The hash diff cannot see
      // this: a fresh instance's sync embeds nothing, and the search
      // finds zero compatible rows. The empty-result heal must purge
      // and re-sync instead of returning empty "semantic" results
      // forever.
      const wide = mapProvider({
         alpha: [1, 0, 0, 0],
         "find alpha": [1, 0, 0, 0],
      });
      const pkg = {} as unknown as Package;
      const result = await searchReady({
         ...base,
         provider: wide.provider,
         pkg,
      });
      if (!("hits" in result)) throw new Error("expected hits");
      expect(result.hits.map((h) => h.name)).toEqual(["alpha"]);
      const rows = await db.all<{ dims: number }>(
         "SELECT CAST(dims AS INTEGER) AS dims FROM entity_embeddings WHERE environment_name = 'env'",
      );
      expect(rows.map((r) => r.dims)).toEqual([4]);
   });

   it("cools down after a provider failure instead of erroring every call", async () => {
      let failing = true;
      const { provider } = mapProvider(
         { ...ENTITY_VECTORS, ...QUERY_VECTORS },
         { fail: () => failing },
      );
      const args = {
         db,
         provider,
         pkg: {} as unknown as Package,
         environmentName: "env",
         packageName: "pkg",
         entities: [entity("alpha", "src")],
         query: "find alpha",
         limit: 10,
      };

      const first = await trySemanticSearch(args);
      expect(first).toEqual({ unavailable: "indexing" });
      // Let the background sync fail.
      await new Promise((resolve) => setTimeout(resolve, 50));
      const second = await trySemanticSearch(args);
      expect(second).toEqual({ unavailable: "cooldown" });

      // After the cool-down clears (test reset) the path recovers.
      failing = false;
      _resetEmbeddingIndexStateForTests();
      const recovered = await searchReady(args);
      expect("hits" in recovered).toBe(true);
   });

   it("stays lexical for oversized packages", async () => {
      const { provider, counts } = mapProvider(QUERY_VECTORS);
      const entities = Array.from({ length: 5_001 }, (_, i) =>
         entity(`e${i}`, "src"),
      );
      const result = await trySemanticSearch({
         db,
         provider,
         pkg: {} as unknown as Package,
         environmentName: "env",
         packageName: "huge",
         entities,
         query: "find alpha",
         limit: 10,
      });
      expect(result).toEqual({ unavailable: "too-many-entities" });
      expect(counts.size).toBe(0);
   });
});
