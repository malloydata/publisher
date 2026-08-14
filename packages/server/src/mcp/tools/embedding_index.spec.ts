// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

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
import {
   EmbeddingProvider,
   prepareEmbeddingInput,
} from "../../service/embedding_provider";
import type { Package } from "../../service/package";
import {
   CHUNK_MAX_CHARS,
   EmbeddableEntity,
   MAX_DOC_CHUNKS,
   MAX_EMBEDDED_ENTITIES,
   MIN_SIMILARITY,
   chunkDoc,
   entityFacets,
   getEmbeddingIndexStatus,
   SemanticSearchResult,
   _clearProviderCooldownForTests,
   _lastPurgeAtMsForTests,
   _resetEmbeddingIndexStateForTests,
   _setTimingForTests,
   _syncMetaSizeForTests,
   deleteEnvironmentEmbeddings,
   deletePackageEmbeddings,
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
   options: {
      model?: string;
      dimensions?: number;
      fail?: () => boolean;
      // Requests whose input includes this text block until the promise
      // resolves, so a test can hold one call mid-flight deterministically.
      gate?: { forText: string; until: Promise<void> };
   } = {},
): { provider: EmbeddingProvider; counts: Map<string, number> } {
   const counts = new Map<string, number>();
   const fetchStub = (async (_url: RequestInfo | URL, init?: RequestInit) => {
      if (options.fail?.()) {
         return new Response("stub failure", { status: 500 });
      }
      const body = JSON.parse(String(init?.body)) as { input: string[] };
      if (options.gate && body.input.includes(options.gate.forText)) {
         await options.gate.until;
      }
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
   embedDoc = "",
): EmbeddableEntity {
   return { kind: "measure", name, source, modelPath: "m.malloy", embedDoc };
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

function isCooldown(result: SemanticSearchResult): boolean {
   return "unavailable" in result && result.unavailable === "cooldown";
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

   it("never produces empty embed text for punctuation-only names", () => {
      // `_` is a legal Malloy identifier; an empty input would 400 the
      // whole package's embedding batch at the provider.
      expect(humanizeName("_")).toBe("");
      expect(embeddingText(entity("_", "s"))).toBe("_");
      expect(embeddingText(entity("_", "s", "odd but documented"))).toBe(
         "_: odd but documented",
      );
   });
});

describe("chunkDoc / entityFacets", () => {
   const sentence = (n: number) =>
      `Fact number ${n} about the grain of this source and how to read it.`;

   it("keeps a short doc as one chunk, so short docs never re-embed", () => {
      // The upgrade property: anything at or under the ceiling produces the
      // same single chunk the unchunked scheme produced, so its content hash
      // is unchanged and it is not re-embedded when chunking ships.
      expect(chunkDoc("One row per product sold.")).toEqual([
         "One row per product sold.",
      ]);
      const exactly = "a".repeat(CHUNK_MAX_CHARS);
      expect(chunkDoc(exactly)).toEqual([exactly]);
   });

   it("returns nothing for an absent or whitespace-only doc", () => {
      expect(chunkDoc("")).toEqual([]);
      expect(chunkDoc("   \n  ")).toEqual([]);
   });

   it("splits a long doc on sentence boundaries, packing toward the target", () => {
      const doc = Array.from({ length: 12 }, (_, i) => sentence(i)).join(" ");
      const chunks = chunkDoc(doc);
      expect(chunks.length).toBeGreaterThan(1);
      for (const chunk of chunks) {
         expect(chunk.length).toBeLessThanOrEqual(CHUNK_MAX_CHARS);
         // Whole sentences: never cut mid-clause.
         expect(chunk).toEndWith(".");
      }
      // Every sentence survives somewhere; chunking must not drop text.
      const rejoined = chunks.join(" ");
      for (let i = 0; i < 12; i++) {
         expect(rejoined).toContain(sentence(i));
      }
   });

   it("caps the chunk count, folding the remainder into the last chunk", () => {
      // The bound on what one lavishly-documented entity can cost, and the
      // reason the fold exists: the previous cap DROPPED the overflow.
      const doc = Array.from({ length: 60 }, (_, i) => sentence(i)).join(" ");
      const chunks = chunkDoc(doc);
      expect(chunks).toHaveLength(MAX_DOC_CHUNKS);
      expect(chunks.join(" ")).toContain(sentence(59));
   });

   it("keeps an over-long single sentence whole rather than cutting it", () => {
      const runOn = `${"word ".repeat(200).trim()}.`;
      expect(chunkDoc(runOn)).toEqual([runOn]);
   });

   it("prefixes each chunk with the entity name and numbers the facets", () => {
      const doc = Array.from({ length: 12 }, (_, i) => sentence(i)).join(" ");
      const facets = entityFacets(entity("fclt_rooms", "src", doc));
      expect(facets[0]).toEqual({ facet: "name", text: "fclt rooms" });
      expect(facets.length).toBeGreaterThan(2);
      facets.slice(1).forEach((f, i) => {
         expect(f.facet).toBe(`doc:${i}`);
         // The name anchors a bare fact to the thing it is about.
         expect(f.text).toStartWith("fclt rooms: ");
      });
   });

   it("keeps every chunk within the provider input cap once prefixed", () => {
      const doc = Array.from({ length: 40 }, (_, i) => sentence(i)).join(" ");
      for (const facet of entityFacets(entity("some_source", "src", doc))) {
         expect(prepareEmbeddingInput(facet.text)).toBe(
            facet.text.replace(/\s+/g, " ").trim(),
         );
      }
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
      // Only the NEW doc facet is embedded. alpha's name facet is unchanged
      // by documenting it, so it is not re-embedded, and neither is beta.
      expect(counts.get("alpha: now documented")).toBe(1);
      expect(counts.get("alpha")).toBe(1);
      expect(counts.get("beta")).toBe(1);

      // The point of faceting: documenting alpha must not cost alpha its own
      // name. Its doc vector here is orthogonal to the query, which under a
      // single averaged vector per entity is exactly what used to sink it
      // below beta. Scored on its best facet, alpha still leads.
      if (!("hits" in changed)) throw new Error("expected hits");
      expect(changed.hits.map((h) => h.name)).toEqual(["alpha", "beta"]);
      expect(changed.hits[0].score).toBeCloseTo(1.0, 3);

      // Three rows: alpha's name and doc, beta's name. One row per entity
      // still comes back from the search, because scoring groups by entity.
      const rows = await db.all<{ n: number }>(
         "SELECT CAST(COUNT(*) AS INTEGER) AS n FROM entity_embeddings WHERE environment_name = 'env'",
      );
      expect(rows[0].n).toBe(3);
   });

   it("reports index readiness without taking locks or writing", async () => {
      // Before this, "is the index warm?" was answerable only by scraping a
      // server log line, so a harness measuring retrieval had no supported
      // way to wait for a fair measurement.
      const { provider } = mapProvider({
         ...ENTITY_VECTORS,
         ...QUERY_VECTORS,
         "alpha: documented": [0, 1, 0],
      });
      const entities = [
         entity("alpha", "src", "documented"),
         entity("beta", "src"),
      ];
      const args = {
         db,
         provider,
         pkg: {} as unknown as Package,
         environmentName: "env",
         packageName: "status",
         query: "find alpha",
         limit: 10,
         entities,
      };

      const cold = await getEmbeddingIndexStatus(
         db,
         "env",
         "status",
         entities.length,
      );
      expect(cold.status).toBe("indexing");
      expect(cold.embeddedRows).toBe(0);
      expect(cold.lastSyncedAt).toBeUndefined();

      await searchReady(args);

      const warm = await getEmbeddingIndexStatus(
         db,
         "env",
         "status",
         entities.length,
      );
      expect(warm.status).toBe("ready");
      // alpha contributes a name row and a doc row, beta only a name row, so
      // rows exceed entities on a documented model.
      expect(warm.embeddedRows).toBe(3);
      expect(warm.totalEntities).toBe(2);
      expect(warm.lastSyncedAt).toBeDefined();
   });

   it("reports a package past the embedding cap as oversize, not as indexing", async () => {
      // A permanent condition an operator must act on, not a transient one to
      // wait out: reporting it as "indexing" would poll forever.
      const status = await getEmbeddingIndexStatus(
         db,
         "env",
         "huge",
         MAX_EMBEDDED_ENTITIES + 1,
      );
      expect(status.status).toBe("oversize");
   });

   it("counts the entities that matched only below the floor", async () => {
      // gamma is orthogonal to the query and dropped by the floor. Without a
      // count, an agent cannot tell "dropped as irrelevant" from "not
      // modelled here at all", and we watched analysts conclude the latter.
      const { provider } = mapProvider({ ...ENTITY_VECTORS, ...QUERY_VECTORS });
      const result = await searchReady({
         db,
         provider,
         pkg: {} as unknown as Package,
         environmentName: "env",
         packageName: "cutoff",
         query: "find alpha",
         limit: 10,
         entities: [
            entity("alpha", "src"),
            entity("beta", "src"),
            entity("gamma", "src"),
         ],
      });
      if (!("hits" in result)) throw new Error("expected hits");
      expect(result.hits.map((h) => h.name)).toEqual(["alpha", "beta"]);
      expect(result.belowCutoffCount).toBe(1);
   });

   it("reports a true negative as no hits and nothing below the floor", async () => {
      // The distinction the count exists for: an empty result with a zero
      // count means the package genuinely models nothing related.
      const { provider } = mapProvider({
         ...ENTITY_VECTORS,
         ...QUERY_VECTORS,
         "find something absent": [0, 1, 0],
      });
      const result = await searchReady({
         db,
         provider,
         pkg: {} as unknown as Package,
         environmentName: "env",
         packageName: "true-negative",
         query: "find something absent",
         limit: 10,
         entities: [entity("alpha", "src")],
      });
      if (!("hits" in result)) throw new Error("expected hits");
      expect(result.hits).toEqual([]);
      // alpha scored 0 against an orthogonal query: below the floor, so it
      // is counted rather than silently absent.
      expect(result.belowCutoffCount).toBe(1);
   });

   it("counts within the drill-down scope, not the whole package", async () => {
      const { provider } = mapProvider({ ...ENTITY_VECTORS, ...QUERY_VECTORS });
      const base = {
         db,
         provider,
         environmentName: "env",
         packageName: "scoped-cutoff",
         query: "find alpha",
         limit: 10,
         entities: [
            entity("alpha", "a"),
            entity("gamma", "a"),
            entity("gamma", "b"),
         ],
      };
      await searchReady({ ...base, pkg: {} as unknown as Package });
      const scoped = await searchReady({
         ...base,
         pkg: {} as unknown as Package,
         sourceName: "a",
      });
      if (!("hits" in scoped)) throw new Error("expected hits");
      // Only source `a`'s gamma is below the floor here; `b`'s is out of
      // scope entirely, exactly as it is for the ranked query.
      expect(scoped.belowCutoffCount).toBe(1);
   });

   it("retrieves a fact buried mid-doc by that fact's own content", async () => {
      // Symptom B of the same finding: on a ~300-word source doc, a rare
      // token retrieved the source at rank 1 but near-verbatim business
      // phrasing from the same doc did not retrieve it at all. Averaged over
      // everything a long doc mentions, no single fact in it is close to
      // anything. Here the population rule lives in the doc's fifth
      // sentence, and the query is that rule in the modeller's own words.
      const filler = Array.from(
         { length: 4 },
         (_, i) => `Unrelated background sentence number ${i} about history.`,
      ).join(" ");
      const rule =
         "Restrict to the buildings Facilities currently holds, excluding leased space.";
      const doc = `${filler} ${rule} ${filler}`;
      const chunks = chunkDoc(doc);
      const ruleChunk = chunks.find((c) => c.includes(rule));
      if (!ruleChunk) throw new Error("fixture: the rule must land in a chunk");

      // Only the chunk carrying the rule points at the query; the entity's
      // name and its other chunks point elsewhere.
      const vectors: Record<string, number[]> = {
         ...QUERY_VECTORS,
         "which buildings does facilities hold": [1, 0, 0],
         "fclt building hist": [0, 0, 1],
      };
      for (const chunk of chunks) {
         vectors[`fclt building hist: ${chunk}`] =
            chunk === ruleChunk ? [1, 0, 0] : [0, 0, 1];
      }

      const result = await searchReady({
         db,
         provider: mapProvider(vectors).provider,
         pkg: {} as unknown as Package,
         environmentName: "env",
         packageName: "symptom-b",
         query: "which buildings does facilities hold",
         limit: 10,
         entities: [entity("fclt_building_hist", "src", doc)],
      });
      if (!("hits" in result)) throw new Error("expected hits");
      expect(result.hits[0].name).toBe("fclt_building_hist");
      expect(result.hits[0].score).toBeCloseTo(1.0, 3);
   });

   it("keeps a documented entity findable by its own name", async () => {
      // The measured §3 failure, reduced: on a real 42-source model a field
      // with a 547-char doc ranked 10th for its own plain name while
      // short-doc siblings ranked 1st. Here alpha's doc points away from the
      // query and its short-doc'd sibling beta points at it; alpha must
      // still win on the name it is named.
      const { provider } = mapProvider({
         ...ENTITY_VECTORS,
         ...QUERY_VECTORS,
         "alpha: a long aside about unrelated matters": [0, 0, 1],
         "beta: alpha-ish": [0.9, 0.4, 0],
      });
      const result = await searchReady({
         db,
         provider,
         pkg: {} as unknown as Package,
         environmentName: "env",
         packageName: "dilution",
         query: "find alpha",
         limit: 10,
         entities: [
            entity("alpha", "src", "a long aside about unrelated matters"),
            entity("beta", "src", "alpha-ish"),
         ],
      });
      if (!("hits" in result)) throw new Error("expected hits");
      expect(result.hits[0].name).toBe("alpha");
      expect(result.hits[0].score).toBeCloseTo(1.0, 3);
   });

   it("drops a doc facet when the doc is removed, keeping the name facet", async () => {
      const { provider } = mapProvider({
         ...ENTITY_VECTORS,
         ...QUERY_VECTORS,
         "alpha: documented": [0, 1, 0],
      });
      const base = {
         db,
         provider,
         environmentName: "env",
         packageName: "undoc",
         query: "find alpha",
         limit: 10,
      };
      // A fresh Package per call: the sync memo is per instance, exactly as
      // a reload swaps the instance in production.
      await searchReady({
         ...base,
         pkg: {} as unknown as Package,
         entities: [entity("alpha", "src", "documented")],
      });
      await searchReady({
         ...base,
         pkg: {} as unknown as Package,
         entities: [entity("alpha", "src")],
      });

      const rows = await db.all<{ facet: string }>(
         `SELECT facet FROM entity_embeddings
          WHERE environment_name = 'env' AND package_name = 'undoc'`,
      );
      expect(rows.map((r) => r.facet)).toEqual(["name"]);
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

   it("a purge invalidates every instance's memo, not just the caller's", async () => {
      // Instance A syncs at 3 dims and stays around with done=true.
      const narrow = mapProvider({ ...ENTITY_VECTORS, ...QUERY_VECTORS });
      const base = {
         db,
         environmentName: "env",
         packageName: "pkg",
         entities: [entity("alpha", "src")],
         query: "find alpha",
         limit: 10,
      };
      const pkgA = {} as unknown as Package;
      await searchReady({ ...base, provider: narrow.provider, pkg: pkgA });

      // Instance B (a reload) triggers the dims heal with a 4-dim
      // provider. Drive B ONLY until the purge lands (row count drops
      // to zero) and then stop, so no re-sync repopulates the table:
      // this reproduces the exact race window where a stale done memo
      // sits over an empty table.
      const wide = mapProvider({
         alpha: [1, 0, 0, 0],
         "find alpha": [1, 0, 0, 0],
      });
      const pkgB = {} as unknown as Package;
      for (let i = 0; i < 200; i++) {
         await trySemanticSearch({
            ...base,
            provider: wide.provider,
            pkg: pkgB,
         });
         const rows = await db.all<{ n: number }>(
            "SELECT CAST(COUNT(*) AS INTEGER) AS n FROM entity_embeddings WHERE environment_name = 'env'",
         );
         if (rows[0].n === 0) break;
         await new Promise((resolve) => setTimeout(resolve, 5));
      }
      const purged = await db.all<{ n: number }>(
         "SELECT CAST(COUNT(*) AS INTEGER) AS n FROM entity_embeddings WHERE environment_name = 'env'",
      );
      expect(purged[0].n).toBe(0);

      // A's memo says done, but the table is empty. Without generation
      // tracking, A would trust its memo, search zero rows, find no
      // stale rows to heal, and serve `{hits: []}` marked semantic
      // forever. With it, A's stale generation forces a re-sync (the
      // table is empty, so A re-embeds at its own 3 dims) and A answers
      // with real hits again.
      const back = await searchReady({
         ...base,
         provider: narrow.provider,
         pkg: pkgA,
      });
      if (!("hits" in back))
         throw new Error("expected hits, got " + JSON.stringify(back));
      expect(back.hits.map((h) => h.name)).toEqual(["alpha"]);
   });

   it("heals the mixed-dims state where a partial sync stranded old rows", async () => {
      // alpha indexed at 3 dims.
      const narrow = mapProvider({ ...ENTITY_VECTORS, ...QUERY_VECTORS });
      const base = {
         db,
         environmentName: "env",
         packageName: "pkg",
         query: "find alpha",
         limit: 10,
      };
      await searchReady({
         ...base,
         provider: narrow.provider,
         pkg: {} as unknown as Package,
         entities: [entity("alpha", "src")],
      });

      // The endpoint now returns 4-dim vectors for the same model, and a
      // new entity (beta) arrives. The sync diff skips alpha (hash and
      // model match) and embeds only beta, leaving a MIXED table: alpha
      // at 3 dims, beta at 4. An empty-result heal trigger would never
      // fire here (compatible rows exist), stranding alpha invisibly.
      const wide = mapProvider({
         alpha: [1, 0, 0, 0],
         beta: [0.8, 0.6, 0, 0],
         "find alpha": [1, 0, 0, 0],
      });
      const pkg = {} as unknown as Package;
      const args = {
         ...base,
         provider: wide.provider,
         pkg,
         entities: [entity("alpha", "src"), entity("beta", "src")],
      };

      const result = await searchReady(args);
      if (!("hits" in result)) throw new Error("expected hits");
      // Both entities retrievable: the stale-row heal purged alpha's
      // 3-dim row and the follow-up sync re-embedded it at 4 dims.
      expect(result.hits.map((h) => h.name)).toEqual(["alpha", "beta"]);
      const dims = await db.all<{ dims: number }>(
         "SELECT DISTINCT CAST(dims AS INTEGER) AS dims FROM entity_embeddings WHERE environment_name = 'env'",
      );
      expect(dims.map((d) => d.dims)).toEqual([4]);
      // The purge deleted ONLY the stale row: beta was embedded exactly
      // once. A delete-all purge would re-embed it and fail this pin.
      expect(wide.counts.get("beta")).toBe(1);
   });

   it("a sync that changes rows invalidates other instances' memos", async () => {
      const { provider } = mapProvider({ ...ENTITY_VECTORS, ...QUERY_VECTORS });
      const base = {
         db,
         provider,
         environmentName: "env",
         packageName: "pkg",
         query: "find alpha",
         limit: 10,
      };
      const pkgA = {} as unknown as Package;
      await searchReady({
         ...base,
         pkg: pkgA,
         entities: [entity("alpha", "src")],
      });

      // A reload's instance syncs with a CHANGED entity text: the rows
      // pkgA's snapshot logic trusts have been rewritten.
      const changedVectors = {
         ...ENTITY_VECTORS,
         ...QUERY_VECTORS,
         "alpha: reworded": [0, 1, 0],
      };
      const changed = mapProvider(changedVectors);
      await searchReady({
         ...base,
         provider: changed.provider,
         pkg: {} as unknown as Package,
         entities: [entity("alpha", "src", "reworded")],
      });

      // pkgA's done memo must now be stale (the sync bumped the
      // generation): its next call re-kicks a sync instead of serving
      // from the trusted memo.
      const next = await trySemanticSearch({
         ...base,
         pkg: pkgA,
         entities: [entity("alpha", "src")],
      });
      expect(next).toEqual({ unavailable: "indexing" });
   });

   it("a call overlapping a purge answers indexing, not empty semantic hits", async () => {
      const { provider } = mapProvider({ ...ENTITY_VECTORS, ...QUERY_VECTORS });
      const base = {
         db,
         environmentName: "env",
         packageName: "pkg",
         entities: [entity("alpha", "src")],
         query: "find alpha",
         limit: 10,
      };
      const pkgA = {} as unknown as Package;
      await searchReady({ ...base, provider, pkg: pkgA });

      // Call C on pkgA holds at its query embed (gated), memo done and
      // generation current at entry.
      let release!: () => void;
      const gate = new Promise<void>((resolve) => {
         release = resolve;
      });
      const gated = mapProvider(
         { ...ENTITY_VECTORS, ...QUERY_VECTORS },
         { gate: { forText: "find alpha", until: gate } },
      );
      const cPromise = trySemanticSearch({
         ...base,
         provider: gated.provider,
         pkg: pkgA,
      });

      // While C is gated, another instance's heal purges the table (a
      // 4-dim provider makes every row stale) and stops before any
      // re-sync repopulates it.
      const wide = mapProvider({
         alpha: [1, 0, 0, 0],
         "find alpha": [1, 0, 0, 0],
      });
      const pkgB = {} as unknown as Package;
      for (let i = 0; i < 200; i++) {
         await trySemanticSearch({
            ...base,
            provider: wide.provider,
            pkg: pkgB,
         });
         const rows = await db.all<{ n: number }>(
            "SELECT CAST(COUNT(*) AS INTEGER) AS n FROM entity_embeddings WHERE environment_name = 'env'",
         );
         if (rows[0].n === 0) break;
         await new Promise((resolve) => setTimeout(resolve, 5));
      }

      // Released, C searches an empty table with no stale rows left to
      // heal. Without the entry-generation re-check it would return
      // {hits: []} (served as semantic "nothing relevant here"); with
      // it, it reports indexing and the tool answers marked lexical.
      release();
      expect(await cPromise).toEqual({ unavailable: "indexing" });
   });

   it("a torn write (sync failing mid-loop) still invalidates other memos", async () => {
      const { provider } = mapProvider({ ...ENTITY_VECTORS, ...QUERY_VECTORS });
      const base = {
         db,
         provider,
         environmentName: "env",
         packageName: "pkg",
         query: "find alpha",
         limit: 10,
      };
      const pkgA = {} as unknown as Package;
      await searchReady({
         ...base,
         pkg: pkgA,
         entities: [entity("alpha", "src")],
      });

      // A reload instance syncs two changed entities through a DB whose
      // SECOND insert fails: one row was already rewritten, so even
      // though the sync rejects, snapshots must be invalidated.
      let inserts = 0;
      const failingDb = new Proxy(db, {
         get(target, prop, receiver) {
            if (prop === "run") {
               return async (query: string, params?: unknown[]) => {
                  if (query.includes("INSERT INTO entity_embeddings")) {
                     inserts++;
                     if (inserts === 2) throw new Error("disk full (test)");
                  }
                  return target.run(query, params);
               };
            }
            return Reflect.get(target, prop, receiver);
         },
      });
      const changed = mapProvider({
         // Name facets included: rewording a doc leaves alpha's name facet
         // alone, but beta is new here and needs both of its facets.
         ...ENTITY_VECTORS,
         ...QUERY_VECTORS,
         "alpha: reworded": [0, 1, 0],
         "beta: new": [0, 0, 1],
      });
      const first = await trySemanticSearch({
         ...base,
         db: failingDb,
         provider: changed.provider,
         pkg: {} as unknown as Package,
         entities: [
            entity("alpha", "src", "reworded"),
            entity("beta", "src", "new"),
         ],
      });
      expect(first).toEqual({ unavailable: "indexing" });

      // Settle phase 1: wait until the second insert has been attempted
      // (it increments the counter before throwing).
      for (let i = 0; i < 200 && inserts < 2; i++) {
         await new Promise((resolve) => setTimeout(resolve, 5));
      }
      expect(inserts).toBe(2);
      // Settle phase 2: the rejection handler (which sets the cool-down)
      // runs asynchronously after the throw; wait until a call observes
      // the cool-down so the failed sync has fully settled. This
      // converges with or without the finally bump, so it does not mask
      // the pin below.
      let settled: SemanticSearchResult = { hits: [], belowCutoffCount: 0 };
      for (let i = 0; i < 200; i++) {
         settled = await trySemanticSearch({
            ...base,
            pkg: pkgA,
            entities: [entity("alpha", "src")],
         });
         if ("unavailable" in settled && settled.unavailable === "cooldown") {
            break;
         }
         await new Promise((resolve) => setTimeout(resolve, 5));
      }
      expect(settled).toEqual({ unavailable: "cooldown" });

      // The pin: with the cool-down cleared, pkgA's done memo must be
      // stale, because the failed sync changed a row before it died. A
      // success-only bump would leave pkgA serving the half-rewritten
      // table as semantic hits here.
      _clearProviderCooldownForTests();
      const next = await trySemanticSearch({
         ...base,
         pkg: pkgA,
         entities: [entity("alpha", "src")],
      });
      expect(next).toEqual({ unavailable: "indexing" });
   });

   it("querying through an old instance after a package delete recovers, never empty-semantic", async () => {
      const { provider } = mapProvider({ ...ENTITY_VECTORS, ...QUERY_VECTORS });
      const base = {
         db,
         provider,
         environmentName: "env",
         packageName: "pkg",
         entities: [entity("alpha", "src")],
         query: "find alpha",
         limit: 10,
      };
      const pkgA = {} as unknown as Package;
      await searchReady({ ...base, pkg: pkgA });

      // Delete removes the rows AND the syncMeta entry (the churn-leak
      // fix); the map must shrink back.
      const before = _syncMetaSizeForTests();
      await deletePackageEmbeddings(db, "env", "pkg");
      expect(_syncMetaSizeForTests()).toBe(before - 1);

      // pkgA still holds a done memo minted under the deleted meta. Its
      // generation can never match the re-minted meta's (globally unique
      // values), so the next call re-syncs and answers with real hits.
      // Trusting the stale memo would serve {hits: []} marked semantic
      // over the emptied table.
      const back = await searchReady({ ...base, pkg: pkgA });
      if (!("hits" in back))
         throw new Error("expected hits, got " + JSON.stringify(back));
      expect(back.hits.map((h) => h.name)).toEqual(["alpha"]);
   });

   it("a sync queued behind a package delete aborts instead of writing under an orphaned meta", async () => {
      // Hold the package mutex via a gated first sync, queue the delete
      // behind it, and queue a second instance's sync behind the delete.
      // When the gate opens: sync 1 completes, the delete purges and
      // orphans the meta, and sync 2 must abort as a no-op rather than
      // re-embedding rows for the deleted package.
      let release!: () => void;
      const gate = new Promise<void>((resolve) => {
         release = resolve;
      });
      const gated = mapProvider(
         { ...ENTITY_VECTORS, ...QUERY_VECTORS },
         { gate: { forText: "alpha", until: gate } },
      );
      const base = {
         db,
         environmentName: "env",
         packageName: "pkg",
         entities: [entity("alpha", "src")],
         query: "find alpha",
         limit: 10,
      };
      // Sync 1 (holds the mutex at its embed once it starts).
      const s1 = trySemanticSearch({
         ...base,
         provider: gated.provider,
         pkg: {} as unknown as Package,
      });
      // Delete queues behind sync 1.
      const del = deletePackageEmbeddings(db, "env", "pkg");
      // Sync 2 queues behind the delete, under the SAME (old) meta.
      const { provider } = mapProvider({ ...ENTITY_VECTORS, ...QUERY_VECTORS });
      const s2kick = trySemanticSearch({
         ...base,
         provider,
         pkg: {} as unknown as Package,
      });

      release();
      await Promise.all([s1, del, s2kick]);

      // The orphaned sync runs as soon as the delete releases the mutex.
      // Poll a failure-detection window: if the orphan guard were gone,
      // sync 2 would re-insert alpha within milliseconds; with it, the
      // table stays empty for the whole window.
      let resurrected = 0;
      for (let i = 0; i < 60; i++) {
         const rows = await db.all<{ n: number }>(
            "SELECT CAST(COUNT(*) AS INTEGER) AS n FROM entity_embeddings WHERE environment_name = 'env'",
         );
         resurrected = rows[0].n;
         if (resurrected > 0) break;
         await new Promise((resolve) => setTimeout(resolve, 5));
      }
      expect(resurrected).toBe(0);
   });

   it("answers indexing when the heal finds the mutex held mid-flight (busy path)", async () => {
      // pkgA synced at 3 dims; a 4-dim query makes its rows stale, so
      // the call reaches the heal.
      const narrow = mapProvider({ ...ENTITY_VECTORS, ...QUERY_VECTORS });
      const base = {
         db,
         environmentName: "env",
         packageName: "pkg",
         entities: [entity("alpha", "src")],
         query: "find alpha",
         limit: 10,
      };
      const pkgA = {} as unknown as Package;
      await searchReady({ ...base, provider: narrow.provider, pkg: pkgA });
      const initialRows = await db.all<{ content_hash: string }>(
         "SELECT content_hash FROM entity_embeddings WHERE environment_name = 'env' AND entity_name = 'alpha'",
      );
      const staleHash = initialRows[0].content_hash;

      // Call C: gate its out-of-mutex stale-count SELECT so we can slip
      // a mutex-holding sync in between the isLocked() check and the
      // heal's acquire, the exact race the tryAcquire busy path covers.
      let releaseSelect!: () => void;
      const selectGate = new Promise<void>((resolve) => {
         releaseSelect = resolve;
      });
      let gatedOnce = false;
      const gatedDb = new Proxy(db, {
         get(target, prop, receiver) {
            if (prop === "get") {
               return async (query: string, params?: unknown[]) => {
                  if (!gatedOnce && query.includes("NOT (embedding_model")) {
                     gatedOnce = true;
                     await selectGate;
                  }
                  return target.get(query, params);
               };
            }
            return Reflect.get(target, prop, receiver);
         },
      });
      const wide = mapProvider({
         alpha: [1, 0, 0, 0],
         "find alpha": [1, 0, 0, 0],
      });
      const cPromise = trySemanticSearch({
         ...base,
         db: gatedDb,
         provider: wide.provider,
         pkg: pkgA,
      });

      // ORDER IS LOAD-BEARING: wait until C is provably parked at the
      // gated SELECT (it has already passed the isLocked() check with
      // the mutex free) BEFORE the holder takes the mutex. Kicking the
      // holder first would park the mutex before C's isLocked() check,
      // and C would exit through that pre-existing guard without ever
      // reaching the tryAcquire busy path this test exists to pin.
      for (let i = 0; i < 200 && !gatedOnce; i++) {
         await new Promise((resolve) => setTimeout(resolve, 5));
      }
      expect(gatedOnce).toBe(true);

      // Now a sync with a changed entity takes and holds the package
      // mutex (its provider fetch is gated).
      let releaseSync!: () => void;
      const syncGate = new Promise<void>((resolve) => {
         releaseSync = resolve;
      });
      const holder = mapProvider(
         { ...QUERY_VECTORS, "alpha: changed": [0, 1, 0] },
         { gate: { forText: "alpha: changed", until: syncGate } },
      );
      const holdKick = trySemanticSearch({
         ...base,
         provider: holder.provider,
         pkg: {} as unknown as Package,
         entities: [entity("alpha", "src", "changed")],
      });
      await holdKick;

      // Release C: it finds stale rows, tries the heal, and must answer
      // indexing immediately (busy) instead of queueing behind the held
      // mutex for the sync's duration. With a blocking acquire instead
      // of tryAcquire, C queues behind the still-gated holder and the
      // race below times out.
      releaseSelect();
      const timeoutMarker = Symbol("timeout");
      const raced = await Promise.race([
         cPromise,
         new Promise((resolve) =>
            setTimeout(() => resolve(timeoutMarker), 1_000),
         ),
      ]);
      expect(raced).toEqual({ unavailable: "indexing" });

      // Let the holder sync settle deterministically (its upsert changes
      // alpha's content hash) so no write leaks past the next test's
      // cleanup.
      releaseSync();
      for (let i = 0; i < 200; i++) {
         const rows = await db.all<{ content_hash: string }>(
            "SELECT content_hash FROM entity_embeddings WHERE environment_name = 'env' AND entity_name = 'alpha'",
         );
         if (rows.length === 1 && rows[0].content_hash !== staleHash) break;
         await new Promise((resolve) => setTimeout(resolve, 5));
      }
   });

   it("deletion helpers drop a package's and an environment's rows", async () => {
      const { provider } = mapProvider({ ...ENTITY_VECTORS, ...QUERY_VECTORS });
      const base = { db, provider, query: "find alpha", limit: 10 };
      await searchReady({
         ...base,
         environmentName: "env",
         packageName: "pkg-a",
         pkg: {} as unknown as Package,
         entities: [entity("alpha", "src")],
      });
      await searchReady({
         ...base,
         environmentName: "env",
         packageName: "pkg-b",
         pkg: {} as unknown as Package,
         entities: [entity("beta", "src")],
      });

      await deletePackageEmbeddings(db, "env", "pkg-a");
      const afterPkg = await db.all<{ package_name: string }>(
         "SELECT DISTINCT package_name FROM entity_embeddings WHERE environment_name = 'env' ORDER BY package_name",
      );
      expect(afterPkg.map((r) => r.package_name)).toEqual(["pkg-b"]);

      // Clear the sync metas so pkg-b has none: the environment delete
      // must then reach its rows via the final env-wide sweep, whose
      // whole purpose is packages never queried in this process. Without
      // this reset the per-meta loop would empty the table first and the
      // sweep would pass vacuously.
      _resetEmbeddingIndexStateForTests();
      await deleteEnvironmentEmbeddings(db, "env");
      const afterEnv = await db.all<{ n: number }>(
         "SELECT CAST(COUNT(*) AS INTEGER) AS n FROM entity_embeddings WHERE environment_name = 'env'",
      );
      expect(afterEnv[0].n).toBe(0);
   });

   it("backs off instead of purging twice within the cool-down window", async () => {
      const narrow = mapProvider({ ...ENTITY_VECTORS, ...QUERY_VECTORS });
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
         provider: narrow.provider,
         pkg: {} as unknown as Package,
      });

      // First dims flip: heals via one purge + re-sync at 4 dims.
      const wide = mapProvider({
         alpha: [1, 0, 0, 0],
         "find alpha": [1, 0, 0, 0],
      });
      const healed = await searchReady({
         ...base,
         provider: wide.provider,
         pkg: {} as unknown as Package,
      });
      if (!("hits" in healed)) throw new Error("expected hits");

      // Second flip inside the window (back to 3 dims): the endpoint is
      // inconsistent; the heal must NOT purge again but cool down, and
      // the 4-dim rows must survive. One instance throughout, so the
      // poll passes its cold start and reaches the heal check.
      const pkgC = {} as unknown as Package;
      let result = await trySemanticSearch({
         ...base,
         provider: narrow.provider,
         pkg: pkgC,
      });
      for (
         let i = 0;
         i < 200 &&
         "unavailable" in result &&
         result.unavailable === "indexing";
         i++
      ) {
         await new Promise((resolve) => setTimeout(resolve, 5));
         result = await trySemanticSearch({
            ...base,
            provider: narrow.provider,
            pkg: pkgC,
         });
      }
      expect(result).toEqual({ unavailable: "cooldown" });
      const rows = await db.all<{ dims: number }>(
         "SELECT CAST(dims AS INTEGER) AS dims FROM entity_embeddings WHERE environment_name = 'env'",
      );
      expect(rows.map((r) => r.dims)).toEqual([4]);
   });

   it("does not re-purge once per cooldown window: suppression outlasts the cooldown", async () => {
      // The core of blocker 4. cooldown 15ms, suppression 400ms: after
      // the cooldown clears (but well inside the suppression window) a
      // fresh mismatch must BACK OFF, not re-purge. With the co-anchored
      // bug (equal windows) the first post-cooldown call re-purges and
      // full-re-embeds; this test fails against that.
      _setTimingForTests({ cooldownMs: 15, purgeSuppressionMs: 400 });
      const base = {
         db,
         environmentName: "env",
         packageName: "pkg",
         entities: [entity("alpha", "src")],
         query: "find alpha",
         limit: 10,
      };
      // Sync at 3 dims, then flip to 4: one purge + resync leaves 4-dim rows.
      const narrow = mapProvider({ ...ENTITY_VECTORS, ...QUERY_VECTORS });
      await searchReady({
         ...base,
         provider: narrow.provider,
         pkg: {} as unknown as Package,
      });
      const wide = mapProvider({
         alpha: [1, 0, 0, 0],
         "find alpha": [1, 0, 0, 0],
      });
      const healed = await searchReady({
         ...base,
         provider: wide.provider,
         pkg: {} as unknown as Package,
      });
      if (!("hits" in healed)) throw new Error("expected hits");
      const purgeAt = _lastPurgeAtMsForTests("env", "pkg") ?? 0;
      expect(purgeAt).toBeGreaterThan(0);

      // Query back at 3 dims (rows are 4-dim, so stale) across several
      // cooldown-expiry cycles, all inside the 400ms suppression window.
      const pkgQ = {} as unknown as Package;
      for (let cycle = 0; cycle < 4; cycle++) {
         let r = await trySemanticSearch({
            ...base,
            provider: narrow.provider,
            pkg: pkgQ,
         });
         for (
            let j = 0;
            j < 200 && "unavailable" in r && r.unavailable === "indexing";
            j++
         ) {
            await new Promise((resolve) => setTimeout(resolve, 3));
            r = await trySemanticSearch({
               ...base,
               provider: narrow.provider,
               pkg: pkgQ,
            });
         }
         expect(r).toEqual({ unavailable: "cooldown" });
         await new Promise((resolve) => setTimeout(resolve, 20)); // clear the 15ms cooldown
      }

      // Never re-purged: lastPurgeAtMs unchanged and the 4-dim rows survive
      // (the 3-dim vector was only ever the query, never re-embedded).
      expect(_lastPurgeAtMsForTests("env", "pkg")).toBe(purgeAt);
      const dims = await db.all<{ dims: number }>(
         "SELECT DISTINCT CAST(dims AS INTEGER) AS dims FROM entity_embeddings WHERE environment_name = 'env'",
      );
      expect(dims.map((d) => d.dims)).toEqual([4]);
   });

   it("re-heals once the suppression window elapses (self-recovery)", async () => {
      // Same setup but a short suppression window: after it elapses a
      // mismatch re-purges and re-adopts the current dims, so a provider
      // that stabilizes on a new dimensionality is not stranded lexical.
      _setTimingForTests({ cooldownMs: 10, purgeSuppressionMs: 60 });
      const base = {
         db,
         environmentName: "env",
         packageName: "pkg",
         entities: [entity("alpha", "src")],
         query: "find alpha",
         limit: 10,
      };
      const narrow = mapProvider({ ...ENTITY_VECTORS, ...QUERY_VECTORS });
      await searchReady({
         ...base,
         provider: narrow.provider,
         pkg: {} as unknown as Package,
      });
      const wide = mapProvider({
         alpha: [1, 0, 0, 0],
         "find alpha": [1, 0, 0, 0],
      });
      const healed = await searchReady({
         ...base,
         provider: wide.provider,
         pkg: {} as unknown as Package,
      });
      if (!("hits" in healed)) throw new Error("expected hits");
      const purgeAt = _lastPurgeAtMsForTests("env", "pkg") ?? 0;

      // Wait past the 60ms suppression window, then the provider is now
      // stably 3-dim: the next mismatch re-purges and re-adopts 3 dims.
      await new Promise((resolve) => setTimeout(resolve, 90));
      const recovered = await searchReady({
         ...base,
         provider: narrow.provider,
         pkg: {} as unknown as Package,
      });
      if (!("hits" in recovered))
         throw new Error("expected hits after re-heal");
      expect(recovered.hits.map((h) => h.name)).toEqual(["alpha"]);
      expect(_lastPurgeAtMsForTests("env", "pkg") ?? 0).toBeGreaterThan(
         purgeAt,
      );
      const dims = await db.all<{ dims: number }>(
         "SELECT DISTINCT CAST(dims AS INTEGER) AS dims FROM entity_embeddings WHERE environment_name = 'env'",
      );
      expect(dims.map((d) => d.dims)).toEqual([3]);
   });

   it("a provider failure cools down only its own package, not healthy siblings", async () => {
      // pkg-a's provider fails on every call; pkg-b (same env) is healthy.
      const failing = mapProvider(
         { ...ENTITY_VECTORS, ...QUERY_VECTORS },
         { fail: () => true },
      );
      const argsA = {
         db,
         provider: failing.provider,
         pkg: {} as unknown as Package,
         environmentName: "env",
         packageName: "pkg-a",
         entities: [entity("alpha", "src")],
         query: "find alpha",
         limit: 10,
      };
      let a = await trySemanticSearch(argsA);
      expect(a).toEqual({ unavailable: "indexing" });
      for (let i = 0; i < 200 && !isCooldown(a); i++) {
         await new Promise((resolve) => setTimeout(resolve, 5));
         a = await trySemanticSearch(argsA);
      }
      expect(a).toEqual({ unavailable: "cooldown" });

      // pkg-b, healthy, same env: a per-package cool-down leaves it able
      // to reach semantic. A global cool-down (the old bug) would make
      // this return cooldown and never produce hits.
      const healthy = mapProvider({ ...ENTITY_VECTORS, ...QUERY_VECTORS });
      const b = await searchReady({
         db,
         provider: healthy.provider,
         pkg: {} as unknown as Package,
         environmentName: "env",
         packageName: "pkg-b",
         entities: [entity("alpha", "src")],
         query: "find alpha",
         limit: 10,
      });
      expect("hits" in b).toBe(true);
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
      // Poll until the background sync failure lands and starts the
      // cool-down; a fixed sleep would race the rejection handler.
      let second = await trySemanticSearch(args);
      for (let i = 0; i < 200 && !isCooldown(second); i++) {
         await new Promise((resolve) => setTimeout(resolve, 5));
         second = await trySemanticSearch(args);
      }
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
