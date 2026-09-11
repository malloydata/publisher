// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { beforeEach, describe, expect, it } from "bun:test";
import type { EmbeddingProvider } from "../../service/embedding_provider";
import { DEFAULT_EMBEDDING_MIN_SIMILARITY } from "../../config";
import {
   MAX_COLUMNS_IN_INDEX_TEXT,
   MAX_CACHED_SCHEMAS,
   MAX_INDEXED_TABLES,
   SchemaTableEntity,
   _resetSchemaIndexStateForTests,
   rankLexically,
   rankTables,
   sanitizeQuery,
   schemaFingerprint,
   tableIndexText,
} from "./schema_index";

function table(
   resource: string,
   columns: string[],
   type = "varchar",
): SchemaTableEntity {
   const [schemaName, tableName] = resource.split(".");
   return {
      connectionName: "warehouse",
      schemaName,
      tableName,
      resource,
      columns: columns.map((name) => ({ name, type })),
   };
}

const ORDERS = table("sales.orders", [
   "order_id",
   "customer_id",
   "total_amount",
]);
const CUSTOMERS = table("sales.customers", [
   "customer_id",
   "email_address",
   "signup_date",
]);
const SHIPMENTS = table("logistics.shipments", [
   "shipment_id",
   "carrier_name",
   "delivered_at",
]);
const ALL = [ORDERS, CUSTOMERS, SHIPMENTS];

/**
 * A provider whose vector is a bag of marker words, so cosine similarity is
 * deterministic and a test can assert ordering rather than a magic float.
 */
const MARKERS = ["order", "customer", "shipment", "email"];
function fakeProvider(
   overrides: Partial<{
      embedBatch: (texts: string[], timeoutMs: number) => Promise<number[][]>;
      model: string;
      dimensions: number | undefined;
      minSimilarity: number;
   }> = {},
): EmbeddingProvider {
   return {
      model: overrides.model ?? "test-model",
      dimensions: overrides.dimensions,
      // Required: the floor travels on the provider, and a double that omits
      // it makes every `score >= provider.minSimilarity` compare against
      // undefined, which is always false -- silently dropping every hit.
      minSimilarity:
         overrides.minSimilarity ?? DEFAULT_EMBEDDING_MIN_SIMILARITY,
      embedBatch:
         overrides.embedBatch ??
         (async (texts: string[]) =>
            texts.map((text) => {
               const lower = text.toLowerCase();
               return MARKERS.map((marker) => (lower.includes(marker) ? 1 : 0));
            })),
   } as unknown as EmbeddingProvider;
}

beforeEach(() => {
   _resetSchemaIndexStateForTests();
});

describe("tableIndexText", () => {
   it("humanizes the schema, table, and column names", () => {
      const text = tableIndexText(ORDERS);
      expect(text).toContain("orders");
      expect(text).toContain("order id");
      expect(text).toContain("customer id");
      expect(text).toContain("total amount");
   });

   it("splits camelCase columns so a word query can match them", () => {
      const text = tableIndexText(table("main.t", ["totalSales"]));
      expect(text).toContain("total sales");
   });

   it("caps the columns it folds in", () => {
      const many = Array.from({ length: 200 }, (_, i) => `col_${i}`);
      const text = tableIndexText(table("main.wide", many));
      expect(text).toContain("col 0");
      expect(text).not.toContain(`col ${MAX_COLUMNS_IN_INDEX_TEXT + 5}`);
   });

   it("deduplicates types rather than repeating one per column", () => {
      const text = tableIndexText(ORDERS);
      expect(text.match(/varchar/g)?.length).toBe(1);
   });

   it("falls back to the raw name when humanizing empties it", () => {
      const text = tableIndexText(table("main._", ["_"]));
      expect(text).toContain("_");
   });
});

describe("schemaFingerprint", () => {
   it("is stable regardless of table order", () => {
      expect(schemaFingerprint([ORDERS, CUSTOMERS])).toBe(
         schemaFingerprint([CUSTOMERS, ORDERS]),
      );
   });

   it("changes when a column is added", () => {
      const before = schemaFingerprint([ORDERS]);
      const after = schemaFingerprint([
         table("sales.orders", [
            "order_id",
            "customer_id",
            "total_amount",
            "x",
         ]),
      ]);
      expect(after).not.toBe(before);
   });

   it("changes when a column type changes", () => {
      const before = schemaFingerprint([ORDERS]);
      const after = schemaFingerprint([
         table(
            "sales.orders",
            ["order_id", "customer_id", "total_amount"],
            "int",
         ),
      ]);
      expect(after).not.toBe(before);
   });

   it("changes when a table is dropped", () => {
      expect(schemaFingerprint([ORDERS])).not.toBe(
         schemaFingerprint([ORDERS, CUSTOMERS]),
      );
   });
});

describe("sanitizeQuery", () => {
   it("strips lunr operators that would otherwise throw", () => {
      expect(sanitizeQuery("orders^2 +customer")).toBe("orders 2  customer");
   });

   it("empties an operator-only query", () => {
      expect(sanitizeQuery("~^*+")).toBe("");
   });
});

describe("rankLexically", () => {
   it("finds a table by a humanized column term", () => {
      const { hits } = rankLexically(ALL, "email address", 10);
      expect(hits[0]?.resource).toBe("sales.customers");
   });

   it("finds a table by its own name", () => {
      const { hits } = rankLexically(ALL, "shipments", 10);
      expect(hits[0]?.resource).toBe("logistics.shipments");
   });

   it("returns nothing for a query no table is about", () => {
      expect(rankLexically(ALL, "photosynthesis", 10).hits).toEqual([]);
   });

   it("returns nothing for an empty or operator-only query", () => {
      expect(rankLexically(ALL, "", 10).hits).toEqual([]);
      expect(rankLexically(ALL, "^~*", 10).hits).toEqual([]);
   });

   it("respects the limit", () => {
      const result = rankLexically(ALL, "id", 1);
      // Exactly one, not "at most one": <= 1 also passes on an empty result,
      // which is the failure this test exists to catch.
      expect(result.hits.length).toBe(1);
      expect(result.matched).toBeGreaterThan(1);
   });

   it("handles an empty table list", () => {
      expect(rankLexically([], "orders", 10).hits).toEqual([]);
   });

   // Without this the caller cannot tell "20 shown of 60 matched" from
   // "20 matched", and ranked results cannot be paged to find out.
   it("reports how many matched before the limit was applied", () => {
      const result = rankLexically(ALL, "id", 1);
      expect(result.hits.length).toBe(1);
      expect(result.matched).toBeGreaterThan(1);
   });
});

describe("rankTables", () => {
   it("ranks lexically when no provider is supplied", async () => {
      const result = await rankTables({
         tables: ALL,
         query: "customers",
         limit: 10,
         provider: null,
         cacheKey: "k",
      });
      expect(result.ranking).toBe("lexical");
      expect(result.hits[0]?.resource).toBe("sales.customers");
   });

   it("ranks semantically when a provider answers", async () => {
      const result = await rankTables({
         tables: ALL,
         query: "shipment",
         limit: 10,
         provider: fakeProvider(),
         cacheKey: "k",
      });
      expect(result.ranking).toBe("semantic");
      expect(result.hits[0]?.resource).toBe("logistics.shipments");
   });

   it("drops hits below the similarity floor rather than returning weak ones", async () => {
      // "photosynthesis" shares no marker with any table, so every vector is
      // orthogonal to the query and nothing survives the floor.
      const result = await rankTables({
         tables: ALL,
         query: "photosynthesis",
         limit: 10,
         provider: fakeProvider(),
         cacheKey: "k",
      });
      expect(result.ranking).toBe("semantic");
      expect(result.hits).toEqual([]);
   });

   it("reuses cached vectors: a second query embeds only the query", async () => {
      const batchSizes: number[] = [];
      const provider = fakeProvider({
         embedBatch: async (texts: string[]) => {
            batchSizes.push(texts.length);
            return texts.map((text) =>
               MARKERS.map((m) => (text.toLowerCase().includes(m) ? 1 : 0)),
            );
         },
      });
      await rankTables({
         tables: ALL,
         query: "orders",
         limit: 10,
         provider,
         cacheKey: "k",
      });
      await rankTables({
         tables: ALL,
         query: "customers",
         limit: 10,
         provider,
         cacheKey: "k",
      });
      // First call: 3 tables + 1 query. Second: query only.
      expect(batchSizes).toEqual([3, 1, 1]);
   });

   it("re-embeds when the schema fingerprint changes", async () => {
      const batchSizes: number[] = [];
      const provider = fakeProvider({
         embedBatch: async (texts: string[]) => {
            batchSizes.push(texts.length);
            return texts.map(() => [1, 0, 0, 0]);
         },
      });
      await rankTables({
         tables: ALL,
         query: "orders",
         limit: 10,
         provider,
         cacheKey: "k",
      });
      await rankTables({
         tables: [...ALL, table("sales.returns", ["return_id"])],
         query: "orders",
         limit: 10,
         provider,
         cacheKey: "k",
      });
      expect(batchSizes).toEqual([3, 1, 4, 1]);
   });

   it("falls back to lexical when the provider throws, and does not report semantic", async () => {
      const provider = fakeProvider({
         embedBatch: async () => {
            throw new Error("provider down");
         },
      });
      const result = await rankTables({
         tables: ALL,
         query: "customers",
         limit: 10,
         provider,
         cacheKey: "k",
      });
      expect(result.ranking).toBe("lexical");
      expect(result.hits[0]?.resource).toBe("sales.customers");
   });

   it("stays lexical during the cooldown instead of retrying a down provider", async () => {
      let calls = 0;
      const provider = fakeProvider({
         embedBatch: async () => {
            calls++;
            throw new Error("provider down");
         },
      });
      const args = {
         tables: ALL,
         query: "customers",
         limit: 10,
         provider,
         cacheKey: "k",
      };
      await rankTables(args);
      await rankTables(args);
      await rankTables(args);
      expect(calls).toBe(1);
   });

   it("cools down per schema, not globally", async () => {
      let calls = 0;
      const provider = fakeProvider({
         embedBatch: async (texts: string[]) => {
            calls++;
            if (calls === 1) throw new Error("provider down");
            return texts.map((text) =>
               MARKERS.map((m) => (text.toLowerCase().includes(m) ? 1 : 0)),
            );
         },
      });
      const first = await rankTables({
         tables: ALL,
         query: "customers",
         limit: 10,
         provider,
         cacheKey: "schema-a",
      });
      const second = await rankTables({
         tables: ALL,
         query: "customers",
         limit: 10,
         provider,
         cacheKey: "schema-b",
      });
      expect(first.ranking).toBe("lexical");
      expect(second.ranking).toBe("semantic");
   });

   it("stays lexical above the table cap without calling the provider", async () => {
      let called = false;
      const provider = fakeProvider({
         embedBatch: async (texts: string[]) => {
            called = true;
            return texts.map(() => [1, 0, 0, 0]);
         },
      });
      const many = Array.from({ length: MAX_INDEXED_TABLES + 1 }, (_, i) =>
         table(`main.t_${i}`, ["orders_col"]),
      );
      const result = await rankTables({
         tables: many,
         query: "orders",
         limit: 5,
         provider,
         cacheKey: "big",
      });
      expect(result.ranking).toBe("lexical");
      expect(called).toBe(false);
   });

   // A whitespace-only query has no content in either mode. The provider
   // substitutes a placeholder token for empty input, so without an explicit
   // guard every table gets scored against the embedding of "-" and arbitrary
   // tables come back presented as ranked matches.
   it("returns nothing for a whitespace-only query, in BOTH modes", async () => {
      let called = false;
      const provider = fakeProvider({
         embedBatch: async (texts: string[]) => {
            called = true;
            return texts.map(() => [1, 1, 1, 1]);
         },
      });
      const semantic = await rankTables({
         tables: ALL,
         query: "   ",
         limit: 10,
         provider,
         cacheKey: "k",
      });
      const lexical = await rankTables({
         tables: ALL,
         query: "   ",
         limit: 10,
         provider: null,
         cacheKey: "k",
      });
      expect(semantic.hits).toEqual([]);
      expect(lexical.hits).toEqual([]);
      expect(called).toBe(false);
   });

   it("evicts the least-recently-used schema past the cache cap", async () => {
      const embedded: number[] = [];
      const provider = fakeProvider({
         embedBatch: async (texts: string[]) => {
            embedded.push(texts.length);
            return texts.map(() => [1, 0, 0, 0]);
         },
      });
      // Fill the cache, then add one more to force an eviction.
      for (let i = 0; i <= MAX_CACHED_SCHEMAS; i++) {
         await rankTables({
            tables: ALL,
            query: "orders",
            limit: 10,
            provider,
            cacheKey: `schema-${i}`,
         });
      }
      const embedsBefore = embedded.length;
      // schema-0 was evicted, so it must re-embed its 3 tables.
      await rankTables({
         tables: ALL,
         query: "orders",
         limit: 10,
         provider,
         cacheKey: "schema-0",
      });
      expect(embedded.slice(embedsBefore)).toContain(3);

      // And a recently-used entry must NOT have been evicted, or this test
      // would pass with a cap of 1.
      const embedsAfterZero = embedded.length;
      await rankTables({
         tables: ALL,
         query: "orders",
         limit: 10,
         provider,
         cacheKey: `schema-${MAX_CACHED_SCHEMAS}`,
      });
      // Query embed only (1), no re-embed of the 3 tables.
      expect(embedded.slice(embedsAfterZero)).toEqual([1]);
   });

   it("falls back to lexical when the provider returns the wrong vector count", async () => {
      const provider = fakeProvider({
         embedBatch: async (texts: string[]) =>
            // One short: a malformed provider response must not silently
            // mis-pair vectors with tables.
            texts.slice(1).map(() => [1, 0, 0, 0]),
      });
      const result = await rankTables({
         tables: ALL,
         query: "customers",
         limit: 10,
         provider,
         cacheKey: "k",
      });
      expect(result.ranking).toBe("lexical");
   });
});

describe("empty-query honesty", () => {
   it("reports lexical, not semantic, when no embedding call was made", async () => {
      // `ranking` reports what happened. A query with no searchable content
      // runs nothing, so claiming "semantic" would describe configuration.
      const result = await rankTables({
         tables: ALL,
         query: "  ^~ ",
         limit: 10,
         provider: fakeProvider(),
         cacheKey: "k",
      });
      expect(result.ranking).toBe("lexical");
      expect(result.emptyQuery).toBe(true);
      expect(result.hits).toEqual([]);
   });

   it("does not set emptyQuery for a real query that simply matched nothing", async () => {
      const result = await rankTables({
         tables: ALL,
         query: "photosynthesis",
         limit: 10,
         provider: null,
         cacheKey: "k",
      });
      expect(result.emptyQuery).toBeUndefined();
      expect(result.hits).toEqual([]);
   });
});

describe("lexical index caching", () => {
   // The default path. Rebuilding per call blocks the event loop for hundreds
   // of ms on a large schema, on every search.
   it("reuses a built index for the same schema, and rebuilds when it changes", () => {
      const first = rankLexically(ALL, "customers", 10, "k");
      const second = rankLexically(ALL, "orders", 10, "k");
      expect(first.hits[0]?.resource).toBe("sales.customers");
      expect(second.hits[0]?.resource).toBe("sales.orders");

      // A changed schema must not be answered from the cached index.
      const grown = [...ALL, table("sales.refunds", ["refund_id", "reason"])];
      const third = rankLexically(grown, "refunds", 10, "k");
      expect(third.hits[0]?.resource).toBe("sales.refunds");
   });

   it("still works with no cache key", () => {
      expect(rankLexically(ALL, "customers", 10).hits[0]?.resource).toBe(
         "sales.customers",
      );
   });
});
