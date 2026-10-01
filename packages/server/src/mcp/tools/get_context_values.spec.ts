// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Dimensional value search through the handler: a question that names only a
// value ("Premium") finds the dimension that holds it, a gated source's values
// are never indexed, and the default response is untouched.

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
   _resetValueIndexStateForTests,
   _settleValueIndexForTests,
} from "../../retrieval/dim_values";
import { _clearOverrideCacheForTests } from "../../retrieval/run";
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
import {
   createDimensionValueTables,
   createEntityEmbeddingsTable,
   createEntityEnrichmentTable,
} from "../../storage/duckdb/schema";
import { _resetEmbeddingIndexStateForTests } from "./embedding_index";
import { getPackageEmbeddingStatus } from "./get_context_tool";
import {
   captureHandler,
   entityNames,
   parse,
   sourceNames,
   storeFor,
} from "./retrieval_test_kit";

const ENV = "vals";
const PKG = "p";

/** Every query the "warehouse" was asked, so a test can prove what was NOT read. */
const asked: string[] = [];

const VALUES: Record<string, Array<[string, number]>> = {
   tier: [
      ["Premium", 50],
      ["Basic", 30],
      ["Enterprise", 5],
   ],
   city: [
      ["Paris", 9],
      ["Berlin", 4],
   ],
   tenant_name: [
      ["Acme", 10],
      ["Globex", 8],
   ],
};

const field = (
   kind: string,
   name: string,
   type: string,
   annotations: string[] = [],
) => ({
   kind,
   name,
   type: { kind: `${type}_type` },
   annotations,
});

const model = {
   getSourceInfos: () => [
      {
         name: "customers",
         annotations: ["#(doc) Every customer."],
         schema: {
            fields: [
               field("dimension", "tier", "string", [
                  "#(doc) Pricing tier.",
                  "#(index)",
               ]),
               field("dimension", "city", "string"),
               field("measure", "customer_count", "number", [
                  "#(doc) Number of customers.",
               ]),
            ],
         },
      },
      {
         name: "tenants",
         annotations: ["#(doc) Per-tenant data."],
         schema: {
            fields: [field("dimension", "tenant_name", "string", ["#(index)"])],
         },
      },
   ],
   getQueries: () => [],
   // `tenants` is gated: its rows differ per caller.
   getSources: () => [
      { name: "customers" },
      { name: "tenants", accessFilter: ["tenant = $TENANT"] },
   ],
   getQueryResults: async (
      _source: unknown,
      _queryName: unknown,
      query: string,
   ) => {
      asked.push(query);
      const dim = Object.keys(VALUES).find((d) => query.includes(`\`${d}\``));
      if (!dim) throw new Error(`unexpected query: ${query}`);
      return {
         compactResult: VALUES[dim].map(([v, w]) => ({
            [dim]: v,
            value_weight__: w,
         })),
      };
   },
};
const pkg = {
   listModels: async () => [{ path: "m.malloy" }],
   getModel: () => model,
};

const valueTarget = (text: string) => ({
   target_type: "dimensional_value",
   search_text: text,
});
const req = (targets: unknown[], extra: Record<string, unknown> = {}) => ({
   search_targets: targets,
   scopes: [{ environment: ENV, package: PKG, ...extra }],
});

function embedder(vectorFor: (text: string) => number[]): EmbeddingProvider {
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

describe("get_context value search", () => {
   let tempDir: string;
   let db: DuckDBConnection;

   beforeAll(async () => {
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "get-context-values-"));
      db = new DuckDBConnection(path.join(tempDir, "test.db"));
      await db.initialize();
      await createEntityEmbeddingsTable(db);
      await createEntityEnrichmentTable(db);
      await createDimensionValueTables(db);
   });
   afterAll(async () => {
      await db.close();
      fs.rmSync(tempDir, { recursive: true, force: true });
      _clearEmbeddingProviderForTests();
   });
   beforeEach(async () => {
      asked.length = 0;
      await db.run("DELETE FROM dimension_values");
      await db.run("DELETE FROM dimension_value_state");
      await db.run("DELETE FROM entity_embeddings");
      _setEmbeddingProviderForTests(null);
      _resetEmbeddingIndexStateForTests();
      _resetValueIndexStateForTests();
      _clearOverrideCacheForTests();
      configure({});
   });
   afterEach(() => _clearRetrievalConfigForTests());

   function configure(
      values: Record<string, unknown>,
      extra: Record<string, unknown> = {},
   ) {
      _setRetrievalConfigForTests({
         dimensionalValues: { mode: "annotated", ...values },
         ...extra,
      });
   }
   const handler = () => captureHandler(storeFor(pkg, db));

   /** Ask once to start the index, wait for it, and return the handler. */
   async function indexed() {
      const h = handler();
      await h(req([{ target_type: "source" }]));
      await _settleValueIndexForTests(ENV, PKG);
      return h;
   }

   describe("when it is off", () => {
      it("answers a value target exactly as it always has", async () => {
         _setRetrievalConfigForTests({});
         const payload = parse(await handler()(req([valueTarget("Premium")])));
         expect(payload.sources).toEqual([]);
         expect(payload.warnings[0]).toContain(
            "No index for target_type dimensional_value",
         );
         expect(asked).toEqual([]);
      });
   });

   describe("when it is on", () => {
      it("finds the dimension that holds a value, and shows the value", async () => {
         const h = await indexed();
         const payload = parse(await h(req([valueTarget("premium")])));
         expect(sourceNames(payload)).toEqual(["customers"]);
         const tier = payload.sources[0].entities[0];
         expect(tier.name).toBe("tier");
         expect(tier.entity_type).toBe("dimension");
         expect(tier.values).toEqual([
            { value: "Premium", relevance: 1, search_text: "premium" },
         ]);
         expect(tier.values_indexed).toBe(true);
         // No relevance of its own, so the card takes its best value's.
         expect(tier).not.toHaveProperty("relevance");
         expect(payload.sources[0].relevance).toBe(1);
      });

      it("carries no retrieval marker on a request made only of values", async () => {
         const h = await indexed();
         const payload = parse(await h(req([valueTarget("premium")])));
         expect(payload).not.toHaveProperty("retrieval");
         expect(payload.ranking).toBe("relevance");
      });

      it("reads only the tagged dimension of an open source", async () => {
         await indexed();
         expect(asked).toHaveLength(1);
         expect(asked[0]).toBe(
            "run: `customers` -> { group_by: `tier`; aggregate: `value_weight__` is count(); order_by: `value_weight__` desc; limit: 101 }",
         );
      });

      it("never reads, stores or returns the values of a gated source", async () => {
         const h = await indexed();
         expect(asked.join("\n")).not.toContain("tenants");
         expect(asked.join("\n")).not.toContain("tenant_name");
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         const rows = await db.all<any>(
            "SELECT DISTINCT source_name FROM dimension_values",
         );
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         expect(rows.map((r: any) => r.source_name)).toEqual(["customers"]);
         const payload = parse(await h(req([valueTarget("Acme")])));
         expect(payload.sources).toEqual([]);
         // Even in auto mode, with the gated source named outright.
         configure({ mode: "auto", include: ["tenants.*"] });
         _resetValueIndexStateForTests();
         const again = handler();
         await again(req([{ target_type: "source" }]));
         await _settleValueIndexForTests(ENV, PKG);
         expect(asked.join("\n")).not.toContain("tenant_name");
      });

      it("marks the dimensions whose values can be searched on an ordinary result", async () => {
         const h = await indexed();
         const payload = parse(
            await h(
               req([
                  { target_type: "dimension", search_text: "tier" },
                  { target_type: "dimension", search_text: "city" },
               ]),
            ),
         );
         const byName = Object.fromEntries(
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            payload.sources.flatMap((c: any) =>
               // eslint-disable-next-line @typescript-eslint/no-explicit-any
               c.entities.map((e: any) => [e.name, e]),
            ),
         );
         expect(byName.tier.values_indexed).toBe(true);
         expect(byName.city.values_indexed).toBeUndefined();
         expect(byName.tier).not.toHaveProperty("values");
      });

      it("attaches values to a dimension the entity search already returned", async () => {
         const h = await indexed();
         const payload = parse(
            await h(
               req([
                  { target_type: "dimension", search_text: "pricing tier" },
                  valueTarget("basic"),
               ]),
            ),
         );
         const tiers = payload.sources
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            .flatMap((c: any) => c.entities)
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            .filter((e: any) => e.name === "tier");
         expect(tiers).toHaveLength(1);
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         expect(tiers[0].values.map((v: any) => v.value)).toEqual(["Basic"]);
         expect(tiers[0].entity_type).toBe("dimension");
      });

      it("answers a mixed request with both the entities and the values", async () => {
         const h = await indexed();
         const payload = parse(
            await h(
               req([
                  { target_type: "measure", search_text: "customer count" },
                  valueTarget("paris"),
               ]),
            ),
         );
         // city is not tagged, so Paris is not in the index; the measure still comes back.
         expect(entityNames(payload)).toEqual(["customer_count"]);
         expect(payload.warnings ?? []).not.toContain(undefined);
      });

      it("flags a dimension whose values were cut at a cap", async () => {
         configure({ maxValuesPerDimension: 2 });
         const h = await indexed();
         const payload = parse(await h(req([valueTarget("premium")])));
         const tier = payload.sources[0].entities[0];
         expect(tier.values_truncated).toBe(true);
         // Enterprise (5) fell outside the top two.
         const missing = parse(await h(req([valueTarget("enterprise")])));
         expect(missing.sources).toEqual([]);
      });

      it("says the index is still building when asked too soon", async () => {
         const h = handler();
         const payload = parse(await h(req([valueTarget("premium")])));
         expect(payload.warnings.join(" ")).toContain("still being indexed");
      });

      it("says so when no dimension is set up for value search", async () => {
         configure({ mode: "annotated", exclude: ["*.*"] });
         const h = handler();
         const payload = parse(await h(req([valueTarget("premium")])));
         expect(payload.warnings.join(" ")).toContain(
            "No dimension in this package is set up",
         );
         expect(payload.warnings.join(" ")).toContain("#(index)");
      });

      it("finds a value by meaning when an embedding provider is set", async () => {
         const provider = embedder((t) =>
            /premium|top tier/i.test(t) ? [1, 0] : [0, 1],
         );
         _setEmbeddingProviderForTests(provider);
         const h = await indexed();
         const payload = parse(await h(req([valueTarget("top tier")])));
         expect(payload.sources[0].entities[0].values[0].value).toBe("Premium");
         expect(payload.sources[0].entities[0].values[0].relevance).toBe(1);
      });

      it("honours a drill-down to one source", async () => {
         const h = await indexed();
         const inScope = parse(
            await h(req([valueTarget("premium")], { source: "customers" })),
         );
         expect(sourceNames(inScope)).toEqual(["customers"]);
         const outOfScope = parse(
            await h(req([valueTarget("premium")], { source: "tenants" })),
         );
         expect(outOfScope.sources).toEqual([]);
      });

      it("reports the value index in the package's index status", async () => {
         _setEmbeddingProviderForTests(embedder(() => [1, 0]));
         const h = handler();
         await h(req([{ target_type: "source" }]));
         await _settleValueIndexForTests(ENV, PKG);
         const status = await getPackageEmbeddingStatus(
            storeFor(pkg, db) as never,
            ENV,
            PKG,
         );
         expect(status?.valueIndex).toMatchObject({
            status: "ready",
            dimensions: 1,
            values: 3,
            truncated: 0,
            failed: 0,
         });
      });
   });
});
