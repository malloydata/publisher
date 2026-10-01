// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Source rerank through the get_context handler, against a scripted LLM.

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
import { _clearOverrideCacheForTests } from "../../retrieval/run";
import {
   _clearRetrievalConfigForTests,
   _setRetrievalConfigForTests,
} from "../../retrieval/retrieval_config";
import {
   _clearEmbeddingProviderForTests,
   _setEmbeddingProviderForTests,
} from "../../service/embedding_provider";
import {
   LlmError,
   _clearLlmProviderForTests,
   _setLlmProviderForTests,
   type LlmRequest,
} from "../../service/llm_provider";
import { _resetLlmRunnerForTests } from "../../service/llm_runner";
import { DuckDBConnection } from "../../storage/duckdb/DuckDBConnection";
import { createEntityEmbeddingsTable } from "../../storage/duckdb/schema";
import { _resetEmbeddingIndexStateForTests } from "./embedding_index";
import {
   afterWarmup,
   captureHandler,
   embeddingsFor,
   entityNames,
   fakeLlm,
   rankReply,
   sourceIndex,
   sourceNames,
   storeFor,
} from "./retrieval_test_kit";

// Three sources, each with one measure at a known cosine to "revenue".
const SOURCES = [
   {
      source: "orders",
      measure: "total_revenue",
      cos: 1,
      doc: "Every order placed.",
   },
   {
      source: "customers",
      measure: "customer_revenue",
      cos: 0.95,
      doc: "Every customer.",
   },
   {
      source: "shipments",
      measure: "freight_revenue",
      cos: 0.8,
      doc: "Every shipment sent.",
   },
];
const humanize = (n: string) => n.replace(/_/g, " ");
const vec = (c: number) => [c, Math.sqrt(1 - c * c)];
const model = {
   getSourceInfos: () =>
      SOURCES.map((s) => ({
         name: s.source,
         annotations: [`#(doc) ${s.doc}`],
         schema: {
            fields: [
               {
                  kind: "measure",
                  name: s.measure,
                  annotations: [`#(doc) Revenue from ${s.source}.`],
               },
            ],
         },
      })),
   getQueries: () => [],
};
const pkg = {
   listModels: async () => [{ path: "m.malloy" }],
   getModel: () => model,
};
const VECTORS: Record<string, number[]> = {
   revenue: [1, 0],
   "revenue by customer": [1, 0],
};
for (const s of SOURCES) {
   VECTORS[s.source] = [0, 1];
   VECTORS[`${s.source}: ${s.doc}`] = [0, 1];
   VECTORS[humanize(s.measure)] = vec(s.cos);
   VECTORS[`${humanize(s.measure)}: Revenue from ${s.source}.`] = vec(s.cos);
}

const params = {
   search_targets: [{ target_type: "measure", search_text: "revenue" }],
   scopes: [{ environment: "rerank", package: "p" }],
};

describe("get_context source rerank", () => {
   let tempDir: string;
   let db: DuckDBConnection;
   const savedGate = process.env.PUBLISHER_RETRIEVAL_OVERRIDES;

   beforeAll(async () => {
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "get-context-rerank-"));
      db = new DuckDBConnection(path.join(tempDir, "test.db"));
      await db.initialize();
      await createEntityEmbeddingsTable(db);
   });
   afterAll(async () => {
      await db.close();
      fs.rmSync(tempDir, { recursive: true, force: true });
      _clearEmbeddingProviderForTests();
      _clearLlmProviderForTests();
   });
   beforeEach(() => {
      _setEmbeddingProviderForTests(embeddingsFor(VECTORS));
      _setLlmProviderForTests(null);
      _resetEmbeddingIndexStateForTests();
      _resetLlmRunnerForTests();
      _clearOverrideCacheForTests();
      process.env.PUBLISHER_RETRIEVAL_OVERRIDES = "1";
      configure({});
   });
   afterEach(() => {
      _clearRetrievalConfigForTests();
      if (savedGate === undefined)
         delete process.env.PUBLISHER_RETRIEVAL_OVERRIDES;
      else process.env.PUBLISHER_RETRIEVAL_OVERRIDES = savedGate;
   });

   function configure(
      rerank: Record<string, unknown>,
      extra: Record<string, unknown> = {},
   ) {
      _setRetrievalConfigForTests({
         rerank: { enabled: true, ...rerank },
         llm: { model: "m", backoffMs: 0, cache: { enabled: false } },
         ...extra,
      });
   }
   const run = () => afterWarmup(captureHandler(storeFor(pkg, db)), params);

   it("orders the cards by the level the model gave, not by similarity", async () => {
      _setLlmProviderForTests(
         fakeLlm((req) =>
            rankReply([
               [sourceIndex(req, "shipments"), 3],
               [sourceIndex(req, "customers"), 2],
               [sourceIndex(req, "orders"), 2],
            ]),
         ),
      );
      const payload = await run();
      // orders is the best similarity match but the model ranked it below.
      expect(sourceNames(payload)).toEqual([
         "shipments",
         "customers",
         "orders",
      ]);
      expect(payload.retrieval_stages).toEqual({ rerank: "ok" });
   });

   it("breaks ties inside a level by the order the model listed them in", async () => {
      _setLlmProviderForTests(
         fakeLlm((req) =>
            rankReply([
               [sourceIndex(req, "customers"), 2],
               [sourceIndex(req, "orders"), 2],
               [sourceIndex(req, "shipments"), 2],
            ]),
         ),
      );
      const payload = await run();
      expect(sourceNames(payload)).toEqual([
         "customers",
         "orders",
         "shipments",
      ]);
   });

   it("trusts the score over the listed order when the model sorts them badly", async () => {
      _setLlmProviderForTests(
         fakeLlm((req) =>
            // Listed worst first, scores say otherwise.
            rankReply([
               [sourceIndex(req, "orders"), 2],
               [sourceIndex(req, "customers"), 3],
               [sourceIndex(req, "shipments"), 2],
            ]),
         ),
      );
      const payload = await run();
      expect(sourceNames(payload)).toEqual([
         "customers",
         "orders",
         "shipments",
      ]);
   });

   it("publishes card relevance that never contradicts the order", async () => {
      _setLlmProviderForTests(
         fakeLlm((req) =>
            rankReply([
               [sourceIndex(req, "shipments"), 3],
               [sourceIndex(req, "customers"), 2],
               [sourceIndex(req, "orders"), 2],
            ]),
         ),
      );
      const payload = await run();
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const rel = payload.sources.map((c: any) => c.relevance);
      expect([...rel].sort((a, b) => b - a)).toEqual(rel);
      expect(rel[0]).toBeGreaterThan(rel[1]);
   });

   it("leaves each entity's own relevance alone", async () => {
      const base = await (async () => {
         _setLlmProviderForTests(fakeLlm(() => rankReply([])));
         configure({ enabled: false });
         return run();
      })();
      configure({});
      _setLlmProviderForTests(
         fakeLlm((req) =>
            rankReply([
               [sourceIndex(req, "shipments"), 3],
               [sourceIndex(req, "customers"), 2],
               [sourceIndex(req, "orders"), 2],
            ]),
         ),
      );
      const payload = await run();
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const rel = (p: any, name: string) =>
         p.sources
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            .flatMap((c: any) => c.entities ?? [])
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            .find((e: any) => e.name === name).relevance;
      for (const name of [
         "total_revenue",
         "customer_revenue",
         "freight_revenue",
      ]) {
         expect(rel(payload, name)).toBe(rel(base, name));
      }
   });

   it("drops a card the model scored below minScore, and says why in the trace", async () => {
      _setLlmProviderForTests(
         fakeLlm((req) =>
            rankReply([
               [sourceIndex(req, "customers"), 3],
               [sourceIndex(req, "shipments"), 1],
               // orders unmentioned: the model found nothing in it
            ]),
         ),
      );
      const handler = captureHandler(storeFor(pkg, db));
      const payload = await afterWarmup(handler, params, {
         requestInfo: { headers: { "x-publisher-retrieval-trace": "summary" } },
      });
      expect(sourceNames(payload)).toEqual(["customers"]);
      const gate = payload.retrieval_trace.gates.find(
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         (g: any) => g.gate === "rerank",
      );
      expect(gate.in).toBe(3);
      expect(gate.out).toBe(1);
      expect(gate.dropped_by_reason).toEqual({ rerank_below_min: 2 });
      expect(gate.llm_calls).toBe(1);
   });

   it("keeps minScore 0 cards when told to", async () => {
      configure({ minScore: 0 });
      _setLlmProviderForTests(
         fakeLlm((req) => rankReply([[sourceIndex(req, "customers"), 3]])),
      );
      const payload = await run();
      expect(sourceNames(payload)).toEqual([
         "customers",
         "orders",
         "shipments",
      ]);
   });

   describe("sources past topSources", () => {
      it("keeps them below the reranked ones, at a lower relevance", async () => {
         configure({ topSources: 2, minScore: 0 });
         const seen: LlmRequest[] = [];
         _setLlmProviderForTests(
            fakeLlm(
               (req) =>
                  rankReply([
                     [sourceIndex(req, "customers"), 3],
                     [sourceIndex(req, "orders"), 2],
                  ]),
               seen,
            ),
         );
         const payload = await run();
         // shipments was third by similarity, so the model never saw it.
         expect(seen[0].user).not.toContain("Source: shipments");
         expect(sourceNames(payload)).toEqual([
            "customers",
            "orders",
            "shipments",
         ]);
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         const rel = payload.sources.map((c: any) => c.relevance);
         expect([...rel].sort((a, b) => b - a)).toEqual(rel);
      });

      it("drops them when beyondTop is drop, as the service does", async () => {
         configure({ topSources: 2, minScore: 0, beyondTop: "drop" });
         _setLlmProviderForTests(
            fakeLlm((req) =>
               rankReply([
                  [sourceIndex(req, "customers"), 3],
                  [sourceIndex(req, "orders"), 2],
               ]),
            ),
         );
         const payload = await run();
         expect(sourceNames(payload)).toEqual(["customers", "orders"]);
      });
   });

   describe("what the model is shown", () => {
      it("gets the whole question, each source's doc and its best fields", async () => {
         const seen: LlmRequest[] = [];
         _setLlmProviderForTests(fakeLlm(() => rankReply([]), seen));
         await run().catch(() => undefined);
         const req = seen[0];
         expect(req.system).toContain(
            "expert at matching natural language queries to data sources",
         );
         expect(req.user).toContain("## Natural Language Query\n\nrevenue");
         expect(req.user).toContain(
            "Source: orders, Model: m.malloy, Package: p",
         );
         expect(req.user).toContain("Description: Every order placed.");
         expect(req.user).toContain(
            "- total_revenue (measure): Revenue from orders.",
         );
      });

      it("sends no docs or descriptions when the docs class is off", async () => {
         configure({}, { egress: { docs: false } });
         const seen: LlmRequest[] = [];
         _setLlmProviderForTests(fakeLlm(() => rankReply([]), seen));
         await run().catch(() => undefined);
         expect(seen[0].user).not.toContain("Every order placed");
         expect(seen[0].user).not.toContain("Revenue from orders");
         expect(seen[0].user).toContain("- total_revenue (measure)");
      });
   });

   describe("failing soft", () => {
      it("keeps the earlier order with a warning when the call fails", async () => {
         _setLlmProviderForTests(
            fakeLlm(() => new LlmError("down", "http", false, 500)),
         );
         configure(
            {},
            { llm: { model: "m", maxAttempts: 1, cache: { enabled: false } } },
         );
         const payload = await run();
         expect(sourceNames(payload)).toEqual([
            "orders",
            "customers",
            "shipments",
         ]);
         expect(payload.retrieval_stages).toEqual({ rerank: "failed:http" });
         expect(payload.warnings.join(" ")).toContain(
            "LLM rerank unavailable (http)",
         );
      });

      it("retries once on a garbled reply", async () => {
         const seen: LlmRequest[] = [];
         _setLlmProviderForTests(
            fakeLlm(
               (req, n) =>
                  n === 1
                     ? "I would rank customers first."
                     : rankReply([[sourceIndex(req, "customers"), 3]]),
               seen,
            ),
         );
         configure({ minScore: 0 });
         const payload = await run();
         expect(seen).toHaveLength(2);
         expect(sourceNames(payload)[0]).toBe("customers");
      });

      it("skips when there is only one card to order", async () => {
         const seen: LlmRequest[] = [];
         _setLlmProviderForTests(fakeLlm(() => rankReply([]), seen));
         const one = {
            search_targets: [
               { target_type: "measure", search_text: "revenue" },
            ],
            scopes: [{ environment: "rerank", package: "p", source: "orders" }],
         };
         const payload = await afterWarmup(
            captureHandler(storeFor(pkg, db)),
            one,
         );
         expect(seen).toHaveLength(0);
         expect(payload.retrieval_stages).toEqual({
            rerank: "skipped:few_candidates",
         });
      });

      it("skips with no LLM configured", async () => {
         const payload = await run();
         expect(payload.retrieval_stages).toEqual({ rerank: "skipped:no_llm" });
         expect(sourceNames(payload)).toEqual([
            "orders",
            "customers",
            "shipments",
         ]);
      });
   });

   describe("with refine", () => {
      it("reranks the cards refine left, and publishes refine's entity scores", async () => {
         _setRetrievalConfigForTests({
            refine: { enabled: true },
            rerank: { enabled: true, minScore: 0 },
            llm: { model: "m", backoffMs: 0, cache: { enabled: false } },
         });
         _setLlmProviderForTests(
            fakeLlm((req) => {
               if (req.stage === "refine") {
                  const idx = [...req.user.matchAll(/- \[(\d+)\]/g)].map((m) =>
                     Number(m[1]),
                  );
                  return JSON.stringify(
                     idx.map((index) => ({
                        index,
                        score: "MEDIUM",
                        reason: "r",
                     })),
                  );
               }
               return rankReply([
                  [sourceIndex(req, "shipments"), 3],
                  [sourceIndex(req, "orders"), 2],
                  [sourceIndex(req, "customers"), 1],
               ]);
            }),
         );
         const payload = await run();
         expect(sourceNames(payload)).toEqual([
            "shipments",
            "orders",
            "customers",
         ]);
         expect(payload.retrieval_stages).toEqual({
            refine: "ok",
            rerank: "ok",
         });
         expect(entityNames(payload)).toHaveLength(3);
      });
   });

   describe("coverage scoring", () => {
      const twoPhrases = {
         search_targets: [
            { target_type: "measure", search_text: "revenue" },
            { target_type: "source", search_text: "revenue by customer" },
         ],
         scopes: [{ environment: "rerank", package: "p" }],
      };

      it("is off by default, so a card ranks on its best hit", async () => {
         configure({ enabled: false });
         const payload = await afterWarmup(
            captureHandler(storeFor(pkg, db)),
            twoPhrases,
         );
         expect(sourceNames(payload)[0]).toBe("orders");
      });

      it("orders cards on how many phrases they answer when switched on", async () => {
         configure(
            { enabled: false },
            { scoring: { sourceRelevance: "coverage" } },
         );
         const payload = await afterWarmup(
            captureHandler(storeFor(pkg, db)),
            twoPhrases,
         );
         // The published relevances must agree with the order.
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         const rel = payload.sources.map((c: any) => c.relevance);
         expect([...rel].sort((a, b) => b - a)).toEqual(rel);
         expect(payload.retrieval_config).toMatch(/^[0-9a-f]{12}$/);
      });
   });
});
