// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Enrichment through the handler: a field whose name and (missing) doc give the
// embedding nothing to match becomes findable once the LLM has written a
// keyphrase for it, without the request path ever waiting on the LLM.

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
import {
   _resetEnrichmentStateForTests,
   _settleEnrichmentForTests,
} from "../../retrieval/enrichment";
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
   _clearLlmProviderForTests,
   _setLlmProviderForTests,
   type LlmProvider,
   type LlmRequest,
} from "../../service/llm_provider";
import { _resetLlmRunnerForTests } from "../../service/llm_runner";
import { DuckDBConnection } from "../../storage/duckdb/DuckDBConnection";
import {
   createEntityEmbeddingsTable,
   createEntityEnrichmentTable,
} from "../../storage/duckdb/schema";
import { _resetEmbeddingIndexStateForTests } from "./embedding_index";
import { getPackageEmbeddingStatus } from "./get_context_tool";
import {
   captureHandler,
   embeddingsFor,
   entityNames,
   fakeLlm,
   parse,
   STAGES_OFF,
   storeFor,
} from "./retrieval_test_kit";

const ENV = "enrich";
const PKG = "p";
const QUERY = "how much do customers spend over their lifetime";
const KEYPHRASE = "Total amount a customer spends over time.";

const model = {
   getSourceInfos: () => [
      {
         name: "orders",
         annotations: ["#(doc) Every order placed."],
         schema: {
            fields: [
               // Cryptic name, no doc: nothing for an embedding to match.
               { kind: "dimension", name: "cust_ltv", annotations: [] },
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

const VECTORS: Record<string, number[]> = {
   [QUERY]: [1, 0],
   orders: [0, 1],
   "orders: Every order placed.": [0, 1],
   "total revenue": [0, 1],
   "total revenue: Revenue.": [0, 1],
   // The name alone says nothing about lifetime spend.
   "cust ltv": [0, 1],
   // What the LLM wrote lands on the query.
   [`cust ltv: ${KEYPHRASE}`]: [1, 0],
   // The source summary is embedded too.
   "Orders. Every order placed, one row each.": [0, 1],
};

const params = {
   search_targets: [{ target_type: "dimension", search_text: QUERY }],
   scopes: [{ environment: ENV, package: PKG }],
};

const llmReply = (req: LlmRequest): string =>
   req.stage === "summary"
      ? JSON.stringify({
           summary: "Every order placed, one row each.",
           one_line_summary: "Orders.",
        })
      : JSON.stringify({ keyphrase: KEYPHRASE });

describe("get_context enrichment", () => {
   let tempDir: string;
   let db: DuckDBConnection;
   const savedGate = process.env.PUBLISHER_RETRIEVAL_OVERRIDES;

   beforeAll(async () => {
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "get-context-enrich-"));
      db = new DuckDBConnection(path.join(tempDir, "test.db"));
      await db.initialize();
      await createEntityEmbeddingsTable(db);
      await createEntityEnrichmentTable(db);
   });
   afterAll(async () => {
      await db.close();
      fs.rmSync(tempDir, { recursive: true, force: true });
      _clearEmbeddingProviderForTests();
      _clearLlmProviderForTests();
   });
   beforeEach(async () => {
      await db.run("DELETE FROM entity_embeddings");
      await db.run("DELETE FROM entity_enrichment");
      _setEmbeddingProviderForTests(embeddingsFor(VECTORS));
      _setLlmProviderForTests(fakeLlm(llmReply));
      _resetEmbeddingIndexStateForTests();
      _resetEnrichmentStateForTests();
      _resetLlmRunnerForTests();
      _clearOverrideCacheForTests();
      process.env.PUBLISHER_RETRIEVAL_OVERRIDES = "1";
      _setRetrievalConfigForTests({
         enrichment: {
            enabled: true,
            sourceSummary: { enabled: true },
            keyphrase: { batchSize: 1 },
         },
         llm: { model: "m", backoffMs: 0, cache: { enabled: false } },
      });
   });
   afterEach(() => {
      _clearRetrievalConfigForTests();
      if (savedGate === undefined)
         delete process.env.PUBLISHER_RETRIEVAL_OVERRIDES;
      else process.env.PUBLISHER_RETRIEVAL_OVERRIDES = savedGate;
   });

   const handler = () => captureHandler(storeFor(pkg, db));

   async function untilSemantic(
      h: ReturnType<typeof handler>,
      extra = STAGES_OFF,
   ) {
      for (let i = 0; i < 400; i++) {
         const payload = parse(await h(params, extra));
         if (payload.retrieval === "semantic") return payload;
         await new Promise((r) => setTimeout(r, 5));
      }
      throw new Error("never became semantic");
   }

   it("finds a cryptic, undocumented field once the model has described it", async () => {
      const h = handler();
      const before = await untilSemantic(h);
      expect(entityNames(before)).not.toContain("cust_ltv");
      expect(before.below_cutoff_count).toBe(before.total_entities);

      await _settleEnrichmentForTests(ENV, PKG);

      const after = await untilSemantic(h);
      expect(entityNames(after)).toEqual(["cust_ltv"]);
      const hit = after.sources[0].entities[0];
      expect(hit.relevance).toBeGreaterThan(0.9);
      // The generated text is what matched, not something the response shows.
      expect(JSON.stringify(after)).not.toContain(KEYPHRASE);
      // Enrichment adds facets, never entities.
      expect(after.total_entities).toBe(before.total_entities);
   });

   it("keeps answering semantically while the model is still thinking", async () => {
      let release!: () => void;
      const gate = new Promise<void>((r) => (release = r));
      const slow: LlmProvider = {
         id: "slow",
         async complete(req) {
            await gate;
            return { text: llmReply(req), model: req.model, latencyMs: 1 };
         },
      };
      _setLlmProviderForTests(slow);
      const h = handler();
      await untilSemantic(h); // kicks the enrichment, which now blocks on the model
      // Ten questions while the LLM has not answered: none may fall back to
      // lexical, because the enrichment holds no lock the search needs.
      for (let i = 0; i < 10; i++) {
         const p = parse(await h(params, STAGES_OFF));
         expect(p.retrieval).toBe("semantic");
      }
      release();
      await _settleEnrichmentForTests(ENV, PKG);
      expect(entityNames(await untilSemantic(h))).toEqual(["cust_ltv"]);
   });

   it("reports its progress in the package's index status", async () => {
      const h = handler();
      await untilSemantic(h);
      await _settleEnrichmentForTests(ENV, PKG);
      const status = await getPackageEmbeddingStatus(
         storeFor(pkg, db) as never,
         ENV,
         PKG,
      );
      expect(status?.status).toBe("ready");
      expect(status?.enrichment).toMatchObject({
         status: "ready",
         eligible: 2,
         enriched: 2,
         failed: 0,
         deferredByBudget: 0,
      });
   });

   it("leaves the status without an enrichment block when it is switched off", async () => {
      _setRetrievalConfigForTests({});
      const h = handler();
      await untilSemantic(h);
      const status = await getPackageEmbeddingStatus(
         storeFor(pkg, db) as never,
         ENV,
         PKG,
      );
      expect(status?.status).toBe("ready");
      expect(status).not.toHaveProperty("enrichment");
   });

   it("lets a query ablate the generated facets without rebuilding anything", async () => {
      const h = handler();
      await untilSemantic(h);
      await _settleEnrichmentForTests(ENV, PKG);
      const only = (facets: string[]) => ({
         requestInfo: {
            headers: {
               "x-publisher-retrieval": JSON.stringify({
                  refine: { enabled: false },
                  rerank: { enabled: false },
                  embedding: { facets },
               }),
            },
         },
      });
      const withoutKw = parse(await h(params, only(["name", "doc", "sum"])));
      expect(entityNames(withoutKw)).not.toContain("cust_ltv");
      const withKw = parse(await h(params, only(["kw"])));
      expect(entityNames(withKw)).toEqual(["cust_ltv"]);
   });

   describe("showing what the model wrote", () => {
      const enabled = {
         enrichment: {
            enabled: true,
            sourceSummary: { enabled: true },
            keyphrase: { batchSize: 1 },
         },
         llm: { model: "m", backoffMs: 0, cache: { enabled: false } },
      };

      it("keeps generated text out of the response by default", async () => {
         const h = handler();
         await untilSemantic(h);
         await _settleEnrichmentForTests(ENV, PKG);
         const after = await untilSemantic(h);
         expect(entityNames(after)).toEqual(["cust_ltv"]);
         expect(JSON.stringify(after)).not.toContain("generated_");
      });

      it("returns it apart from the authored docs when the operator asks", async () => {
         _setRetrievalConfigForTests({
            ...enabled,
            response: { surfaceGenerated: true },
         });
         const h = handler();
         await untilSemantic(h);
         await _settleEnrichmentForTests(ENV, PKG);
         const after = await untilSemantic(h);
         const card = after.sources[0];
         expect(card.source_info.generated_summary).toBe(
            "Every order placed, one row each.",
         );
         expect(card.source_info.generated_one_line_summary).toBe("Orders.");
         // The author's own doc is untouched beside it.
         expect(card.source_info.docs).toBe("Every order placed.");
         expect(card.entities[0].generated_description).toBe(KEYPHRASE);
         expect(card.entities[0]).not.toHaveProperty("description");
      });

      it("shows nothing before there is anything to show", async () => {
         _setRetrievalConfigForTests({
            ...enabled,
            response: { surfaceGenerated: true },
         });
         const before = await untilSemantic(handler());
         expect(JSON.stringify(before)).not.toContain("generated_");
      });
   });

   describe("with refine", () => {
      it("describes an undocumented field to the model by what was written for it", async () => {
         _setRetrievalConfigForTests({
            enrichment: {
               enabled: true,
               sourceSummary: { enabled: true },
               keyphrase: { batchSize: 1 },
            },
            refine: { enabled: true },
            llm: { model: "m", backoffMs: 0, cache: { enabled: false } },
         });
         const seen: LlmRequest[] = [];
         _setLlmProviderForTests(
            fakeLlm(
               (req) => (req.stage === "refine" ? "[]" : llmReply(req)),
               seen,
            ),
         );
         const h = handler();
         await untilSemantic(h);
         await _settleEnrichmentForTests(ENV, PKG);
         await untilSemantic(h); // now enriched
         seen.length = 0;
         await h(params); // refine runs on this one
         const refine = seen.find((r) => r.stage === "refine")!;
         expect(refine.user).toContain(
            `- [0] cust_ltv (dimension, source: orders): ${KEYPHRASE}`,
         );
      });
   });

   it("does nothing, and asks nothing, when enrichment is off", async () => {
      _setRetrievalConfigForTests({});
      const seen: LlmRequest[] = [];
      _setLlmProviderForTests(fakeLlm(llmReply, seen));
      const h = handler();
      const p = await untilSemantic(h);
      await new Promise((r) => setTimeout(r, 30));
      expect(seen).toHaveLength(0);
      expect(entityNames(p)).not.toContain("cust_ltv");
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const rows = await db.all<any>(
         "SELECT count(*) AS n FROM entity_enrichment",
      );
      expect(Number(rows[0].n)).toBe(0);
   });
});
