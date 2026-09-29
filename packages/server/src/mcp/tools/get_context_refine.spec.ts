// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Entity refine end to end through the get_context handler: a scripted fake
// LLM answers with canned ratings, and the response shows what the pipeline
// did with them. The prompts themselves are checked by what the fake sees.

import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { DEFAULT_EMBEDDING_MIN_SIMILARITY } from "../../config";
import { _clearOverrideCacheForTests } from "../../retrieval/run";
import {
   _clearRetrievalConfigForTests,
   _setRetrievalConfigForTests,
} from "../../retrieval/retrieval_config";
import type { EnvironmentStore } from "../../service/environment_store";
import {
   EmbeddingProvider,
   _clearEmbeddingProviderForTests,
   _setEmbeddingProviderForTests,
} from "../../service/embedding_provider";
import {
   LlmError,
   _clearLlmProviderForTests,
   _setLlmProviderForTests,
   type LlmProvider,
   type LlmRequest,
} from "../../service/llm_provider";
import { _resetLlmRunnerForTests } from "../../service/llm_runner";
import { DuckDBConnection } from "../../storage/duckdb/DuckDBConnection";
import { createEntityEmbeddingsTable } from "../../storage/duckdb/schema";
import { _resetEmbeddingIndexStateForTests } from "./embedding_index";
import { registerGetContextTool } from "./get_context_tool";

type Content = Array<{ type?: string; resource?: { text: string } }>;
type Extra = { requestInfo?: { headers?: Record<string, string> } };
type Result = { isError?: boolean; content: Content };
type Handler = (p: Record<string, unknown>, extra?: Extra) => Promise<Result>;

function captureHandler(store: Partial<EnvironmentStore>): Handler {
   const handlers = new Map<string, Handler>();
   registerGetContextTool(
      {
         tool: (n: string, _d: string, _s: unknown, h: Handler) => handlers.set(n, h),
      } as never,
      store as EnvironmentStore,
   );
   return handlers.get("get_context")!;
}
const parse = (r: Result) => JSON.parse(r.content[0].resource!.text);

// One source, five measures with known cosines to the query "revenue".
const MEASURES = [
   { name: "total_revenue", cos: 1, doc: "Sum of all order revenue." },
   { name: "net_revenue", cos: 0.9, doc: "Revenue after refunds." },
   { name: "gross_revenue", cos: 0.6, doc: "Revenue before discounts." },
   { name: "avg_discount", cos: 0.4, doc: "Average discount given." },
   { name: "order_count", cos: 0.3, doc: "Number of orders." },
];
const humanize = (n: string) => n.replace(/_/g, " ");
const vec = (c: number) => [c, Math.sqrt(1 - c * c)];
const model = {
   getSourceInfos: () => [
      {
         name: "orders",
         annotations: ["#(doc) One row per order."],
         schema: {
            fields: MEASURES.map((m) => ({
               kind: "measure",
               name: m.name,
               annotations: [`#(doc) ${m.doc}`, "#(access_filter) \"$TENANT = 'acme'\""],
            })),
         },
      },
   ],
   getQueries: () => [],
};
const pkg = { listModels: async () => [{ path: "m.malloy" }], getModel: () => model };

const VECTORS: Record<string, number[]> = {
   orders: [0, 1],
   "orders: One row per order.": [0, 1],
   revenue: [1, 0],
};
for (const m of MEASURES) {
   VECTORS[humanize(m.name)] = vec(m.cos);
   VECTORS[`${humanize(m.name)}: ${m.doc}`] = vec(m.cos);
}

function embeddings(): EmbeddingProvider {
   const fetchStub = (async (_u: RequestInfo | URL, init?: RequestInit) => {
      const body = JSON.parse(String(init?.body)) as { input: string[] };
      return new Response(
         JSON.stringify({
            data: body.input.map((t, index) => {
               const embedding = VECTORS[t];
               if (!embedding) throw new Error(`no stub vector for "${t}"`);
               return { index, embedding };
            }),
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

/** A fake LLM: `reply` sees each request and answers, or throws. */
function fakeLlm(
   reply: (req: LlmRequest, n: number) => string | LlmError,
   seen: LlmRequest[] = [],
): LlmProvider {
   return {
      id: "fake-llm",
      async complete(req) {
         seen.push(req);
         const out = reply(req, seen.length);
         if (out instanceof LlmError) throw out;
         return { text: out, model: req.model, latencyMs: 1 };
      },
   };
}

/** Indices of the candidate lines in a refine prompt, by measure name. */
function indexOfName(req: LlmRequest, name: string): number {
   const m = req.user.match(new RegExp(`- \\[(\\d+)\\] ${name} \\(`));
   if (!m) throw new Error(`${name} not in prompt`);
   return Number(m[1]);
}

const rate = (items: Array<[number, string, string?]>) =>
   JSON.stringify(
      items.map(([index, score, reason]) => ({ index, score, reason: reason ?? "r" })),
   );

const params = {
   search_targets: [{ target_type: "measure", search_text: "revenue" }],
   scopes: [{ environment: "refine", package: "p" }],
};
const names = (payload: any): string[] =>
   payload.sources.flatMap((c: any) => (c.entities ?? []).map((e: any) => e.name));

describe("get_context entity refine", () => {
   let tempDir: string;
   let db: DuckDBConnection;
   const savedGate = process.env.PUBLISHER_RETRIEVAL_OVERRIDES;

   beforeAll(async () => {
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "get-context-refine-"));
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
      _setEmbeddingProviderForTests(null);
      _setLlmProviderForTests(null);
      _resetEmbeddingIndexStateForTests();
      _resetLlmRunnerForTests();
      _clearOverrideCacheForTests();
      process.env.PUBLISHER_RETRIEVAL_OVERRIDES = "1";
      _setRetrievalConfigForTests({
         refine: { enabled: true },
         llm: { model: "test-model", backoffMs: 0, cache: { enabled: false } },
      });
   });
   afterEach(() => {
      _clearRetrievalConfigForTests();
      if (savedGate === undefined) delete process.env.PUBLISHER_RETRIEVAL_OVERRIDES;
      else process.env.PUBLISHER_RETRIEVAL_OVERRIDES = savedGate;
   });

   const store = (): Partial<EnvironmentStore> => ({
      getEnvironment: async () =>
         ({ getPackage: async () => pkg, getStaleCompileErrors: () => new Map() }) as never,
      storageManager: { getDuckDbConnection: () => db } as never,
   });

   /**
    * Wait for the embedding index with the LLM stages off, then make the one
    * call under test. Polling with refine on would spend LLM calls (and trip
    * the breaker) on the lexical answers given while the index builds.
    */
   async function semantic(handler: Handler, extra?: Extra) {
      const off: Extra = {
         requestInfo: {
            headers: {
               "x-publisher-retrieval": JSON.stringify({
                  refine: { enabled: false },
                  rerank: { enabled: false },
               }),
            },
         },
      };
      for (let i = 0; i < 400; i++) {
         const warm = parse(await handler(params, off));
         if (warm.retrieval === "semantic") return parse(await handler(params, extra));
         await new Promise((r) => setTimeout(r, 5));
      }
      throw new Error("never became semantic");
   }

   describe("on the semantic path", () => {
      it("prunes below MEDIUM, drops what the model omits, and explains the rest", async () => {
         _setEmbeddingProviderForTests(embeddings());
         _setLlmProviderForTests(
            fakeLlm((req) =>
               rate([
                  [indexOfName(req, "total_revenue"), "HIGH", "Directly measures revenue."],
                  [indexOfName(req, "net_revenue"), "MEDIUM", "Revenue net of refunds."],
                  [indexOfName(req, "avg_discount"), "LOW", "Loosely related."],
                  // gross_revenue and order_count are omitted
               ]),
            ),
         );
         const handler = captureHandler(store());
         const payload = await semantic(handler);
         expect(names(payload)).toEqual(["total_revenue", "net_revenue"]);
         const [first, second] = payload.sources[0].entities;
         expect(first.matched_targets[0].match_reason).toBe("Directly measures revenue.");
         expect(second.matched_targets[0].match_reason).toBe("Revenue net of refunds.");
         // HIGH outranks MEDIUM whatever the cosine.
         expect(first.relevance).toBeGreaterThan(second.relevance);
         expect(payload.retrieval_stages).toEqual({ refine: "ok" });
         expect(payload.retrieval_config).toMatch(/^[0-9a-f]{12}$/);
      });

      it("lets the level beat the cosine", async () => {
         _setEmbeddingProviderForTests(embeddings());
         // The model likes net_revenue (cos .9) HIGH and total_revenue (cos 1) MEDIUM.
         _setLlmProviderForTests(
            fakeLlm((req) =>
               rate([
                  [indexOfName(req, "total_revenue"), "MEDIUM"],
                  [indexOfName(req, "net_revenue"), "HIGH"],
               ]),
            ),
         );
         const payload = await semantic(captureHandler(store()));
         expect(names(payload)).toEqual(["net_revenue", "total_revenue"]);
      });

      it("keeps LOW when the prune level is LOW", async () => {
         _setRetrievalConfigForTests({
            refine: { enabled: true, minLevel: "LOW" },
            llm: { model: "m", cache: { enabled: false } },
         });
         _setEmbeddingProviderForTests(embeddings());
         _setLlmProviderForTests(
            fakeLlm((req) =>
               rate([
                  [indexOfName(req, "total_revenue"), "HIGH"],
                  [indexOfName(req, "avg_discount"), "LOW"],
               ]),
            ),
         );
         const payload = await semantic(captureHandler(store()));
         expect(names(payload).sort()).toEqual(["avg_discount", "total_revenue"]);
      });

      it("keeps an omitted candidate when dropOmitted is off", async () => {
         _setRetrievalConfigForTests({
            refine: { enabled: true, dropOmitted: false, minLevel: "LOW" },
            llm: { model: "m", cache: { enabled: false } },
         });
         _setEmbeddingProviderForTests(embeddings());
         _setLlmProviderForTests(
            fakeLlm((req) => rate([[indexOfName(req, "total_revenue"), "HIGH"]])),
         );
         const payload = await semantic(captureHandler(store()));
         expect(names(payload)).toHaveLength(5);
         expect(names(payload)[0]).toBe("total_revenue");
      });

      it("shows the model the phrase, the query, and the doc but not the predicate", async () => {
         _setEmbeddingProviderForTests(embeddings());
         const seen: LlmRequest[] = [];
         _setLlmProviderForTests(fakeLlm(() => "[]", seen));
         await semantic(captureHandler(store()));
         const req = seen[0];
         expect(req.model).toBe("test-model");
         expect(req.system).toContain("expert at evaluating how well database entities");
         expect(req.user).toContain('PHRASE:\nText: "revenue"');
         expect(req.user).toContain("QUERY:\nrevenue");
         expect(req.user).toContain(
            "- [0] total_revenue (measure, source: orders): Sum of all order revenue.",
         );
         expect(req.temperature).toBe(0);
         // The access_filter annotation must never reach the model.
         expect(req.user).not.toContain("acme");
         expect(req.user).not.toContain("TENANT");
      });

      it("sends no descriptions when the docs class is switched off", async () => {
         _setRetrievalConfigForTests({
            refine: { enabled: true },
            llm: { model: "m", cache: { enabled: false } },
            egress: { docs: false },
         });
         _setEmbeddingProviderForTests(embeddings());
         const seen: LlmRequest[] = [];
         _setLlmProviderForTests(fakeLlm(() => "[]", seen));
         await semantic(captureHandler(store()));
         expect(seen[0].user).toContain("- [0] total_revenue (measure, source: orders): \n");
         expect(seen[0].user).not.toContain("Sum of all order revenue");
      });

      it("batches candidates and rates every batch", async () => {
         _setRetrievalConfigForTests({
            refine: { enabled: true, batchSize: 2, minLevel: "LOW" },
            llm: { model: "m", cache: { enabled: false } },
         });
         _setEmbeddingProviderForTests(embeddings());
         const seen: LlmRequest[] = [];
         _setLlmProviderForTests(
            fakeLlm((req) => {
               const idx = [...req.user.matchAll(/- \[(\d+)\]/g)].map((m) => Number(m[1]));
               return rate(idx.map((i) => [i, "MEDIUM"] as [number, string]));
            }, seen),
         );
         const payload = await semantic(captureHandler(store()));
         expect(seen).toHaveLength(3); // 5 candidates in batches of 2
         expect(names(payload)).toHaveLength(5);
      });

      it("caps candidates per source before spending a call on them", async () => {
         _setRetrievalConfigForTests({
            refine: { enabled: true, maxPerSource: 2 },
            llm: { model: "m", cache: { enabled: false } },
         });
         _setEmbeddingProviderForTests(embeddings());
         const seen: LlmRequest[] = [];
         _setLlmProviderForTests(
            fakeLlm((req) => {
               const idx = [...req.user.matchAll(/- \[(\d+)\]/g)].map((m) => Number(m[1]));
               return rate(idx.map((i) => [i, "HIGH"] as [number, string]));
            }, seen),
         );
         const payload = await semantic(captureHandler(store()));
         expect(seen[0].user.match(/- \[\d+\]/g)).toHaveLength(2);
         expect(names(payload)).toEqual(["total_revenue", "net_revenue"]);
      });

      it("skips when there are few enough candidates that nothing needs pruning", async () => {
         _setRetrievalConfigForTests({
            refine: { enabled: true, skipIfAtMost: 5 },
            llm: { model: "m", cache: { enabled: false } },
         });
         _setEmbeddingProviderForTests(embeddings());
         const seen: LlmRequest[] = [];
         _setLlmProviderForTests(fakeLlm(() => "[]", seen));
         const payload = await semantic(captureHandler(store()));
         expect(seen).toHaveLength(0);
         expect(payload.retrieval_stages).toEqual({ refine: "skipped:few_candidates" });
         expect(names(payload)).toHaveLength(5);
      });

      it("retries a garbled reply once before giving up on the batch", async () => {
         _setEmbeddingProviderForTests(embeddings());
         const seen: LlmRequest[] = [];
         _setLlmProviderForTests(
            fakeLlm((req, n) =>
               n === 1
                  ? "Sure, here are my thoughts on these measures."
                  : rate([[indexOfName(req, "total_revenue"), "HIGH"]]),
            seen),
         );
         const payload = await semantic(captureHandler(store()));
         expect(seen).toHaveLength(2);
         expect(seen[1].user).toContain("could not be used");
         expect(names(payload)).toEqual(["total_revenue"]);
         expect(payload.retrieval_stages.refine).toBe("ok");
      });

      it("understands a fenced reply, a wrapped array and a trailing comma", async () => {
         _setEmbeddingProviderForTests(embeddings());
         _setLlmProviderForTests(
            fakeLlm(
               (req) =>
                  "```json\n" +
                  `{"results": [{"index": ${indexOfName(req, "total_revenue")}, "score": "high", "reason": "ok",},]}` +
                  "\n```",
            ),
         );
         const payload = await semantic(captureHandler(store()));
         expect(names(payload)).toEqual(["total_revenue"]);
      });

      it("serves a repeat call from the cache", async () => {
         _setRetrievalConfigForTests({
            refine: { enabled: true },
            llm: { model: "m", cache: { enabled: true } },
         });
         _setEmbeddingProviderForTests(embeddings());
         const seen: LlmRequest[] = [];
         _setLlmProviderForTests(
            fakeLlm((req) => rate([[indexOfName(req, "total_revenue"), "HIGH"]]), seen),
         );
         const handler = captureHandler(store());
         await semantic(handler);
         const calls = seen.length;
         const again = parse(await handler(params));
         expect(seen.length).toBe(calls);
         expect(names(again)).toEqual(["total_revenue"]);
      });

      it("hides match_reason when the config says to", async () => {
         _setRetrievalConfigForTests({
            refine: { enabled: true },
            llm: { model: "m", cache: { enabled: false } },
            response: { matchReason: false },
         });
         _setEmbeddingProviderForTests(embeddings());
         _setLlmProviderForTests(
            fakeLlm((req) => rate([[indexOfName(req, "total_revenue"), "HIGH", "why"]])),
         );
         const payload = await semantic(captureHandler(store()));
         expect(payload.sources[0].entities[0].matched_targets[0]).not.toHaveProperty(
            "match_reason",
         );
      });
   });

   describe("failing soft", () => {
      it("leaves the ranking untouched, with a warning, when every call fails", async () => {
         _setEmbeddingProviderForTests(embeddings());
         const before = await (async () => {
            _setRetrievalConfigForTests({});
            return semantic(captureHandler(store()));
         })();
         _setRetrievalConfigForTests({
            refine: { enabled: true },
            llm: { model: "m", maxAttempts: 1, cache: { enabled: false } },
         });
         _setLlmProviderForTests(fakeLlm(() => new LlmError("down", "http", false, 500)));
         const payload = await semantic(captureHandler(store()));
         expect(names(payload)).toEqual(names(before));
         expect(payload.retrieval_stages).toEqual({ refine: "failed:http" });
         expect(payload.warnings.join(" ")).toContain("LLM refine unavailable (http)");
         // Cosine relevances are untouched: no half-applied level scores.
         expect(payload.sources[0].entities.map((e: any) => e.relevance)).toEqual(
            before.sources[0].entities.map((e: any) => e.relevance),
         );
      });

      it("keeps the candidates of a failed batch at their similarity order", async () => {
         _setRetrievalConfigForTests({
            refine: { enabled: true, batchSize: 2, minLevel: "MEDIUM" },
            llm: {
               model: "m",
               maxAttempts: 1,
               cache: { enabled: false },
               breaker: { failures: 100, cooldownMs: 1_000 },
            },
         });
         _setEmbeddingProviderForTests(embeddings());
         _setLlmProviderForTests(
            fakeLlm((req, n) =>
               n === 1
                  ? new LlmError("boom", "http", false, 500)
                  : rate(
                       [...req.user.matchAll(/- \[(\d+)\]/g)].map(
                          (m) => [Number(m[1]), "HIGH"] as [number, string],
                       ),
                    ),
            ),
         );
         const payload = await semantic(captureHandler(store()));
         expect(payload.retrieval_stages.refine).toMatch(/^partial:1\/3$/);
         expect(payload.warnings.join(" ")).toContain("failed for 1 of 3 batches");
         // Nothing is lost to the failure: all five survive.
         expect(names(payload)).toHaveLength(5);
      });

      it("skips with a reason, and no call, when no LLM is configured", async () => {
         _setEmbeddingProviderForTests(embeddings());
         const payload = await semantic(captureHandler(store()));
         expect(payload.retrieval_stages).toEqual({ refine: "skipped:no_llm" });
         expect(names(payload)).toHaveLength(5);
      });

      it("skips when the LLM is switched off in the config", async () => {
         _setRetrievalConfigForTests({
            refine: { enabled: true },
            llm: { enabled: false, model: "m" },
         });
         _setEmbeddingProviderForTests(embeddings());
         const seen: LlmRequest[] = [];
         _setLlmProviderForTests(fakeLlm(() => "[]", seen));
         const payload = await semantic(captureHandler(store()));
         expect(seen).toHaveLength(0);
         expect(payload.retrieval_stages).toEqual({ refine: "skipped:no_llm" });
      });

      it("skips with no model to call rather than sending an empty one", async () => {
         _setRetrievalConfigForTests({ refine: { enabled: true } });
         _setEmbeddingProviderForTests(embeddings());
         const seen: LlmRequest[] = [];
         _setLlmProviderForTests(fakeLlm(() => "[]", seen));
         const saved = process.env.LLM_MODEL;
         delete process.env.LLM_MODEL;
         try {
            const payload = await semantic(captureHandler(store()));
            expect(seen).toHaveLength(0);
            expect(payload.retrieval_stages).toEqual({ refine: "skipped:no_model" });
         } finally {
            if (saved !== undefined) process.env.LLM_MODEL = saved;
         }
      });

      it("skips while the circuit breaker is open", async () => {
         _setRetrievalConfigForTests({
            refine: { enabled: true, batchSize: 1 },
            llm: {
               model: "m",
               maxAttempts: 1,
               cache: { enabled: false },
               breaker: { failures: 1, cooldownMs: 60_000 },
            },
         });
         _setEmbeddingProviderForTests(embeddings());
         const seen: LlmRequest[] = [];
         _setLlmProviderForTests(
            fakeLlm(() => new LlmError("down", "http", false, 500), seen),
         );
         const handler = captureHandler(store());
         await semantic(handler); // trips the breaker
         const calls = seen.length;
         const payload = parse(await handler(params));
         expect(seen.length).toBe(calls);
         expect(payload.retrieval_stages).toEqual({ refine: "skipped:cooldown" });
      });
   });

   describe("on the lexical path", () => {
      it("rates keyword matches too, and publishes the level-based relevance", async () => {
         _setLlmProviderForTests(
            fakeLlm((req) =>
               rate([
                  [indexOfName(req, "total_revenue"), "HIGH", "Exact."],
                  [indexOfName(req, "net_revenue"), "MEDIUM", "Close."],
               ]),
            ),
         );
         const payload = parse(await captureHandler(store())(params));
         expect(names(payload)).toEqual(["total_revenue", "net_revenue"]);
         const e = payload.sources[0].entities[0];
         expect(e.relevance).toBeGreaterThan(0.7);
         expect(e.matched_targets[0].match_reason).toBe("Exact.");
         expect(payload.retrieval_stages).toEqual({ refine: "ok" });
      });

      it("is skipped when refine.onLexical is off", async () => {
         _setRetrievalConfigForTests({
            refine: { enabled: true, onLexical: false },
            llm: { model: "m" },
         });
         const seen: LlmRequest[] = [];
         _setLlmProviderForTests(fakeLlm(() => "[]", seen));
         const payload = parse(await captureHandler(store())(params));
         expect(seen).toHaveLength(0);
         expect(payload.retrieval_stages).toEqual({ refine: "skipped:lexical" });
      });
   });

   describe("the trace", () => {
      it("records the refine gate with its drops and its LLM spend", async () => {
         process.env.PUBLISHER_RETRIEVAL_OVERRIDES = "1";
         _setEmbeddingProviderForTests(embeddings());
         _setLlmProviderForTests(
            fakeLlm((req) =>
               rate([
                  [indexOfName(req, "total_revenue"), "HIGH"],
                  [indexOfName(req, "avg_discount"), "LOW"],
               ]),
            ),
         );
         const payload = await semantic(captureHandler(store()), {
            requestInfo: { headers: { "x-publisher-retrieval-trace": "full" } },
         });
         const gate = payload.retrieval_trace.gates.find((g: any) => g.gate === "refine");
         expect(gate.status).toBe("ok");
         expect(gate.in).toBe(5);
         expect(gate.out).toBe(1);
         expect(gate.dropped_by_reason).toEqual({ llm_omitted: 3, below_min_level: 1 });
         expect(gate.llm_calls).toBe(1);
         expect(payload.retrieval_trace.llm.calls).toBe(1);
         const kept = payload.retrieval_trace.candidates.filter((c: any) => c.kept);
         expect(kept.map((c: any) => c.entity_id)).toEqual(["measure:orders:total_revenue"]);
      });

      it("gives every candidate its level, dropped ones too, so a level sweep can be replayed", async () => {
         process.env.PUBLISHER_RETRIEVAL_OVERRIDES = "1";
         _setEmbeddingProviderForTests(embeddings());
         _setLlmProviderForTests(
            fakeLlm((req) =>
               rate([
                  [indexOfName(req, "total_revenue"), "HIGH"],
                  [indexOfName(req, "avg_discount"), "LOW"],
               ]),
            ),
         );
         const payload = await semantic(captureHandler(store()), {
            requestInfo: { headers: { "x-publisher-retrieval-trace": "full" } },
         });
         const byId = Object.fromEntries(
            payload.retrieval_trace.candidates.map((c: any) => [c.entity_id, c]),
         );
         const kept = byId["measure:orders:total_revenue"];
         expect(kept.level).toBe("HIGH");
         expect(kept.levels).toEqual({ "0": "HIGH" });
         // Rated LOW and cut by minLevel: the level survives the cut.
         const low = byId["measure:orders:avg_discount"];
         expect(low.kept).toBe(false);
         expect(low.level).toBe("LOW");
         expect(low.dropped_by).toBe("refine:below_min_level");
         // Left out by the model: no level, and the reason says so.
         const omitted = payload.retrieval_trace.candidates.filter(
            (c: any) => c.dropped_by === "refine:llm_omitted",
         );
         expect(omitted).toHaveLength(3);
         expect(omitted.every((c: any) => c.levels["0"] === "omitted")).toBe(true);
      });

      it("can turn refine off for one call with the override header", async () => {
         process.env.PUBLISHER_RETRIEVAL_OVERRIDES = "1";
         _setEmbeddingProviderForTests(embeddings());
         const seen: LlmRequest[] = [];
         _setLlmProviderForTests(fakeLlm(() => "[]", seen));
         const payload = await semantic(captureHandler(store()), {
            requestInfo: {
               headers: { "x-publisher-retrieval": JSON.stringify({ refine: { enabled: false } }) },
            },
         });
         expect(seen).toHaveLength(0);
         expect(payload.retrieval_stages).toBeUndefined();
         expect(names(payload)).toHaveLength(5);
      });
   });
});
