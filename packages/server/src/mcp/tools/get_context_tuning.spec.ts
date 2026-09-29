// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The get_context tuning surface: the per-request override header, the trace,
// and the no-LLM precision levers (gap cut, per-source cap, character budget,
// similarity floor, facet allow-list). Every case here is about a knob moving
// a result; the default path is pinned separately in
// get_context_payload_pin.spec.ts.

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
import { DuckDBConnection } from "../../storage/duckdb/DuckDBConnection";
import { createEntityEmbeddingsTable } from "../../storage/duckdb/schema";
import { _resetEmbeddingIndexStateForTests } from "./embedding_index";
import { registerGetContextTool } from "./get_context_tool";

type Content = Array<{ type?: string; resource?: { text: string } }>;
type Extra = { requestInfo?: { headers?: Record<string, string> } };
type Result = { isError?: boolean; content: Content };
type Handler = (
   params: Record<string, unknown>,
   extra?: Extra,
) => Promise<Result>;

function captureHandler(store: Partial<EnvironmentStore>): Handler {
   const handlers = new Map<string, Handler>();
   registerGetContextTool(
      {
         tool: (name: string, _d: string, _s: unknown, h: Handler) => {
            handlers.set(name, h);
         },
      } as never,
      store as EnvironmentStore,
   );
   return handlers.get("get_context")!;
}

const parse = (r: Result) => JSON.parse(r.content[0].resource!.text);

// Measures with a known cosine to the query [1, 0], across two sources.
const MEASURES: Array<{ source: string; name: string; cos: number }> = [
   { source: "orders", name: "total_revenue", cos: 1 },
   { source: "orders", name: "net_revenue", cos: 0.9 },
   { source: "orders", name: "gross_revenue", cos: 0.6 },
   { source: "orders", name: "avg_discount", cos: 0.3 },
   { source: "orders", name: "order_count", cos: 0 },
   { source: "customers", name: "customer_revenue", cos: 0.95 },
];
const vecFor = (cos: number) => [cos, Math.sqrt(Math.max(0, 1 - cos * cos))];
const humanize = (n: string) => n.replace(/_/g, " ");
// Trimmed, because the index embeds whitespace-collapsed text.
const DOC = (name: string) =>
   `Doc for ${humanize(name)}. ${"Padding sentence. ".repeat(12)}`.trim();

const sourceInfo = (source: string) => ({
   name: source,
   annotations: [`#(doc) One row per ${source}.`],
   schema: {
      fields: MEASURES.filter((m) => m.source === source).map((m) => ({
         kind: "measure",
         name: m.name,
         annotations: [`#(doc) ${DOC(m.name)}`],
      })),
   },
});
const model = {
   getSourceInfos: () => ["orders", "customers"].map(sourceInfo),
   getQueries: () => [],
};
const pkg = {
   listModels: async () => [{ path: "m.malloy" }],
   getModel: () => model,
};

function vectors(): Record<string, number[]> {
   const v: Record<string, number[]> = {
      orders: [0, 1],
      "orders: One row per orders.": [0, 1],
      customers: [0, 1],
      "customers: One row per customers.": [0, 1],
      revenue: [1, 0],
   };
   for (const m of MEASURES) {
      v[humanize(m.name)] = vecFor(m.cos);
      v[`${humanize(m.name)}: ${DOC(m.name)}`] = vecFor(m.cos);
   }
   return v;
}

function stubProvider(): EmbeddingProvider {
   const table = vectors();
   const fetchStub = (async (_u: RequestInfo | URL, init?: RequestInit) => {
      const body = JSON.parse(String(init?.body)) as { input: string[] };
      const data = body.input.map((text, index) => {
         const embedding = table[text];
         if (!embedding) throw new Error(`no stub vector for "${text}"`);
         return { index, embedding };
      });
      return new Response(JSON.stringify({ data }), { status: 200 });
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

const params = (text = "revenue") => ({
   search_targets: [{ target_type: "measure", search_text: text }],
   scopes: [{ environment: "tune", package: "p" }],
});

const H = (override?: unknown, trace?: string): Extra => ({
   requestInfo: {
      headers: {
         ...(override !== undefined
            ? { "x-publisher-retrieval": JSON.stringify(override) }
            : {}),
         ...(trace ? { "x-publisher-retrieval-trace": trace } : {}),
      },
   },
});

const entityNames = (payload: any): string[] =>
   payload.sources.flatMap((c: any) => (c.entities ?? []).map((e: any) => e.name));

describe("get_context tuning", () => {
   let tempDir: string;
   let db: DuckDBConnection;
   const savedGate = process.env.PUBLISHER_RETRIEVAL_OVERRIDES;

   beforeAll(async () => {
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "get-context-tuning-"));
      db = new DuckDBConnection(path.join(tempDir, "test.db"));
      await db.initialize();
      await createEntityEmbeddingsTable(db);
   });

   afterAll(async () => {
      await db.close();
      fs.rmSync(tempDir, { recursive: true, force: true });
      _clearEmbeddingProviderForTests();
   });

   beforeEach(() => {
      _setEmbeddingProviderForTests(null);
      _resetEmbeddingIndexStateForTests();
      _clearOverrideCacheForTests();
      process.env.PUBLISHER_RETRIEVAL_OVERRIDES = "1";
   });

   afterEach(() => {
      _clearRetrievalConfigForTests();
      if (savedGate === undefined) delete process.env.PUBLISHER_RETRIEVAL_OVERRIDES;
      else process.env.PUBLISHER_RETRIEVAL_OVERRIDES = savedGate;
   });

   const store = (): Partial<EnvironmentStore> => ({
      getEnvironment: async () =>
         ({
            getPackage: async () => pkg,
            getStaleCompileErrors: () => new Map(),
         }) as never,
      storageManager: { getDuckDbConnection: () => db } as never,
   });

   async function semantic(handler: Handler, p: object, extra?: Extra) {
      for (let i = 0; i < 400; i++) {
         const payload = parse(await handler(p as never, extra));
         if (payload.retrieval === "semantic") return payload;
         await new Promise((r) => setTimeout(r, 5));
      }
      throw new Error("never became semantic");
   }

   describe("the override header", () => {
      it("adds nothing to a default response", async () => {
         const handler = captureHandler(store());
         const payload = parse(await handler(params(), H()));
         expect(payload.retrieval_config).toBeUndefined();
         expect(payload.retrieval_trace).toBeUndefined();
      });

      it("is ignored, with a warning, when the gate is closed", async () => {
         delete process.env.PUBLISHER_RETRIEVAL_OVERRIDES;
         const handler = captureHandler(store());
         const payload = parse(
            await handler(params(), H({ response: { maxEntitiesPerSourceTarget: 1 } })),
         );
         expect(payload.warnings.join(" ")).toContain("PUBLISHER_RETRIEVAL_OVERRIDES");
         expect(payload.retrieval_config).toBeUndefined();
         // Not applied: the default cap of 10 still admits every match.
         expect(entityNames(payload).length).toBeGreaterThan(1);
      });

      it("is applied when the gate is open, and stamps a config fingerprint", async () => {
         const handler = captureHandler(store());
         const payload = parse(
            await handler(params(), H({ response: { maxEntitiesPerSourceTarget: 1 } })),
         );
         expect(payload.retrieval_config).toMatch(/^[0-9a-f]{12}$/);
         for (const card of payload.sources) {
            expect((card.entities ?? []).length).toBeLessThanOrEqual(1);
         }
      });

      it("fails the call on an invalid override instead of half applying it", async () => {
         const handler = captureHandler(store());
         const result = await handler(params(), H({ refine: { batchSize: 0 } }));
         expect(result.isError).toBe(true);
         const payload = parse(result);
         expect(payload.sources).toEqual([]);
         expect(payload.error).toContain("Invalid retrieval.refine.batchSize");
      });

      it("refuses a key it may not set, naming it", async () => {
         const handler = captureHandler(store());
         const result = await handler(params(), H({ egress: { preset: "full" } }));
         expect(result.isError).toBe(true);
         expect(parse(result).error).toContain("egress.preset");
      });

      it("rejects malformed JSON and an unknown trace level", async () => {
         const handler = captureHandler(store());
         const bad = await handler(params(), {
            requestInfo: { headers: { "x-publisher-retrieval": "{not json" } },
         });
         expect(bad.isError).toBe(true);
         expect(parse(bad).error).toContain("expected JSON");
         const lvl = await handler(params(), H(undefined, "verbose"));
         expect(lvl.isError).toBe(true);
         expect(parse(lvl).error).toContain("X-Publisher-Retrieval-Trace");
      });

      it("leaves the operator's block in force under the override", async () => {
         _setRetrievalConfigForTests({ response: { maxEntitiesPerSourceTarget: 2 } });
         const handler = captureHandler(store());
         const payload = parse(await handler(params(), H({ hybrid: { rrfK: 30 } })));
         for (const card of payload.sources) {
            expect((card.entities ?? []).length).toBeLessThanOrEqual(2);
         }
      });
   });

   describe("the trace", () => {
      it("lists gates whose drop reasons add up, on the lexical path", async () => {
         const handler = captureHandler(store());
         const payload = parse(await handler(params("revenue"), H(undefined, "summary")));
         const t = payload.retrieval_trace;
         expect(t.level).toBe("summary");
         expect(t.retrieval).toBe("lexical");
         expect(t.gates.map((g: any) => g.gate)).toEqual(["candidate", "page", "delivered"]);
         for (const g of t.gates) {
            const dropped = Object.values(g.dropped_by_reason as Record<string, number>).reduce(
               (a, b) => a + b,
               0,
            );
            expect(dropped).toBe(g.in - g.out);
            expect(g.dropped_by_reason.unattributed).toBeUndefined();
         }
         expect(t.response_chars).toBeGreaterThan(100);
         expect(t.candidates).toBeUndefined();
      });

      it("accounts for the floor and the window on the semantic path", async () => {
         _setEmbeddingProviderForTests(stubProvider());
         const handler = captureHandler(store());
         const payload = await semantic(handler, params(), H(undefined, "summary"));
         const candidate = payload.retrieval_trace.gates.find((g: any) => g.gate === "candidate");
         expect(candidate.in).toBe(payload.total_entities);
         expect(candidate.dropped_by_reason.below_floor).toBe(payload.below_cutoff_count);
         expect(candidate.dropped_by_reason.unattributed).toBeUndefined();
      });

      it("adds per-candidate detail only in full mode", async () => {
         const handler = captureHandler(store());
         const payload = parse(await handler(params(), H(undefined, "full")));
         const cands = payload.retrieval_trace.candidates;
         expect(cands.length).toBeGreaterThan(0);
         expect(cands[0]).toHaveProperty("entity_id");
         expect(cands.every((c: any) => typeof c.kept === "boolean")).toBe(true);
      });

      it("says why a candidate was dropped, in full mode", async () => {
         const handler = captureHandler(store());
         const payload = parse(
            await handler(
               params(),
               H({ response: { maxEntitiesPerSourceTarget: 1 } }, "full"),
            ),
         );
         const dropped = payload.retrieval_trace.candidates.filter((c: any) => !c.kept);
         expect(dropped.length).toBeGreaterThan(0);
         expect(dropped.every((c: any) => typeof c.dropped_by === "string")).toBe(true);
      });

      it("can be the operator's default without any header", async () => {
         _setRetrievalConfigForTests({ trace: { defaultLevel: "summary" } });
         const handler = captureHandler(store());
         const payload = parse(await handler(params()));
         expect(payload.retrieval_trace.level).toBe("summary");
      });
   });

   describe("precision levers", () => {
      it("caps entities per source per target", async () => {
         const handler = captureHandler(store());
         const wide = parse(await handler(params("revenue"), H()));
         const capped = parse(
            await handler(params("revenue"), H({ response: { maxEntitiesPerSourceTarget: 1 } })),
         );
         expect(entityNames(capped).length).toBeLessThan(entityNames(wide).length);
         expect(capped.warnings.join(" ")).toContain("cut at 1 per source per target");
      });

      it("cuts entities far below their target's best (gap cut, lexical)", async () => {
         const handler = captureHandler(store());
         const all = parse(await handler(params("total revenue"), H()));
         const cut = parse(
            await handler(params("total revenue"), H({ response: { gapCut: 0.9 } }, "summary")),
         );
         expect(entityNames(cut).length).toBeLessThan(entityNames(all).length);
         const gate = cut.retrieval_trace.gates.find((g: any) => g.gate === "gap_cut");
         expect(gate.dropped_by_reason.gap_cut).toBeGreaterThan(0);
         // The target's own best hit always survives.
         expect(entityNames(cut)).toContain("total_revenue");
      });

      it("gap cut works on the semantic path too, keeping each target's top hit", async () => {
         _setEmbeddingProviderForTests(stubProvider());
         const handler = captureHandler(store());
         const all = await semantic(handler, params());
         const cut = await semantic(handler, params(), H({ response: { gapCut: 0.8 } }));
         expect(entityNames(cut).length).toBeLessThan(entityNames(all).length);
         expect(entityNames(cut)).toContain("total_revenue");
         // 0.3 and 0.6 are under 0.8 * 1.0; 0.9 and 0.95 are not.
         expect(entityNames(cut)).not.toContain("avg_discount");
         expect(entityNames(cut)).toContain("net_revenue");
      });

      it("raises the similarity floor for one call and moves the counts with it", async () => {
         _setEmbeddingProviderForTests(stubProvider());
         const handler = captureHandler(store());
         const base = await semantic(handler, params());
         const strict = await semantic(handler, params(), H({ embedding: { minSimilarity: 0.92 } }));
         // 1.0 and 0.95 clear 0.92; 0.9, 0.6 and 0.3 do not.
         expect([...entityNames(strict)].sort()).toEqual([
            "customer_revenue",
            "total_revenue",
         ]);
         expect(strict.below_cutoff_count).toBeGreaterThan(base.below_cutoff_count);
         expect(strict.total_entities).toBe(base.total_entities);
      });

      it("a floor just under 1 keeps only an exact match", async () => {
         _setEmbeddingProviderForTests(stubProvider());
         const handler = captureHandler(store());
         await semantic(handler, params());
         const exact = parse(
            await handler(params(), H({ embedding: { minSimilarity: 0.999 } })),
         );
         // total_revenue is exactly 1.0, so it still clears 0.999.
         expect(entityNames(exact)).toEqual(["total_revenue"]);
      });

      it("reports a true negative when nothing clears the floor", async () => {
         _setEmbeddingProviderForTests(stubProvider());
         const handler = captureHandler(store());
         await semantic(handler, params());
         // "orders"/"customers" sources sit at cosine 0 to this query.
         const none = parse(
            await handler(
               { ...params(), search_targets: [{ target_type: "source", search_text: "revenue" }] },
               H({ embedding: { minSimilarity: 0.5 } }),
            ),
         );
         expect(none.sources).toEqual([]);
         expect(none.below_cutoff_count).toBe(none.total_entities);
      });

      it("scores only the facets it is told to", async () => {
         _setEmbeddingProviderForTests(stubProvider());
         const handler = captureHandler(store());
         const both = await semantic(handler, params());
         const nameOnly = await semantic(handler, params(), H({ embedding: { facets: ["name"] } }));
         // Every entity's name and doc facet share a vector here, so the
         // ranking is the same; what changes is that a facet list which
         // matches nothing weighs nothing at all.
         expect(entityNames(nameOnly)).toEqual(entityNames(both));
         const kwOnly = parse(await handler(params(), H({ embedding: { facets: ["kw"] } })));
         expect(kwOnly.sources).toEqual([]);
         expect(kwOnly.total_entities).toBe(0);
      });

      it("limits the candidate window per target", async () => {
         _setEmbeddingProviderForTests(stubProvider());
         const handler = captureHandler(store());
         const payload = await semantic(handler, params(), H({ candidates: { perTargetLimit: 2 } }, "summary"));
         expect(entityNames(payload).length).toBeLessThanOrEqual(2);
         const cand = payload.retrieval_trace.gates.find((g: any) => g.gate === "candidate");
         expect(cand.dropped_by_reason.outside_candidate_window).toBeGreaterThan(0);
      });
   });

   describe("the character budget", () => {
      it("trims the worst entities and cards first, and says so", async () => {
         const handler = captureHandler(store());
         const full = parse(await handler(params("revenue"), H()));
         const size = JSON.stringify(full).length;
         const budget = Math.floor(size * 0.6);
         const trimmed = parse(
            await handler(params("revenue"), H({ response: { maxChars: budget } }, "summary")),
         );
         expect(JSON.stringify({ ...trimmed, retrieval_trace: undefined, retrieval_config: undefined }).length).toBeLessThanOrEqual(budget);
         expect(trimmed.warnings.join(" ")).toContain("Trimmed to fit response.maxChars");
         expect(trimmed.sources.length).toBeGreaterThanOrEqual(1);
         expect(trimmed.returned).toBe(trimmed.sources.length);
         // Best-first survives: the first card is the first card.
         expect(trimmed.sources[0].source_info.resource_id.source).toBe(
            full.sources[0].source_info.resource_id.source,
         );
         const gate = trimmed.retrieval_trace.gates.find((g: any) => g.gate === "budget");
         expect(gate.dropped_by_reason.char_budget).toBeGreaterThan(0);
      });

      it("does nothing when the response already fits", async () => {
         const handler = captureHandler(store());
         const payload = parse(
            await handler(params("revenue"), H({ response: { maxChars: 1_000_000 } })),
         );
         expect((payload.warnings ?? []).join(" ")).not.toContain("Trimmed");
      });

      it("never empties the response to nothing", async () => {
         const handler = captureHandler(store());
         const payload = parse(
            await handler(params("revenue"), H({ response: { maxChars: 1_000 } })),
         );
         expect(payload.sources.length).toBe(1);
      });
   });
});
