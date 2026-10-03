// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Source match: the stage on its own (a scripted chat model through the real
 * provider layer) and through get_context (a real vector cache, a stub
 * embedding model, the keyword stand-in chat model).
 */

import {
   afterAll,
   afterEach,
   beforeAll,
   beforeEach,
   describe,
   expect,
   it,
} from "bun:test";
import { _clearChatModelForTests } from "../../providers/active";
import { shopPackage } from "../../test_helpers/get_context_llm_fixture";
import {
   callPayload,
   keywordReply,
   numberedLines,
   scriptedChat,
   semanticHarness,
   untilSemantic,
   useChat,
   type ScriptedChat,
   type SemanticHarness,
} from "../../test_helpers/get_context_llm_harness";
import { DEFAULT_SOURCE_MATCH_INSTRUCTIONS } from "../../prompts/source_match";
import { StageError, LlmMeter } from "./get_context_llm";
import type { PipelineContext, RankedState } from "./get_context_pipeline";
import {
   SOURCE_MATCH_BATCH_SIZE,
   SOURCE_MATCH_MAX_HIGH,
   selectSourceCandidates,
   sourceDescription,
   sourceMatchStage,
} from "./get_context_source_match";
import type { Entity, ResolvedRequest } from "./get_context_tool";

// ---------------------------------------------------------------------------
// The stage alone
// ---------------------------------------------------------------------------

function source(name: string, embedDoc = "", modelPath = "m.malloy"): Entity {
   return {
      id: name,
      kind: "source",
      name,
      source: name,
      modelPath,
      doc: embedDoc,
      embedDoc,
   };
}

const names = (n: number) =>
   Array.from({ length: n }, (_, i) => `s${String(i).padStart(2, "0")}`);

interface CtxOptions {
   entities: Entity[];
   chat: ScriptedChat;
   texts?: string[];
   sourceName?: string;
   concurrency?: number;
   dropped?: string[];
   topology?: Map<string, Array<{ targetSource: string }>>;
   joins?: Record<string, string[]>;
   /** Stored LLM summaries by source name. */
   summaries?: Record<string, string>;
   meter?: LlmMeter;
   /** Configure rerank (one call) so the stage has to leave room for it. */
   rerank?: boolean;
}

function ctxFor(o: CtxOptions): PipelineContext {
   const chat = o.meter ? o.meter.wrap(o.chat.model) : o.chat.model;
   const warnings: string[] = [];
   return {
      warnings,
      ...(o.meter ? { meter: o.meter } : {}),
      request: {
         environmentName: "e",
         packageName: "pkg",
         sourceName: o.sourceName,
         searches: (o.texts ?? ["customers"]).map((text, targetIndex) => ({
            targetIndex,
            targetType: "source",
            text,
            kinds: ["source"],
         })),
      } as unknown as ResolvedRequest,
      pkgIndex: {
         directEntities: o.entities,
         droppedSources: new Set(
            (o.dropped ?? []).map((n) => `m.malloy\u0000${n}`),
         ),
         topology: o.topology ?? new Map(),
         sourceContext: new Map(
            Object.entries(o.joins ?? {}).map(([name, joins]) => [
               `m.malloy\u0000${name}`,
               { joins: joins.map((j) => ({ name: j })) },
            ]),
         ),
      },
      ...(o.summaries
         ? {
              sourceSummaries: new Map(
                 Object.entries(o.summaries).map(([name, summary]) => [
                    name,
                    { summary, oneLineSummary: "one line" },
                 ]),
              ),
           }
         : {}),
      llmStages: {
         concurrency: o.concurrency ?? 4,
         sourceMatch: { chat, instructions: DEFAULT_SOURCE_MATCH_INSTRUCTIONS },
         ...(o.rerank ? { rerank: { chat, topSources: 10 } } : {}),
      },
   } as unknown as PipelineContext;
}

const empty: RankedState = {
   retrieval: "semantic",
   belowCutoffCount: 0,
   rows: [],
};

/** The source lines of a prompt, as `package/model/source`. */
const linesOf = (prompt: string) =>
   numberedLines(prompt).map(([, rest]) => rest.split("/").pop()!);

/** Replies HIGH or MEDIUM per source name, by position in the prompt. */
const rate = (levels: Record<string, "HIGH" | "MEDIUM">) => (prompt: string) =>
   JSON.stringify(
      linesOf(prompt).flatMap((name, i) =>
         levels[name] ? [{ index: i + 1, score: levels[name] }] : [],
      ),
   );

describe("source match stage", () => {
   it("sends batches of 10 and keeps at most the configured number in flight", async () => {
      const chat = scriptedChat(() => "[]", { delayMs: 15 });
      const ctx = ctxFor({
         entities: names(25).map((n) => source(n)),
         chat,
         concurrency: 2,
      });
      await sourceMatchStage.run(empty, ctx);
      expect(SOURCE_MATCH_BATCH_SIZE).toBe(10);
      expect(
         chat.prompts.map((p) => linesOf(p).length).sort((x, y) => x - y),
      ).toEqual([5, 10, 10]);
      expect(chat.maxInFlight()).toBe(2);
   });

   it("runs one set of batches per source target", async () => {
      const chat = scriptedChat(() => "[]");
      const ctx = ctxFor({
         entities: names(12).map((n) => source(n)),
         chat,
         texts: ["customers", "shipments"],
      });
      await sourceMatchStage.run(empty, ctx);
      expect(chat.prompts).toHaveLength(4);
      expect(
         chat.prompts.filter((p) => p.includes('"shipments"')),
      ).toHaveLength(2);
      // Every prompt carries the whole question.
      for (const p of chat.prompts) {
         expect(p).toContain("customers. shipments");
      }
   });

   it("shows a stored summary after the documentation, whole and on one line", async () => {
      const longDoc = `${"Doc words. ".repeat(80)}DOC-TAIL`;
      const summary = `First paragraph of the summary.\n\nSecond paragraph. ${"More detail. ".repeat(100)}SUMMARY-TAIL`;
      const chat = scriptedChat(() => "[]");
      const ctx = ctxFor({
         entities: [source("orders", longDoc), source("bare", "")],
         chat,
         summaries: { orders: summary },
      });
      await sourceMatchStage.run(empty, ctx);
      const prompt = chat.prompts[0];
      // The documentation is cut; the summary is not, and a blank line inside
      // it does not split the candidate.
      expect(prompt).not.toContain("DOC-TAIL");
      expect(prompt).toContain("SUMMARY-TAIL");
      expect(prompt).toMatch(
         /Documentation: [^\n]{500}\.\.\.\nSummary: First paragraph of the summary\. Second paragraph\./,
      );
      const blocks = prompt
         .split("<candidates>\n")[1]
         .split("\n</candidates>")[0]
         .split("\n\n");
      expect(blocks).toHaveLength(2);
   });

   it("leaves a source with no stored summary exactly as it was", async () => {
      const entities = [source("orders", "One row per order."), source("bare")];
      const without = scriptedChat(() => "[]");
      await sourceMatchStage.run(empty, ctxFor({ entities, chat: without }));
      const withSome = scriptedChat(() => "[]");
      await sourceMatchStage.run(
         empty,
         ctxFor({
            entities,
            chat: withSome,
            summaries: { orders: "About orders." },
         }),
      );
      expect(without.prompts[0]).not.toContain("Summary:");
      // Only the one source with a summary gained a line.
      const gained = withSome.prompts[0].replace(
         "Documentation: One row per order.\nSummary: About orders.",
         "Documentation: One row per order.",
      );
      expect(gained).toBe(without.prompts[0]);
      expect(
         withSome.prompts[0].match(/^Summary: /gm) as RegExpMatchArray,
      ).toHaveLength(1);
   });

   it("scrubs a summary before it is sent, like the documentation", async () => {
      const chat = scriptedChat(() => "[]");
      await sourceMatchStage.run(
         empty,
         ctxFor({
            entities: [source("orders", "Docs.")],
            chat,
            summaries: { orders: "Orders. #(access_filter) tenant = 'SECRET'" },
         }),
      );
      expect(chat.prompts[0]).toContain("Summary: Orders.");
      expect(chat.prompts[0]).not.toContain("SECRET");
   });

   it("does not offer a source that is out of scope or not queryable", () => {
      const entities = [source("a"), source("hidden"), source("b")];
      const chat = scriptedChat(() => "[]");
      // `hidden` stands for a source the index must never have held.
      const candidates = selectSourceCandidates(
         ctxFor({ entities, chat, dropped: ["hidden"] }),
      );
      expect(candidates.map((e) => e.name)).toEqual(["a", "b"]);
      // A drill-down scope narrows the candidates to that source.
      expect(
         selectSourceCandidates(
            ctxFor({ entities, chat, sourceName: "b" }),
         ).map((e) => e.name),
      ).toEqual(["b"]);
      // Only sources are candidates.
      expect(
         selectSourceCandidates(
            ctxFor({
               entities: [...entities, { ...source("f"), kind: "dimension" }],
               chat,
            }),
         ).map((e) => e.name),
      ).toEqual(["a", "hidden", "b"]);
   });

   it("never sends a dropped source to the model", async () => {
      const chat = scriptedChat(() => "[]");
      await sourceMatchStage.run(
         empty,
         ctxFor({
            entities: [source("a"), source("secret"), source("b")],
            chat,
            dropped: ["secret"],
         }),
      );
      expect(chat.prompts).toHaveLength(1);
      expect(chat.prompts[0]).not.toContain("secret");
   });

   it("cuts the doc at 500 characters with '...' and flattens it to one line", () => {
      const ctx = ctxFor({ entities: [], chat: scriptedChat(() => "[]") });
      const long = source("a", "x".repeat(600));
      expect(sourceDescription(ctx, long)).toBe(`${"x".repeat(500)}...`);
      const exact = source("a", "y".repeat(500));
      expect(sourceDescription(ctx, exact)).toBe("y".repeat(500));
      const multi = source("a", "first line\n  second   line\n\nthird");
      expect(sourceDescription(ctx, multi)).toBe(
         "first line second line third",
      );
   });

   it("shows the candidate as `[i] package/model/source` then its documentation", async () => {
      const chat = scriptedChat(() => "[]");
      await sourceMatchStage.run(
         empty,
         ctxFor({ entities: [source("orders", "One row per order.")], chat }),
      );
      expect(chat.prompts[0]).toContain(
         "[1] pkg/m.malloy/orders\nDocumentation: One row per order.",
      );
      expect(chat.prompts[0]).toContain('Source search phrase:\n"customers"');
   });

   it("builds a line from the joins when a source has no doc", () => {
      const ctx = ctxFor({
         entities: [],
         chat: scriptedChat(() => "[]"),
         topology: new Map([
            [
               "m.malloy\u0000orders",
               [
                  { targetSource: "customers" },
                  { targetSource: "products" },
                  { targetSource: "customers" },
               ],
            ],
         ]),
         joins: { items: ["owner_alias"] },
      });
      expect(sourceDescription(ctx, source("orders"))).toBe(
         "Source orders with joined sources: customers, products",
      );
      // No topology (no compiled model): the declared join names stand in.
      expect(sourceDescription(ctx, source("items"))).toBe(
         "Source items with joined sources: owner_alias",
      );
      expect(sourceDescription(ctx, source("lonely"))).toBe(
         "Source lonely (no joined sources)",
      );
   });

   it("scores HIGH 0.9 and MEDIUM 0.7, raw 3 and 2, HIGH first", async () => {
      const chat = scriptedChat(rate({ s01: "MEDIUM", s02: "HIGH" }));
      const out = await sourceMatchStage.run(
         empty,
         ctxFor({ entities: names(4).map((n) => source(n)), chat }),
      );
      expect(out.rows.map((r) => [r.source, r.score, r.raw, r.level])).toEqual([
         ["s02", 0.9, 3, 3],
         ["s01", 0.7, 2, 2],
      ]);
      expect(out.rows[0].kind).toBe("source");
      expect(out.rows[0].targetScores).toEqual(new Map([[0, 0.9]]));
      expect(out.rows[0].bestTarget).toBe(0);
   });

   it("drops every MEDIUM when more than 8 sources rate HIGH, and keeps them at 8", async () => {
      const all = names(12);
      const levels = (high: number) =>
         Object.fromEntries(
            all.map((n, i) => [n, i < high ? "HIGH" : "MEDIUM"] as const),
         );
      expect(SOURCE_MATCH_MAX_HIGH).toBe(8);
      const nine = await sourceMatchStage.run(
         empty,
         ctxFor({
            entities: all.map((n) => source(n)),
            chat: scriptedChat(rate(levels(9))),
         }),
      );
      expect(nine.rows).toHaveLength(9);
      expect(nine.rows.every((r) => r.score === 0.9)).toBe(true);
      const eight = await sourceMatchStage.run(
         empty,
         ctxFor({
            entities: all.map((n) => source(n)),
            chat: scriptedChat(rate(levels(8))),
         }),
      );
      expect(eight.rows).toHaveLength(12);
      expect(eight.rows.filter((r) => r.score === 0.7)).toHaveLength(4);
   });

   it("counts the HIGH sources of each target on its own", async () => {
      // Target 0 rates nine sources HIGH and the tenth MEDIUM, so that MEDIUM
      // goes. Target 1 rates one HIGH and the same tenth MEDIUM, so it stays.
      const chat = scriptedChat((prompt) => {
         const first = prompt.includes('"customers"');
         return JSON.stringify(
            linesOf(prompt).flatMap((_, i) =>
               i === 9
                  ? [{ index: i + 1, score: "MEDIUM" }]
                  : first || i === 0
                    ? [{ index: i + 1, score: "HIGH" }]
                    : [],
            ),
         );
      });
      const out = await sourceMatchStage.run(
         empty,
         ctxFor({
            entities: names(10).map((n) => source(n)),
            chat,
            texts: ["customers", "shipments"],
         }),
      );
      const s09 = out.rows.find((r) => r.source === "s09")!;
      expect(s09.targetScores).toEqual(new Map([[1, 0.7]]));
      expect(out.rows.filter((r) => r.score === 0.9)).toHaveLength(9);
   });

   it("drops sources the model left out and ignores indexes outside the batch", async () => {
      const chat = scriptedChat(() =>
         JSON.stringify([
            { index: 2, score: "HIGH" },
            { index: 0, score: "HIGH" },
            { index: 99, score: "MEDIUM" },
            { index: 2, score: "MEDIUM" },
         ]),
      );
      const out = await sourceMatchStage.run(
         empty,
         ctxFor({ entities: names(3).map((n) => source(n)), chat }),
      );
      expect(out.rows.map((r) => [r.source, r.score])).toEqual([["s01", 0.9]]);
   });

   it("merges two source targets into one row carrying both scores", async () => {
      const chat = scriptedChat((prompt) =>
         JSON.stringify([
            {
               index: 1,
               score: prompt.includes('"customers"') ? "MEDIUM" : "HIGH",
            },
         ]),
      );
      const out = await sourceMatchStage.run(
         empty,
         ctxFor({
            entities: [source("a")],
            chat,
            texts: ["customers", "shipments"],
         }),
      );
      expect(out.rows).toHaveLength(1);
      expect(out.rows[0].score).toBe(0.9);
      expect(out.rows[0].targetScores).toEqual(
         new Map([
            [0, 0.7],
            [1, 0.9],
         ]),
      );
      expect(out.rows[0].bestTarget).toBe(1);
   });

   it("keeps the rows the other retrievers found", async () => {
      const kept = {
         kind: "dimension",
         name: "d",
         source: "z",
         environmentName: "e",
         packageName: "pkg",
         modelPath: "m.malloy",
         doc: "",
         score: 0.8,
      };
      const out = await sourceMatchStage.run(
         { ...empty, rows: [kept] },
         ctxFor({
            entities: [source("a")],
            chat: scriptedChat(rate({ a: "HIGH" })),
         }),
      );
      expect(out.rows.map((r) => r.name)).toEqual(["a", "d"]);
   });

   it("repairs one invalid reply, then fails naming source_match", async () => {
      const repaired = scriptedChat((_p, n) =>
         n === 1 ? "not json at all" : '[{"index": 1, "score": "HIGH"}]',
      );
      const ok = await sourceMatchStage.run(
         empty,
         ctxFor({ entities: [source("a")], chat: repaired }),
      );
      expect(ok.rows).toHaveLength(1);
      expect(repaired.prompts).toHaveLength(2);

      const broken = scriptedChat(() => "still not json");
      const failure = await sourceMatchStage
         .run(empty, ctxFor({ entities: [source("a")], chat: broken }))
         .catch((e) => e);
      expect(failure).toBeInstanceOf(StageError);
      expect((failure as StageError).stage).toBe("source_match");
      expect(broken.prompts).toHaveLength(2);
   });

   it("rejects a score that is not HIGH or MEDIUM", async () => {
      const chat = scriptedChat(() =>
         JSON.stringify([{ index: 1, score: "LOW" }]),
      );
      const failure = await sourceMatchStage
         .run(empty, ctxFor({ entities: [source("a")], chat }))
         .catch((e) => e);
      expect((failure as StageError).reason).toContain("HIGH, MEDIUM");
   });

   it("counts its calls against the per-request ceiling", async () => {
      const chat = scriptedChat(() => "[]");
      const meter = new LlmMeter(2);
      // Three source targets need three calls at the least; the ceiling is two.
      const failure = await sourceMatchStage
         .run(
            empty,
            ctxFor({
               entities: names(25).map((n) => source(n)),
               chat,
               meter,
               texts: ["a", "b", "c"],
            }),
         )
         .catch((e) => e);
      expect(failure).toBeInstanceOf(StageError);
      expect((failure as StageError).stage).toBe("source_match");
      expect((failure as StageError).reason).toContain(
         "retrieval.llm.maxCallsPerRequest",
      );
      expect(chat.prompts.length).toBeLessThanOrEqual(2);
      expect(meter.snapshot().calls).toBe(2);
   });

   describe("a package with more sources than the call budget can batch", () => {
      // The defaults: 3 source targets, rerank on, maxCallsPerRequest 20.
      const manySources = (n: number) =>
         names(n).map((name) => source(name, `Table ${name}.`));

      it("stays inside the default call budget with 300 sources and 3 source targets", async () => {
         const chat = scriptedChat(() => "[]");
         const meter = new LlmMeter(20);
         const ctx = ctxFor({
            entities: manySources(300),
            chat,
            meter,
            rerank: true,
            texts: ["customers", "shipments", "invoices"],
         });
         await sourceMatchStage.run(empty, ctx);
         // Rerank keeps one call; source match uses at most the other 19.
         expect(meter.snapshot().calls).toBeLessThanOrEqual(19);
         expect(meter.snapshot().calls).toBeGreaterThan(3);
      });

      it("says how many sources each target did not see", async () => {
         const chat = scriptedChat(() => "[]");
         const ctx = ctxFor({
            entities: manySources(300),
            chat,
            meter: new LlmMeter(20),
            rerank: true,
            texts: ["customers", "shipments", "invoices"],
         });
         await sourceMatchStage.run(empty, ctx);
         expect(ctx.warnings).toHaveLength(1);
         expect(ctx.warnings[0]).toContain("60 of 300 sources");
         expect(ctx.warnings[0]).toContain("240");
         expect(ctx.warnings[0]).toContain("retrieval.llm.maxCallsPerRequest");
      });

      it("keeps the sources whose name or doc shares a word with the target", async () => {
         const chat = scriptedChat(() => "[]");
         const entities = [
            ...manySources(299),
            source("customer_orders", "One row per customer order."),
         ];
         const ctx = ctxFor({
            entities,
            chat,
            meter: new LlmMeter(20),
            rerank: true,
            texts: ["customer orders", "x1", "x2"],
         });
         await sourceMatchStage.run(empty, ctx);
         const first = chat.prompts.filter((p) =>
            p.includes('"customer orders"'),
         );
         expect(first.join("\n")).toContain("customer_orders");
      });

      it("sends every source, and no warning, when they fit the budget", async () => {
         const chat = scriptedChat(() => "[]");
         const ctx = ctxFor({
            entities: manySources(60),
            chat,
            meter: new LlmMeter(20),
            rerank: true,
            texts: ["customers", "shipments", "invoices"],
         });
         await sourceMatchStage.run(empty, ctx);
         expect(chat.prompts).toHaveLength(18);
         expect(ctx.warnings).toEqual([]);
      });

      it("sends every source when there is no ceiling", async () => {
         const chat = scriptedChat(() => "[]");
         const ctx = ctxFor({
            entities: manySources(300),
            chat,
            texts: ["customers"],
         });
         await sourceMatchStage.run(empty, ctx);
         expect(chat.prompts).toHaveLength(30);
         expect(ctx.warnings).toEqual([]);
      });
   });
});

// ---------------------------------------------------------------------------
// Through get_context
// ---------------------------------------------------------------------------

describe("get_context with source match", () => {
   let h: SemanticHarness;
   beforeAll(async () => {
      h = await semanticHarness();
   });
   afterAll(async () => {
      await h.close();
   });
   beforeEach(() => h.reset());
   afterEach(() => _clearChatModelForTests());

   const target = (target_type: string, search_text?: string) => ({
      target_type,
      ...(search_text === undefined ? {} : { search_text }),
   });
   const params = (pkg: string, targets: unknown[], extra = {}) => ({
      search_targets: targets,
      scopes: [{ environment: "sm", package: pkg }],
      ...extra,
   });

   it("answers a source target with the sources the model rated, HIGH before MEDIUM", async () => {
      const chat = scriptedChat(keywordReply);
      useChat(chat.model);
      const handler = h.handlerFor(
         shopPackage({ rerank: { enabled: false, topSources: 8 } }),
      );
      const { isError, payload } = await untilSemantic(
         handler,
         params("a", [target("source", "customer orders")]),
      );
      expect(isError).toBe(false);
      expect(
         payload.sources.map(
            (s: { source_info: { resource_id: { source: string } } }) =>
               s.source_info.resource_id.source,
         ),
      ).toEqual(["orders", "customers"]);
      expect(
         payload.sources.map((s: { relevance: number }) => s.relevance),
      ).toEqual([0.9, 0.7]);
      // Entity-less cards, exactly like a source target's today.
      expect(payload.sources[0].entities).toBeUndefined();
      expect(payload.total_available).toBe(2);
      // One source-match call, and the embedding path was not asked.
      expect(chat.prompts).toHaveLength(1);
   });

   it("is the stage the trace names", async () => {
      useChat(scriptedChat(keywordReply).model);
      const handler = h.handlerFor(
         shopPackage({ rerank: { enabled: false, topSources: 8 } }),
      );
      const { payload } = await untilSemantic(
         handler,
         params("b", [target("source", "customer orders")]),
         {
            requestInfo: {
               headers: { "x-publisher-retrieval-trace": "summary" },
            },
         },
      );
      const [row] = payload.retrieval_trace.stages;
      expect(row).toMatchObject({
         name: "source_match",
         status: "ran",
         llm_calls: 1,
         tokens: { input: 11, output: 5 },
      });
      // Two sources matched.
      expect(row.out).toBe(2);
   });

   it("runs a source target and a measure target together and merges them", async () => {
      const chat = scriptedChat(keywordReply);
      useChat(chat.model);
      const handler = h.handlerFor(
         shopPackage({ rerank: { enabled: false, topSources: 8 } }),
      );
      const { isError, payload } = await untilSemantic(
         handler,
         params("c", [
            target("measure", "sum of order revenue"),
            target("source", "customer orders"),
         ]),
      );
      expect(isError).toBe(false);
      const orders = payload.sources.find(
         (s: { source_info: { resource_id: { source: string } } }) =>
            s.source_info.resource_id.source === "orders",
      );
      expect(orders.entities.map((e: { name: string }) => e.name)).toContain(
         "total_revenue",
      );
      const customers = payload.sources.find(
         (s: { source_info: { resource_id: { source: string } } }) =>
            s.source_info.resource_id.source === "customers",
      );
      // Matched only by the source target: a card without entities.
      expect(customers).toBeDefined();
      expect(customers.entities).toBeUndefined();
      // One call for the source target, one refine batch for the measure target.
      const kinds = chat.prompts.map((p) =>
         p.includes("Source search phrase:") ? "source" : "entity",
      );
      expect(kinds.sort()).toEqual(["entity", "source"]);
   });

   it("lets rerank order the matched sources", async () => {
      const chat = scriptedChat(keywordReply);
      useChat(chat.model);
      const handler = h.handlerFor(shopPackage());
      const { isError, payload } = await untilSemantic(
         handler,
         params("d", [target("source", "customer orders")]),
      );
      expect(isError).toBe(false);
      // Source match, then rerank over the two matched cards.
      expect(chat.prompts).toHaveLength(2);
      expect(chat.prompts[1]).toContain("<sources>");
      expect(payload.sources.length).toBeGreaterThan(0);
   });

   it("cuts matched sources to the response budget", async () => {
      const filler =
         "These readings describe the device group in detail. ".repeat(8);
      const model = {
         getSourceInfos: () =>
            Array.from({ length: 120 }, (_, i) => ({
               name: `group_${String(i).padStart(3, "0")}`,
               annotations: [`#(doc) Metric readings group. ${filler}`],
               schema: { fields: [] },
            })),
         getQueries: () => [],
      };
      const pkg = {
         listModels: async () => [{ path: "big.malloy" }],
         getModel: () => model,
         getRetrievalSettings: () => ({
            representation: "single",
            keyphrases: "never",
            prompts: {},
            rerank: { enabled: false, topSources: 8 },
         }),
      };
      // Every source rates HIGH; the keyword stand-in would, but the reply
      // is spelled out so the test does not depend on its word rules.
      useChat(
         scriptedChat((prompt) =>
            JSON.stringify(
               numberedLines(prompt).map(([index]) => ({
                  index,
                  score: "HIGH",
               })),
            ),
         ).model,
         { maxCallsPerRequest: 50 },
      );
      const handler = h.handlerFor(pkg);
      const { isError, payload } = await untilSemantic(
         handler,
         params("e", [target("source", "metric readings")], { limit: 120 }),
      );
      expect(isError).toBe(false);
      expect(payload.total_available).toBe(120);
      expect(payload.returned).toBeLessThan(120);
      expect(JSON.stringify(payload.warnings)).toContain("characters");
   });

   it("fails loudly, naming source_match, when the model is down", async () => {
      const chat = scriptedChat(() => {
         throw new Error("HTTP 500 from the model");
      });
      useChat(chat.model);
      const handler = h.handlerFor(shopPackage());
      const { isError, payload } = await callPayload(
         handler,
         params("f", [target("source", "customer orders")]),
      );
      expect(isError).toBe(true);
      expect(payload.retrieval_stage).toBe("source_match");
      expect(payload.error).toContain("failed in the source_match step");
      expect(payload.sources).toEqual([]);
   });

   it("fails the same way after a reply that stays invalid through the repair", async () => {
      const chat = scriptedChat(() => "I cannot help with that.");
      useChat(chat.model);
      const handler = h.handlerFor(shopPackage());
      const { isError, payload } = await callPayload(
         handler,
         params("g", [target("source", "customer orders")]),
      );
      expect(isError).toBe(true);
      expect(payload.retrieval_stage).toBe("source_match");
      expect(chat.prompts).toHaveLength(2);
   });

   it("shares the per-request ceiling with the other stages", async () => {
      const chat = scriptedChat(keywordReply);
      useChat(chat.model, { maxCallsPerRequest: 1 });
      const handler = h.handlerFor(shopPackage());
      const { isError, payload } = await untilSemantic(
         handler,
         params("h", [target("source", "customer orders")]),
      );
      // Source match used the one call; rerank, which needs another, is refused.
      expect(isError).toBe(true);
      expect(payload.retrieval_stage).toBe("rerank");
      expect(payload.error).toContain("retrieval.llm.maxCallsPerRequest");
      expect(chat.prompts).toHaveLength(1);
   });

   it("does nothing when sourceMatch is off: the source target ranks as before", async () => {
      const chat = scriptedChat(keywordReply);
      useChat(chat.model);
      const off = shopPackage({
         sourceMatch: { enabled: false },
         rerank: { enabled: false, topSources: 8 },
      });
      const { payload } = await untilSemantic(
         h.handlerFor(off),
         params("i", [target("source", "one row per customer")]),
      );
      // Ranked by embedding: no call went to the model.
      expect(chat.prompts).toHaveLength(0);
      expect(payload.retrieval).toBe("semantic");
      expect(payload.sources.length).toBeGreaterThan(0);
   });

   it("does nothing with no LLM configured, and a listing runs no model call", async () => {
      useChat(null);
      const handler = h.handlerFor(shopPackage());
      const ranked = await untilSemantic(
         handler,
         params("j", [target("source", "one row per customer")]),
      );
      expect(ranked.isError).toBe(false);
      expect(ranked.payload.retrieval).toBe("semantic");
      const chat = scriptedChat(keywordReply);
      useChat(chat.model);
      const listing = await callPayload(
         h.handlerFor(shopPackage()),
         params("k", [target("source")]),
      );
      expect(listing.isError).toBe(false);
      expect(chat.prompts).toHaveLength(0);
   });
});
