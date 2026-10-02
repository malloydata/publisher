// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   numberedLines,
   scriptedChat,
   type ScriptedChat,
} from "../../test_helpers/get_context_llm_harness";
import { StageError } from "./get_context_llm";
import type { PipelineContext, RankedState } from "./get_context_pipeline";
import {
   REFINE_BATCH_SIZE,
   REFINE_PER_SOURCE,
   REFINE_TOTAL,
   refineStage,
   selectCandidates,
} from "./get_context_refine";
import type { ResolvedRequest, ResultEntity } from "./get_context_tool";
import { mapRawScore } from "./get_context_scoring";

type Search = ResolvedRequest["searches"][number];

const search = (
   targetIndex: number,
   targetType: Search["targetType"],
   text: string,
): Search => ({
   targetIndex,
   targetType,
   text,
   kinds: targetType === "source" ? ["source"] : [targetType],
});

interface RowOptions {
   target?: number;
   modelPath?: string;
   kind?: string;
   dataType?: string;
   extraTargets?: Array<[number, number]>;
}

function row(
   source: string,
   name: string,
   cosine: number,
   o: RowOptions = {},
): ResultEntity {
   const target = o.target ?? 0;
   return {
      kind: o.kind ?? "dimension",
      name,
      source,
      environmentName: "e",
      packageName: "p",
      modelPath: o.modelPath ?? "m.malloy",
      doc: "",
      ...(o.dataType ? { dataType: o.dataType } : {}),
      score: cosine,
      targetScores: new Map([[target, cosine], ...(o.extraTargets ?? [])]),
      bestTarget: target,
   };
}

function ctxFor(
   chat: ScriptedChat,
   options: {
      searches?: Search[];
      minLevel?: "LOW" | "MEDIUM" | "HIGH";
      concurrency?: number;
      docs?: Record<string, string>;
   } = {},
): PipelineContext {
   const searches = options.searches ?? [search(0, "dimension", "state")];
   return {
      request: { searches } as unknown as ResolvedRequest,
      pkgIndex: {
         directEntities: Object.entries(options.docs ?? {}).map(
            ([key, doc]) => {
               const [source, name] = key.split(".");
               return {
                  modelPath: "m.malloy",
                  kind: "dimension",
                  source,
                  name,
                  embedDoc: doc,
               };
            },
         ),
      },
      llmStages: {
         concurrency: options.concurrency ?? 4,
         refine: {
            chat: chat.model,
            minLevel: options.minLevel ?? "MEDIUM",
            instructions: "INSTRUCTIONS",
         },
      },
   } as unknown as PipelineContext;
}

const state = (rows: ResultEntity[]): RankedState => ({
   retrieval: "semantic",
   belowCutoffCount: 0,
   rows,
});

/** Rate every numbered candidate in the prompt with `level`. */
const rateAll = (level: string) => (prompt: string) =>
   JSON.stringify(
      numberedLines(prompt).map(([index]) => ({ index, score: level })),
   );

/** Candidate names a prompt listed, in order. */
const namesIn = (prompt: string) =>
   numberedLines(prompt).map(([, rest]) => rest.split(" ")[0]);

describe("refine candidate selection", () => {
   it("keeps the best 10 per source", () => {
      const rows = Array.from({ length: 14 }, (_, i) =>
         row("s", `f${String(i).padStart(2, "0")}`, 0.9 - i * 0.01),
      );
      const picked = selectCandidates(rows, 0);
      expect(picked).toHaveLength(REFINE_PER_SOURCE);
      expect(picked.map((c) => c.row.name)).toEqual(
         rows.slice(0, 10).map((r) => r.name),
      );
   });

   it("keeps the best 120 overall after the per-source cut", () => {
      // 13 sources x 10 = 130 candidates survive the per-source cut.
      const rows = Array.from({ length: 13 }, (_, s) =>
         Array.from({ length: 12 }, (_, i) =>
            row(`s${s}`, `f${i}`, 0.9 - s * 0.01 - i * 0.0001),
         ),
      ).flat();
      const picked = selectCandidates(rows, 0);
      expect(picked).toHaveLength(REFINE_TOTAL);
      // The worst source (s12) lost its candidates first.
      expect(picked.filter((c) => c.row.source === "s12")).toHaveLength(0);
      expect(picked.filter((c) => c.row.source === "s11")).toHaveLength(10);
   });

   it("dedupes by (name, source) at the best cosine", () => {
      const rows = [
         row("s", "f", 0.5, { modelPath: "a.malloy" }),
         row("s", "f", 0.7, { modelPath: "b.malloy" }),
         row("t", "f", 0.4),
      ];
      const picked = selectCandidates(rows, 0);
      expect(picked.map((c) => [c.row.source, c.cosine])).toEqual([
         ["s", 0.7],
         ["t", 0.4],
      ]);
   });

   it("ignores rows another target found and source rows", () => {
      const rows = [
         row("s", "a", 0.5, { target: 1 }),
         row("s", "b", 0.5, { kind: "source" }),
         row("s", "c", 0.5),
      ];
      expect(selectCandidates(rows, 0).map((c) => c.row.name)).toEqual(["c"]);
   });
});

describe("refine stage", () => {
   it("sends batches of 15 and counts the calls", async () => {
      // 3 sources x 10 candidates = 30 -> 2 batches; 4 sources -> 40 -> 3.
      const rows = Array.from({ length: 4 }, (_, s) =>
         Array.from({ length: 12 }, (_, i) =>
            row(`s${s}`, `f${i}`, 0.9 - s * 0.01 - i * 0.001),
         ),
      ).flat();
      const chat = scriptedChat(rateAll("HIGH"));
      await refineStage.run(state(rows), ctxFor(chat));
      expect(chat.prompts).toHaveLength(3);
      const sizes = chat.prompts.map((p) => numberedLines(p).length);
      expect(sizes.sort((a, b) => b - a)).toEqual([
         REFINE_BATCH_SIZE,
         REFINE_BATCH_SIZE,
         10,
      ]);
   });

   it("makes 8 calls for the 120-candidate maximum", async () => {
      const rows = Array.from({ length: 13 }, (_, s) =>
         Array.from({ length: 10 }, (_, i) =>
            row(`s${s}`, `f${i}`, 0.9 - s * 0.01 - i * 0.0001),
         ),
      ).flat();
      const chat = scriptedChat(rateAll("MEDIUM"));
      const out = await refineStage.run(state(rows), ctxFor(chat));
      expect(chat.prompts).toHaveLength(REFINE_TOTAL / REFINE_BATCH_SIZE);
      // 130 rows went in; the 10 past the overall cap were never rated.
      expect(out.rows).toHaveLength(REFINE_TOTAL);
   });

   it("sends a duplicated entity once and rates every row that shares it", async () => {
      const rows = [
         row("s", "f", 0.6, { modelPath: "a.malloy" }),
         row("s", "f", 0.6, { modelPath: "b.malloy" }),
      ];
      const chat = scriptedChat(rateAll("HIGH"));
      const out = await refineStage.run(state(rows), ctxFor(chat));
      expect(numberedLines(chat.prompts[0])).toHaveLength(1);
      expect(out.rows.map((r) => r.modelPath).sort()).toEqual([
         "a.malloy",
         "b.malloy",
      ]);
      expect(out.rows.every((r) => r.level === 3)).toBe(true);
   });

   it("drops candidates below minLevel and the ones the model did not return", async () => {
      const rows = [
         row("s", "high", 0.8),
         row("s", "medium", 0.7),
         row("s", "low", 0.6),
         row("s", "omitted", 0.5),
      ];
      const reply = (prompt: string) => {
         const level: Record<string, string> = {
            high: "HIGH",
            medium: "MEDIUM",
            low: "LOW",
         };
         return JSON.stringify(
            numberedLines(prompt).flatMap(([index, rest]) => {
               const score = level[rest.split(" ")[0]];
               return score ? [{ index, score }] : [];
            }),
         );
      };
      const atMedium = await refineStage.run(
         state(rows),
         ctxFor(scriptedChat(reply)),
      );
      expect(atMedium.rows.map((r) => r.name).sort()).toEqual([
         "high",
         "medium",
      ]);
      const atLow = await refineStage.run(
         state(rows),
         ctxFor(scriptedChat(reply), { minLevel: "LOW" }),
      );
      expect(atLow.rows.map((r) => r.name).sort()).toEqual([
         "high",
         "low",
         "medium",
      ]);
      const atHigh = await refineStage.run(
         state(rows),
         ctxFor(scriptedChat(reply), { minLevel: "HIGH" }),
      );
      expect(atHigh.rows.map((r) => r.name)).toEqual(["high"]);
   });

   it("scores a survivor level + cosine, published through the knots", async () => {
      const rows = [row("s", "a", 0.5), row("s", "b", 0.4)];
      const reply = (prompt: string) =>
         JSON.stringify(
            numberedLines(prompt).map(([index, rest]) => ({
               index,
               score: rest.startsWith("a ") ? "MEDIUM" : "HIGH",
            })),
         );
      const out = await refineStage.run(
         state(rows),
         ctxFor(scriptedChat(reply)),
      );
      const a = out.rows.find((r) => r.name === "a") as ResultEntity;
      const b = out.rows.find((r) => r.name === "b") as ResultEntity;
      expect(a.raw).toBe(2.5);
      expect(a.score).toBe(0.8);
      expect(b.raw).toBe(3.4);
      expect(b.score).toBe(0.94);
      expect(b.targetScores?.get(0)).toBe(0.94);
      expect(b.targetRaw?.get(0)).toBe(3.4);
      expect(a.level).toBe(2);
      // Best first, and the unpublished reason is never kept.
      expect(out.rows.map((r) => r.name)).toEqual(["b", "a"]);
      expect(a.reason).toBeUndefined();
   });

   it("ignores out-of-range and duplicate indexes", async () => {
      const rows = [row("s", "a", 0.5), row("s", "b", 0.5)];
      const reply = () =>
         JSON.stringify([
            { index: 0, score: "HIGH" },
            { index: 99, score: "HIGH" },
            { index: 1, score: "HIGH", reason: "because" },
            { index: 1, score: "LOW" },
         ]);
      const out = await refineStage.run(
         state(rows),
         ctxFor(scriptedChat(reply)),
      );
      expect(out.rows.map((r) => [r.name, r.level])).toEqual([["a", 3]]);
   });

   it("accepts an array wrapped in an object, which a vendor JSON mode returns", async () => {
      const rows = [row("s", "a", 0.5)];
      const reply = () =>
         JSON.stringify({ ratings: [{ index: 1, score: "HIGH" }] });
      const out = await refineStage.run(
         state(rows),
         ctxFor(scriptedChat(reply)),
      );
      expect(out.rows).toHaveLength(1);
   });

   it("re-asks once on invalid JSON and uses the repaired reply", async () => {
      const rows = [row("s", "a", 0.5)];
      const chat = scriptedChat((_prompt, n) =>
         n === 1 ? "I think it is relevant." : '[{"index":1,"score":"HIGH"}]',
      );
      const out = await refineStage.run(state(rows), ctxFor(chat));
      expect(chat.prompts).toHaveLength(2);
      expect(chat.prompts[1]).toContain("It was rejected:");
      expect(out.rows[0].level).toBe(3);
   });

   it("re-asks when a score is not LOW, MEDIUM or HIGH, and says why", async () => {
      const chat = scriptedChat((_p, n) =>
         n === 1
            ? '[{"index":1,"score":"VERY"}]'
            : '[{"index":1,"score":"LOW"}]',
      );
      await refineStage.run(
         state([row("s", "a", 0.5)]),
         ctxFor(chat, { minLevel: "LOW" }),
      );
      expect(chat.prompts[1]).toContain(
         '"score" to be one of LOW, MEDIUM, HIGH',
      );
   });

   it("fails the stage, naming it, when the reply is still invalid after the repair", async () => {
      const chat = scriptedChat(() => "still not json");
      const failure = await refineStage
         .run(state([row("s", "a", 0.5)]), ctxFor(chat))
         .catch((e) => e);
      expect(failure).toBeInstanceOf(StageError);
      expect(failure.stage).toBe("refine");
      expect(failure.reason).toContain(
         "did not return usable JSON after one repair",
      );
      expect(failure.message).toStartWith("refine: ");
   });

   it("fails the stage when the model call itself fails", async () => {
      const chat = scriptedChat(() => {
         throw new Error("connection refused");
      });
      const failure = await refineStage
         .run(state([row("s", "a", 0.5)]), ctxFor(chat))
         .catch((e) => e);
      expect(failure).toBeInstanceOf(StageError);
      expect(failure.reason).toContain("connection refused");
   });

   it("never has more batches in flight than the concurrency setting", async () => {
      const rows = Array.from({ length: 13 }, (_, s) =>
         Array.from({ length: 10 }, (_, i) =>
            row(`s${s}`, `f${i}`, 0.9 - s * 0.01 - i * 0.0001),
         ),
      ).flat();
      const chat = scriptedChat(rateAll("HIGH"), { delayMs: 15 });
      await refineStage.run(state(rows), ctxFor(chat, { concurrency: 3 }));
      expect(chat.prompts).toHaveLength(8);
      expect(chat.maxInFlight()).toBe(3);
      const serial = scriptedChat(rateAll("HIGH"), { delayMs: 5 });
      await refineStage.run(state(rows), ctxFor(serial, { concurrency: 1 }));
      expect(serial.maxInFlight()).toBe(1);
   });

   it("rates each entity-search target on its own phrase and merges a row both found", async () => {
      const rows = [
         row("s", "shared", 0.6, { extraTargets: [[1, 0.5]] }),
         row("s", "only_second", 0.4, { target: 1 }),
      ];
      const searches = [
         search(0, "dimension", "first phrase"),
         search(1, "dimension", "second phrase"),
         search(2, "source", "a source phrase"),
      ];
      const chat = scriptedChat((prompt) =>
         JSON.stringify(
            numberedLines(prompt).map(([index, rest]) => ({
               index,
               // `shared` is MEDIUM for the first phrase and HIGH for the second.
               score:
                  rest.startsWith("shared ") &&
                  prompt.includes(`"first phrase"`)
                     ? "MEDIUM"
                     : "HIGH",
            })),
         ),
      );
      const out = await refineStage.run(
         state(rows),
         ctxFor(chat, { searches }),
      );
      // One call per entity-search target; the source target is not refined.
      expect(chat.prompts).toHaveLength(2);
      for (const p of chat.prompts) {
         expect(p).toContain("first phrase. second phrase. a source phrase");
      }
      const shared = out.rows.find((r) => r.name === "shared") as ResultEntity;
      expect(shared.targetRaw?.get(0)).toBe(2.6);
      expect(shared.targetRaw?.get(1)).toBe(3.5);
      expect(shared.raw).toBe(3.5);
      expect(shared.level).toBe(3);
      expect(shared.bestTarget).toBe(1);
      expect(shared.targetScores?.get(0)).toBe(mapRawScore(2.6));
   });

   it("puts a source-target row on the knots without rating it", async () => {
      const rows = [
         row("s", "s", 0.5, { kind: "source", target: 1 }),
         row("s", "f", 0.5),
      ];
      const searches = [
         search(0, "dimension", "a field"),
         search(1, "source", "a source"),
      ];
      const chat = scriptedChat(rateAll("HIGH"));
      const out = await refineStage.run(
         state(rows),
         ctxFor(chat, { searches }),
      );
      const source = out.rows.find((r) => r.kind === "source") as ResultEntity;
      expect(source.raw).toBe(0.5);
      expect(source.score).toBe(0.2);
      expect(source.level).toBeUndefined();
      expect(chat.prompts).toHaveLength(1);
   });
});

describe("refine prompt", () => {
   it("carries the question, the phrase and numbered candidate lines", async () => {
      const rows = [
         row("orders", "state", 0.6, { dataType: "string" }),
         row("orders", "total", 0.5, {
            kind: "measure",
            dataType: "number",
            target: 1,
         }),
      ];
      const chat = scriptedChat(rateAll("HIGH"));
      await refineStage.run(
         state(rows),
         ctxFor(chat, {
            searches: [
               search(0, "dimension", "state"),
               search(1, "measure", "revenue"),
            ],
            docs: {
               "orders.state": "State the order\n  ships to.   ",
            },
         }),
      );
      const first = chat.prompts.find((p) => p.includes('"state"')) as string;
      expect(first).toContain("state. revenue");
      expect(first).toContain(
         "[1] state (dimension / string, source: orders): State the order ships to.",
      );
      expect(first).not.toContain("total");
   });

   it("does not truncate a long description and flattens it to one line", async () => {
      const long = `${"word ".repeat(400)}end`;
      const chat = scriptedChat(rateAll("HIGH"));
      await refineStage.run(
         state([row("s", "f", 0.5)]),
         ctxFor(chat, { docs: { "s.f": long.replace(/ /g, "\n") } }),
      );
      const line = numberedLines(chat.prompts[0])[0][1];
      expect(line).toContain(long);
      expect(line).not.toContain("\n");
   });

   it("never sends an access predicate", async () => {
      const chat = scriptedChat(rateAll("HIGH"));
      await refineStage.run(
         state([row("s", "f", 0.5)]),
         ctxFor(chat, {
            docs: {
               "s.f": "Order total. #(access_filter) region = 'secret-region'",
            },
         }),
      );
      expect(chat.prompts[0]).toContain("Order total.");
      expect(chat.prompts[0]).not.toContain("secret-region");
      expect(chat.prompts[0]).not.toContain("access_filter");
   });

   it("uses the doc-only text, never the row's raw annotation fallback", async () => {
      const chat = scriptedChat(rateAll("HIGH"));
      const r = row("s", "f", 0.5);
      r.doc = "#(authorize) region = 'leaked'";
      await refineStage.run(state([r]), ctxFor(chat, { docs: {} }));
      expect(chat.prompts[0]).not.toContain("leaked");
   });

   it("asks for recall, a JSON array, and marks candidates as data", async () => {
      const chat = scriptedChat(rateAll("HIGH"));
      await refineStage.run(state([row("s", "f", 0.5)]), ctxFor(chat));
      expect(chat.prompts[0]).toContain("JSON array");
      expect(chat.prompts[0]).toContain("<candidates>");
      expect(namesIn(chat.prompts[0])).toEqual(["f"]);
   });
});

describe("refine stage enabled()", () => {
   const enabled = (ctx: PipelineContext, s: RankedState) =>
      refineStage.enabled(ctx, s);

   it("is on only with an LLM, a semantic ranking and an entity target", () => {
      const chat = scriptedChat(rateAll("HIGH"));
      const ctx = ctxFor(chat);
      expect(enabled(ctx, state([]))).toBe(true);
      expect(enabled(ctx, { ...state([]), retrieval: "lexical" })).toBe(false);
      expect(enabled({ ...ctx, llmStages: undefined }, state([]))).toBe(false);
      expect(
         enabled(
            ctxFor(chat, { searches: [search(0, "source", "x")] }),
            state([]),
         ),
      ).toBe(false);
   });
});
