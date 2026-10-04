// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   numberedLines,
   scriptedChat,
   type ScriptedChat,
} from "../../test_helpers/get_context_llm_harness";
import { StageError } from "./get_context_llm";
import type {
   CardDraft,
   CardState,
   PipelineContext,
} from "./get_context_pipeline";
import { relevanceFromReply, rerankStage } from "./get_context_rerank";
import type { ResolvedRequest, ResultEntity } from "./get_context_tool";

function entity(
   name: string,
   score: number,
   o: Partial<ResultEntity> = {},
): ResultEntity {
   return {
      kind: "dimension",
      name,
      source: "s",
      environmentName: "e",
      packageName: "pkg",
      modelPath: "m.malloy",
      doc: "",
      score,
      ...o,
   };
}

/** A card whose best raw score is `raw` (refine ran). */
function card(
   source: string,
   raw: number,
   rows: ResultEntity[] = [entity("f", 0.5, { source })],
): CardDraft {
   return {
      key: `m.malloy\u0000${source}`,
      modelPath: "m.malloy",
      source,
      relevance: 0.5,
      raw,
      rows,
      entitiesDropped: 0,
   };
}

const state = (cards: CardDraft[]): CardState => ({
   retrieval: "semantic",
   belowCutoffCount: 0,
   cards,
});

function ctxFor(
   chat: ScriptedChat,
   options: {
      topSources?: number;
      sourceName?: string;
      sourceDocs?: Record<string, string>;
   } = {},
): PipelineContext {
   return {
      request: {
         packageName: "pkg",
         sourceName: options.sourceName,
         searches: [
            { targetIndex: 0, targetType: "dimension", text: "first" },
            { targetIndex: 1, targetType: "measure", text: "second" },
         ],
      } as unknown as ResolvedRequest,
      pkgIndex: {
         directEntities: Object.entries(options.sourceDocs ?? {}).map(
            ([source, embedDoc]) => ({
               kind: "source",
               name: source,
               source,
               modelPath: "m.malloy",
               embedDoc,
            }),
         ),
      },
      llmStages: {
         concurrency: 4,
         rerank: {
            chat: chat.model,
            topSources: options.topSources ?? 8,
            instructions: "INSTRUCTIONS",
         },
      },
   } as unknown as PipelineContext;
}

/** The sources a prompt lists, by name, in order. */
const sourcesIn = (prompt: string) =>
   numberedLines(prompt).map(([, rest]) => /^Source: ([^,]+),/.exec(rest)![1]);

/** A reply that scores sources as listed: name -> score, in the order given. */
const reply = (scores: Record<string, number>) => (prompt: string) => {
   const index = new Map(sourcesIn(prompt).map((s, i) => [s, i + 1]));
   return JSON.stringify(
      Object.entries(scores).flatMap(([source, score]) => {
         const at = index.get(source);
         return at === undefined ? [] : [{ index: at, score }];
      }),
   );
};

describe("relevanceFromReply", () => {
   it("gives each score its tiebreak by the order the model listed it", () => {
      const out = relevanceFromReply([
         { index: 3, score: 3 },
         { index: 1, score: 3 },
         { index: 4, score: 3 },
         { index: 2, score: 2 },
      ]);
      expect(out.get(3)).toBeCloseTo(3.2, 10);
      expect(out.get(1)).toBeCloseTo(3.1, 10);
      expect(out.get(4)).toBeCloseTo(3.0, 10);
      // Alone in its level: tiebreak 0.
      expect(out.get(2)).toBe(2);
   });

   it("keeps the tiebreak under one however many sources share a score", () => {
      // 0.1 per rank would give the first of 25 sources 2.4, which is past the
      // next level: a source rated 1 would outrank one rated 2 or even 3.
      const same = Array.from({ length: 25 }, (_, i) => ({
         index: i + 1,
         score: 1,
      }));
      const out = relevanceFromReply(same);
      const values = same.map((s) => out.get(s.index) as number);
      expect(Math.max(...values)).toBeLessThan(2);
      expect(Math.min(...values)).toBe(1);
      // The model's order inside the level is still strictly kept.
      for (let i = 1; i < values.length; i++) {
         expect(values[i]).toBeLessThan(values[i - 1]);
      }
   });

   it("is unchanged for up to ten sources in a level", () => {
      const out = relevanceFromReply(
         Array.from({ length: 10 }, (_, i) => ({ index: i + 1, score: 2 })),
      );
      expect(out.get(1)).toBeCloseTo(2.9, 10);
      expect(out.get(10)).toBe(2);
   });
});

describe("rerank stage", () => {
   it("prunes on the model's score, so a crowd of 1s cannot survive on tiebreak", async () => {
      // 11 sources rated 1 and one rated 2. With a tiebreak of 0.1 per rank
      // the first 1 reached 2.0, passed the cut at 2 and tied with the real 2.
      const cards = Array.from({ length: 12 }, (_, i) =>
         card(`s${String(i).padStart(2, "0")}`, 3.9 - i * 0.01),
      );
      const chat = scriptedChat((prompt) =>
         JSON.stringify(
            numberedLines(prompt).map(([index]) => ({
               index,
               score: index === 12 ? 2 : 1,
            })),
         ),
      );
      const out = await rerankStage.run(
         state(cards),
         ctxFor(chat, { topSources: 12 }),
      );
      expect(out.cards.map((c) => c.source)).toEqual(["s11"]);
   });

   it("ranks every 3 above every 2 when many sources share a score", async () => {
      const cards = Array.from({ length: 24 }, (_, i) =>
         card(`s${String(i).padStart(2, "0")}`, 3.9 - i * 0.01),
      );
      // 20 rated 2 (listed first), then 4 rated 3.
      const chat = scriptedChat((prompt) =>
         JSON.stringify(
            numberedLines(prompt).map(([index]) => ({
               index,
               score: index <= 20 ? 2 : 3,
            })),
         ),
      );
      const out = await rerankStage.run(
         state(cards),
         ctxFor(chat, { topSources: 24 }),
      );
      const bySource = new Map(out.cards.map((c, i) => [c.source, i]));
      const threes = ["s20", "s21", "s22", "s23"].map(
         (s) => bySource.get(s) as number,
      );
      const twos = Array.from(
         { length: 20 },
         (_, i) => bySource.get(`s${String(i).padStart(2, "0")}`) as number,
      );
      expect(Math.max(...threes)).toBeLessThan(Math.min(...twos));
   });

   it("is skipped for 0 or 1 cards and runs for 2", () => {
      const chat = scriptedChat(reply({}));
      const ctx = ctxFor(chat);
      expect(rerankStage.enabled(ctx, state([]))).toBe(false);
      expect(rerankStage.enabled(ctx, state([card("a", 3)]))).toBe(false);
      expect(
         rerankStage.enabled(ctx, state([card("a", 3), card("b", 3)])),
      ).toBe(true);
      expect(
         rerankStage.enabled(
            { ...ctx, llmStages: undefined },
            state([card("a", 3), card("b", 3)]),
         ),
      ).toBe(false);
      expect(
         rerankStage.enabled(ctx, {
            ...state([card("a", 3), card("b", 3)]),
            retrieval: "lexical",
         }),
      ).toBe(false);
   });

   it("keeps the top 8 by raw relevance, and the rest still count", async () => {
      const cards = Array.from({ length: 12 }, (_, i) =>
         card(`s${String(i).padStart(2, "0")}`, 3.9 - i * 0.1),
      );
      const chat = scriptedChat((prompt) =>
         JSON.stringify(
            numberedLines(prompt).map(([index]) => ({ index, score: 3 })),
         ),
      );
      // Hand them over shuffled: the sort is by raw, not by position.
      const shuffled = [...cards].reverse();
      const out = await rerankStage.run(state(shuffled), ctxFor(chat));
      expect(chat.prompts).toHaveLength(1);
      expect(sourcesIn(chat.prompts[0])).toEqual(
         cards.slice(0, 8).map((c) => c.source),
      );
      expect(out.cards).toHaveLength(8);
      expect(out.discarded).toBe(4);
      expect(out.reranked).toBe(true);
   });

   it("honours a package's topSources", async () => {
      const cards = [card("a", 3.5), card("b", 3.4), card("c", 3.3)];
      const chat = scriptedChat(reply({ a: 3, b: 3 }));
      const out = await rerankStage.run(
         state(cards),
         ctxFor(chat, { topSources: 2 }),
      );
      expect(sourcesIn(chat.prompts[0])).toEqual(["a", "b"]);
      expect(out.discarded).toBe(1);
   });

   it("orders cards by score, then by the order the model listed within a score", async () => {
      const cards = [
         card("a", 3.9),
         card("b", 3.8),
         card("c", 3.7),
         card("d", 3.6),
      ];
      const chat = scriptedChat(reply({ c: 3, a: 3, d: 3, b: 2 }));
      const out = await rerankStage.run(state(cards), ctxFor(chat));
      expect(out.cards.map((c) => c.source)).toEqual(["c", "a", "d", "b"]);
      expect(out.cards.map((c) => c.raw)).toEqual([
         expect.closeTo(3.2, 10),
         expect.closeTo(3.1, 10),
         expect.closeTo(3.0, 10),
         2,
      ]);
      // Level 3 with a 0.0 tiebreak publishes as 0.9; the knots do the rest.
      expect(out.cards.map((c) => c.relevance)).toEqual([0.92, 0.91, 0.9, 0.7]);
   });

   it("prunes cards scored below 2, including ones the model left out", async () => {
      const cards = [
         card("a", 3.9),
         card("b", 3.8),
         card("c", 3.7),
         card("d", 3.6),
      ];
      const chat = scriptedChat(reply({ a: 3, b: 2, c: 1 }));
      const out = await rerankStage.run(state(cards), ctxFor(chat));
      expect(out.cards.map((c) => c.source)).toEqual(["a", "b"]);
      // d was never scored; it is gone, not kept at 0.
      expect(out.discarded).toBe(0);
   });

   it("keeps a 2 with a high tiebreak and drops a 1 with one", async () => {
      const cards = [card("a", 3), card("b", 3), card("c", 3), card("d", 3)];
      const chat = scriptedChat(reply({ a: 2, b: 2, c: 2, d: 1 }));
      const out = await rerankStage.run(state(cards), ctxFor(chat));
      expect(out.cards.map((c) => c.source)).toEqual(["a", "b", "c"]);
   });

   it("does not prune when the request pins a source, and an omitted card gets 0", async () => {
      const cards = [card("a", 3.9), card("b", 3.8), card("c", 3.7)];
      const chat = scriptedChat(reply({ a: 3, b: 1 }));
      const out = await rerankStage.run(
         state(cards),
         ctxFor(chat, { sourceName: "a" }),
      );
      expect(out.cards.map((c) => [c.source, c.raw, c.relevance])).toEqual([
         ["a", 3, 0.9],
         ["b", 1, 0.4],
         ["c", 0, 0],
      ]);
   });

   it("leaves every entity's relevance alone", async () => {
      const rows = [entity("f", 0.77, { source: "a" })];
      const cards = [card("a", 3.9, rows), card("b", 3.8)];
      const chat = scriptedChat(reply({ a: 3, b: 3 }));
      const out = await rerankStage.run(state(cards), ctxFor(chat));
      const a = out.cards.find((c) => c.source === "a") as CardDraft;
      expect(a.rows[0].score).toBe(0.77);
   });

   it("sorts on the cosine when refine did not run", async () => {
      const cards = [
         { ...card("a", 0), raw: undefined, relevance: 0.3 },
         { ...card("b", 0), raw: undefined, relevance: 0.9 },
      ];
      const chat = scriptedChat(reply({ a: 3, b: 3 }));
      await rerankStage.run(state(cards), ctxFor(chat));
      expect(sourcesIn(chat.prompts[0])).toEqual(["b", "a"]);
   });

   it("fails the stage, naming it, when the call fails or the reply is unusable", async () => {
      const cards = [card("a", 3), card("b", 3)];
      const down = scriptedChat(() => {
         throw new Error("503 upstream");
      });
      const failure = await rerankStage
         .run(state(cards), ctxFor(down))
         .catch((e) => e);
      expect(failure).toBeInstanceOf(StageError);
      expect(failure.stage).toBe("rerank");
      expect(failure.reason).toContain("503 upstream");

      const bad = scriptedChat(() => '[{"index":1,"score":7}]');
      const badFailure = await rerankStage
         .run(state(cards), ctxFor(bad))
         .catch((e) => e);
      expect(badFailure.stage).toBe("rerank");
      expect(bad.prompts).toHaveLength(2);
      expect(bad.prompts[1]).toContain('"score" to be 0, 1, 2 or 3');
   });

   it("ignores out-of-range and duplicate indexes", async () => {
      const cards = [card("a", 3), card("b", 3)];
      const chat = scriptedChat(
         () =>
            '[{"index":9,"score":3},{"index":1,"score":3},{"index":1,"score":0},{"index":2,"score":2}]',
      );
      const out = await rerankStage.run(state(cards), ctxFor(chat));
      expect(out.cards.map((c) => [c.source, c.raw])).toEqual([
         ["a", 3],
         ["b", 2],
      ]);
   });
});

describe("rerank prompt", () => {
   it("describes each source and lists up to 20 entities, best first", async () => {
      const rows = Array.from({ length: 25 }, (_, i) =>
         entity(`f${String(i).padStart(2, "0")}`, 0.9 - i * 0.01, {
            source: "orders",
            embedDoc: `Field number ${i}.`,
         }),
      );
      const own = entity("orders", 0.5, {
         kind: "source",
         source: "orders",
         embedDoc: "One row per\n order.",
      });
      const chat = scriptedChat(reply({ orders: 3, other: 3 }));
      await rerankStage.run(
         state([card("orders", 3.9, [own, ...rows]), card("other", 3.8)]),
         ctxFor(chat),
      );
      const prompt = chat.prompts[0];
      expect(prompt).toContain(
         "[1] Source: orders, Model: m.malloy, Package: pkg",
      );
      expect(prompt).toContain("Description: One row per order.");
      expect(prompt).toContain("- f00 (dimension): Field number 0.");
      expect(prompt).toContain("- f19 (dimension): Field number 19.");
      expect(prompt).not.toContain("f20");
      expect(prompt).toContain("first. second");
   });

   it("takes the source description from the model when the source row is absent", async () => {
      const chat = scriptedChat(reply({ a: 3, b: 3 }));
      await rerankStage.run(
         state([card("a", 3.9), card("b", 3.8)]),
         ctxFor(chat, { sourceDocs: { a: "Docs of a.", b: "Docs of b." } }),
      );
      expect(chat.prompts[0]).toContain("Description: Docs of a.");
      expect(chat.prompts[0]).toContain("Description: Docs of b.");
   });

   it("lists at most 5 values per entity when values exist", async () => {
      const f = entity("state", 0.9, {
         source: "a",
         values: Array.from({ length: 8 }, (_, i) => ({
            value: `v${i}`,
            score: 1,
         })),
      });
      const chat = scriptedChat(reply({ a: 3, b: 3 }));
      await rerankStage.run(
         state([card("a", 3.9, [f]), card("b", 3.8)]),
         ctxFor(chat),
      );
      expect(chat.prompts[0]).toContain("Values: v0, v1, v2, v3, v4");
      expect(chat.prompts[0]).not.toContain("v5");
   });

   it("never sends an access predicate or the raw annotation fallback", async () => {
      const f = entity("state", 0.9, {
         source: "a",
         doc: "#(authorize) region = 'leaked'",
         embedDoc: "State. #(access_filter) region = 'secret'",
      });
      const chat = scriptedChat(reply({ a: 3, b: 3 }));
      await rerankStage.run(
         state([card("a", 3.9, [f]), card("b", 3.8)]),
         ctxFor(chat, { sourceDocs: { a: "Doc. #(authorize) x = 'hidden'" } }),
      );
      const prompt = chat.prompts[0];
      expect(prompt).toContain("State.");
      for (const word of ["leaked", "secret", "hidden", "access_filter"]) {
         expect(prompt).not.toContain(word);
      }
   });
});
