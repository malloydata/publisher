// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   runCardStages,
   type CardState,
   type CardStage,
   type PipelineContext,
} from "./get_context_pipeline";
import { fitBudget } from "./get_context_tool";

const ctx = {} as PipelineContext;

function state(keys: string[]): CardState {
   return {
      retrieval: "lexical",
      belowCutoffCount: 0,
      cards: keys.map((key) => ({
         key,
         modelPath: "m.malloy",
         source: key,
         rows: [],
         entitiesDropped: 0,
      })),
   };
}

describe("runCardStages", () => {
   it("returns the state untouched when no stage is registered", async () => {
      const input = state(["b", "a", "c"]);
      const out = await runCardStages([], input, ctx);
      expect(out).toBe(input);
      expect(out.cards.map((c) => c.key)).toEqual(["b", "a", "c"]);
   });

   it("skips a disabled stage and runs an enabled one", async () => {
      const calls: string[] = [];
      const make = (name: string, enabled: boolean): CardStage => ({
         name,
         enabled: () => enabled,
         run: async (s) => {
            calls.push(name);
            return { ...s, cards: s.cards.slice(0, 1) };
         },
      });
      const out = await runCardStages(
         [make("off", false), make("on", true)],
         state(["a", "b"]),
         ctx,
      );
      expect(calls).toEqual(["on"]);
      expect(out.cards.map((c) => c.key)).toEqual(["a"]);
   });
});

describe("fitBudget", () => {
   const card = (n: number) =>
      ({
         source_info: { resource_id: { source: "s".repeat(n) } },
      }) as unknown as Parameters<typeof fitBudget>[0][number];
   const cards = [card(100), card(100), card(100)];
   const one = JSON.stringify(cards[0]).length;

   it("returns the input unchanged when the budget is null", () => {
      const out = fitBudget(cards, null, 1_000);
      expect(out.cards).toBe(cards);
      expect(out.dropped).toBe(0);
   });

   it("keeps whole cards and drops the rest", () => {
      // Room for two cards and their separators, not three.
      const out = fitBudget(cards, 2 * one + 3 + 50, 50);
      expect(out.cards).toEqual(cards.slice(0, 2));
      expect(out.dropped).toBe(1);
   });

   it("keeps at least one card when the budget is smaller than the first", () => {
      const out = fitBudget(cards, 10, 0);
      expect(out.cards).toEqual(cards.slice(0, 1));
      expect(out.dropped).toBe(2);
   });
});
