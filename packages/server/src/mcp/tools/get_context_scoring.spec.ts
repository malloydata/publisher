// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Scores when a refine stage rated the rows: the knots, and damping of joined
 * copies on the whole raw score. Assembly runs over the real-compiler join
 * fixture, as get_context_assembly.spec.ts does.
 */

import { beforeAll, describe, expect, it } from "bun:test";
import {
   compileJoinFixture,
   storeServing,
} from "../../test_helpers/get_context_join_fixture";
import { assembleCards } from "./get_context_assembly";
import type { PipelineContext, PipelineSettings } from "./get_context_pipeline";
import { mapRawScore } from "./get_context_scoring";
import {
   getPackageIndex,
   projectEntity,
   type PackageIndex,
   type ResolvedRequest,
   type ResultEntity,
} from "./get_context_tool";

describe("mapRawScore", () => {
   it("returns the published value at each knot", () => {
      expect(mapRawScore(0)).toBe(0);
      expect(mapRawScore(1)).toBe(0.4);
      expect(mapRawScore(2)).toBe(0.7);
      expect(mapRawScore(3)).toBe(0.9);
      expect(mapRawScore(4)).toBe(1);
   });

   it("is linear between knots and rounds to 2 places", () => {
      expect(mapRawScore(0.5)).toBe(0.2);
      expect(mapRawScore(1.5)).toBe(0.55);
      // MEDIUM with cosine 0.5.
      expect(mapRawScore(2.5)).toBe(0.8);
      // HIGH with cosine 0.4.
      expect(mapRawScore(3.4)).toBe(0.94);
      expect(mapRawScore(2.333)).toBe(0.77);
   });

   it("clamps outside 0 to 4", () => {
      expect(mapRawScore(-1)).toBe(0);
      expect(mapRawScore(5)).toBe(1);
   });
});

const SETTINGS: PipelineSettings = {
   joins: "assembly",
   entityWindow: { perSourcePerTarget: 10 },
   joinMaxDepth: 10,
   joinDamping: 0.9,
   scoring: "cosine",
   maxChars: null,
   reserveChars: 1_000,
};

let index: PackageIndex;
beforeAll(async () => {
   const { pkg } = await compileJoinFixture();
   index = await getPackageIndex(storeServing(pkg), "env", "pkg");
});

const request = (): ResolvedRequest => ({
   pureSourceListing: false,
   environmentName: "env",
   packageName: "pkg",
   kinds: new Set(["dimension", "measure"]),
   searches: [],
   listingOnly: false,
   includeCode: false,
   unsupported: [],
   limit: 150,
   offset: 0,
});

/** A direct row, rated by refine (raw set) or not (cosine only). */
function row(
   source: string,
   name: string,
   scores: { cosine: number; level?: number },
): ResultEntity {
   const e = index.directEntities.find(
      (c) => c.source === source && c.name === name,
   );
   if (!e) throw new Error(`no direct entity ${source}.${name}`);
   if (scores.level === undefined) {
      return {
         ...projectEntity(e, "env", "pkg"),
         score: scores.cosine,
         targetScores: new Map([[0, scores.cosine]]),
         bestTarget: 0,
      };
   }
   const raw = scores.level + scores.cosine;
   return {
      ...projectEntity(e, "env", "pkg"),
      level: scores.level,
      raw,
      targetRaw: new Map([[0, raw]]),
      score: mapRawScore(raw),
      targetScores: new Map([[0, mapRawScore(raw)]]),
      bestTarget: 0,
   };
}

function assemble(rows: ResultEntity[]) {
   const ctx = {
      request: request(),
      pkgIndex: index,
      settings: SETTINGS,
   } as unknown as PipelineContext;
   return assembleCards(
      { rows, retrieval: "semantic", belowCutoffCount: 0 },
      ctx,
   );
}

const find = (
   state: ReturnType<typeof assemble>,
   source: string,
   name: string,
) =>
   state.cards
      .find((c) => c.source === source)
      ?.rows.find((r) => r.name === name) as ResultEntity;

describe("assembly scoring when refine ran", () => {
   it("damps the WHOLE raw score of a joined copy, then maps it", () => {
      // HIGH (3) + cosine 0.5 = 3.5. One hop: 3.5 * 0.81 = 2.835, which maps
      // to 0.7 + 0.835 * 0.2 = 0.867 -> 0.87.
      const state = assemble([row("cust", "name", { level: 3, cosine: 0.5 })]);
      const copy = find(state, "inv", "customer.name");
      expect(copy.raw).toBeCloseTo(2.835, 10);
      expect(copy.score).toBe(0.87);
      expect(copy.targetScores?.get(0)).toBe(0.87);
      // Damping the cosine alone would have given 3 + 0.405 = 3.405 -> 0.94.
      expect(copy.score).not.toBe(mapRawScore(3 + 0.5 * 0.81));
   });

   it("does not damp the direct field", () => {
      const state = assemble([row("cust", "name", { level: 3, cosine: 0.5 })]);
      const direct = find(state, "cust", "name");
      expect(direct.raw).toBe(3.5);
      expect(direct.score).toBe(0.95);
   });

   it("damps two hops by 0.9 ** 3 of the raw score", () => {
      const state = assemble([row("cust", "name", { level: 2, cosine: 0.5 })]);
      const copy = find(state, "ord", "region_r.c.name");
      expect(copy.raw).toBeCloseTo(2.5 * 0.729, 10);
      expect(copy.score).toBe(mapRawScore(2.5 * 0.729));
   });

   it("carries the best raw score onto each card", () => {
      const state = assemble([
         row("cust", "name", { level: 3, cosine: 0.5 }),
         row("cust", "id", { level: 2, cosine: 0.9 }),
      ]);
      const card = state.cards.find((c) => c.source === "cust");
      expect(card?.raw).toBe(3.5);
      expect(card?.relevance).toBe(0.95);
   });
});

describe("assembly scoring when refine did not run", () => {
   it("keeps cosine-only damping and rounding to 4 places", () => {
      const state = assemble([row("cust", "name", { cosine: 0.5 })]);
      expect(find(state, "cust", "name").score).toBe(0.5);
      expect(find(state, "inv", "customer.name").score).toBe(0.405);
      expect(find(state, "ord", "region_r.c.name").score).toBe(0.3645);
   });

   it("sets no raw score anywhere", () => {
      const state = assemble([row("cust", "name", { cosine: 0.5 })]);
      for (const card of state.cards) {
         expect(card.raw).toBeUndefined();
         for (const r of card.rows) expect(r.raw).toBeUndefined();
      }
   });
});
