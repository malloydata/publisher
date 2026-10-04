// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Assembly makes joined copies of ranked direct fields from the join topology
 * and damps them. Runs over the real-compiler fixture (see
 * get_context_join_fixture), so the topology under test is the one the server
 * builds, not a hand-written one.
 */

import { beforeAll, describe, expect, it } from "bun:test";
import {
   compileJoinFixture,
   storeServing,
} from "../../test_helpers/get_context_join_fixture";
import { assembleCards, scopeKeysWithJoins } from "./get_context_assembly";
import type {
   CardState,
   PipelineContext,
   PipelineSettings,
   RankedState,
} from "./get_context_pipeline";
import {
   getPackageIndex,
   projectEntity,
   type PackageIndex,
   type ResolvedRequest,
   type ResultEntity,
} from "./get_context_tool";

const ASSEMBLY: PipelineSettings = {
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

function request(extra: Partial<ResolvedRequest> = {}): ResolvedRequest {
   return {
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
      ...extra,
   };
}

/** A ranked direct row for one field, as the semantic retriever hands it over. */
function ranked(
   source: string,
   name: string,
   score: number,
   modelPath?: string,
): ResultEntity {
   const e = index.directEntities.find(
      (c) => c.source === source && c.name === name,
   );
   if (!e) throw new Error(`no direct entity ${source}.${name}`);
   return {
      ...projectEntity(e, "env", "pkg"),
      ...(modelPath ? { modelPath } : {}),
      score,
      targetScores: new Map([[0, score]]),
      bestTarget: 0,
   };
}

function assemble(
   rows: ResultEntity[],
   opts: {
      settings?: Partial<PipelineSettings>;
      request?: Partial<ResolvedRequest>;
      retrieval?: RankedState["retrieval"];
      entitiesCutBySource?: Map<string, number>;
   } = {},
): CardState {
   const ctx = {
      request: request(opts.request),
      pkgIndex: index,
      settings: { ...ASSEMBLY, ...opts.settings },
   } as unknown as PipelineContext;
   return assembleCards(
      {
         rows,
         retrieval: opts.retrieval ?? "semantic",
         belowCutoffCount: 0,
         entitiesCutBySource: opts.entitiesCutBySource,
      },
      ctx,
   );
}

/** name -> score for one card's rows. */
function cardScores(state: CardState, source: string): Record<string, number> {
   const card = state.cards.find((c) => c.source === source);
   return Object.fromEntries(
      (card?.rows ?? []).map((r) => [r.name, r.score as number]),
   );
}

describe("assembleCards: rows the scan's window cut", () => {
   it("adds the scan's per-source count to that source's card", () => {
      const state = assemble([ranked("cust", "name", 0.5)], {
         entitiesCutBySource: new Map([["cust", 3]]),
      });
      const drops = Object.fromEntries(
         state.cards.map((c) => [c.source, c.entitiesDropped]),
      );
      expect(drops).toEqual({ cust: 3, inv: 0, ord: 0, reg: 0 });
   });

   it("adds nothing when the scan cut nothing", () => {
      const state = assemble([ranked("cust", "name", 0.5)]);
      expect(state.cards.every((c) => c.entitiesDropped === 0)).toBe(true);
   });
});

describe("assembleCards: joined copies", () => {
   it("returns cards for every root that reaches the field, damped per hop", () => {
      const state = assemble([ranked("cust", "name", 0.5)]);
      expect(state.cards.map((c) => c.source).sort()).toEqual([
         "cust",
         "inv",
         "ord",
         "reg",
      ]);
      // The direct field keeps its score.
      expect(cardScores(state, "cust")).toEqual({ name: 0.5 });
      // One join: 0.5 * 0.9 ** 2 = 0.405.
      expect(cardScores(state, "inv")).toEqual({ "customer.name": 0.405 });
      expect(cardScores(state, "reg")).toEqual({ "c.name": 0.405 });
      // ord reaches cust twice by role (buyer, seller) and once through reg:
      // two joins is 0.5 * 0.9 ** 3 = 0.3645.
      expect(cardScores(state, "ord")).toEqual({
         "buyer.name": 0.405,
         "seller.name": 0.405,
         "region_r.c.name": 0.3645,
      });
   });

   it("scores a joined copy at 0.81 of the direct score at one hop and 0.729 at two", () => {
      const state = assemble([ranked("cust", "name", 1)]);
      const ord = cardScores(state, "ord");
      expect(ord["buyer.name"]).toBe(0.81);
      expect(ord["region_r.c.name"]).toBe(0.729);
   });

   it("reaches three joins deep, past what the index copies", () => {
      const state = assemble([ranked("country", "country_code", 1)]);
      const ord = cardScores(state, "ord");
      // buyer.origin is two joins (0.729); region_r.c.origin is three (0.6561).
      expect(ord["buyer.origin.country_code"]).toBe(0.729);
      expect(ord["region_r.c.origin.country_code"]).toBe(0.6561);
      // The index has no copy of that path, so it can only come from here.
      expect(
         index.retrievalEntities.some(
            (e) => e.name === "region_r.c.origin.country_code",
         ),
      ).toBe(false);
   });

   it("stops at the configured depth", () => {
      const state = assemble([ranked("country", "country_code", 1)], {
         settings: { joinMaxDepth: 2 },
      });
      const ord = cardScores(state, "ord");
      expect(ord["buyer.origin.country_code"]).toBe(0.729);
      expect("region_r.c.origin.country_code" in ord).toBe(false);
   });

   it("gives a copy the name, join path, relationship and type the index copies have", () => {
      const state = assemble([ranked("cust", "name", 0.5)]);
      const copy = state.cards
         .find((c) => c.source === "ord")
         ?.rows.find((r) => r.name === "region_r.c.name") as ResultEntity;
      const indexed = index.retrievalEntities.find(
         (e) => e.source === "ord" && e.name === "region_r.c.name",
      );
      expect(indexed).toBeDefined();
      expect(copy.joinPath).toBe(indexed?.joinPath as string);
      expect(copy.relationship).toBe(indexed?.relationship as never);
      expect(copy.dataType).toBe(indexed?.dataType as string);
      expect(copy.doc).toBe(indexed?.doc as string);
      expect(copy.kind).toBe("dimension");
      expect(copy.joinHops).toBe(2);
   });

   it("scores a source by its best field, joined or direct", () => {
      const state = assemble([
         ranked("cust", "name", 0.5),
         ranked("ord", "status", 0.2),
      ]);
      const ord = state.cards.find((c) => c.source === "ord");
      // The joined copy (0.405) beats the direct field (0.2).
      expect(ord?.relevance).toBe(0.405);
      expect(state.cards.map((c) => c.source)).toEqual([
         "cust",
         "inv",
         "ord",
         "reg",
      ]);
   });

   it("damps the per-target scores a copy reports", () => {
      const state = assemble([ranked("cust", "name", 1)]);
      const copy = state.cards
         .find((c) => c.source === "inv")
         ?.rows.find((r) => r.name === "customer.name") as ResultEntity;
      expect([...(copy.targetScores ?? [])]).toEqual([[0, 0.81]]);
      expect(copy.bestTarget).toBe(0);
   });

   it("keeps the higher score when one display name is reached twice", () => {
      // The target source resolvable from two files ranks as two rows.
      const state = assemble([
         ranked("cust", "name", 0.5),
         ranked("cust", "name", 0.3, "other.malloy"),
      ]);
      const inv = state.cards.filter((c) => c.source === "inv");
      // Copies live under the root's own model path, so there is one card.
      expect(inv).toHaveLength(1);
      expect(inv[0].rows.map((r) => [r.name, r.score])).toEqual([
         ["customer.name", 0.405],
      ]);
   });

   it("does not copy views, joins or sources", () => {
      const e = index.directEntities.find((c) => c.kind === "source");
      const row: ResultEntity = {
         ...projectEntity(e!, "env", "pkg"),
         score: 0.9,
      };
      const state = assemble([{ ...row, source: "cust", name: "cust" }]);
      expect(state.cards.map((c) => c.source)).toEqual(["cust"]);
   });

   it("makes no copies on the lexical path", () => {
      const state = assemble([ranked("cust", "name", 0.5)], {
         retrieval: "lexical",
      });
      expect(state.cards.map((c) => c.source)).toEqual(["cust"]);
   });

   it("makes no copies when the index owns them", () => {
      const state = assemble([ranked("cust", "name", 0.5)], {
         settings: { joins: "index" },
      });
      expect(state.cards.map((c) => c.source)).toEqual(["cust"]);
   });

   it("leaves a copy undamped when no damping is configured", () => {
      const state = assemble([ranked("cust", "name", 0.5)], {
         settings: { joinDamping: null },
      });
      expect(cardScores(state, "inv")).toEqual({ "customer.name": 0.5 });
   });
});

describe("assembleCards: scope", () => {
   it("drops the target's own card when the scope is a root source", () => {
      const state = assemble([ranked("cust", "name", 0.5)], {
         request: { sourceName: "ord" },
      });
      expect(state.cards.map((c) => c.source)).toEqual(["ord"]);
      expect(Object.keys(cardScores(state, "ord")).sort()).toEqual([
         "buyer.name",
         "region_r.c.name",
         "seller.name",
      ]);
   });

   it("applies the model path to the root a copy lands in", () => {
      const state = assemble([ranked("cust", "name", 0.5)], {
         request: { modelPath: "elsewhere.malloy" },
      });
      expect(state.cards).toEqual([]);
   });

   it("searches the target fields a scoped source reaches", () => {
      const keys = scopeKeysWithJoins(
         index.directEntities,
         index.topology,
         request({ sourceName: "inv" }),
         10,
      );
      const bySource = (s: string) =>
         keys.filter((k) => k.source === s).map((k) => k.name);
      // inv's own fields, plus the fields of cust and country it reaches.
      expect(bySource("inv")).toContain("ref");
      expect(bySource("cust")).toContain("name");
      expect(bySource("country")).toContain("country_code");
      // Not a source inv cannot reach.
      expect(bySource("ord")).toEqual([]);
      expect(bySource("reg")).toEqual([]);
   });
});

describe("assembleCards: a pinned dotted entity_name", () => {
   // entity_name pins one field by its full name. A joined field's name is the
   // dotted path, so a caller pins `buyer.name`, not `name`. This is the same
   // on the semantic path as on the lexical one.
   it("searches the target field a dotted name reaches", () => {
      const keys = scopeKeysWithJoins(
         index.directEntities,
         index.topology,
         request({ sourceName: "ord", entityName: "buyer.name" }),
         10,
      );
      expect(
         keys.filter((k) => k.source === "cust").map((k) => k.name),
      ).toEqual(["name"]);
   });

   it("returns that joined field and no other copy of the target field", () => {
      const state = assemble([ranked("cust", "name", 0.5)], {
         request: { sourceName: "ord", entityName: "buyer.name" },
      });
      expect(state.cards.map((c) => c.source)).toEqual(["ord"]);
      expect(Object.keys(cardScores(state, "ord"))).toEqual(["buyer.name"]);
   });

   it("does not match a joined field by its bare name", () => {
      const state = assemble([ranked("cust", "name", 0.5)], {
         request: { sourceName: "ord", entityName: "name" },
      });
      expect(state.cards).toEqual([]);
   });
});
