// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A field the index keeps because assembly cannot rebuild it (`lines.total` on
 * `cust`, from a join to an inline table) is still reached through a join from
 * another source (`ord` joins `cust` as `buyer`). Assembly copies it into `ord`
 * as `buyer.lines.total`. The copy has to say what the path really is:
 *
 * - its join path is both joins, `buyer.lines`, not only the outer `buyer`;
 * - its relationship is the widest of the two: `ord -> buyer` is one but
 *   `cust -> lines` is many, so the copy fans out;
 * - the index already holds `ord`'s own `buyer.lines.total` (assembly cannot
 *   rebuild it either), so when that row is ranked the copy is not added beside it.
 */

import { beforeAll, describe, expect, it } from "bun:test";
import {
   compileModelFiles,
   storeServing,
} from "../../test_helpers/get_context_join_fixture";
import { assembleCards } from "./get_context_assembly";
import type { PipelineContext, PipelineSettings } from "./get_context_pipeline";
import {
   getPackageIndex,
   projectEntity,
   type PackageIndex,
   type ResolvedRequest,
   type ResultEntity,
} from "./get_context_tool";

const SETTINGS: PipelineSettings = {
   joins: "assembly",
   entityWindow: { perSourcePerTarget: 10 },
   joinMaxDepth: 10,
   joinDamping: 0.9,
   scoring: "cosine",
   maxChars: null,
   reserveChars: 1_000,
};

const MODEL = `
source: cust is duckdb.sql("select 1 as id, 'a' as nm") extend {
  dimension: name is nm
  join_many: lines is duckdb.sql("select 1 as id, 1 as cust_id, 5 as amt") extend {
    #(doc) Line total.
    dimension: total is amt
  } on lines.cust_id = id
}

source: ord is duckdb.sql("select 1 as id, 1 as cust_id") extend {
  dimension: status is 's'
  join_one: buyer is cust on buyer.id = cust_id
}
`;

let index: PackageIndex;
beforeAll(async () => {
   const { pkg } = await compileModelFiles({ "m.malloy": MODEL });
   index = await getPackageIndex(storeServing(pkg), "env", "pkg");
});

function ranked(source: string, name: string, score: number): ResultEntity {
   const e = index.directEntities.find(
      (c) => c.source === source && c.name === name,
   );
   if (!e) throw new Error(`no indexed entity ${source}.${name}`);
   return {
      ...projectEntity(e, "env", "pkg"),
      score,
      targetScores: new Map([[0, score]]),
      bestTarget: 0,
   };
}

function assemble(rows: ResultEntity[]) {
   const request: ResolvedRequest = {
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
   };
   const ctx = {
      request,
      pkgIndex: index,
      settings: SETTINGS,
   } as unknown as PipelineContext;
   return assembleCards(
      { rows, retrieval: "semantic", belowCutoffCount: 0 },
      ctx,
   );
}

const rowsOf = (state: ReturnType<typeof assemble>, source: string) =>
   state.cards.find((c) => c.source === source)?.rows ?? [];

describe("the index keeps both rows (the premise)", () => {
   it("indexes lines.total on cust and buyer.lines.total on ord", () => {
      const cust = index.directEntities.find(
         (e) => e.source === "cust" && e.name === "lines.total",
      );
      const ord = index.directEntities.find(
         (e) => e.source === "ord" && e.name === "buyer.lines.total",
      );
      expect(cust?.joinPath).toBe("lines");
      expect(cust?.relationship).toBe("many");
      expect(ord?.joinPath).toBe("buyer.lines");
      expect(ord?.relationship).toBe("many");
   });
});

describe("a copy of a retained dotted field", () => {
   it("takes both joins as its path, and the widest relationship", () => {
      const state = assemble([ranked("cust", "lines.total", 0.8)]);
      const copy = rowsOf(state, "ord").find(
         (r) => r.name === "buyer.lines.total",
      );
      expect(copy).toBeDefined();
      expect(copy?.joinPath).toBe("buyer.lines");
      // ord -> buyer is `one`; cust -> lines is `many`: the copy fans out.
      expect(copy?.relationship).toBe("many");
      // Two joins crossed, damped as two (0.9 ** 3 = 0.729).
      expect(copy?.joinHops).toBe(2);
      expect(copy?.score).toBeCloseTo(0.8 * 0.729, 4);
   });

   it("a direct field's copy is unchanged: one join, the outer relationship", () => {
      const state = assemble([ranked("cust", "name", 0.8)]);
      const copy = rowsOf(state, "ord").find((r) => r.name === "buyer.name");
      expect(copy?.joinPath).toBe("buyer");
      expect(copy?.relationship).toBe("one");
      expect(copy?.joinHops).toBe(1);
   });

   it("is not added beside the index's own row for the same field", () => {
      const state = assemble([
         ranked("cust", "lines.total", 0.8),
         ranked("ord", "buyer.lines.total", 0.5),
      ]);
      const same = rowsOf(state, "ord").filter(
         (r) => r.name === "buyer.lines.total",
      );
      expect(same).toHaveLength(1);
      // The index's row wins: its own score and metadata, not a rebuilt copy.
      expect(same[0].score).toBe(0.5);
      expect(same[0].joinPath).toBe("buyer.lines");
      expect(same[0].relationship).toBe("many");
   });

   it("respects the deepest join chain assembly follows", () => {
      const ctx = {
         request: {
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
         },
         pkgIndex: index,
         settings: { ...SETTINGS, joinMaxDepth: 1 },
      } as unknown as PipelineContext;
      const state = assembleCards(
         {
            rows: [ranked("cust", "lines.total", 0.8)],
            retrieval: "semantic",
            belowCutoffCount: 0,
         },
         ctx,
      );
      // buyer.lines is two joins deep: beyond a limit of 1.
      expect(
         state.cards
            .find((c) => c.source === "ord")
            ?.rows.some((r) => r.name === "buyer.lines.total") ?? false,
      ).toBe(false);
   });
});
