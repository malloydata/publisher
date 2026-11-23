// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The rollup leg reads which rollups a query's source could answer from
 * (its own, through its bases, through a join) from the list built with the
 * plans, not by walking the source map per query: a query walks nothing
 * unless no list was built, and that walk is counted.
 */

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { clearQueryWalks, queryWalks } from "./admission_parity";
import { RollupVariantServe } from "./rollup_variants";

const PLAN = {
   baseSourceID: "orders@m",
   rollupMeasures: [{ name: "n" }],
};

function serveWith(
   candidates: Map<string, unknown> | undefined,
   walked: unknown,
): Record<string, unknown> {
   const serve = Object.create(RollupVariantServe.prototype) as Record<
      string,
      unknown
   >;
   serve.plansByBase = () => new Map();
   serve.planMemo = candidates ? { candidates } : {};
   let walks = 0;
   serve.walkCandidates = () => {
      walks++;
      return walked;
   };
   serve.walks = () => walks;
   return serve;
}

function candidatesFor(serve: Record<string, unknown>, id: string): unknown {
   return (serve.candidatesFor as (id: string) => unknown).call(serve, id);
}

describe("the rollup leg's candidate list", () => {
   // Other specs in the run may have walked already.
   beforeEach(() => clearQueryWalks());
   afterEach(() => clearQueryWalks());

   it("is looked up per query, walking nothing", async () => {
      const own = { target: { plans: [PLAN] }, branches: [] };
      const serve = serveWith(new Map([["orders@m", own]]), undefined);
      expect(candidatesFor(serve, "orders@m")).toBe(own);
      // A source the list leaves out has no rollup.
      expect(candidatesFor(serve, "items@m")).toEqual({ branches: [] });
      expect((serve.walks as () => number)()).toBe(0);
      expect(queryWalks().get("rollup_candidates")).toBeUndefined();
   });

   it("walks, and counts the walk, when no list was built", async () => {
      const walked = { branches: [] };
      const serve = serveWith(undefined, walked);
      expect(candidatesFor(serve, "orders@m")).toBe(walked);
      expect(queryWalks().get("rollup_candidates")).toBe(1);
   });

   describe("what a candidate source declines by before its shape is read", () => {
      const candidates = { target: { plans: [PLAN] }, branches: [] };
      function staticsFor(
         serve: Record<string, unknown>,
         struct: object,
      ): unknown {
         return (
            serve.staticsFor as (
               id: string,
               struct: object,
               modelDef: unknown,
               c: unknown,
            ) => unknown
         ).call(serve, "orders@m", struct, { contents: {} }, candidates);
      }

      it("is looked up per query, walking nothing", async () => {
         const recorded = { timeZoned: true, branchGates: [] };
         const serve = serveWith(new Map(), undefined);
         serve.planMemo = { statics: new Map([["orders@m", recorded]]) };
         expect(staticsFor(serve, {})).toBe(recorded);
         expect(queryWalks().get("rollup_candidates")).toBeUndefined();
      });

      it("walks the query's own compile, and counts it, when none was recorded", async () => {
         const serve = serveWith(new Map(), undefined);
         serve.planMemo = { statics: new Map() };
         const walked = staticsFor(serve, {}) as { timeZoned: boolean };
         expect(walked.timeZoned).toBe(false);
         expect(queryWalks().get("rollup_candidates")).toBe(1);
      });
   });
});
