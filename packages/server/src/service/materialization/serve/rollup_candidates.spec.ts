// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The rollup leg reads which rollups a query's source could answer from
 * (its own, through its bases, through a join) from the list built with the
 * plans, not by walking the source map per query: under `lookup` a query
 * walks nothing; under `shadow` a list that disagrees with the walk is drift.
 */

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import {
   clearParityDrifts,
   clearQueryWalks,
   queryWalks,
   withAdmissionMode,
} from "../../../test_helpers/admission_parity";
import { parityDrifts } from "./admission_parity";
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
   // Other specs in the run may have walked or drifted already.
   beforeEach(() => {
      clearParityDrifts();
      clearQueryWalks();
   });
   afterEach(() => {
      clearParityDrifts();
      clearQueryWalks();
   });

   it("is looked up per query, walking nothing", async () => {
      const own = { target: { plans: [PLAN] }, branches: [] };
      const serve = serveWith(new Map([["orders@m", own]]), undefined);
      await withAdmissionMode("lookup", async () => {
         expect(candidatesFor(serve, "orders@m")).toBe(own);
         // A source the list leaves out has no rollup.
         expect(candidatesFor(serve, "items@m")).toEqual({ branches: [] });
      });
      expect((serve.walks as () => number)()).toBe(0);
      expect(queryWalks().get("rollup_candidates")).toBeUndefined();
   });

   it("walks, and counts the walk, when no list was built", async () => {
      const walked = { branches: [] };
      const serve = serveWith(undefined, walked);
      await withAdmissionMode("lookup", async () => {
         expect(candidatesFor(serve, "orders@m")).toBe(walked);
      });
      expect(queryWalks().get("rollup_candidates")).toBe(1);
   });

   it("answers the walk in shadow, and records drift when the list differs", async () => {
      const walked = { target: { plans: [PLAN] }, branches: [] };
      const serve = serveWith(new Map(), walked);
      await withAdmissionMode("shadow", async () => {
         expect(candidatesFor(serve, "orders@m")).toBe(walked);
      });
      expect(parityDrifts().map((d) => [d.side, d.lookup])).toEqual([
         ["rollup_candidates", undefined],
      ]);
   });
});
