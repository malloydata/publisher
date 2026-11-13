// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { isolateCompilable } from "./model_serve_shape";

/** A `fits` over strings that fails while any of `bad` is in the candidate. */
function failingOn(bad: Set<string>, calls: string[][] = []) {
   return async (candidate: readonly string[]) => {
      calls.push([...candidate]);
      const hit = candidate.find((c) => bad.has(c));
      return hit ? new Error(`'${hit}' is not defined`) : undefined;
   };
}

const views = (n: number) => Array.from({ length: n }, (_, i) => `v${i}`);

describe("isolateCompilable", () => {
   it("keeps every item but the bad one, in order, with its own error", async () => {
      const out = await isolateCompilable(
         views(8),
         failingOn(new Set(["v5"])),
         { remaining: 100 },
      );
      expect(out.kept).toEqual(["v0", "v1", "v2", "v3", "v4", "v6", "v7"]);
      expect(out.dropped.map((d) => [d.item, d.error?.message])).toEqual([
         ["v5", "'v5' is not defined"],
      ]);
   });

   it("finds one bad item among many in a few calls, not one per item", async () => {
      const calls: string[][] = [];
      const budget = { remaining: 100 };
      await isolateCompilable(
         views(64),
         failingOn(new Set(["v40"]), calls),
         budget,
      );
      // All 64 at once, then a halving path to the one: about 2·log2(64).
      expect(calls.length).toBeLessThanOrEqual(13);
      expect(budget.remaining).toBe(100 - calls.length);
   });

   it("tries each candidate with everything kept so far, so a later item can use an earlier one", async () => {
      // v3 compiles only when v1 is already kept.
      const fits = async (candidate: readonly string[]) =>
         candidate.includes("v3") && !candidate.includes("v1")
            ? new Error("'v1' is not defined")
            : candidate.includes("v2")
              ? new Error("'x' is not defined")
              : undefined;
      const out = await isolateCompilable(views(4), fits, { remaining: 100 });
      expect(out.kept).toEqual(["v0", "v1", "v3"]);
      expect(out.dropped.map((d) => d.item)).toEqual(["v2"]);
   });

   it("uses a known failure instead of trying everything again", async () => {
      const calls: string[][] = [];
      await isolateCompilable(
         ["a"],
         failingOn(new Set(["a"]), calls),
         { remaining: 100 },
         new Error("known"),
      );
      expect(calls).toEqual([]);
   });

   it("drops what it never reached, with no error, once the budget is spent", async () => {
      const out = await isolateCompilable(
         views(8),
         failingOn(new Set(views(8))),
         { remaining: 3 },
      );
      expect(out.kept).toEqual([]);
      expect(out.dropped).toHaveLength(8);
      expect(
         out.dropped.filter((d) => d.error === undefined).length,
      ).toBeGreaterThan(0);
   });
});
