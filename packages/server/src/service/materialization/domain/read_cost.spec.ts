// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { byReadCost } from "./read_cost";

const rollup = (name: string, grain: string[], storage?: string) => ({
   rollupSourceName: name,
   grainDimensions: grain,
   ...(storage !== undefined ? { storage } : {}),
});

describe("byReadCost", () => {
   it("tries a coarser rollup first, then a database one before a storage= one, then by name", () => {
      const plans = [
         rollup("d_fine_database", ["a", "b"]),
         rollup("c_coarse_storage", ["a"], "lake"),
         rollup("b_coarse_database", ["b"]),
         rollup("a_coarse_storage", ["c"], "lake"),
      ];
      expect(
         [...plans].sort(byReadCost).map((p) => p.rollupSourceName),
      ).toEqual([
         "b_coarse_database",
         "a_coarse_storage",
         "c_coarse_storage",
         "d_fine_database",
      ]);
   });

   it("keeps the plan's own order where tiers agree: fewest grain dimensions, then name", () => {
      const plans = [
         rollup("z", ["a"]),
         rollup("y", ["a", "b"]),
         rollup("x", ["b"]),
      ];
      expect(
         [...plans].sort(byReadCost).map((p) => p.rollupSourceName),
      ).toEqual(["x", "z", "y"]);
   });
});
