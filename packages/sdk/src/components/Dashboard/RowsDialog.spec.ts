// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { rowsQuery, sourceOf, stepsOf } from "./RowsDialog";

describe("the rows query", () => {
   it("drills through the tile's view to the value's rows", () => {
      expect(
         rowsQuery({
            source: "overview",
            view: "best_sellers",
            field: "name",
            rawValue: "Loft Insulated Jacket",
            label: "Loft Insulated Jacket",
         }),
      ).toBe(
         "run: overview -> { drill: best_sellers.name = 'Loft Insulated Jacket'; select: *; limit: 200 }",
      );
      expect(
         rowsQuery({
            source: "a",
            view: "v",
            field: "f",
            rawValue: {},
            label: "",
         }),
      ).toBeUndefined();
   });

   it("reads the source and view off a tile expression, and nothing off any other shape", () => {
      expect(stepsOf("overview -> revenue_trend")).toEqual({
         source: "overview",
         view: "revenue_trend",
      });
      expect(sourceOf("overview->kpis")).toBe("overview");
      expect(stepsOf("{ group_by: x } -> y")).toBeUndefined();
      expect(stepsOf("a -> b -> c")).toBeUndefined();
      expect(stepsOf("a -> b + { limit: 2 }")).toBeUndefined();
      expect(stepsOf(undefined)).toBeUndefined();
   });
});
