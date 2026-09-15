// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { malloyLiteral, rowsQuery, sourceOf, stepsOf } from "./RowsDialog";

describe("the rows query", () => {
   it("spells each kind of clicked value as Malloy", () => {
      expect(malloyLiteral("Jeans")).toBe("'Jeans'");
      expect(malloyLiteral("Ben's & Jerry\\s")).toBe("'Ben\\'s & Jerry\\\\s'");
      expect(malloyLiteral(42)).toBe("42");
      expect(malloyLiteral(true)).toBe("true");
      expect(malloyLiteral(new Date("2024-03-05T00:00:00Z"))).toBe(
         "@2024-03-05",
      );
      expect(malloyLiteral(new Date("2024-03-05T13:45:00Z"))).toBe(
         "@2024-03-05 13:45:00",
      );
      expect(malloyLiteral(null)).toBeUndefined();
      expect(malloyLiteral(Number.NaN)).toBeUndefined();
   });

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
      expect(stepsOf(undefined)).toBeUndefined();
   });
});
