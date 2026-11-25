// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { rowsQuery, sourceOf, stepsOf } from "./RowsDialog";

describe("the rows query", () => {
   it("filters the source by the tile's filters and each dimension of the row", () => {
      const target = {
         source: "overview",
         filters: ["region ~ $REGION", "is_online = true"],
      };
      expect(
         rowsQuery({
            target,
            field: "name",
            rawValue: "x",
            label: "x",
            dimensions: [
               {
                  code: "products.name",
                  value: { kind: "string_literal", string_value: "O'Brien" },
               },
               { code: "region", value: { kind: "null_literal" } },
               {
                  code: "sold_at.hour",
                  value: {
                     kind: "timestamp_literal",
                     timestamp_value: "2026-02-03T23:00:00Z",
                  },
               },
            ],
            timezone: "Asia/Tokyo",
         }),
      ).toBe(
         "run: overview -> { timezone: 'Asia/Tokyo'; where: region ~ $REGION, is_online = true, products.name = 'O\\'Brien', region is null, sold_at.hour = @2026-02-04 08:00:00; select: *; limit: 200 }",
      );
   });

   it("writes a click with no row by its own value, and nothing it cannot spell", () => {
      const target = { source: "overview", filters: [] };
      expect(
         rowsQuery({
            target,
            field: "name",
            rawValue: "Loft Insulated Jacket",
            label: "Loft Insulated Jacket",
            code: "products.name",
         }),
      ).toBe(
         "run: overview -> { where: products.name = 'Loft Insulated Jacket'; select: *; limit: 200 }",
      );
      expect(
         rowsQuery({ target, field: "f", rawValue: {}, label: "", code: "f" }),
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
