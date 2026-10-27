// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { DimensionSpec } from "./useDimensionalFilterRangeData";
import { buildDimensionalIndexQuery } from "./useDimensionalFilterRangeData";
import type { FilterSelection } from "./useDimensionFilters";

const specs = [
   {
      dimensionName: "category",
      filterType: "Star",
      source: "orders",
      model: "orders.malloy",
   },
] as DimensionSpec[];

function whereOf(filters: FilterSelection[]): string | undefined {
   const query = buildDimensionalIndexQuery("orders", specs, 10, filters);
   return /where: (.*)\n/.exec(query)?.[1];
}

describe("buildDimensionalIndexQuery", () => {
   it("writes a Contains filter as the text it was given, not as alternatives or wildcards", () => {
      const where = whereOf([
         {
            dimensionName: "name",
            matchType: "Contains",
            value: "a,b",
            source: "orders",
         } as FilterSelection,
      ]);
      // The same text `filterCondition` writes for the result rows: the
      // comma escaped, one `contains` clause.
      expect(where).toBe("`name` ~ f'%a\\,b%'");
   });

   it("spells a date on Greater Than and Less Than as a date", () => {
      const where = whereOf([
         {
            dimensionName: "ordered_at",
            matchType: "Greater Than",
            value: new Date(2024, 0, 5),
            source: "orders",
         } as FilterSelection,
         {
            dimensionName: "ordered_at",
            matchType: "Less Than",
            value: new Date(2024, 1, 5),
            source: "orders",
         } as FilterSelection,
      ]);
      expect(where).toBe(
         "`ordered_at` > @2024-01-05 and `ordered_at` < @2024-02-05",
      );
   });
});
