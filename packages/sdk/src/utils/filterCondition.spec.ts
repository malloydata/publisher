// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { StringFilterExpression } from "@malloydata/malloy-filter";
import { describe, expect, it } from "bun:test";
import { generateFilterClause } from "../components/filter/utils";
import type { FilterSelection } from "../hooks/useDimensionFilters";
import { filterCondition } from "./filterCondition";

function selection(
   matchType: FilterSelection["matchType"],
   value: FilterSelection["value"],
   value2?: FilterSelection["value2"],
): FilterSelection {
   return { dimensionName: "name", source: "items", matchType, value, value2 };
}

/** The filter a `field ~ f'…'` condition runs: Malloy hands the text between the quotes to this parser as written. */
function parsedFilter(condition: string) {
   const body = /^`name` ~ f'(.*)'$/s.exec(condition)?.[1];
   expect(body).toBeDefined();
   return StringFilterExpression.parse(body!).parsed;
}

describe("a Contains filter", () => {
   // Each of these is special in the string filter grammar: `,` separates
   // alternatives, `%` and `_` are wildcards, a leading `-` negates, `\`
   // escapes, and `null` is a keyword. Spliced into `%…%` as they were, "a,b"
   // matched `a` and `b` but not "a,b", and "50%" also matched "500".
   for (const value of [
      "a,b",
      "50%",
      "x_y",
      "-sale",
      "C:\\temp",
      "it's",
      "null",
      " pad ",
   ]) {
      it(`runs as one contains clause for ${JSON.stringify(value)}`, () => {
         const condition = generateFilterClause(
            [selection("Contains", value)],
            new Map(),
            "items",
         );
         expect(parsedFilter(condition)).toEqual({
            operator: "contains",
            values: [value],
         });
      });
   }
});

describe("filterCondition", () => {
   it("matches any pick of a multi-select, dates as days", () => {
      expect(
         filterCondition(
            "`day`",
            selection("Equals", [new Date(2024, 0, 5), new Date(2024, 0, 6)]),
         ),
      ).toBe("(`day` = @2024-01-05 or `day` = @2024-01-06)");
      expect(filterCondition("`name`", selection("Equals", []))).toBe("");
      expect(filterCondition("`name`", selection("Contains", ["a"]))).toBe("");
   });

   it("writes a date bound as a date for every comparison", () => {
      const day = new Date(2024, 0, 5);
      expect(filterCondition("`day`", selection("Greater Than", day))).toBe(
         "`day` > @2024-01-05",
      );
      expect(filterCondition("`day`", selection("Before", day))).toBe(
         "`day` < @2024-01-05",
      );
      expect(
         filterCondition(
            "`day`",
            selection("Between", day, new Date(2024, 0, 9)),
         ),
      ).toBe("`day` >= @2024-01-05 and `day` <= @2024-01-09");
   });

   it("quotes a string value for an equality", () => {
      expect(filterCondition("`name`", selection("Equals", "it's"))).toBe(
         "`name` = 'it\\'s'",
      );
   });
});
