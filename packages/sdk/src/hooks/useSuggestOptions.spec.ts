// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   buildSuggestQuery,
   readOptionValues,
   SUGGEST_OPTION_LIMIT,
} from "./useSuggestOptions";

describe("readOptionValues", () => {
   it("takes the named dimension's column", () => {
      const rows = JSON.stringify([
         { region: "us-east", orders: 12 },
         { region: "us-west", orders: 8 },
      ]);
      expect(readOptionValues(rows, "region")).toEqual(["us-east", "us-west"]);
   });

   it("falls back to the first column", () => {
      // A `suggest { query=… }` names no dimension, and its column can be
      // called anything.
      const rows = JSON.stringify([{ name: "Nike" }, { name: "Levi's" }]);
      expect(readOptionValues(rows, undefined)).toEqual(["Nike", "Levi's"]);
   });

   it("drops nulls and duplicates", () => {
      // "No value" is expressed by clearing the control, not by picking an
      // empty option.
      const rows = JSON.stringify([
         { region: "us-east" },
         { region: null },
         { region: "us-east" },
      ]);
      expect(readOptionValues(rows, "region")).toEqual(["us-east"]);
   });

   it("renders a non-string value as text", () => {
      const rows = JSON.stringify([{ year: 2024 }, { year: 2025 }]);
      expect(readOptionValues(rows, "year")).toEqual(["2024", "2025"]);
   });

   it("is empty for the shapes that legitimately mean no values", () => {
      // `result` is optional in the API contract, so an absent one is a
      // response the server is allowed to send, and an empty list plainly
      // means no values.
      expect(readOptionValues(undefined, undefined)).toEqual([]);
      expect(readOptionValues("[]", undefined)).toEqual([]);
   });

   it("throws on a response it cannot read, rather than reporting no values", () => {
      // This used to return `[]` for both, which collapsed "the query failed"
      // into "this dimension is empty": the control said "No matching values"
      // about a populated dimension. Throwing is what reaches `failed` and
      // puts "Options unavailable" on the control instead.
      expect(() => readOptionValues("not json", undefined)).toThrow();
      expect(() => readOptionValues('{"not":"an array"}', undefined)).toThrow();
   });
});

describe("readOptionValues takes the OUTPUT column name", () => {
   it("finds a joined dimension's column, which is its last segment", () => {
      // `group_by: products.category` names its column `category`. Looking up
      // the full path finds nothing and falls back to the first column, which is
      // right only by luck when the query selects exactly one.
      const rows = JSON.stringify([
         { state: "CA", category: "Outerwear" },
         { state: "NY", category: "Jeans" },
      ]);
      expect(readOptionValues(rows, "category")).toEqual([
         "Outerwear",
         "Jeans",
      ]);
      // The path spelling misses, and takes the wrong column.
      expect(readOptionValues(rows, "products.category")).toEqual(["CA", "NY"]);
   });
});

describe("the generated suggest query", () => {
   // Asserts the text the hook actually sends, by calling the exported builder
   // rather than a copy declared here. Rebuilding the string alongside the
   // implementation would prove only that the test agrees with itself, which is
   // no use for text that reaches a warehouse.
   it("orders by the output field name, not the field path", () => {
      // `order_by: products.category` is refused by the compiler ("order_by
      // takes the name of a field in the query output, not a path"), which would
      // have failed the option query for every joined dimension. Verified against
      // a real DuckDB-backed model: the path form errors, this form returns the
      // 10 categories.
      expect(buildSuggestQuery("order_items", "products.category")).toBe(
         "run: order_items -> {\n" +
            "  group_by: products.category\n" +
            "  order_by: category asc\n" +
            `  limit: ${SUGGEST_OPTION_LIMIT}\n` +
            "}",
      );
   });

   it("is unchanged for a dimension that is already a bare name", () => {
      expect(buildSuggestQuery("products", "category")).toContain(
         "order_by: category asc",
      );
   });

   it("always carries an explicit limit", () => {
      // Otherwise the server's default row cap truncates in whatever order the
      // warehouse returned, silently and differently each time.
      expect(buildSuggestQuery("products", "category")).toContain(
         `limit: ${SUGGEST_OPTION_LIMIT}`,
      );
   });
});
