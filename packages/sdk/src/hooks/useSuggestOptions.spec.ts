// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   buildSuggestQuery,
   malloyStringLiteral,
   readOptionPage,
   readOptionValues,
   SUGGEST_OPTION_LIMIT,
   SUGGEST_SEARCH_LIMIT,
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
            // One past the limit, so a list that was cut off is known to be.
            `  limit: ${SUGGEST_OPTION_LIMIT + 1}\n` +
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
         `limit: ${SUGGEST_OPTION_LIMIT + 1}`,
      );
   });
});

describe("a search of a cut-off list", () => {
   it("matches ignoring case, as a plain substring, with a limit and the same order", () => {
      expect(buildSuggestQuery("recalls", "maker.name", "General Motors")).toBe(
         "run: recalls -> {\n" +
            "  group_by: maker.name\n" +
            "  where: strpos(lower(maker.name::string), 'general motors') > 0\n" +
            "  order_by: name asc\n" +
            `  limit: ${SUGGEST_SEARCH_LIMIT + 1}\n` +
            "}",
      );
   });
});

describe("malloyStringLiteral: typed text can never leave the literal", () => {
   it("escapes a single quote", () => {
      expect(malloyStringLiteral("Levi's")).toBe("'Levi\\'s'");
   });

   it("escapes a backslash before the quote, so `\\'` cannot close it", () => {
      // Escaped quote first would turn `\'` into `\\'`: an escaped backslash
      // followed by a bare quote, which ends the literal.
      expect(malloyStringLiteral("a\\'b")).toBe("'a\\\\\\'b'");
      expect(malloyStringLiteral("back\\slash")).toBe("'back\\\\slash'");
   });

   it("leaves regular-expression and LIKE metacharacters as text", () => {
      // Matched with strpos, not a pattern, so none of these means anything.
      const text = ".*+?^$()[]{}|%_";
      expect(malloyStringLiteral(text)).toBe(`'${text}'`);
   });

   it("drops control characters, which a literal cannot hold", () => {
      expect(malloyStringLiteral("a\nb\tc\u0000d")).toBe("'abcd'");
   });

   it("keeps an attempt to close the literal and add Malloy inside it", () => {
      const attack = "x') > 0 or (1 = 1";
      const literal = malloyStringLiteral(attack);
      expect(literal).toBe("'x\\') > 0 or (1 = 1'");
      // One literal: the only unescaped quotes are its own two.
      expect(literal.replace(/\\\\|\\'/g, "").match(/'/g)).toHaveLength(2);
   });
});

describe("readOptionPage: was the list cut off?", () => {
   const rows = (n: number) =>
      JSON.stringify(Array.from({ length: n }, (_, i) => ({ v: `v${i}` })));

   it("is, when a generated query returns the extra row, and keeps only the limit", () => {
      const page = readOptionPage(rows(4), "v", { limit: 3 });
      expect(page).toEqual({ values: ["v0", "v1", "v2"], truncated: true });
   });

   it("is not, at exactly the limit", () => {
      expect(readOptionPage(rows(3), "v", { limit: 3 }).truncated).toBe(false);
   });

   it("is, for a named query that fills the server's cap", () => {
      expect(
         readOptionPage(rows(3), "v", { limit: 3, cutOffAt: 3 }).truncated,
      ).toBe(true);
      expect(
         readOptionPage(rows(2), "v", { limit: 3, cutOffAt: 3 }).truncated,
      ).toBe(false);
   });
});
