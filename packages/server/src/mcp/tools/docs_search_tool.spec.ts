import { describe, expect, it } from "bun:test";
import { sanitize, searchDocsIndex } from "./docs_search_tool";

describe("docs_search sanitize", () => {
   it("strips lunr operator characters", () => {
      expect(sanitize('window:functions +lag -"exact"')).not.toMatch(
         /[~^:*+\-"]/,
      );
   });
});

describe("docs_search searchDocsIndex (bundled index)", () => {
   it("returns relevant matches for a common Malloy term", () => {
      const results = searchDocsIndex("source", 8);
      expect(results.length).toBeGreaterThan(0);
      expect(results.length).toBeLessThanOrEqual(8);
      for (const r of results) {
         expect(typeof r.title).toBe("string");
         expect(r.url.length).toBeGreaterThan(0);
         expect(typeof r.excerpt).toBe("string");
      }
   });

   it("respects the limit", () => {
      expect(searchDocsIndex("query", 3).length).toBeLessThanOrEqual(3);
   });

   it("returns [] for an empty or operator-only query without throwing", () => {
      expect(searchDocsIndex("", 8)).toEqual([]);
      expect(searchDocsIndex('~ ^ : * + - "', 8)).toEqual([]);
   });
});
