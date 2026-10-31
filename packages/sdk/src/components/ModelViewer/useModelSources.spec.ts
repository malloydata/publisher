// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { fetchCompiledModel } from "./useModelSources";

// The compiled-model shaping is shared by two hooks under one cache key, and it
// is where the wire's shape is normalized: `sourceText` must be a STRING to be
// the file's text. Through the router a declared-but-unsent field arrives as
// `null`, and a cast would have let that through to `.replace(...)`.
function clientReturning(data: unknown) {
   return {
      models: { getModel: async () => ({ data }) },
   } as unknown as Parameters<typeof fetchCompiledModel>[0];
}

describe("fetchCompiledModel", () => {
   it("keeps a string sourceText and reads nothing else as the text", async () => {
      const text = "source: flights is duckdb.table('flights.parquet')\n";
      expect(
         (
            await fetchCompiledModel(
               clientReturning({ sourceText: text }),
               "p",
               "k",
               "m.malloy",
            )
         ).sourceText,
      ).toBe(text);
      expect(
         (
            await fetchCompiledModel(
               clientReturning({ sourceText: null }),
               "p",
               "k",
               "m.malloy",
            )
         ).sourceText,
      ).toBeUndefined();
      expect(
         (await fetchCompiledModel(clientReturning({}), "p", "k", "m.malloy"))
            .sourceText,
      ).toBeUndefined();
      expect(
         (
            await fetchCompiledModel(
               clientReturning({ sourceText: 42 }),
               "p",
               "k",
               "m.malloy",
            )
         ).sourceText,
      ).toBeUndefined();
   });

   it("parses sourceInfos, carries queries, and shape-checks givens", async () => {
      const data = {
         sourceInfos: [
            JSON.stringify({ name: "flights", schema: { fields: [] } }),
         ],
         queries: [{ name: "q1" }],
         givens: [
            { name: "ROLE", type: "string", default: "'analyst'" },
            { bogus: true },
         ],
      };
      const out = await fetchCompiledModel(
         clientReturning(data),
         "p",
         "k",
         "m.malloy",
      );
      expect(out.sourceInfos.map((s) => s.name)).toEqual(["flights"]);
      expect(out.queries).toEqual([{ name: "q1" }]);
      expect(out.givens.map((g) => [g.name, g.default])).toEqual([
         ["ROLE", "'analyst'"],
      ]);
   });
});

describe("fetchCompiledModel access declarations", () => {
   it("keeps each source's #(authorize) and #(access_filter) rather than dropping sources[]", async () => {
      const data = {
         sources: [
            { name: "open", authorize: [], accessFilter: [] },
            { name: "payroll", authorize: ["'finance' in $GROUPS"] },
            {
               name: "regional",
               accessFilter: ["region in $GROUPS"],
               authorize: null,
            },
         ],
      };
      const out = await fetchCompiledModel(
         clientReturning(data),
         "p",
         "k",
         "m.malloy",
      );
      expect(out.access.get("payroll")).toEqual({
         authorize: ["'finance' in $GROUPS"],
         accessFilter: [],
      });
      expect(out.access.get("regional")).toEqual({
         authorize: [],
         accessFilter: ["region in $GROUPS"],
      });
      expect(out.access.has("open")).toBe(false);
   });
});
