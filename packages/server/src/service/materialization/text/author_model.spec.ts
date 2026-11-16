// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// One view of a compiled author model for code that reads its text.

import { describe, expect, it } from "bun:test";

import { authorModelLiftContext, sliceSourceRange } from "./author_model";

describe("sliceSourceRange", () => {
   const src =
      "line0\nsource: orders is x extend {\n  join_one: r is regions on a = r.a\n}\n";
   it("slices a single-line range (the join declaration)", () => {
      // Recover `r is regions on a = r.a` from line 2.
      expect(
         sliceSourceRange(src, {
            start: { line: 2, character: 12 },
            end: { line: 2, character: 35 },
         }),
      ).toBe("r is regions on a = r.a");
   });
   it("slices a multi-line range", () => {
      expect(
         sliceSourceRange(src, {
            start: { line: 1, character: 8 },
            end: { line: 3, character: 1 },
         }),
      ).toBe("orders is x extend {\n  join_one: r is regions on a = r.a\n}");
   });
   it("returns undefined for an out-of-bounds range (stale source)", () => {
      expect(
         sliceSourceRange("short", {
            start: { line: 0, character: 0 },
            end: { line: 9, character: 0 },
         }),
      ).toBeUndefined();
   });
});

describe("authorModelLiftContext sees the registry's hidden dependencies", () => {
   it("adds a hidden dependency under its declared name, namespace winning a clash", () => {
      const ctx = authorModelLiftContext(
         {
            contents: {
               weekly: {
                  sourceID: "weekly@file:///p/orders.malloy",
                  type: "query_source",
                  query: { structRef: "daily@file:///p/orders.malloy" },
               },
            },
            sourceRegistry: {
               "weekly@file:///p/orders.malloy": {
                  entry: { type: "source_registry_reference", name: "weekly" },
               },
               "daily@file:///p/orders.malloy": {
                  entry: {
                     type: "query_source",
                     as: "daily",
                     name: "QuerySource-uuid",
                     sourceID: "daily@file:///p/orders.malloy",
                     query: { structRef: "t@file:///p/orders.malloy" },
                  },
               },
               "t@file:///p/orders.malloy": {
                  entry: {
                     type: "sql_select",
                     as: "t",
                     name: "sql://x",
                     sourceID: "t@file:///p/orders.malloy",
                  },
               },
               "weekly@file:///p/other.malloy": {
                  entry: {
                     type: "query_source",
                     as: "weekly",
                     name: "QuerySource-other",
                     sourceID: "weekly@file:///p/other.malloy",
                  },
               },
            },
         },
         () => undefined,
      );
      expect(Object.keys(ctx.contents).sort()).toEqual([
         "daily",
         "t",
         "weekly",
      ]);
      expect(ctx.sourceNameById.get("daily@file:///p/orders.malloy")).toBe(
         "daily",
      );
      expect(ctx.sourceNameById.get("weekly@file:///p/orders.malloy")).toBe(
         "weekly",
      );
      expect(
         ctx.sourceNameById.get("weekly@file:///p/other.malloy"),
      ).toBeUndefined();
   });
});
