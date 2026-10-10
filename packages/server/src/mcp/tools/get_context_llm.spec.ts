// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { replyArray } from "./get_context_llm";

// Every shape below was returned by OpenAI's json_object mode for the prompts
// that ask for an array, which that mode cannot return at the root.
describe("replyArray", () => {
   it("returns a root array as is", () => {
      expect(replyArray([{ index: 1, score: "HIGH" }])).toEqual([
         { index: 1, score: "HIGH" },
      ]);
   });

   it("unwraps an object with exactly one array-valued key", () => {
      expect(replyArray({ ratings: [{ index: 2, score: "LOW" }] })).toEqual([
         { index: 2, score: "LOW" },
      ]);
   });

   it("treats one element returned bare as an array of that element", () => {
      expect(replyArray({ index: 3, score: "HIGH" })).toEqual([
         { index: 3, score: "HIGH" },
      ]);
   });

   it("treats an empty object as no results", () => {
      expect(replyArray({})).toEqual([]);
   });

   it("still refuses shapes it cannot read", () => {
      for (const bad of [
         "text",
         null,
         7,
         { a: [1], b: [2] },
         { score: "HIGH" },
      ]) {
         expect(() => replyArray(bad)).toThrow("expected a JSON array");
      }
   });
});
