// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { rrfFuse, type TargetScores } from "./hybrid";

const scores = (o: Record<string, number>, target = 0): TargetScores =>
   new Map(Object.entries(o).map(([k, v]) => [k, new Map([[target, v]])]));

describe("rrfFuse", () => {
   it("gives a row first in both lists a score of exactly 1", () => {
      const out = rrfFuse({
         semantic: scores({ a: 0.9, b: 0.5 }),
         lexical: scores({ a: 3, b: 1 }),
         k: 60,
         mode: "union",
      });
      expect(out.get("a")).toBe(1);
      expect(out.get("b")!).toBeLessThan(1);
   });

   it("lifts a row that both lists like above one only cosine liked", () => {
      const out = rrfFuse({
         semantic: scores({ a: 0.9, b: 0.8, c: 0.7 }),
         lexical: scores({ c: 5 }),
         k: 60,
         mode: "rerank-only",
      });
      // c is 3rd by cosine but 1st by lunr: 1/63 + 1/61 beats b's 1/62 alone.
      expect(out.get("c")!).toBeGreaterThan(out.get("b")!);
   });

   it("rerank-only never admits a row the embedding search did not find", () => {
      const out = rrfFuse({
         semantic: scores({ a: 0.9 }),
         lexical: scores({ z: 9, a: 1 }),
         k: 60,
         mode: "rerank-only",
      });
      expect([...out.keys()]).toEqual(["a"]);
   });

   it("union admits a lexical-only row", () => {
      const out = rrfFuse({
         semantic: scores({ a: 0.9 }),
         lexical: scores({ z: 9 }),
         k: 60,
         mode: "union",
      });
      expect(out.has("z")).toBe(true);
   });

   it("ranks each target on its own, so a busy target does not sink a quiet one", () => {
      const semantic: TargetScores = new Map([
         ["a", new Map([[0, 0.9]])],
         ["b", new Map([[0, 0.8]])],
         ["c", new Map([[0, 0.7]])],
         ["d", new Map([[1, 0.4]])],
      ]);
      const out = rrfFuse({ semantic, lexical: new Map(), k: 60, mode: "union" });
      // d is first for target 1, exactly as a is for target 0.
      expect(out.get("d")).toBe(out.get("a"));
   });

   it("is empty when there is nothing to fuse", () => {
      expect(rrfFuse({ semantic: new Map(), lexical: new Map(), k: 60, mode: "union" }).size).toBe(0);
   });

   it("smaller k weights the top ranks more", () => {
      const args = { semantic: scores({ a: 0.9, b: 0.8 }), lexical: new Map(), mode: "union" as const };
      const sharp = rrfFuse({ ...args, k: 1 });
      const flat = rrfFuse({ ...args, k: 100 });
      expect(sharp.get("b")! / sharp.get("a")!).toBeLessThan(flat.get("b")! / flat.get("a")!);
   });
});
