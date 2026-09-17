// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { artifactLine, isIdentifier, tileSteps } from "./malloyText";

/**
 * What is left here reads the two grammars that are not Malloy's. The shapes
 * the deleted scanners used to cover are pinned end to end in
 * `scannerShapes.spec.ts` instead, so they outlive the scanners.
 */
describe("the tiles=[…] grammar", () => {
   it("splits a tile expression into its steps", () => {
      expect(tileSteps("orders -> by_brand")).toEqual({
         source: "orders",
         view: "by_brand",
      });
      expect(tileSteps("orders->by_brand + { limit: 2 }")).toEqual({
         source: "orders",
         view: "by_brand",
         refinement: "+ { limit: 2 }",
      });
      expect(tileSteps("{ group_by: x } -> y -> z")).toBeUndefined();
      expect(tileSteps(undefined)).toBeUndefined();
   });

   it("accepts only a bare identifier as a name", () => {
      expect(isIdentifier("by_brand")).toBe(true);
      expect(isIdentifier("a.b")).toBe(false);
      expect(isIdentifier("")).toBe(false);
   });
});

describe("the model-level ## lines", () => {
   it("finds the artifact tag, and nothing else", () => {
      const lines = [
         "##! experimental.givens",
         '##" prose',
         '## artifact { title="T" tiles=["a -> x"] }',
         "source: a is b",
      ];
      expect(artifactLine(lines)).toBe(2);
      expect(artifactLine(["source: a is b"])).toBe(-1);
   });
});
