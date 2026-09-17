// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   artifactLine,
   declarationLine,
   declarationsUnder,
   givenDeclarations,
   splitTrailingComment,
   tileSteps,
} from "./malloyText";

const LINES = `##! experimental.givens
## artifact { title="T" tiles=["a -> x"] }
import "../m.malloy"

given: CATEGORY :: filter<string> is f''
given:
  SINCE :: date is @2023-01-01
  MIN :: number is 0

source: a is one extend {
  # drill { to=self }
  dimension: cat is products.category
  view: x is vx + { limit: 5, where: cat ~ $CATEGORY }
  view: y is {
    group_by: cat
  }
}

source: b is two extend {
  view: z is vz
}`.split("\n");

describe("the text a dashboard file is read as", () => {
   it("finds each source's own declarations and where a body ends", () => {
      expect([...declarationsUnder(LINES, "a", "view")]).toEqual([
         ["x", { line: 12, rest: "vx + { limit: 5, where: cat ~ $CATEGORY }" }],
         ["y", { line: 13, rest: "{" }],
      ]);
      expect([...declarationsUnder(LINES, "a", "dimension")]).toEqual([
         ["cat", { line: 11, rest: "products.category" }],
      ]);
      expect([...declarationsUnder(LINES, "b", "view").keys()]).toEqual(["z"]);
      expect(declarationLine(LINES, "source", "b")).toBe(18);
      expect(declarationLine(LINES, "view", "missing")).toBe(-1);
      expect(artifactLine(LINES)).toBe(1);
   });

   it("reads givens in both spellings, remembering a block's header", () => {
      expect([...givenDeclarations(LINES)]).toEqual([
         [
            "CATEGORY",
            { line: 4, declaration: "CATEGORY :: filter<string> is f''" },
         ],
         [
            "SINCE",
            {
               line: 6,
               blockHeader: 5,
               declaration: "SINCE :: date is @2023-01-01",
            },
         ],
         [
            "MIN",
            { line: 7, blockHeader: 5, declaration: "MIN :: number is 0" },
         ],
      ]);
   });

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
});

describe("splitTrailingComment", () => {
   it("finds no comment in a plain line", () => {
      expect(splitTrailingComment("  view: x is y")).toEqual({
         code: "  view: x is y",
         comment: "",
      });
   });

   it("keeps the gap with the code so an unchanged line stays unchanged", () => {
      const { code, comment } = splitTrailingComment("  view: x is y  // note");
      expect(code).toBe("  view: x is y  ");
      expect(comment).toBe("// note");
      expect(code + comment).toBe("  view: x is y  // note");
   });

   // Both spellings a dashboard reaches for: a filter literal is single-quoted,
   // and a SQL block is triple-double-quoted and spans lines.
   it("does not split inside a quoted literal", () => {
      expect(splitTrailingComment("where: path ~ 'a//b'").comment).toBe("");
      expect(splitTrailingComment('label="http://x"').comment).toBe("");
      expect(splitTrailingComment('sql("""select // not a comment')).toEqual({
         code: 'sql("""select // not a comment',
         comment: "",
      });
   });

   it("splits after a literal has closed", () => {
      expect(splitTrailingComment("where: path ~ 'a//b' // why")).toEqual({
         code: "where: path ~ 'a//b' ",
         comment: "// why",
      });
   });

   it("reads an escaped quote as text rather than a close", () => {
      expect(splitTrailingComment('label="a\\"b" // why').comment).toBe(
         "// why",
      );
   });
});
