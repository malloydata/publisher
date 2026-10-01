// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   artifactLine,
   blockSpans,
   closesBlock,
   isBareName,
   isIdentifier,
   isStrictName,
   malloyPath,
   readPath,
   markdownNote,
   tileSteps,
} from "./malloyText";

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

describe("markdownNote", () => {
   it("reads Malloy's route from the first token, in every bracket form", () => {
      expect(markdownNote("#(markdown) x")).toEqual({ level: 1, block: false });
      expect(markdownNote("  ##[markdown]")).toEqual({
         level: 2,
         block: false,
      });
      expect(markdownNote("#|<markdown>")).toEqual({ level: 1, block: true });
      expect(markdownNote("##|{markdown} x")).toEqual({
         level: 2,
         block: true,
      });
   });

   it("refuses a malformed or different route", () => {
      for (const line of [
         "#(markdown)hi",
         "#(markdown]",
         "#markdown x",
         "#(Markdown) x",
         "#(markdown_help) x",
         "#(doc) x",
         "# (markdown)",
         "###(markdown)",
      ])
         expect(markdownNote(line)).toBeUndefined();
   });
});

describe("closesBlock", () => {
   it("wants the opener's own column", () => {
      expect(closesBlock("|#", 0, "|#")).toBe(true);
      expect(closesBlock("  |#", 0, "|#")).toBe(false);
      expect(closesBlock("  |#", 2, "|#")).toBe(true);
      expect(closesBlock("|#", 2, "|#")).toBe(false);
      expect(closesBlock("    |#", 2, "|#")).toBe(false);
      expect(closesBlock("    |#", undefined, "|#")).toBe(true);
   });

   it("never closes a `#|` block on `|##`", () => {
      expect(closesBlock("|##", 0, "|#")).toBe(false);
      expect(closesBlock("|##", 0, "|##")).toBe(true);
      expect(closesBlock("|# tail", 0, "|#")).toBe(true);
   });
});

describe("blockSpans", () => {
   it("closes on the opener's column and skips openers a comment holds", () => {
      const lines = [
         "  #|(markdown)",
         "|# wrong column",
         "  |## wrong closer",
         "  |#",
         "/*",
         "#| in a comment",
         "|#",
         "*/",
      ];
      expect(blockSpans(lines, (i) => i >= 4)).toEqual([[0, 3]]);
   });
});

describe("reserved words", () => {
   it("keeps statement keywords bare as source and view names, but not as given names or fields", () => {
      for (const name of ["top", "index", "type", "limit", "view", "where"]) {
         expect(isBareName(name)).toBe(true);
         expect(isStrictName(name)).toBe(false);
      }
      for (const name of ["date", "Source", "IS", "year"]) {
         expect(isBareName(name)).toBe(false);
         expect(isStrictName(name)).toBe(false);
      }
      expect(isStrictName("revenue")).toBe(true);
   });

   it("back-quotes only a lone reserved field name", () => {
      expect(malloyPath("date")).toBe("`date`");
      // A dotted path stays verbatim: `.year` is reserved and is an accessor, not a name.
      expect(malloyPath("orders.type.name")).toBe("orders.type.name");
      expect(malloyPath("created_at.year")).toBe("created_at.year");
      expect(malloyPath("type")).toBe("type");
      expect(readPath("`date`")).toBe("date");
      expect(readPath("lower(`date`)")).toBe("lower(`date`)");
      expect(readPath("a.`b`")).toBe("a.`b`");
      expect(malloyPath("products.category")).toBe("products.category");
      expect(malloyPath("`odd name`.x")).toBe("`odd name`.x");
   });
});
