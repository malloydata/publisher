// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { cellCaption, cellRuns, definitionSummary } from "./cellKind";

describe("cellRuns", () => {
   it("runs a .malloynb code cell and skips its markdown", () => {
      expect(cellRuns({ type: "code" })).toBe(true);
      expect(cellRuns({ type: "markdown" })).toBe(false);
   });

   it("runs only a query cell of a served notebook", () => {
      expect(cellRuns({ type: "code", kind: "query" })).toBe(true);
      expect(cellRuns({ type: "code", kind: "definition" })).toBe(false);
      expect(cellRuns({ type: "markdown", kind: "markdown" })).toBe(false);
   });
});

describe("cellCaption", () => {
   it('reads the #" lines of the tag block above the run', () => {
      expect(
         cellCaption(
            '#" Revenue by month\n# bar_chart\nrun: sales -> by_month',
         ),
      ).toBe("Revenue by month");
   });

   it("joins a multi-line caption and finds it among other tags", () => {
      expect(
         cellCaption('# bar_chart\n#" First line\n#" second line\nrun: q'),
      ).toBe("First line second line");
   });

   it('ignores a #" that sits inside the statement, past the tag block', () => {
      expect(cellCaption('run: q -> {\n#" not a caption\n select: a }')).toBe(
         undefined,
      );
   });

   it("finds the caption under a // comment inside the tag block", () => {
      expect(cellCaption('// why\n#" Shown\nrun: q')).toBe("Shown");
   });

   it("is undefined when there is no caption", () => {
      expect(cellCaption("# bar_chart\nrun: q")).toBeUndefined();
      expect(cellCaption("")).toBeUndefined();
   });
});

describe("definitionSummary", () => {
   it("names the statement kind and what it defines", () => {
      expect(definitionSummary("source: sales is orders extend {\n}")).toBe(
         "source: sales",
      );
      expect(definitionSummary("query: top is sales -> q")).toBe("query: top");
      expect(
         definitionSummary('# label="Region"\ngiven: REGION :: filter<string>'),
      ).toBe("given: REGION");
   });

   it("says only import for an import, whatever it brings in", () => {
      expect(definitionSummary('import { a, b } from "./m.malloy"')).toBe(
         "import",
      );
   });

   it("falls back to the first line for a statement it does not recognize", () => {
      expect(definitionSummary("export { a }")).toBe("export");
      expect(definitionSummary("// note\nmystery thing")).toBe("mystery thing");
   });
});
