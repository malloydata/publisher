// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   cellCaption,
   cellRuns,
   definitionSummary,
   stripProse,
} from "./cellKind";

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
   it("prefers the caption the server sent", () => {
      expect(
         cellCaption({ text: '#" From the text\nrun: q', caption: "Served" }),
      ).toBe("Served");
   });

   it('falls back to the #" lines of the tag block above the run', () => {
      expect(
         cellCaption({
            text: '#" Revenue by month\n# bar_chart\nrun: sales -> by_month',
         }),
      ).toBe("Revenue by month");
      expect(
         cellCaption({ text: '# bar_chart\n#" First\n#" second\nrun: q' }),
      ).toBe("First second");
   });

   it('ignores a #" past the tag block, and reads past a // comment', () => {
      expect(
         cellCaption({ text: 'run: q -> {\n#" not a caption\n select: a }' }),
      ).toBeUndefined();
      expect(cellCaption({ text: '// why\n#" Shown\nrun: q' })).toBe("Shown");
   });

   it("does not read a caption out of the prose lines the server named", () => {
      expect(
         cellCaption({
            text: '#|(markdown)\n#" not a caption\n|#\nrun: q',
            proseLines: [[0, 2]],
         }),
      ).toBeUndefined();
   });

   it("is undefined when there is no caption", () => {
      expect(cellCaption({ text: "# bar_chart\nrun: q" })).toBeUndefined();
      expect(cellCaption({ text: "" })).toBeUndefined();
   });
});

describe("definitionSummary", () => {
   it("names the statement kind and what it defines", () => {
      expect(
         definitionSummary({ text: "source: sales is orders extend {\n}" }),
      ).toBe("source: sales");
      expect(definitionSummary({ text: "query: top is sales -> q" })).toBe(
         "query: top",
      );
      expect(
         definitionSummary({
            text: '# label="Region"\ngiven: REGION :: filter<string>',
         }),
      ).toBe("given: REGION");
   });

   it("says only import for an import, whatever it brings in", () => {
      expect(
         definitionSummary({ text: 'import { a, b } from "./m.malloy"' }),
      ).toBe("import");
   });

   it("skips the prose lines the server named", () => {
      expect(
         definitionSummary({
            text: "#|(markdown)\nThe orders source.\n|#\nsource: o is t",
            proseLines: [[0, 2]],
         }),
      ).toBe("source: o");
      expect(
         definitionSummary({
            text: "#|(markdown)\nsource: not this\n|#\nsource: o is t",
            proseLines: [[0, 2]],
         }),
      ).toBe("source: o");
   });

   it("falls back to the first line for a statement it does not recognize", () => {
      expect(definitionSummary({ text: "export { a }" })).toBe("export");
      expect(definitionSummary({ text: "// note\nmystery thing" })).toBe(
         "mystery thing",
      );
   });
});

describe("stripProse", () => {
   it("removes exactly the lines the server named, and nothing else", () => {
      expect(
         stripProse({
            text: '#|(markdown)\nBody\n|#\n#" Caption\n#(markdown) note\n# bar_chart\nrun: q',
            proseLines: [
               [0, 2],
               [4, 4],
            ],
         }),
      ).toBe('#" Caption\n# bar_chart\nrun: q');
   });

   it("keeps a ## line the server did not name", () => {
      expect(
         stripProse({ text: "## kept\nrun: q", proseLines: [[1, 1]] }),
      ).toBe("## kept");
   });

   it("does not guess at prose in a nested note the server left out", () => {
      expect(
         stripProse({
            text: "#(markdown) lead\nsource: s is a extend {\n  #(markdown) about v\n  view: v is x\n}",
            proseLines: [[0, 0]],
         }),
      ).toBe(
         "source: s is a extend {\n  #(markdown) about v\n  view: v is x\n}",
      );
   });

   it("without ranges, drops the ## lines as the released viewer did", () => {
      expect(
         stripProse({ text: "## title\n  ## indented\nrun: q\n#(markdown) x" }),
      ).toBe("run: q\n#(markdown) x");
   });

   it("without ranges, drops a .malloynb ##! line exactly as the released code did", () => {
      expect(stripProse({ text: "##! experimental.parameters\nrun: q" })).toBe(
         "run: q",
      );
   });

   it("without ranges, leaves (markdown) lines alone", () => {
      const cell = '#(markdown) note\n#|(markdown)\nbody\n|#\n#" c\nrun: q';
      expect(stripProse({ text: cell })).toBe(cell);
   });
});

describe("a served cell trusts only the server", () => {
   it("keeps a ## line inside a multi-line string when it has no prose", () => {
      const text = 'run: duckdb.sql("""\n## not prose\nselect 1""")';
      expect(stripProse({ text, proseLines: [], codeLine: 0 })).toBe(text);
   });

   it("shows no caption the server did not send", () => {
      for (const text of [
         '#"x\nrun: q',
         '#"\u00a0nbsp\nrun: q',
         '#|\nlabel="a"\n#" hidden\n|#\nrun: q',
      ]) {
         expect(cellCaption({ text, proseLines: [], codeLine: 0 })).toBe(
            undefined,
         );
      }
   });

   it("labels a definition by the line the server points at", () => {
      expect(
         definitionSummary({
            text: '#|"\nRegion to filter by\n|#\ngiven: REGION :: string is "x"',
            proseLines: [],
            codeLine: 3,
         }),
      ).toBe("given: REGION");
      expect(
         definitionSummary({
            text: '#|\nlabel="Orders"\n|#\nsource: s is a',
            proseLines: [],
            codeLine: 3,
         }),
      ).toBe("source: s");
   });
});
