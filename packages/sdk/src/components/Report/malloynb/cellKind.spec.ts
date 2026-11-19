// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// DEPRECATED FEATURE COVERAGE — delete together with
// `.malloynb` notebooks (see RELEASE_NOTES deprecations).
import { describe, expect, it } from "bun:test";
import { cellRuns, malloynbCellQuery, stripProse } from "./cellKind";

describe("cellRuns", () => {
   it("runs a .malloynb code cell and skips its markdown", () => {
      expect(cellRuns({ type: "code" })).toBe(true);
      expect(cellRuns({ type: "markdown" })).toBe(false);
   });
});

describe("stripProse", () => {
   it("drops the ## lines, as the released viewer did", () => {
      expect(
         stripProse({ text: "## title\n  ## indented\nrun: q\n#(markdown) x" }),
      ).toBe("run: q\n#(markdown) x");
   });

   it("drops a .malloynb ##! line", () => {
      expect(stripProse({ text: "##! experimental.parameters\nrun: q" })).toBe(
         "run: q",
      );
   });

   it("leaves (markdown) lines alone", () => {
      const cell = '#(markdown) note\n#|(markdown)\nbody\n|#\n#" c\nrun: q';
      expect(stripProse({ text: cell })).toBe(cell);
   });
});

describe("malloynbCellQuery", () => {
   it("sends a cell's run: without the imports, flags and declarations before it", () => {
      expect(
         malloynbCellQuery({
            text: "##! experimental.givens\nimport \"a.malloy\"\ngiven: R :: string is 'x'\nsource: s is t extend {}\nrun: s -> {\n  aggregate: n is count()\n}",
         }),
      ).toBe("run: s -> {\n  aggregate: n is count()\n}");
   });

   it("keeps the render tags directly above the run:, not a ## line", () => {
      expect(
         malloynbCellQuery({
            text: '## title="T"\n# bar_chart\n# label="By month"\nrun: s -> v',
         }),
      ).toBe('# bar_chart\n# label="By month"\nrun: s -> v');
   });

   it("sends the last run: when a cell has two", () => {
      expect(malloynbCellQuery({ text: "run: a -> v\nrun: b -> v" })).toBe(
         "run: b -> v",
      );
   });

   it("sends nothing for a cell that only imports or declares", () => {
      expect(malloynbCellQuery({ text: 'import "a.malloy"' })).toBeUndefined();
      expect(
         malloynbCellQuery({
            text: "source: s is t extend { view: run_me is { group_by: x } }",
         }),
      ).toBeUndefined();
      expect(malloynbCellQuery({ text: "" })).toBeUndefined();
   });
});
