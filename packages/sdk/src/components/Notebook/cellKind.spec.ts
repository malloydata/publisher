// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { cellCaption, cellRuns } from "./cellKind";

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

   it("is undefined when there is no caption", () => {
      expect(cellCaption("# bar_chart\nrun: q")).toBeUndefined();
      expect(cellCaption("")).toBeUndefined();
   });
});
