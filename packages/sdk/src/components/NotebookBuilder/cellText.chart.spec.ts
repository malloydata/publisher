// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { chartLineText } from "../DashboardBuilder/chartLine";
import { runTargetOf, withChart } from "./cellText";

const LINE = chartLineText("line_chart");
const BAR = chartLineText("bar_chart");

describe("withChart", () => {
   it("leaves the text alone for no change, a custom line, or a cell with two chart lines", () => {
      const text = `${BAR}\nrun: a -> v\n`;
      expect(withChart(text, undefined, [BAR])).toBe(text);
      expect(withChart(text, "custom", [BAR])).toBe(text);
      expect(
         withChart(text, "line_chart", ["# line_chart", "# bar_chart"]),
      ).toBe(text);
   });

   it("replaces the cell's chart line in place", () => {
      expect(
         withChart(`#" Cap\n${BAR}\nrun: a -> v\n`, "line_chart", [BAR]),
      ).toBe(`#" Cap\n${LINE}\nrun: a -> v\n`);
   });

   it("removes the line on default, and keeps a table as all negations", () => {
      expect(withChart(`${BAR}\nrun: a -> v\n`, "default", [BAR])).toBe(
         "run: a -> v\n",
      );
      expect(withChart(`${BAR}\nrun: a -> v\n`, "none", [BAR])).toBe(
         `${chartLineText("none")}\nrun: a -> v\n`,
      );
   });

   it("puts a new line directly above the code, below the caption and any prose block", () => {
      const text = `#" Cap\n#(markdown) Note.\n#|(markdown)\nnot a tag line\n|#\n// why\nrun: a -> v\n`;
      expect(withChart(text, "bar_chart", [])).toBe(
         `#" Cap\n#(markdown) Note.\n#|(markdown)\nnot a tag line\n|#\n// why\n${BAR}\nrun: a -> v\n`,
      );
   });

   it("keeps the cell's line endings", () => {
      expect(withChart("run: a -> v\r\n", "line_chart", [])).toBe(
         `${LINE}\r\nrun: a -> v\r\n`,
      );
   });
});

describe("runTargetOf", () => {
   it("reads source and view from a plain run, with back-quoted names", () => {
      expect(runTargetOf('#" Cap\n# bar_chart\nrun: a -> by_cat\n')).toEqual({
         source: "a",
         view: "by_cat",
      });
      expect(runTargetOf("run: `my src` -> `my view`")).toEqual({
         source: "my src",
         view: "my view",
      });
   });

   it("is undefined for anything else", () => {
      expect(runTargetOf("run: a -> { select: x }")).toBeUndefined();
      expect(runTargetOf("run: a -> v + { limit: 3 }")).toBeUndefined();
      expect(runTargetOf("")).toBeUndefined();
   });
});
