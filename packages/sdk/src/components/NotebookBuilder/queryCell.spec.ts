// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { queryCellText, queryRunProblem } from "./queryCell";

describe("queryCellText", () => {
   it("writes caption, chart line, then the run, tags last", () => {
      expect(
         queryCellText(
            { source: "orders", view: "by_month", caption: " Revenue " },
            "line_chart",
         ),
      ).toBe(
         '#" Revenue\n# -bar_chart -big_value -scatter_chart -shape_map -segment_map -viz line_chart\nrun: orders -> by_month\n',
      );
   });

   it("writes just the run for the view's own chart", () => {
      expect(queryCellText({ source: "a", view: "v" }, undefined)).toBe(
         "run: a -> v\n",
      );
      expect(queryCellText({ source: "a", view: "v" }, "default", "\r\n")).toBe(
         "run: a -> v\r\n",
      );
   });

   it("back-quotes a name that is not a bare identifier", () => {
      expect(queryCellText({ source: "my src", view: "v" }, undefined)).toBe(
         "run: `my src` -> v\n",
      );
   });
});

describe("queryRunProblem", () => {
   const ok = { source: "a", view: "v" };

   it("accepts a reachable source and refuses an unknown or unlisted one", () => {
      expect(queryRunProblem(ok, undefined, ["a"])).toBeUndefined();
      expect(queryRunProblem(ok, undefined, ["b"])).toContain("not one this");
      expect(queryRunProblem(ok, undefined, undefined)).toContain("not known");
   });

   it("refuses names and captions that would break the line", () => {
      expect(
         queryRunProblem({ source: "a`b", view: "v" }, undefined, ["a`b"]),
      ).toContain("cannot be written");
      expect(
         queryRunProblem({ ...ok, view: "v\nrun: x" }, undefined, ["a"]),
      ).toContain("cannot be written");
      expect(
         queryRunProblem({ ...ok, caption: "a\nb" }, undefined, ["a"]),
      ).toContain("line break");
      expect(
         queryRunProblem({ ...ok, caption: "  " }, undefined, ["a"]),
      ).toContain("empty");
      expect(
         queryRunProblem({ ...ok, caption: "#(authorize) x" }, undefined, [
            "a",
         ]),
      ).toContain("access-control");
   });

   it("refuses a chart the picker does not hold", () => {
      expect(queryRunProblem(ok, "custom", ["a"])).toContain("not model");
      expect(queryRunProblem(ok, "sparkline" as never, ["a"])).toContain(
         "not a chart",
      );
   });
});
