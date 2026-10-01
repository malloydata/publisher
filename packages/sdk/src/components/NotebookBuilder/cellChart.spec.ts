// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { chartLineText } from "../DashboardBuilder/chartLine";
import { chartLocked, pickerState } from "./cellChart";

describe("chartLocked", () => {
   const line = (text: string) => ({
      span: { start: 0, end: text.length },
      text,
   });

   it("is open for none or one recognized line", () => {
      expect(chartLocked({ lines: [], insertAt: 0 })).toBeUndefined();
      expect(
         chartLocked({ lines: [line("# line_chart")], insertAt: 0 }),
      ).toBeUndefined();
   });

   it("says why for an unmodelled line, two lines, or no place", () => {
      expect(
         chartLocked({
            lines: [],
            unmodelled: "# bar_chart { size=spark }",
            insertAt: 0,
         }),
      ).toContain("# bar_chart { size=spark }");
      expect(
         chartLocked({
            lines: [line("# line_chart"), line("# bar_chart")],
            insertAt: 0,
         }),
      ).toContain("more than one chart line");
      expect(chartLocked(undefined)).toContain("no place");
   });
});

describe("pickerState", () => {
   const line = (text: string) => ({
      span: { start: 0, end: text.length },
      text,
   });

   it("prefers the document's state, else reads the file's own lines", () => {
      const opened = {
         lines: [line(chartLineText("bar_chart"))],
         insertAt: 0,
      };
      expect(pickerState("line_chart", opened)).toBe("line_chart");
      expect(pickerState(undefined, opened)).toBe("bar_chart");
      expect(pickerState(undefined, { lines: [], insertAt: 0 })).toBe(
         "default",
      );
      expect(pickerState(undefined, undefined)).toBe("default");
   });
});
