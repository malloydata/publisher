// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { chartLineText } from "../DashboardBuilder/chartLine";
import { chartChoices, chartLocked, pickerState } from "./ChartPicker";

const values = (...args: Parameters<typeof chartChoices>) =>
   chartChoices(...args).map((choice) => choice.value);

describe("chartChoices", () => {
   it("always offers default, no chart, line, bar and scatter, and never a sparkline", () => {
      expect(values(undefined, "default")).toEqual([
         "default",
         "none",
         "line_chart",
         "bar_chart",
         "scatter_chart",
      ]);
   });

   it("offers big_value only for an aggregate-only view", () => {
      expect(values({ aggregateOnly: true }, "default")).toContain("big_value");
      expect(values({}, "default")).not.toContain("big_value");
      expect(values(undefined, "default")).not.toContain("big_value");
   });

   it("offers a map only when the view carries that map tag", () => {
      expect(values({ chart: "shape_map" }, "default")).toContain("shape_map");
      expect(values({ chart: "shape_map" }, "default")).not.toContain(
         "segment_map",
      );
      expect(values({ chart: "bar_chart" }, "default")).not.toContain(
         "shape_map",
      );
   });

   it("keeps what the cell has now in the list, even when it would not be offered", () => {
      expect(values({}, "big_value")).toContain("big_value");
      expect(values({}, "segment_map")).toContain("segment_map");
      expect(values({}, "custom")).toContain("custom");
   });
});

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
