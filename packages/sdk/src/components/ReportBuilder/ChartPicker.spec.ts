// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { chartChoices } from "./ChartPicker";

const values = (...args: Parameters<typeof chartChoices>) =>
   chartChoices(...args).map((choice) => choice.value);
const enabled = (...args: Parameters<typeof chartChoices>) =>
   chartChoices(...args)
      .filter((choice) => !choice.disabled)
      .map((choice) => choice.value);
const choice = (
   pick: string,
   ...args: Parameters<typeof chartChoices>
): ReturnType<typeof chartChoices>[number] =>
   chartChoices(...args).find((c) => c.value === pick)!;

describe("chartChoices", () => {
   it("lists every chart, never a sparkline, with the inapplicable ones disabled", () => {
      expect(values(undefined, "default")).toEqual([
         "default",
         "none",
         "line_chart",
         "bar_chart",
         "big_value",
         "scatter_chart",
         "shape_map",
         "segment_map",
         "combo_chart",
      ]);
      expect(enabled(undefined, "default")).toEqual([
         "default",
         "none",
         "line_chart",
         "bar_chart",
         "scatter_chart",
      ]);
   });

   it("names the model's own chart Default, with a quiet line saying where it is set", () => {
      expect(chartChoices(undefined, "default")[0]).toEqual({
         value: "default",
         label: "Default",
         hint: "As set in the model",
      });
   });

   it("enables big_value only for an aggregate-only view, and says what it needs otherwise", () => {
      expect(enabled({ aggregateOnly: true }, "default")).toContain(
         "big_value",
      );
      expect(choice("big_value", {}, "default")).toMatchObject({
         disabled: true,
         reason: "Needs results that are only totals (no group by)",
      });
   });

   it("enables a map only when the view carries that map tag, and says what it needs otherwise", () => {
      expect(enabled({ chart: "shape_map" }, "default")).toContain("shape_map");
      expect(enabled({ chart: "shape_map" }, "default")).not.toContain(
         "segment_map",
      );
      expect(
         choice("shape_map", { chart: "bar_chart" }, "default"),
      ).toMatchObject({
         disabled: true,
         reason: "Needs a map chart set in the model",
      });
   });

   it("enables a combo chart for a view with a group by and exactly two totals, and says what it needs otherwise", () => {
      expect(
         enabled({ shape: { groupBys: 1, totals: 2 } }, "default"),
      ).toContain("combo_chart");
      expect(enabled({ chart: "combo_chart" }, "default")).toContain(
         "combo_chart",
      );
      expect(choice("combo_chart", {}, "default")).toMatchObject({
         label: "Bar and line",
         disabled: true,
         reason:
            "Needs a view with a group by and exactly two totals: the first is drawn as bars, the second as a line",
      });
   });

   it("words the reason by why the view's shape is unknown", () => {
      expect(choice("big_value", undefined, "default", "loading").reason).toBe(
         "Still loading this tile's details",
      );
      expect(choice("shape_map", undefined, "default", "unlisted").reason).toBe(
         "Not in the model's list, so its shape is unknown",
      );
   });

   it("keeps what the cell has now selectable, even when it would not be offered", () => {
      expect(enabled({}, "big_value")).toContain("big_value");
      expect(enabled({}, "segment_map")).toContain("segment_map");
      expect(values({}, "custom")).toContain("custom");
   });
});

describe("chartChoices on a view whose shape the model reported", () => {
   const totalsOnly = {
      aggregateOnly: true,
      shape: { groupBys: 0, totals: 3 },
   } as const;

   it("greys out every chart that plots rows on a view of totals only, saying what is missing", () => {
      expect(enabled(totalsOnly, "default")).toEqual([
         "default",
         "none",
         "big_value",
      ]);
      for (const pick of ["line_chart", "bar_chart", "scatter_chart"])
         expect(choice(pick, totalsOnly, "default").reason).toMatch(
            /^This view has no field for the x-axis: a .* chart needs a group by, such as a date or a category$/,
         );
      expect(choice("combo_chart", totalsOnly, "default").reason).toBe(
         "This view has no field for the x-axis: a bar and line chart needs a group by, such as a date or a category",
      );
   });

   it("says a grouped view is more than one row when big numbers are asked for", () => {
      const grouped = { shape: { groupBys: 1, totals: 1 } };
      expect(choice("big_value", grouped, "default").reason).toBe(
         "This view has a group by, so it is more than one row; big numbers need a view of totals only",
      );
      expect(enabled(grouped, "default")).toContain("line_chart");
   });

   it("says a view with no total has nothing for the y-axis", () => {
      const noTotals = { shape: { groupBys: 2, totals: 0 } };
      expect(choice("bar_chart", noTotals, "default").reason).toBe(
         "This view has no total for the y-axis: a bar chart needs at least one aggregate",
      );
      // A scatter chart may plot two group-bys against each other, as the
      // result panel also allows.
      expect(enabled(noTotals, "default")).toContain("scatter_chart");
   });

   it("does not say a view of totals and a nest has a group by", () => {
      const nested = { shape: { groupBys: 0, totals: 1 } };
      expect(choice("big_value", nested, "default").reason).toBe(
         "Needs results that are only totals (no group by)",
      );
   });

   it("names the count when a bar and line chart gets the wrong number of totals", () => {
      const three = { shape: { groupBys: 1, totals: 3 } };
      expect(choice("combo_chart", three, "default").reason).toBe(
         "A bar and line chart needs exactly two totals, the first drawn as bars and the second as a line; this view has 3",
      );
   });
});
