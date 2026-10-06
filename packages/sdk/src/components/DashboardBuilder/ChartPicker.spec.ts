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
      ]);
      expect(enabled(undefined, "default")).toEqual([
         "default",
         "none",
         "line_chart",
         "bar_chart",
         "scatter_chart",
      ]);
   });

   it("enables big_value only for an aggregate-only view, and says what it needs otherwise", () => {
      expect(enabled({ aggregateOnly: true }, "default")).toContain(
         "big_value",
      );
      expect(choice("big_value", {}, "default")).toMatchObject({
         disabled: true,
         reason: "Needs a view with only totals (no group by)",
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
         reason: "Needs a view that already carries a map chart",
      });
   });

   it("words the reason by why the view's shape is unknown", () => {
      expect(choice("big_value", undefined, "default", "loading").reason).toBe(
         "Still loading this view's details",
      );
      expect(choice("shape_map", undefined, "default", "unlisted").reason).toBe(
         "Not a view the catalog lists, so its shape is unknown",
      );
   });

   it("keeps what the cell has now selectable, even when it would not be offered", () => {
      expect(enabled({}, "big_value")).toContain("big_value");
      expect(enabled({}, "segment_map")).toContain("segment_map");
      expect(values({}, "custom")).toContain("custom");
   });
});
