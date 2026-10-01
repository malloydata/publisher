// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { chartChoices } from "./ChartPicker";

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
