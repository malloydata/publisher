// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { buildVegaThemeOverride } from "./buildVegaThemeOverride";
import { resolveTheme } from "./resolveTheme";

describe("buildVegaThemeOverride", () => {
   it("populates range.category from the resolved series", () => {
      const t = resolveTheme(
         [{ palette: { series: ["#ff0080", "#ff6b00"] } }],
         "light",
      );
      const config = buildVegaThemeOverride(t)("bar");
      const ranges = (config as { range: { category: string[] } }).range;
      expect(ranges.category).toEqual(["#ff0080", "#ff6b00"]);
   });

   it("sets background to the resolved theme background", () => {
      const t = resolveTheme(
         [{ palette: { background: { dark: "#001122" } } }],
         "dark",
      );
      const config = buildVegaThemeOverride(t)("line");
      expect((config as { background: string }).background).toBe("#001122");
   });

   it("propagates the font family to axis/legend/title text marks", () => {
      const t = resolveTheme([{ font: { family: "Roboto Mono" } }], "light");
      const cfg = buildVegaThemeOverride(t)("bar") as {
         font: string;
         axis: { labelFont: string; titleFont: string };
         legend: { labelFont: string; titleFont: string };
         title: { font: string };
      };
      expect(cfg.font).toBe("Roboto Mono");
      expect(cfg.axis.labelFont).toBe("Roboto Mono");
      expect(cfg.legend.titleFont).toBe("Roboto Mono");
      expect(cfg.title.font).toBe("Roboto Mono");
   });

   it("uses the resolved foreground, axis and gridline values (no mode branch)", () => {
      const dark = resolveTheme([], "dark");
      const cfg = buildVegaThemeOverride(dark)("bar") as {
         axis: { labelColor: string; gridColor: string };
      };
      expect(cfg.axis.labelColor).toBe(dark.foreground);
      expect(cfg.axis.gridColor).toBe(dark.gridline);
   });

   it("routes palette.axis, gridline and chartText to their Vega slots", () => {
      const t = resolveTheme(
         [
            {
               palette: {
                  axis: { light: "#111111" },
                  gridline: { light: "#222222" },
                  chartText: { light: "#333333" },
               },
            },
         ],
         "light",
      );
      const cfg = buildVegaThemeOverride(t)("bar") as {
         title: { color: string };
         axis: {
            labelColor: string;
            titleColor: string;
            domainColor: string;
            tickColor: string;
            gridColor: string;
         };
         legend: { labelColor: string; titleColor: string };
         header: { labelColor: string; titleColor: string };
      };
      expect(cfg.axis.domainColor).toBe("#111111");
      expect(cfg.axis.tickColor).toBe("#111111");
      expect(cfg.axis.gridColor).toBe("#222222");
      expect(cfg.axis.labelColor).toBe("#333333");
      expect(cfg.axis.titleColor).toBe("#333333");
      expect(cfg.legend.labelColor).toBe("#333333");
      expect(cfg.legend.titleColor).toBe("#333333");
      expect(cfg.title.color).toBe("#333333");
      expect(cfg.header.labelColor).toBe("#333333");
   });

   it("returns the same config across chart types in v1", () => {
      const t = resolveTheme([], "light");
      const cb = buildVegaThemeOverride(t);
      expect(cb("bar")).toBe(cb("line"));
   });
});
