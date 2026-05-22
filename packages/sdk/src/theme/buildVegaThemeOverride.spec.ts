import { describe, expect, it } from "bun:test";
import { buildVegaThemeOverride } from "./buildVegaThemeOverride";
import { resolveTheme } from "./resolveTheme";

describe("buildVegaThemeOverride", () => {
   it("populates range.category from the resolved series", () => {
      const t = resolveTheme(
         [{ palette: { series: { light: ["#ff0080", "#ff6b00"] } } }],
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
      const t = resolveTheme(
         [{ font: { family: { light: "Roboto Mono" } } }],
         "light",
      );
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

   it("returns the same config across chart types in v1", () => {
      const t = resolveTheme([], "light");
      const cb = buildVegaThemeOverride(t);
      expect(cb("bar")).toBe(cb("line"));
   });
});
