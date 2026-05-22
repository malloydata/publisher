import { describe, expect, it } from "bun:test";
import { resolveMode, resolveTheme } from "./resolveTheme";
import type { Theme } from "./types";

describe("resolveTheme cascade", () => {
   it("falls back to defaults when no layers are provided", () => {
      const t = resolveTheme([], "light");
      expect(t.series.length).toBeGreaterThan(0);
      expect(t.background).toBe("#ffffff");
      expect(t.font.family).toContain("Inter");
      expect(t.font.size).toBe(12);
   });

   it("applies the instance layer over defaults", () => {
      const instance: Theme = {
         palette: { series: ["#aaa", "#bbb"] },
         font: { family: "Roboto, sans-serif" },
      };
      const t = resolveTheme([instance], "light");
      expect(t.series).toEqual(["#aaa", "#bbb"]);
      expect(t.font.family).toBe("Roboto, sans-serif");
   });

   it("environment layer overrides instance per-key", () => {
      const instance: Theme = {
         palette: { series: ["#aaa"] },
         font: { family: "Roboto" },
      };
      const env: Theme = {
         palette: { series: ["#cc0000"] },
      };
      const t = resolveTheme([instance, env], "light");
      expect(t.series).toEqual(["#cc0000"]);
      expect(t.font.family).toBe("Roboto");
   });

   it("per-mode colour keys merge per-mode across layers", () => {
      const instance: Theme = {
         palette: {
            background: { light: "#f0f0f0", dark: "#000" },
            tableHeader: { light: "#111" },
         },
      };
      const env: Theme = {
         palette: { background: { dark: "#222" } },
      };
      const lightT = resolveTheme([instance, env], "light");
      const darkT = resolveTheme([instance, env], "dark");
      expect(lightT.background).toBe("#f0f0f0");
      expect(darkT.background).toBe("#222");
      expect(lightT.tableHeader).toBe("#111");
   });

   it("series and font are shared across modes", () => {
      const layer: Theme = {
         palette: { series: ["#aaa", "#bbb"] },
         font: { family: "Roboto", size: 16 },
      };
      const light = resolveTheme([layer], "light");
      const dark = resolveTheme([layer], "dark");
      expect(light.series).toEqual(dark.series);
      expect(light.font.family).toBe(dark.font.family);
      expect(light.font.size).toBe(dark.font.size);
   });

   it("an explicit empty series array clears the cascade", () => {
      const t = resolveTheme([{ palette: { series: [] } }], "light");
      expect(t.series).toEqual([]);
   });

   it("undefined layers are skipped without throwing", () => {
      const t = resolveTheme(
         [undefined, { palette: { series: ["#z"] } }, undefined],
         "light",
      );
      expect(t.series).toEqual(["#z"]);
   });

   it("exposes derived mode values (border, foreground, axisFaint)", () => {
      const light = resolveTheme([], "light");
      const dark = resolveTheme([], "dark");
      expect(light.border).toBe("1px solid #e5e7eb");
      expect(dark.border).toBe("1px solid #334155");
      expect(light.foreground).toBe("#1f2937");
      expect(dark.foreground).toBe("#e2e8f0");
      expect(light.axisFaint).toBe("#d1d5db");
      expect(dark.axisFaint).toBe("#475569");
      expect(light.valueColor).toBe("#1f2937");
      expect(dark.valueColor).toBe("#f1f5f9");
   });

   it("dashboardRoot is mode-keyed and immune to operator background overrides", () => {
      // Light keeps white (no regression on existing installs).
      expect(resolveTheme([], "light").dashboardRoot).toBe("#ffffff");
      // Dark paints slate so the panel doesn't read as a bright box.
      expect(resolveTheme([], "dark").dashboardRoot).toBe("#1e293b");
      // An operator picking a bold accent for `background` (the chart
      // canvas) must NOT bleed into the surrounding panel.
      const t = resolveTheme(
         [{ palette: { background: { dark: "#ff8800" } } }],
         "dark",
      );
      expect(t.background).toBe("#ff8800");
      expect(t.dashboardRoot).toBe("#1e293b");
   });
});

describe("resolveMode", () => {
   it("user choice wins over default and OS", () => {
      expect(resolveMode("light", "dark", false)).toBe("dark");
      expect(resolveMode("dark", "light", true)).toBe("light");
   });

   it("auto follows the OS preference", () => {
      expect(resolveMode("auto", undefined, true)).toBe("dark");
      expect(resolveMode("auto", undefined, false)).toBe("light");
   });

   it("default applies when no user choice is set", () => {
      expect(resolveMode("dark", undefined, false)).toBe("dark");
   });

   it("falls back to light when nothing is set", () => {
      expect(resolveMode(undefined, undefined, false)).toBe("light");
   });
});
