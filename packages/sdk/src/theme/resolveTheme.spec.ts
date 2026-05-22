import { describe, expect, it } from "bun:test";
import { resolveMode, resolveTheme } from "./resolveTheme";
import type { Theme } from "./types";

describe("resolveTheme cascade", () => {
   it("falls back to defaults when no layers are provided", () => {
      const t = resolveTheme([], "light");
      expect(t.series.length).toBeGreaterThan(0);
      expect(t.background).toBe("#ffffff");
      expect(t.font.family).toContain("Inter");
   });

   it("applies the instance layer over defaults", () => {
      const instance: Theme = {
         palette: { series: { light: ["#aaa", "#bbb"] } },
         font: { family: { light: "Roboto, sans-serif" } },
      };
      const t = resolveTheme([instance], "light");
      expect(t.series).toEqual(["#aaa", "#bbb"]);
      expect(t.font.family).toBe("Roboto, sans-serif");
   });

   it("environment layer overrides instance per-key", () => {
      const instance: Theme = {
         palette: { series: { light: ["#aaa"] } },
         font: { family: { light: "Roboto" } },
      };
      const env: Theme = {
         palette: { series: { light: ["#cc0000"] } },
      };
      const t = resolveTheme([instance, env], "light");
      expect(t.series).toEqual(["#cc0000"]);
      expect(t.font.family).toBe("Roboto");
   });

   it("background and tableHeader merge per-mode", () => {
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

   it("light and dark series palettes are independent", () => {
      const layer: Theme = {
         palette: { series: { light: ["#aaa"], dark: ["#111"] } },
      };
      expect(resolveTheme([layer], "light").series).toEqual(["#aaa"]);
      expect(resolveTheme([layer], "dark").series).toEqual(["#111"]);
   });

   it("an explicit empty series array clears the cascade for that mode", () => {
      const t = resolveTheme([{ palette: { series: { light: [] } } }], "light");
      expect(t.series).toEqual([]);
   });

   it("undefined layers are skipped without throwing", () => {
      const t = resolveTheme(
         [undefined, { palette: { series: { light: ["#z"] } } }, undefined],
         "light",
      );
      expect(t.series).toEqual(["#z"]);
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
