// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

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
      // All on one slate ramp, in both modes: these are the chrome around
      // the data, and a neutral with a colour cast in it reads as a failed
      // attempt at whatever the data is painted in.
      expect(light.border).toBe("1px solid #e2e8f0");
      expect(dark.border).toBe("1px solid #334155");
      // A card's edge is a step darker than a table's gridline in both modes:
      // the gridline separates rows inside a card, the card edge says where
      // the card stops, and at one weight the second job went undone.
      expect(light.cardBorder).toBe("1px solid #cbd5e1");
      expect(dark.cardBorder).toBe("1px solid #475569");
      expect(light.cardBorder).not.toBe(light.border);
      expect(dark.cardBorder).not.toBe(dark.border);
      expect(light.foreground).toBe("#0f172a");
      expect(dark.foreground).toBe("#e2e8f0");
      expect(light.axisFaint).toBe("#cbd5e1");
      expect(dark.axisFaint).toBe("#475569");
      expect(light.valueColor).toBe("#0f172a");
      expect(dark.valueColor).toBe("#f1f5f9");
      expect(light.pinnedBorder).toBe("1px solid #cbd5e1");
      expect(dark.pinnedBorder).toBe("1px solid #475569");
   });

   it("drillLink is a link colour per mode, not a series colour", () => {
      // A drillable cell has to read as a link against whatever the operator
      // picked for the data, so this comes from the mode rather than the palette
      //, and it lightens in dark mode to stay legible on the slate panel.
      expect(resolveTheme([], "light").drillLink).toBe("#2563eb");
      expect(resolveTheme([], "dark").drillLink).toBe("#60a5fa");
      const branded = resolveTheme(
         [{ palette: { series: ["#ff00aa", "#111111"] } }],
         "light",
      );
      expect(branded.drillLink).toBe("#2563eb");
   });

   it("dashboardRoot is mode-keyed and immune to operator background overrides", () => {
      // The page's own ground in each mode: the panel, the cards on it and
      // the canvases inside them are one surface that borders divide up.
      expect(resolveTheme([], "light").dashboardRoot).toBe("#ffffff");
      expect(resolveTheme([], "dark").dashboardRoot).toBe("#0f172a");
      // An operator picking a bold accent for `background` (the chart
      // canvas) must NOT bleed into the surrounding panel.
      const t = resolveTheme(
         [{ palette: { background: { dark: "#ff8800" } } }],
         "dark",
      );
      expect(t.background).toBe("#ff8800");
      expect(t.dashboardRoot).toBe("#0f172a");
   });

   it("tableBackground follows the operator's palette.background", () => {
      // Defaults track the per-mode background, which is the page's ground
      // in both modes.
      expect(resolveTheme([], "light").tableBackground).toBe("#ffffff");
      expect(resolveTheme([], "dark").tableBackground).toBe("#0f172a");
      // An operator accent on palette.background bleeds into the table
      // interior so charts and tables share a single viz surface
      // colour. The dashboard panel between tiles stays neutral —
      // that's dashboardRoot's job.
      const t = resolveTheme(
         [{ palette: { background: { light: "#aacd85" } } }],
         "light",
      );
      expect(t.background).toBe("#aacd85");
      expect(t.tableBackground).toBe("#aacd85");
      expect(t.dashboardRoot).toBe("#ffffff");
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
