// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { PALETTE } from "../components/styles";
import { accentFor } from "./accent";
import { resolveTheme } from "./resolveTheme";

describe("accentFor", () => {
   it("keeps the Console's own blue pair when nothing is configured", () => {
      expect(accentFor(PALETTE.blue, "light")).toEqual({
         accent: PALETTE.blue,
         accentHover: "#1d4ed8",
         accentContrast: "#ffffff",
      });
      expect(accentFor(undefined, "dark").accent).toBe("#60a5fa");
   });

   it("takes the first series colour as is in light mode, with a darker hover and a label that reads", () => {
      const { accent, accentHover, accentContrast } = accentFor(
         "#2d323d",
         "light",
      );
      expect(accent).toBe("#2d323d");
      expect(accentHover).not.toBe(accent);
      expect(accentContrast).toBe("#ffffff");
   });

   it("lifts a dark brand colour in dark mode, and puts a dark label on it", () => {
      const { accent, accentContrast } = accentFor("#2d323d", "dark");
      expect(accent).not.toBe("#2d323d");
      expect(parseInt(accent.slice(1, 3), 16)).toBeGreaterThan(0x2d);
      expect(accentContrast).toBe("#0f172a");
   });

   it("uses a non-hex colour as given in light mode and keeps the dark default", () => {
      expect(accentFor("rebeccapurple", "light").accent).toBe("rebeccapurple");
      expect(accentFor("rebeccapurple", "dark").accent).toBe("#60a5fa");
   });

   it("follows the instance palette through resolveTheme", () => {
      const theme = resolveTheme(
         [{ palette: { series: ["#2d323d", "#573f35"] } }],
         "light",
      );
      expect(theme.accent).toBe("#2d323d");
   });
});

describe("dark mode legibility", () => {
   it("lifts a series colour that disappears on the dark canvas, and leaves light mode alone", () => {
      const layers = [{ palette: { series: ["#2d323d", "#b45309"] } }];
      expect(resolveTheme(layers, "light").series[0]).toBe("#2d323d");
      const dark = resolveTheme(layers, "dark").series;
      expect(dark[0]).not.toBe("#2d323d");
      // Already readable on slate: kept as picked.
      expect(dark[1]).toBe("#b45309");
   });

   it("carries a light-only map colour into dark instead of the default", () => {
      const layers = [{ palette: { mapColor: { light: "#465472" } } }];
      const dark = resolveTheme(layers, "dark").mapColor;
      const fallback = resolveTheme([], "dark").mapColor;
      expect(dark).not.toBe(fallback);
      expect(resolveTheme(layers, "light").mapColor).toBe("#465472");
   });
});
