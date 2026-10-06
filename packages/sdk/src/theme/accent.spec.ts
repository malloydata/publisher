// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { PALETTE } from "../components/styles";
import { accentFor, contrastRatio, legibleOn } from "./accent";
import { resolveTheme } from "./resolveTheme";

const WHITE = "#ffffff";
const SLATE = "#0f172a";
const ratio = (a: string, b: string) => contrastRatio(a, b) as number;

describe("accentFor", () => {
   it("keeps the Console's own blue pair when nothing is configured", () => {
      expect(accentFor(PALETTE.blue, "light")).toEqual({
         accent: PALETTE.blue,
         accentHover: "#1d4ed8",
         accentContrast: "#ffffff",
      });
      expect(accentFor(undefined, "dark").accent).toBe("#60a5fa");
   });

   it("keeps a colour that already reads, in either mode, exactly as picked", () => {
      // A dark brand colour on white, and a bright one on slate.
      expect(accentFor("#2d323d", "light").accent).toBe("#2d323d");
      expect(accentFor("#f59e0b", "dark").accent).toBe("#f59e0b");
   });

   it("deepens a pale first colour on the light page until it reads at 3:1", () => {
      expect(ratio("#fde047", WHITE)).toBeLessThan(3);
      const { accent } = accentFor("#fde047", "light");
      expect(ratio(accent, WHITE)).toBeGreaterThanOrEqual(3);
      // Only as far as it takes: not all the way to black.
      expect(ratio(accent, WHITE)).toBeLessThan(4);
   });

   it("lifts a dark first colour on the dark page only as far as it takes", () => {
      expect(ratio("#2d323d", SLATE)).toBeLessThan(3);
      const { accent } = accentFor("#2d323d", "dark");
      expect(ratio(accent, SLATE)).toBeGreaterThanOrEqual(3);
      expect(ratio(accent, SLATE)).toBeLessThan(4);
   });

   it("puts whichever label reads better on the accent", () => {
      for (const [first, mode] of [
         ["#2d323d", "light"],
         ["#2d323d", "dark"],
         ["#fde047", "light"],
         ["#f59e0b", "dark"],
      ] as const) {
         const { accent, accentContrast } = accentFor(first, mode);
         const other = accentContrast === WHITE ? SLATE : WHITE;
         expect(ratio(accent, accentContrast)).toBeGreaterThanOrEqual(
            ratio(accent, other),
         );
      }
   });

   it("uses a non-hex colour as given in light mode and keeps the dark default", () => {
      expect(accentFor("rebeccapurple", "light").accent).toBe("rebeccapurple");
      expect(accentFor("rebeccapurple", "dark").accent).toBe("#60a5fa");
   });

   it("follows the instance palette through resolveTheme, measured against its own page", () => {
      const theme = resolveTheme(
         [{ palette: { series: ["#2d323d", "#573f35"] } }],
         "dark",
      );
      expect(ratio(theme.accent, theme.background)).toBeGreaterThanOrEqual(3);
      expect(
         resolveTheme([{ palette: { series: ["#2d323d"] } }], "light").accent,
      ).toBe("#2d323d");
   });

   it("keeps drill links on the Console's default accent, whatever the palette", () => {
      const theme = resolveTheme(
         [{ palette: { series: ["#7e1d47"] } }],
         "light",
      );
      expect(theme.drillLink).toBe(accentFor(undefined, "light").accent);
   });
});

describe("legibleOn", () => {
   it("returns a colour that already reads unchanged", () => {
      expect(legibleOn("#b45309", SLATE)).toBe("#b45309");
   });

   it("moves toward white on a dark ground and toward black on a light one", () => {
      expect(ratio(legibleOn("#2d323d", SLATE), SLATE)).toBeGreaterThanOrEqual(
         3,
      );
      expect(ratio(legibleOn("#fde047", WHITE), WHITE)).toBeGreaterThanOrEqual(
         3,
      );
   });
});

describe("dark mode legibility", () => {
   it("lifts a series colour that disappears on the dark canvas, and leaves light mode alone", () => {
      const layers = [{ palette: { series: ["#2d323d", "#b45309"] } }];
      expect(resolveTheme(layers, "light").series[0]).toBe("#2d323d");
      const dark = resolveTheme(layers, "dark");
      expect(ratio(dark.series[0], dark.background)).toBeGreaterThanOrEqual(3);
      // Already readable on slate: kept as picked.
      expect(dark.series[1]).toBe("#b45309");
   });

   it("carries a light-only map colour into dark instead of the default", () => {
      const layers = [{ palette: { mapColor: { light: "#465472" } } }];
      const dark = resolveTheme(layers, "dark").mapColor;
      const fallback = resolveTheme([], "dark").mapColor;
      expect(dark).not.toBe(fallback);
      expect(resolveTheme(layers, "light").mapColor).toBe("#465472");
   });
});
