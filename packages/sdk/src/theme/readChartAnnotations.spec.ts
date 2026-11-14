// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { parseAnnotation } from "@malloydata/malloy-tag";
import { describe, expect, it } from "bun:test";
import { readChartAnnotations } from "./readChartAnnotations";

function parse(lines: string[]) {
   return parseAnnotation(lines).tag;
}

describe("readChartAnnotations", () => {
   it("returns undefined when no theme annotation is present", () => {
      const tag = parse(["# doc = 'just a note'"]);
      expect(readChartAnnotations(tag)).toBeUndefined();
   });

   it("extracts a series palette (shared across modes)", () => {
      const tag = parse([
         '# theme.palette.series = ["#ff0080", "#ff6b00", "#ffd000"]',
      ]);
      const t = readChartAnnotations(tag);
      expect(t?.palette?.series).toEqual(["#ff0080", "#ff6b00", "#ffd000"]);
   });

   it("extracts font family and size (shared across modes)", () => {
      const tag = parse([
         '# theme.font.family = "Roboto"',
         "# theme.font.size = 13",
      ]);
      const t = readChartAnnotations(tag);
      expect(t?.font?.family).toBe("Roboto");
      expect(t?.font?.size).toBe(13);
   });

   it("extracts per-mode background and tableHeader overrides", () => {
      const tag = parse([
         '# theme.palette.background.light = "#fafafa"',
         '# theme.palette.background.dark = "#111"',
         '# theme.palette.tableHeader.dark = "#94a3b8"',
      ]);
      const t = readChartAnnotations(tag);
      expect(t?.palette?.background).toEqual({
         light: "#fafafa",
         dark: "#111",
      });
      expect(t?.palette?.tableHeader).toEqual({ dark: "#94a3b8" });
   });

   it("extracts the chrome colour keys (border, axis, chartText, ...)", () => {
      const tag = parse([
         '# theme.palette.border.light = "#e5e7eb"',
         '# theme.palette.cardBorder.dark = "#374151"',
         '# theme.palette.axis.light = "#9ca3af"',
         '# theme.palette.gridline.light = "#f3f4f6"',
         '# theme.palette.chartText.dark = "#d1d5db"',
         '# theme.palette.value.light = "#111111"',
      ]);
      const p = readChartAnnotations(tag)?.palette;
      expect(p?.border).toEqual({ light: "#e5e7eb" });
      expect(p?.cardBorder).toEqual({ dark: "#374151" });
      expect(p?.axis).toEqual({ light: "#9ca3af" });
      expect(p?.gridline).toEqual({ light: "#f3f4f6" });
      expect(p?.chartText).toEqual({ dark: "#d1d5db" });
      expect(p?.value).toEqual({ light: "#111111" });
   });

   it("ignores theme keys with the wrong type without throwing", () => {
      const tag = parse([
         "# theme.palette.series = 42",
         "# theme.font.size = 'not a number'",
      ]);
      const t = readChartAnnotations(tag);
      expect(t).toBeUndefined();
   });
});
