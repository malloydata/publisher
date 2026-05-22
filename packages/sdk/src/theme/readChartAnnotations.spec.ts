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

   it("extracts a per-mode series palette", () => {
      const tag = parse([
         '# theme.palette.series.light = ["#ff0080", "#ff6b00", "#ffd000"]',
         '# theme.palette.series.dark = ["#ff66b3"]',
      ]);
      const t = readChartAnnotations(tag);
      expect(t?.palette?.series?.light).toEqual([
         "#ff0080",
         "#ff6b00",
         "#ffd000",
      ]);
      expect(t?.palette?.series?.dark).toEqual(["#ff66b3"]);
   });

   it("extracts a per-mode font family and size", () => {
      const tag = parse([
         '# theme.font.family.light = "Roboto"',
         "# theme.font.size.light = 13",
      ]);
      const t = readChartAnnotations(tag);
      expect(t?.font?.family?.light).toBe("Roboto");
      expect(t?.font?.size?.light).toBe(13);
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

   it("ignores theme keys with the wrong type without throwing", () => {
      const tag = parse([
         "# theme.palette.series.light = 42",
         "# theme.font.size.light = 'not a number'",
      ]);
      const t = readChartAnnotations(tag);
      expect(t).toBeUndefined();
   });
});
