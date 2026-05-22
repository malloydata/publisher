import type { Tag } from "@malloydata/malloy-tag";
import type { Theme } from "./types";

/**
 * Read a `theme.*` annotation block out of a parsed Malloy {@link Tag} and
 * turn it into a partial {@link Theme} suitable for the cascade. Returns
 * `undefined` if the tag has no `theme` namespace.
 *
 * Recognized annotation forms (all optional, all sit under the `theme`
 * namespace):
 *
 *   # theme.palette.series = ["#ff0080", "#ff6b00"]
 *   # theme.palette.background.light = "#fff"
 *   # theme.palette.background.dark  = "#000"
 *   # theme.palette.tableHeader.light = "#666"
 *   # theme.palette.tableHeader.dark  = "#aaa"
 *   # theme.font.family = "Roboto, sans-serif"
 *   # theme.font.size = 13
 *
 * Annotations are silently ignored when the field has the wrong type. The
 * goal is "the worst a typo can do is no styling" rather than "a typo
 * crashes the renderer."
 */
export function readChartAnnotations(tag: Tag | undefined): Theme | undefined {
   if (!tag) return undefined;
   const theme = tag.tag("theme");
   if (!theme) return undefined;

   const out: Theme = {};

   const palette = theme.tag("palette");
   if (palette) {
      const p: NonNullable<Theme["palette"]> = {};
      const series = palette.textArray("series");
      if (series && series.length > 0) p.series = series;

      const background = palette.tag("background");
      if (background) {
         const bg: NonNullable<NonNullable<Theme["palette"]>["background"]> =
            {};
         const light = background.text("light");
         const dark = background.text("dark");
         if (light) bg.light = light;
         if (dark) bg.dark = dark;
         if (Object.keys(bg).length > 0) p.background = bg;
      }

      const tableHeader = palette.tag("tableHeader");
      if (tableHeader) {
         const th: NonNullable<NonNullable<Theme["palette"]>["tableHeader"]> =
            {};
         const light = tableHeader.text("light");
         const dark = tableHeader.text("dark");
         if (light) th.light = light;
         if (dark) th.dark = dark;
         if (Object.keys(th).length > 0) p.tableHeader = th;
      }

      if (Object.keys(p).length > 0) out.palette = p;
   }

   const font = theme.tag("font");
   if (font) {
      const f: NonNullable<Theme["font"]> = {};
      const family = font.text("family");
      const size = font.numeric("size");
      if (family) f.family = family;
      if (typeof size === "number" && Number.isFinite(size)) f.size = size;
      if (Object.keys(f).length > 0) out.font = f;
   }

   return Object.keys(out).length > 0 ? out : undefined;
}
