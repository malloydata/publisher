import type { Tag } from "@malloydata/malloy-tag";
import type { Theme } from "./types";

/**
 * Read a `theme.*` annotation block out of a parsed Malloy {@link Tag} and
 * turn it into a partial {@link Theme} suitable for the cascade. Returns
 * `undefined` if the tag has no `theme` namespace.
 *
 * Recognized annotation forms (all optional, all sit under the `theme`
 * namespace, all per-mode):
 *
 *   # theme.palette.series.light = ["#ff0080", "#ff6b00"]
 *   # theme.palette.series.dark  = ["#ff66b3"]
 *   # theme.palette.background.light = "#fff"
 *   # theme.palette.background.dark  = "#000"
 *   # theme.palette.tableHeader.light = "#666"
 *   # theme.palette.tableHeader.dark  = "#aaa"
 *   # theme.font.family.light = "Roboto, sans-serif"
 *   # theme.font.family.dark  = "Roboto, sans-serif"
 *   # theme.font.size.light = 13
 *   # theme.font.size.dark  = 13
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

      const series = palette.tag("series");
      if (series) {
         const s: { light?: string[]; dark?: string[] } = {};
         const light = series.textArray("light");
         const dark = series.textArray("dark");
         if (light && light.length > 0) s.light = light;
         if (dark && dark.length > 0) s.dark = dark;
         if (Object.keys(s).length > 0) p.series = s;
      }

      // Plain per-mode colour keys: { light?: string, dark?: string }.
      const colorKeys = [
         "background",
         "tableHeader",
         "tableBody",
         "tile",
         "tileTitle",
      ] as const;
      for (const key of colorKeys) {
         const sub = palette.tag(key);
         if (!sub) continue;
         const v: { light?: string; dark?: string } = {};
         const light = sub.text("light");
         const dark = sub.text("dark");
         if (light) v.light = light;
         if (dark) v.dark = dark;
         if (Object.keys(v).length > 0) {
            (p as Record<string, unknown>)[key] = v;
         }
      }

      if (Object.keys(p).length > 0) out.palette = p;
   }

   const font = theme.tag("font");
   if (font) {
      const f: NonNullable<Theme["font"]> = {};

      const family = font.tag("family");
      if (family) {
         const v: { light?: string; dark?: string } = {};
         const light = family.text("light");
         const dark = family.text("dark");
         if (light) v.light = light;
         if (dark) v.dark = dark;
         if (Object.keys(v).length > 0) f.family = v;
      }

      const size = font.tag("size");
      if (size) {
         const v: { light?: number; dark?: number } = {};
         const light = size.numeric("light");
         const dark = size.numeric("dark");
         if (typeof light === "number" && Number.isFinite(light))
            v.light = light;
         if (typeof dark === "number" && Number.isFinite(dark)) v.dark = dark;
         if (Object.keys(v).length > 0) f.size = v;
      }

      if (Object.keys(f).length > 0) out.font = f;
   }

   return Object.keys(out).length > 0 ? out : undefined;
}
