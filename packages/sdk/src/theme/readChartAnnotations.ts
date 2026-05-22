import type { Tag } from "@malloydata/malloy-tag";
import { PER_MODE_COLOR_KEYS } from "./keys";
import type { Theme } from "./types";

/**
 * Read a `theme.*` annotation block out of a parsed Malloy {@link Tag}
 * and turn it into a partial {@link Theme} suitable for the cascade.
 * Returns `undefined` if the tag has no `theme` namespace.
 *
 * Recognised annotation forms (all optional, all sit under `theme`):
 *
 *   # theme.palette.series = ["#ff0080", "#ff6b00"]
 *   # theme.palette.background.light = "#fff"
 *   # theme.palette.background.dark  = "#000"
 *   # theme.palette.tableHeader.light = "#666"
 *   # theme.palette.tableHeader.dark  = "#aaa"
 *   # theme.font.family = "Roboto, sans-serif"
 *   # theme.font.size = 13
 *
 * Series and font are shared across modes; only the five
 * {@link PER_MODE_COLOR_KEYS} accept light/dark variants. Annotations
 * with the wrong type are silently ignored. The goal is "worst-case a
 * typo means no styling", not "a typo crashes the renderer."
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

      for (const key of PER_MODE_COLOR_KEYS) {
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
      const family = font.text("family");
      if (family) f.family = family;
      const size = font.numeric("size");
      if (typeof size === "number" && Number.isFinite(size)) f.size = size;
      if (Object.keys(f).length > 0) out.font = f;
   }

   return Object.keys(out).length > 0 ? out : undefined;
}
