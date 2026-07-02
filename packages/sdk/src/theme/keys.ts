/**
 * The set of palette colour keys that have separate light and dark
 * variants. This is the SDK-side canonical list, consumed by the
 * annotation reader (readChartAnnotations) and the theme resolver
 * (resolveTheme). The config sanitizer and JSON schema live server-side
 * (packages/server/src/config.ts) and keep their own in-sync copy,
 * because the dependency arrow points server -> SDK via the generated
 * API types, not the other way. Adding a new per-mode colour means
 * updating both this list and the server copy, plus the matching
 * schema/default entries.
 *
 * Non-per-mode fields (series palette, font.family, font.size,
 * defaultMode, allowUserToggle) are intentionally absent. Operators
 * picked colours that change between modes; brand identity (series,
 * fonts) stays consistent across modes.
 */
export const PER_MODE_COLOR_KEYS = [
   "background",
   "tableHeader",
   "tableHeaderBackground",
   "tableBody",
   "tile",
   "tileTitle",
   "mapColor",
] as const;

export type PerModeColorKey = (typeof PER_MODE_COLOR_KEYS)[number];
