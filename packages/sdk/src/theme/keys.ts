/**
 * The set of palette colour keys that have separate light and dark
 * variants. Owned here so the schema, sanitizer, renderer integration,
 * and editor all agree on a single list. Adding a new per-mode colour
 * is a one-line change in this file plus the matching schema/default
 * entries.
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
