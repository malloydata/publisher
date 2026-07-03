/**
 * The set of palette colour keys that have separate light and dark
 * variants. This is the SDK-side canonical list, consumed by the
 * annotation reader (readChartAnnotations) and the theme resolver
 * (resolveTheme). The server keeps its own copy in
 * packages/server/src/config.ts: the two packages are intentionally
 * decoupled (neither imports the other; their only shared contract is
 * api-doc.yaml, from which the server generates its Theme type), so the
 * list is hand-duplicated. A server-side parity test
 * (theme_key_parity.spec.ts) fails if this list, the server copy, or the
 * api-doc Theme.palette schema drift apart. Adding a new per-mode colour
 * means updating all three.
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
