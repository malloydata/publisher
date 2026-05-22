import type { MalloyExplicitTheme } from "@malloydata/render";
import type { ResolvedTheme } from "./types";

/**
 * Map our {@link ResolvedTheme} onto the explicit `theme` option that
 * `@malloydata/render` accepts. Values flow into the renderer's inline
 * `--malloy-render--*` style ahead of any `# theme.*` annotations the
 * Malloy result carries, so operator choices made in the Theme Editor
 * reach table chrome and dashboard tiles directly instead of going
 * through the outer-wrapper var cascade.
 *
 * Keys we don't surface in the editor (font weights, gutter sizes) are
 * left undefined so the renderer's own defaults apply. We also
 * deliberately omit `background` and `tableBackground`: those would
 * paint the dashboard root and the entire table interior, which makes
 * full-page tables look heavy. Chart canvases get the operator's
 * background through `buildVegaThemeOverride`; tables read on the
 * neutral page background with the accent on the header row.
 */
export function buildMalloyExplicitTheme(
   theme: ResolvedTheme,
): MalloyExplicitTheme {
   return {
      tableHeaderColor: theme.tableHeader,
      tableBodyColor: theme.tableBody,
      tableBorder: theme.border,
      // The renderer's `tablePinnedBackground` field also paints the
      // dashboard tile background (the padded container around each
      // chart). Operator-facing key in our schema is `palette.tile`.
      tablePinnedBackground: theme.tile,
      tablePinnedBorder: theme.pinnedBorder,
      tableFontSize: `${theme.font.size}px`,
      fontFamily: theme.font.family,
   };
}
