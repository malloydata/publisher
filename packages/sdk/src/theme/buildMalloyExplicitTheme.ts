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
 * `background` here paints the renderer's HTML chrome (the area
 * between dashboard tiles). It's sourced from the mode-keyed
 * `dashboardRoot` field, NOT from the operator-customisable
 * `palette.background` (which is for the chart canvas via Vega). That
 * separation keeps a bold accent colour scoped to charts and tiles
 * without bleeding into the surrounding panel.
 *
 * `tableBackground` is deliberately omitted because it would paint the
 * entire table interior; a full-page table would become overwhelmingly
 * coloured. Tables read on the page's default background with the
 * accent on the header row only.
 */
export function buildMalloyExplicitTheme(
   theme: ResolvedTheme,
): MalloyExplicitTheme {
   return {
      background: theme.dashboardRoot,
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
