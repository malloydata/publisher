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
 * Two non-operator-customizable fields:
 *
 * - `background` paints the renderer's HTML chrome (the area between
 *   dashboard tiles). Sourced from `theme.dashboardRoot` — mode-keyed,
 *   stays decoupled from `palette.background` so a bold accent on the
 *   chart canvas doesn't bleed across the panel.
 * - `tableBackground` paints the table interior. Sourced from
 *   `theme.tableBackground` — currently tracks `palette.background`
 *   so charts and tables share a single viz surface colour.
 *
 * Operator-customizable mappings:
 *
 * - `tablePinnedBackground` = `theme.tableHeaderBackground` (a NEW
 *   schema field). Despite the renderer's name, this paints the
 *   table header row (`.malloy-table .pinned-header.th` in
 *   scroll-pinned tables; for non-pinned tables our CSS override
 *   forces the same value onto `.th.column-cell`). Distinct from
 *   `tile` (below).
 * - `tile` does NOT flow to `tablePinnedBackground` anymore — the
 *   dashboard tile container picks it up via our outer-wrapper
 *   `--malloy-render--tile-background` var and the CSS override on
 *   `.dashboard-item`. See `buildTableCssVars`.
 */
export function buildMalloyExplicitTheme(
   theme: ResolvedTheme,
): MalloyExplicitTheme {
   return {
      background: theme.dashboardRoot,
      tableBackground: theme.tableBackground,
      tableHeaderColor: theme.tableHeader,
      tableBodyColor: theme.tableBody,
      tableBorder: theme.border,
      // Header row background (operator-customizable, NEW). The
      // renderer reuses tablePinnedBackground for any pinned header
      // cell; we additionally paint the regular header row from the
      // same var via injectRendererOverrides so non-pinned tables
      // also show the header band.
      tablePinnedBackground: theme.tableHeaderBackground,
      tablePinnedBorder: theme.pinnedBorder,
      tableFontSize: `${theme.font.size}px`,
      fontFamily: theme.font.family,
   };
}
