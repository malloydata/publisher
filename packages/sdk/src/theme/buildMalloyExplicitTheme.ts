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
 * Two background fields here are mode-keyed and intentionally NOT
 * operator-customizable:
 *
 * - `background` paints the renderer's HTML chrome (the area between
 *   dashboard tiles). Sourced from `theme.dashboardRoot`.
 * - `tableBackground` paints the renderer's table interior. Sourced
 *   from `theme.tableBackground`.
 *
 * Both default to white in light mode (no regression on existing
 * installs) and slate in dark mode (so table header/body text doesn't
 * paint light-on-white). They're decoupled from `palette.background`
 * (which the operator CAN customize, and which flows to the chart
 * canvas via Vega) so a bold accent color stays scoped to charts and
 * tiles without bleeding into the surrounding panel or table interior.
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
      // The renderer's `tablePinnedBackground` field also paints the
      // dashboard tile background (the padded container around each
      // chart). Operator-facing key in our schema is `palette.tile`.
      tablePinnedBackground: theme.tile,
      tablePinnedBorder: theme.pinnedBorder,
      tableFontSize: `${theme.font.size}px`,
      fontFamily: theme.font.family,
   };
}
