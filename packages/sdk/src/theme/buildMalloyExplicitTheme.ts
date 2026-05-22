import type { MalloyExplicitTheme } from "@malloydata/render";
import type { ResolvedTheme } from "./types";

/**
 * Map our {@link ResolvedTheme} onto the explicit `theme` option that
 * `@malloydata/render` accepts. Values flow into the renderer's inline
 * `--malloy-render--*` style ahead of any `# theme.*` annotations the
 * Malloy result might carry, so operator choices made in the Theme
 * Editor reach table chrome and dashboard tiles directly rather than
 * relying on outer CSS variables that the renderer would shadow.
 *
 * Keys we don't surface in the editor (border widths, font weights,
 * gutter sizes) are left undefined so the renderer's own defaults
 * apply.
 */
export function buildMalloyExplicitTheme(
   theme: ResolvedTheme,
): MalloyExplicitTheme {
   const border =
      theme.mode === "dark" ? "1px solid #334155" : "1px solid #e5e7eb";
   const pinnedBorder =
      theme.mode === "dark" ? "1px solid #475569" : "1px solid #daedf3";

   return {
      tableHeaderColor: theme.tableHeader,
      tableBodyColor: theme.tableBody,
      tableBorder: border,
      // `tablePinnedBackground` is the dashboard tile background — the
      // padded container that wraps each chart / table.
      tablePinnedBackground: theme.tile,
      tablePinnedBorder: pinnedBorder,
      tableFontSize: `${theme.font.size}px`,
      fontFamily: theme.font.family,
      // Deliberately not setting `background` or `tableBackground`.
      // `background` paints the dashboard root (the area between tiles)
      // and `tableBackground` paints the entire table body, which makes
      // a small table-in-tile look fine but a full-page table become
      // overwhelmingly coloured. Operators expect the accent colour to
      // land on the viz surfaces (charts, header bars) rather than
      // taking over the whole table interior. Chart canvases keep the
      // colour via buildVegaThemeOverride; tables read on the page's
      // default background with the accent on the header row only.
   };
}
